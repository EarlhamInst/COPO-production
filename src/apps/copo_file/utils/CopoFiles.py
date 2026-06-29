import os

from django.conf import settings
from django.contrib.auth.models import User
from io import BytesIO
from PIL import Image

from common.utils.helpers import get_thumbnail_folder
from common.utils.logger import Logger

l = Logger()

def generate_files_record(profile_id=str()):
    from common.s3.s3Connection import S3Connection as s3

    label = ['file_name', "S3_ETag", "last_uploaded", "size_in_bytes", "size_bytes"]
    data_set = []
    columns = []
    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))

    detail_dict = dict(orderable=False, data=None,
                       title='', defaultContent='', width="5%")

    columns.insert(0, detail_dict)
    size_render = (
        "(function(data, type, row) {"
        "  if (type !== 'display' || data == null) return data;"
        "  if (data < 1024) return data + ' B';"
        "  if (data < 1048576) return (data / 1024).toFixed(1) + ' KB';"
        "  if (data < 1073741824) return (data / 1048576).toFixed(1) + ' MB';"
        "  return (data / 1073741824).toFixed(2) + ' GB';"
        "})"
    )
    bytes_render = (
        "(function(data, type, row) {"
        "  if (type !== 'display' || data == null) return data;"
        "  return data.toLocaleString('en-GB') + ' B';"
        "})"
    )
    for x in label:
        col = dict(data=x, title=x.upper().replace("_", " "))
        if x == "size_in_bytes":
            col["title"] = "Size"
            col["render"] = size_render
        elif x == "size_bytes":
            col["title"] = "Size (bytes)"
            col["render"] = bytes_render
        elif x == "S3_ETag":
            col["title"] = "Checksum"
        columns.append(col)

    s3obj = s3()
    # user = User.objects.get(pk=user_id)
    # if not user:
    #    return dict(dataSet=data_set,
    #                columns=columns,
    #                )
    bucket_name = profile_id
    # bucket_size = 0
    if s3obj.check_for_s3_bucket(bucket_name):
        files = s3obj.list_objects(bucket_name)
        if files:
            for file in files:
                row_data = dict()
                row_data["record_id"] = file["Key"]
                row_data["file_name"] = file["Key"].replace("/", "_")
                row_data["DT_RowId"] = "row_" + file["Key"].replace("/", "_")
                row_data["size_in_bytes"] = file["Size"]
                row_data["size_bytes"] = file["Size"]
                row_data["last_uploaded"] = file["LastModified"]
                row_data["S3_ETag"] = file["ETag"].replace('"', '')
                data_set.append(row_data)
                # bucket_size += file["Size"]

    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict


def delete_image_thumbnail(file_name, profile_id):
    # Remove generated thumbnails when an image file is deleted
    try:
        final_dot = file_name.rfind('.')
        
        if final_dot == -1:
            return  # Invalid file name, so there is nothing to be deleted
        
        file_extension = file_name[final_dot:]

        if file_extension.lower() in settings.IMAGE_FILE_EXTENSIONS:
            thumbnail_path = f'{get_thumbnail_folder(profile_id)}/{file_name[:final_dot]}_thumb{file_extension}'

            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
    except Exception as e:
        l.error(f'Failed to delete thumbnail for {file_name}: {e}')


def create_image_thumbnail(
    file_name, uploaded_file, profile_id, local_path=None, thumbnail_size=(128, 128)
):
    try:
        final_dot = file_name.rfind('.')
        file_extension = file_name[final_dot:]

        if not file_extension.lower() in settings.IMAGE_FILE_EXTENSIONS:
            return  # It is not an image file, so a thumbnail cannot be created

        if not local_path:
            local_path = f'{settings.UPLOAD_URL}/{profile_id}/{file_name}'

        # Create a thumbnail directory if it doesn't exist
        thumbnail_path = (
            f'{get_thumbnail_folder(profile_id)}/{file_name[:final_dot]}_thumb{file_extension}'
        )
        os.makedirs(os.path.dirname(thumbnail_path), exist_ok=True)

        img = Image.open(uploaded_file)
        img.thumbnail(thumbnail_size)
        thumb_io = BytesIO()
        img.save(thumb_io, format=img.format)

        # Create or overwrite the thumbnail file
        with open(thumbnail_path, 'wb') as f:
            f.write(thumb_io.getvalue())
        thumb_io.seek(0)
    except Exception as e:
        l.error(f'Failed to create thumbnail for {local_path}: {e}')
        delete_image_thumbnail(file_name, profile_id)
