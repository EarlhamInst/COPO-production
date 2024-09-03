from common.utils.logger import Logger
from common.s3.s3Connection import S3Connection as s3
from django.contrib.auth.models import User

l = Logger()

def generate_files_record(user_id=str()):
    label = ['file_name', "S3_ETag", "last_uploaded", "size_in_bytes"]
    data_set = []
    columns = []
    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))

    detail_dict = dict(orderable=False, data=None,
                       title='', defaultContent='', width="5%")

    columns.insert(0, detail_dict)
    for x in label:
        columns.append(dict(data=x, title=x.upper().replace("_", " ")))

    s3obj = s3()
    user = User.objects.get(pk=user_id)
    if not user:
        return dict(dataSet=data_set,
                    columns=columns,
                    )
    bucket_name = str(user_id) + "_" + user.username
    #bucket_size = 0
    if s3obj.check_for_s3_bucket(bucket_name):
        files = s3obj.list_objects(bucket_name)
        if files:
            for file in files:
                row_data = dict()
                row_data["record_id"] = file["Key"]
                row_data["file_name"] = file["Key"].replace("/", "_")
                row_data["DT_RowId"] = "row_" + file["Key"].replace("/", "_")
                row_data["size_in_bytes"] = file["Size"]
                row_data["last_uploaded"] = file["LastModified"]
                row_data["S3_ETag"] = file["ETag"].replace('"', '')
                data_set.append(row_data)
                #bucket_size += file["Size"]

    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict