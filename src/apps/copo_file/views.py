from django.conf import settings
from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from common.s3.s3Connection import S3Connection
from django.shortcuts import render
import jsonpickle
from django.http import HttpResponse
from .utils.CopoFiles import generate_files_record, create_image_thumbnail
from common.utils import helpers
from boto3.s3.transfer import TransferConfig
import json
import threading
from common.dal.copo_da import DataFile, DataFileChecksum
from common.s3.checksum import md5_of_uploaded_file, normalise_checksum
from common.schemas.utils.data_utils import join_with_and


@login_required
def copo_files(request, profile_id, ui_component):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)

    profile_type =  profile.get("type", "")
    profile_title =  profile.get('title', '')

    return render(request, "copo/copo_files.html", {"profile_id": profile_id, "profile_title": profile_title, "profile_type": profile_type, "ui_component": ui_component})


@login_required
def process_urls(request):
    profile_id = helpers.get_current_request().session['profile_id']
    channels_group_name = "s3_" + profile_id
    helpers.notify_frontend(data={"profile_id": profile_id},
                            msg='', action="info",
                            html_id="sample_info", group_name=channels_group_name)
    file_list = json.loads(request.POST["data"])
    bucket_name = profile_id

    s3con = S3Connection()

    if not s3con.check_for_s3_bucket(bucket_name):
        # msg='s3 bucket not found, creating it
        helpers.notify_frontend(
            data={"profile_id": profile_id},
            msg='No data file storage was found for this profile, creating it now...',
            action="info",
            html_id="file_info",
            group_name=channels_group_name,
        )
        s3con.make_s3_bucket(bucket_name)
        # msg = 's3 bucket created'
        helpers.notify_frontend(data={"profile_id": profile_id},
                                msg='A data file storage was created for this profile', action="info",
                                html_id="file_info", group_name=channels_group_name)
    urls_list = list()
    for file_name in file_list:
        if file_name and not file_name.endswith("/"):
            file_name = file_name.replace("*", "")
            url = s3con.get_presigned_url(bucket=bucket_name, key=file_name)
            file_url = {"name": file_name, "url": url}
            urls_list.append(file_url)
    return HttpResponse(json.dumps(urls_list))


def find_manifest_checksum_mismatches(profile_id, md5s):
    # Find data files whose md5 file checksum differ 
    # from the checksum provided in uploaded manifests
    datafiles = DataFile().get_all_records_columns(
        filter_by={'profile_id': profile_id, 'file_name': {'$in': list(md5s.keys())}},
        projection={'_id': 0, 'file_name': 1, 'file_hash': 1})
    return [d['file_name'] for d in datafiles
            if d.get('file_hash') and normalise_checksum(d['file_hash']) != md5s[d['file_name']]]


@login_required
def upload_ecs_files(request, profile_id):
    channels_group_name = "s3_" + profile_id
    files = request.FILES
    if not files:
        helpers.notify_frontend(data={"profile_id": profile_id},
                                msg='At least one file is required',
                                action="error",
                                html_id="file_info", group_name=channels_group_name)

    bucket_name = profile_id

    # Upload the file
    s3 = S3Connection()
    if not s3.check_for_s3_bucket(bucket_name):
        s3.make_s3_bucket(bucket_name)
    KB = 1024
    MB = KB * KB

    chunk_size = 64 * MB
    transfer_config = TransferConfig(
        multipart_threshold=chunk_size,
        multipart_chunksize=chunk_size,
        max_concurrency=10,
        use_threads=True,
    )

    total_files = len(files)
    mismatched_files = []
    for file_idx, f in enumerate(files, start=1):
        file = files[f]
        key = file.name.replace(" ", "-")

        total_chunks = max(1, -(-file.size // chunk_size))  # ceiling division

        # Generate a thumbnail for the uploaded file if it's an image
        if file.name.lower().endswith(tuple(settings.IMAGE_FILE_EXTENSIONS)):
            file.seek(0) # Reset the cursor back to the start of the file
            create_image_thumbnail(file.name, file, profile_id)
            file.seek(0)

        helpers.notify_frontend(
            data={"profile_id": profile_id},
            msg=json.dumps({"file_name": key, "file_num": file_idx,
                            "total_files": total_files, "chunk": 0,
                            "total_chunks": total_chunks}),
            action="upload_progress",
            html_id="upload_progress", group_name=channels_group_name)

        progress_state = {"bytes": 0, "last_pct": -1}
        progress_lock = threading.Lock()

        def _progress(bytes_amount, _key=key, _file_idx=file_idx,
                      _total_chunks=total_chunks, _size=file.size):
            with progress_lock:
                progress_state["bytes"] += bytes_amount
                pct = int(progress_state["bytes"] * 100 / _size) if _size else 100
                # Throttle: only notify when integer percent advances by >=2
                if pct - progress_state["last_pct"] < 2 and pct < 100:
                    return
                progress_state["last_pct"] = pct
                chunk = min(_total_chunks, max(1, (pct * _total_chunks) // 100))
            helpers.notify_frontend(
                data={"profile_id": profile_id},
                msg=json.dumps({"file_name": _key, "file_num": _file_idx,
                                "total_files": total_files, "chunk": chunk,
                                "total_chunks": _total_chunks}),
                action="upload_progress",
                html_id="upload_progress", group_name=channels_group_name)

        md5 = md5_of_uploaded_file(file)
        s3.s3_client.upload_fileobj(
            Fileobj=file,
            Bucket=bucket_name,
            Key=key,
            Config=transfer_config,
            Callback=_progress,
        )
        
        etag = s3.s3_client.head_object(Bucket=bucket_name, Key=key)['ETag'].strip('"')
        DataFileChecksum().set_md5(bucket_name, key, etag, md5)
        mismatched_files += find_manifest_checksum_mismatches(profile_id, {key: md5})

    if mismatched_files:
        helpers.notify_frontend(
            data={'profile_id': profile_id},
            msg=(
                'Uploaded files do not match the checksums provided in the manifest: '
                f'{join_with_and(mismatched_files)}. Please check that the correct '
                'data files were uploaded or that the manifest contains the correct checksums.'
            ),
            action='error',
            html_id='file_info', group_name=channels_group_name)

    context = dict()
    context["table_data"] = generate_files_record(profile_id=profile_id)
    context["component"] = "files"
    out = jsonpickle.encode(context, unpicklable=False)
    return HttpResponse(status=200, content=out, content_type='application/json')
