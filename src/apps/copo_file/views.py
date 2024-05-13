from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from common.s3.s3Connection import S3Connection
from django.shortcuts import render
import jsonpickle
from django.http import HttpResponse
from .utils.CopoFiles import generate_files_record
from common.utils import helpers
import json


@login_required()
def copo_files(request, profile_id):
    request.session["profile_id"] = profile_id
    profile_type = Profile().get_type(profile_id)
    profile_title = Profile().get_name(profile_id)
    return render(request, "copo/copo_files.html", {"profile_id": profile_id, "profile_title": profile_title, "profile_type": profile_type})


@login_required()
def process_urls(request):
    profile_id = helpers.get_current_request().session['profile_id']
    channels_group_name = "s3_" + profile_id
    helpers.notify_frontend(data={"profile_id": profile_id},
                            msg='', action="info",
                            html_id="sample_info", group_name=channels_group_name)
    file_list = json.loads(request.POST["data"])
    bucket_name = str(request.user.id) + "_" + request.user.username
    # bucket_name = request.user.username

    s3con = S3Connection()

    if not s3con.check_for_s3_bucket(bucket_name):
        helpers.notify_frontend(data={"profile_id": profile_id},
                                msg='s3 bucket not found, creating it', action="info",
                                html_id="file_info", group_name=channels_group_name)
        s3con.make_s3_bucket(bucket_name)
        helpers.notify_frontend(data={"profile_id": profile_id},
                                msg='s3 bucket created', action="info",
                                html_id="file_info", group_name=channels_group_name)
    urls_list = list()
    for file_name in file_list:
        if file_name and not file_name.endswith("/"):
            file_name = file_name.replace("*", "")
            url = s3con.get_presigned_url(bucket=bucket_name, key=file_name)
            file_url = {"name": file_name, "url": url}
            urls_list.append(file_url)
    return HttpResponse(json.dumps(urls_list))


@login_required()
def upload_ecs_files(request, profile_id):
    channels_group_name = "s3_" + profile_id
    files = request.FILES
    if not files:
        helpers.notify_frontend(data={"profile_id": profile_id},
                                msg='At least one file is required',
                                action="error",
                                html_id="file_info", group_name=channels_group_name)

    bucket = str(request.user.id) + "_" + request.user.username

    # Upload the file
    s3 = S3Connection()
    if not s3.check_for_s3_bucket(bucket):
        s3.make_s3_bucket(bucket)
    KB = 1024
    MB = KB * KB

    for f in files:
        i  = 0
        file = files[f]
        key = file.name.replace(" ", "_")
        response = s3.s3_client.create_multipart_upload(Bucket=bucket, Key=key)

        parts = []
        for chunk in file.chunks(chunk_size=50*MB):
            i += 1
            part = s3.s3_client.upload_part(Body=chunk, Bucket=bucket, Key=key, PartNumber=i, UploadId=response["UploadId"])
            parts.append({"PartNumber":i, "ETag":part["ETag"]})
        s3.s3_client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=response["UploadId"], MultipartUpload={"Parts":parts})

    context = dict()
    context["table_data"] = generate_files_record(user_id=request.user.id)
    context["component"] = "files"
    out = jsonpickle.encode(context, unpicklable=False)
    return HttpResponse(status=200, content=out, content_type='application/json')
