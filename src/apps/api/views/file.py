from django.http import JsonResponse
from rest_framework.views import APIView
from common.s3.s3Connection import S3Connection as s3
from src.apps.copo_core.views import web_page_access_checker
from rest_framework.decorators import api_view

@api_view(['GET'])
@web_page_access_checker    
def api_files(request, profile_id):
    if request.method == 'GET':
        s3obj = s3()
        bucket_name = profile_id
        files = s3obj.list_objects(bucket_name)
        result = list()
        if files:
            for file in files:
                row_data = dict()
                row_data["file_name"] = file["Key"].replace("/", "_")
                row_data["size_in_bytes"] = file["Size"]
                row_data["last_uploaded"] = file["LastModified"]
                row_data["S3_ETag"] = file["ETag"].replace('"', '')
                result.append(row_data)
        return JsonResponse(result, safe=False)
 

@api_view(['POST'])
@web_page_access_checker       
def api_file_presigned_urls(request, profile_id):
    if request.method == 'POST':
        bucket_name = profile_id
        file_names = request.data.get("file_names", [])
        s3obj = s3()
        if not s3obj.check_for_s3_bucket(bucket_name):
            s3obj.make_s3_bucket(bucket_name)

        urls_list = list()
        for file_name in file_names:
            if file_name and not file_name.endswith("/"):
                file_name = file_name.replace("*", "")
                url = s3obj.get_presigned_url(bucket=bucket_name, key=file_name)
                file_url = {"name": file_name, "url": url}
                urls_list.append(file_url)
        return JsonResponse(urls_list, safe=False)

 