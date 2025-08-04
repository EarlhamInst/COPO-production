from django.http import JsonResponse
from rest_framework.views import APIView
from common.s3.s3Connection import S3Connection as s3

class APIFiles(APIView):
    def get(self, request, profile_id):
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
 
    
class APIFilesPresigned(APIView):
    def post(self, request, profile_id):
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

 