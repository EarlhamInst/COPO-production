from django.http import  HttpResponse, JsonResponse
from rest_framework.views import APIView
from src.apps.copo_single_cell_submission.utils.SingleCellSchemasHandler import SinglecellschemasSpreadsheet, SingleCellSchemasHandler
from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas, Singlecell
import src.apps.copo_single_cell_submission.utils.copo_single_cell as copo_single_cell_utils
import src.apps.copo_single_cell_submission.views as views
from common.dal.copo_da import  DataFile
from common.utils.helpers import get_not_deleted_flag
import pandas as pd

from common.utils.logger import Logger
l = Logger()

def get_current_supported_checklists(request):
    schema_name = "COPO_SINGLE_CELL"
    checklists = SinglecellSchemas().get_checklists(schema_name=schema_name)
    df = pd.DataFrame(checklists.values(), index=checklists.keys())
    df = df.reset_index().rename(columns={'index': 'checklist_id'})
    return JsonResponse(list(df.to_dict("index").values()), safe=False)

class APIChecklist(APIView):
    schema_name = "COPO_SINGLE_CELL"
    def get(self, request, profile_id):
        checklists = SinglecellSchemas().get_checklists(schema_name=self.schema_name)
        return JsonResponse(checklists, safe=False)
    
class APIStudyDownload(APIView):
    schema_name = "COPO_SINGLE_CELL"
    def get(self, request, profile_id, study_id):
        format = request.GET.get("return_type", "xlsx")
        return views.download_manifest(request, self.schema_name, profile_id, study_id, format=format)
    

class APIStudy(APIView):
    schema_name = "COPO_SINGLE_CELL"
    def get(self, request, profile_id):
        """
        Get a list of studies
        """
        checklist_id = request.GET.get("checklist_id", "")
        result = []
        if not checklist_id:
            studies = Singlecell().get_all_records_columns(filter_by={"profile_id":profile_id, "deleted": get_not_deleted_flag(), "schema_name":self.schema_name},projection={"components.study":1, "checklist_id":1})
        else:
            studies = Singlecell().get_all_records_columns(filter_by={"profile_id":profile_id, "deleted": get_not_deleted_flag(), "checklist_id": checklist_id, "schema_name":self.schema_name},projection={"components.study":1, "checklist_id":1})
        checklists = { study["checklist_id"] for study in studies}

        for checklist_id in checklists:
            checklist_result = copo_single_cell_utils.generate_singlecell_record(profile_id=profile_id, checklist_id=checklist_id, schema_name=self.schema_name)
            for study in checklist_result["dataSet"]["study"]:
                study["checklist_id"] = checklist_id
                study["profile_id"] = profile_id
                study.pop("record_id", "")
                study.pop("DT_RowId", "")
                result.append(study)
            
        return JsonResponse(result, safe=False)
    
    def post(self, request, profile_id):
        response = views.parse_singlecell_spreadsheet(request, profile_id, self.schema_name)
        if response.status_code != 200:
            return JsonResponse({"status": "error", "message":  response.content.decode()}, status=response.status_code)
        elif response.content:
            return JsonResponse({"status": "error", "message":  response.content.decode()}, status=400)
        
        response = views.save_singlecell_records(request, profile_id, self.schema_name)
        if response.status_code != 200:
            return JsonResponse({"status": "error", "message":  response.content.decode()}, status=response.status_code)
        else:
            return JsonResponse({"status": "success", "message": "Single Cell records saved successfully."}, status=200)
    

class APIStudySubmit(APIView):
    schema_name = "COPO_SINGLE_CELL"
    def post(self, request, profile_id, study_id):
        repository = request.GET.get("repository", "ena")

        result = copo_single_cell_utils.submit_singlecell(profile_id=profile_id, study_id=study_id, repository=repository)

        if result["status"] == "error":
            return JsonResponse(result, status=400)
        else:
            return JsonResponse(result, status=200)
        
    def get(self, request, profile_id, study_id):
        repository = request.GET.get("repository", "ena")

        result = copo_single_cell_utils.query_submit_result(profile_id=profile_id, study_id=study_id, repository=repository)

        if result["status"] == "error":
            return JsonResponse(result, status=400, safe=False)
        else:
            return JsonResponse(result, status=200, safe=False)
        
class APIStudyAccession(APIView):
    schema_name = "COPO_SINGLE_CELL"

    def get(self, request, profile_id, study_id):
        repository = request.GET.get("repository", "")
        return_type = request.GET.get("return_type", "json")
        accessions = copo_single_cell_utils.get_accession(profile_id=profile_id, study_id=study_id, repository=repository)

        if not accessions:
            return JsonResponse("Not found", status=400, safe=False)
        
        if return_type == "csv":
            df = pd.DataFrame(accessions)
            response = HttpResponse(content_type='text/tab-separated-values')
            response['Content-Disposition'] = f'attachment; filename="{study_id}_accessions.csv"'
            df.to_csv(path_or_buf=response, sep="\t", index=False)
            return response
        elif return_type == "json":     
            return JsonResponse(accessions, status=200, safe=False)

class APIStudyPublish(APIView):
    schema_name = "COPO_SINGLE_CELL"
    def post(self, request, profile_id, study_id):
        repository = request.GET.get("repository", "ena")

        result = copo_single_cell_utils.publish_singlecell(profile_id=profile_id, study_id=study_id, repository=repository)

        if result["status"] == "error":
            return JsonResponse(result, status=400)
        else:
            return JsonResponse(result, status=200)

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



 