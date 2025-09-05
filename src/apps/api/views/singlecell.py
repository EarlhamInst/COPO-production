from django.http import  HttpResponse, JsonResponse
from rest_framework.views import APIView
from src.apps.copo_single_cell_submission.utils.SingleCellSchemasHandler import SinglecellschemasSpreadsheet, SingleCellSchemasHandler
from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas, Singlecell
import src.apps.copo_single_cell_submission.utils.copo_single_cell as copo_single_cell_utils
import src.apps.copo_single_cell_submission.views as views
from common.dal.copo_da import  DataFile
from common.utils.helpers import get_not_deleted_flag
import pandas as pd
from src.apps.copo_core.views import web_page_access_checker
from rest_framework.decorators import api_view

from common.utils.logger import Logger
l = Logger()

def get_current_supported_checklists(request,schema_name):
    schema_name = schema_name.upper()
    checklists = SinglecellSchemas().get_checklists(schema_name=schema_name)
    if not checklists:
        return JsonResponse({"status": "error", "message": f"No checklist found for schema {schema_name}"}, status=404)
    
    df = pd.DataFrame(checklists.values(), index=checklists.keys())
    df = df.reset_index().rename(columns={'index': 'checklist_id'})
    df = df.fillna("")
    return JsonResponse(list(df.to_dict("index").values()), safe=False)

@api_view(['GET'])
@web_page_access_checker    
def api_download_study(request, profile_id, schema_name, study_id):
    if request.method == 'GET':
        format = request.GET.get("return_type", "xlsx")
        schema_name = schema_name.upper()
        return views.download_manifest(request, schema_name, profile_id, study_id, format=format)
    

@api_view(['GET', 'POST'])
@web_page_access_checker  
def api_studies(request, profile_id, schema_name):
    if request.method == 'GET':
        """
        Get a list of studies
        """
        checklist_id = request.GET.get("checklist_id", "")
        schema_name=schema_name.upper()
        result = []
        if not checklist_id:
            studies = Singlecell().get_all_records_columns(filter_by={"profile_id":profile_id, "deleted": get_not_deleted_flag(), "schema_name":schema_name},projection={"components.study":1, "checklist_id":1})
        else:
            studies = Singlecell().get_all_records_columns(filter_by={"profile_id":profile_id, "deleted": get_not_deleted_flag(), "checklist_id": checklist_id, "schema_name":schema_name},projection={"components.study":1, "checklist_id":1})
        checklists = { study["checklist_id"] for study in studies}

        for checklist_id in checklists:
            checklist_result = copo_single_cell_utils.generate_singlecell_record(profile_id=profile_id, checklist_id=checklist_id, schema_name=schema_name)
            for study in checklist_result["dataSet"]["study"]:
                study["checklist_id"] = checklist_id
                study["profile_id"] = profile_id
                study.pop("record_id", "")
                study.pop("DT_RowId", "")
                result.append(study)

        if not result:
            return JsonResponse({"status": "error", "message": "No studies found"}, status=404)
            
        return JsonResponse(result, safe=False)
    
    elif request.method == 'POST':
        schema_name = schema_name.upper()
        response = views.parse_singlecell_spreadsheet(request, profile_id, schema_name)
        if response.status_code != 200:
            return JsonResponse({"status": "error", "message":  response.content.decode()}, status=response.status_code)
        elif response.content:
            return JsonResponse({"status": "error", "message":  response.content.decode()}, status=400)
        
        response = views.save_singlecell_records(request, profile_id, schema_name)
        if response.status_code != 200:
            return JsonResponse({"status": "error", "message":  response.content.decode()}, status=response.status_code)
        else:
            return JsonResponse({"status": "success", "message": "Single Cell records saved successfully."}, status=200)
    

@api_view(['GET', 'POST'])
@web_page_access_checker  
def api_submit_study(request, profile_id, schema_name, study_id):
    if request.method == 'POST':
        repository = request.GET.get("repository", "ena")
        schema_name = schema_name.upper()
        result = copo_single_cell_utils.submit_singlecell(profile_id=profile_id, study_id=study_id, schema_name=schema_name, repository=repository)

        if result["status"] == "error":
            return JsonResponse(result, status=400)
        else:
            return JsonResponse(result, status=200)
        
    elif request.method == 'GET':
        repository = request.GET.get("repository", "ena")

        result = copo_single_cell_utils.query_submit_result(profile_id=profile_id, study_id=study_id, schema_name=schema_name, repository=repository)

        if result["status"] == "error":
            return JsonResponse(result, status=400, safe=False)
        else:
            return JsonResponse(result, status=200, safe=False)
        
@api_view(['GET'])
@web_page_access_checker          
def api_study_accessions(request, profile_id, schema_name, study_id):
    if request.method == 'GET':
        schema_name = schema_name.upper()
        repository = request.GET.get("repository", "")
        return_type = request.GET.get("return_type", "json")
        accessions = copo_single_cell_utils.get_accession(profile_id=profile_id, study_id=study_id, schema_name=schema_name, repository=repository)

        if not accessions:
            result = { "status" : "error", "message": "No record found"}
            return JsonResponse(result, status=400, safe=False)
        
        if return_type == "csv":
            df = pd.DataFrame(accessions)
            response = HttpResponse(content_type='text/tab-separated-values')
            response['Content-Disposition'] = f'attachment; filename="{study_id}_accessions.csv"'
            df.to_csv(path_or_buf=response, sep="\t", index=False)
            return response
        elif return_type == "json":     
            return JsonResponse(accessions, status=200, safe=False)

@api_view(['POST'])
@web_page_access_checker  
def api_publish_study(request, profile_id, schema_name, study_id):
    if request.method == 'POST':
        schema_name = schema_name.upper()
        repository = request.GET.get("repository", "ena")

        result = copo_single_cell_utils.publish_singlecell(profile_id=profile_id, study_id=study_id, schema_name=schema_name, repository=repository)

        if result["status"] == "error":
            return JsonResponse(result, status=400)
        else:
            return JsonResponse(result, status=200)
 


 