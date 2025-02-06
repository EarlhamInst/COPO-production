from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from .utils.copo_single_cell import generate_singlecell_record
from .utils.da import SinglecellSchemas, Singlecell
from common.utils.helpers import get_datetime, notify_singlecell_status
from .utils.SingleCellSchemasHandler import SinglecellschemasSpreadsheet
from common.s3.s3Connection import S3Connection as s3
from common.utils.logger import Logger
import pandas as pd
from pymongo import ReturnDocument
from common.utils import helpers

l = Logger()

@login_required()
def singlecell_manifest_validate(request, profile_id):
    request.session["profile_id"] = profile_id
    checklist_id = request.GET.get("checklist_id")
    data = {"profile_id": profile_id}

    if checklist_id:
        checklists = SinglecellSchemas().get_checklists(checklist_id)
        if checklists:
            data["checklist_id"] = checklist_id
            data["checklist_name"] = checklists.get(checklist_id, {}).get("name", "")
            
    return render(request, "copo/single_cell_manifest_validate.html", data)

@login_required()
def parse_singlecell_spreadsheet(request):
    profile_id = request.session["profile_id"]
    notify_singlecell_status(data={"profile_id": profile_id},
                       msg='', action="info",
                       html_id="singlecell_info")
    # method called by rest
    file = request.FILES["file"]
    checklist_id = request.POST["checklist_id"]
    name = file.name
    

    required_validators = []
    '''
    required = dict(globals().items())["required_validators"]
    for element_name in dir(required):
        element = getattr(required, element_name)
        if inspect.isclass(element) and issubclass(element, Validator) and not element.__name__ == "Validator":
            required_validators.append(element)
    '''
    singlecell = SinglecellschemasSpreadsheet(file=file, checklist_id=checklist_id, component="singlecell", validators=required_validators)
    s3obj = s3()
    if name.endswith("xlsx") or name.endswith("xls"):
        fmt = 'xls'
    else:
        return HttpResponse(status=415, content="Please make sure your manifest is in xlxs format")

    if singlecell.loadManifest(fmt):
        l.log("Single cell manifest loaded")
        if singlecell.validate():
            l.log("About to collect Single cell manifest")
            """
            # check s3 for bucket and files files
            bucket_name = str(request.user.id) + "_" + request.user.username
            # bucket_name = request.user.username
            file_names = singlecell.get_filenames_from_manifest()

            if s3obj.check_for_s3_bucket(bucket_name):
                # get filenames from manifest
                # check for files
                if not s3obj.check_s3_bucket_for_files(bucket_name=bucket_name, file_list=file_names):
                    # error message has been sent to frontend by check_s3_bucket_for_files so return so prevent ena.collect() from running
                    return HttpResponse(status=400)
            else:
                # bucket is missing, therefore create bucket and notify user to upload files
                notify_singlecell_status(data={"profile_id": profile_id},
                                   msg='s3 bucket not found, creating it', action="info",
                                   html_id="sample_info")
                s3obj.make_s3_bucket(bucket_name=bucket_name)
                notify_singlecell_status(data={"profile_id": profile_id},
                                msg='Files not found, please click "Upload Data into COPO" and follow the '
                                    'instructions.', action="error",
                                html_id="sample_info")
                return HttpResponse(status=400)
            notify_singlecell_status(data={"profile_id": profile_id},
                            msg='Spreadsheet is valid', action="info",
                            html_id="sample_info")
            """
            singlecell.collect()
            return HttpResponse()
        return HttpResponse(status=400)
    return HttpResponse(status=400)



@login_required()
def save_singlecell_records(request):
    # create mongo sample objects from info parsed from manifest and saved to session variable
    singlecell_data = request.session.get("singlecell_data")
    profile_id = request.session["profile_id"]
    #profile_name = Profile().get_name(profile_id)
    uid = str(request.user.id)
    checklist_id = request.session["checklist_id"]
    schemas = SinglecellSchemas().get_schema(target_id=checklist_id)

    singlecell_record = dict()
    singlecell_record["components"] = dict()
    now = get_datetime()

    for component_name, component_schema in schemas.items():
        if len(singlecell_data.get(component_name,[])) > 1:
            component_data_df = pd.DataFrame(singlecell_data[component_name][1:], columns=singlecell_data[component_name][0])

            new_column_name = { name : name.replace(" (optional)", "",-1) for name in component_data_df.columns.values.tolist() }

            component_data_df.rename(columns=new_column_name, inplace=True)    
            new_column_name = {field["term_label"] : field["term_name"] for field in component_schema }
            component_data_df.rename(columns=new_column_name, inplace=True)
            singlecell_record["components"][component_name] = component_data_df.to_dict(orient="records")

    if not singlecell_record['components']:
        return HttpResponse(status=400, content="Empty manifest")
    
    study_id = singlecell_record["components"]["study"][0]["study_id"]
    condition = {"profile_id": profile_id, "study_id": study_id}

    singlecell_record["updated_by"] = uid
    singlecell_record["date_updated"] = now
    singlecell_record["checklist_id"] = checklist_id

    insert_record = {}
    insert_record["created_by"] = uid
    insert_record["date_created"] = now
    insert_record["profile_id"] = profile_id
    insert_record["study_id"] = study_id
    insert_record["deleted"] = helpers.get_not_deleted_flag()

    singlecell_record = Singlecell().get_collection_handle().find_one_and_update(condition,
                                                            {"$set": singlecell_record, "$setOnInsert": insert_record },
                                                            upsert=True,  return_document=ReturnDocument.AFTER)   



    table_data = generate_singlecell_record(profile_id=profile_id, checklist_id=checklist_id, study_id=study_id)
    result = {"table_data": table_data, "component": "singlecell"}
    return JsonResponse(status=200, data=result)

@login_required
def copo_singlecell(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    singlecell_checklists = SinglecellSchemas().get_checklists(checklist_id="")
    profile_checklist_ids = []
    checklists = []
    if singlecell_checklists:
        for key, item in singlecell_checklists.items():
            checklist = {"primary_id": key, "name": item.get("name", ""), "description": item.get("desciption", "")}
            checklists.append(checklist)

    return render(request, 'copo/copo_single_cell.html', {'profile_id': profile_id, 'profile': profile, 'checklists': checklists, "profile_checklist_ids": profile_checklist_ids})
