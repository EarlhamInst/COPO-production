from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from .utils.copo_single_cell import generate_singlecell_record
from .utils.da import SinglecellSchemas, Singlecell
from common.utils.helpers import get_datetime, notify_singlecell_status
from .utils.SingleCellSchemasHandler import SinglecellschemasSpreadsheet, SingleCellSchemasHandler
from common.s3.s3Connection import S3Connection as s3
from common.utils.logger import Logger
import pandas as pd
from pymongo import ReturnDocument
from common.utils import helpers
from io import BytesIO

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
    identifier_map, _ = SinglecellSchemas().get_key_map(schemas)
    additional_fields_default_value_map = {"status":"pending", "accession" :"", "error":""}
    additional_fields = list(additional_fields_default_value_map.keys())

    singlecell_record = dict()
    singlecell_record["components"] = dict()
    now = get_datetime()
    errors = []

    for component_name, component_schema in schemas.items():
        if len(singlecell_data.get(component_name,[])) > 1:
            component_data_df = pd.DataFrame(singlecell_data[component_name][1:], columns=singlecell_data[component_name][0])

            new_column_name = { name : name.replace(" (optional)", "",-1) for name in component_data_df.columns.values.tolist() }
            component_data_df.rename(columns=new_column_name, inplace=True)    
            new_column_name = {field["term_label"] : field["term_name"] for field in component_schema }
            component_data_df.rename(columns=new_column_name, inplace=True)
            singlecell_record["components"][component_name] = component_data_df 

    if not singlecell_record['components']:
        return HttpResponse(status=400, content="Empty manifest")
    
    study_id = singlecell_record["components"]["study"].iloc[0]["study_id"]
    existing_record = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id})
    
    if existing_record:
        for component_name, existing_component_data in existing_record["components"].items():
            component_data_df = singlecell_record["components"].get(component_name,pd.DataFrame())
            existing_component_data_df = pd.DataFrame.from_records(existing_component_data)
            is_error = False
            if not existing_component_data_df.empty:
                identifier = identifier_map.get(component_name, "")
                if identifier:
                    if "status" not in existing_component_data_df.columns:
                        existing_component_data_df["status"] = "pending"
                    if "accession" not in existing_component_data_df.columns:
                        existing_component_data_df["accession"] = ""
                    existing_component_data_cannnot_delete_df = existing_component_data_df.drop(existing_component_data_df[(existing_component_data_df["status"] == "pending") & (existing_component_data_df["accession"] =="")].index)
                    existing_component_data_cannnot_update_df = existing_component_data_df.drop(existing_component_data_df[(existing_component_data_df["status"] != "processing")].index)
                    if not component_data_df.empty:
                        existing_component_data_cannnot_delete_df.drop(existing_component_data_cannnot_delete_df[existing_component_data_cannnot_delete_df[identifier].isin(component_data_df[identifier])].index, inplace=True)
                    if not existing_component_data_cannnot_delete_df.empty:
                        errors.append( component_name + ": Cannot delete records with status not \"pending\" or with accession number : " + existing_component_data_cannnot_delete_df[identifier].to_string(index=False))
                        is_error = True
                    if not existing_component_data_cannnot_update_df.empty:
                        errors.append( component_name + ": Cannot update records with status \"processing\" : " + existing_component_data_cannnot_update_df[identifier].to_string(index=False))
                        is_error = True
                    if is_error:
                        continue


                    #if the data has been changed, set the status to pending
                    common_columns = list(set(existing_component_data_df.columns) & set(component_data_df.columns))

                    #probably it is uploaded from new manifest with new columns
                    if not (set(component_data_df.columns) - set(common_columns)):
                        existing_component_common_columns_df = existing_component_data_df[common_columns]
                        existing_component_common_columns_df.sort_index(axis=1, inplace=True)
                        component_sorted_data_df = component_data_df.sort_index(axis=1)

                        for index, row in existing_component_data_df.iterrows():
                            if row["status"] != "pending":
                                tmp_data = component_sorted_data_df.loc[component_sorted_data_df[identifier] == row[identifier]].sort_index(axis=1)
                         
                                if not tmp_data.iloc[0].compare(existing_component_common_columns_df.loc[(existing_component_common_columns_df[identifier] == row[identifier])].iloc[0]).empty:
                                    existing_component_data_df.loc[(existing_component_data_df[identifier] == row[identifier]), "status"] = "pending"
                    else:
                        existing_component_data_df["status"] = "pending"

                    componnet_additional_fields = list(set(additional_fields) & set(existing_component_data_df.columns))
                    if componnet_additional_fields:
                        existing_component_additional_fields_df = existing_component_data_df[[identifier]+ componnet_additional_fields]
                        singlecell_record["components"][component_name] = component_data_df.merge(existing_component_additional_fields_df, on=identifier, how="left" ) 
            
    if errors:
        return HttpResponse(status=400, content="\n"+"\n".join(errors))

    for component_name, component_data_df in singlecell_record["components"].items():
        for field in additional_fields:
            if field not in component_data_df.columns:
                component_data_df[field] = additional_fields_default_value_map[field]
            else:
                component_data_df[field].fillna(additional_fields_default_value_map[field], inplace=True)

        singlecell_record["components"][component_name] = component_data_df.to_dict(orient="records")

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

    table_data = generate_singlecell_record(profile_id=profile_id, checklist_id=checklist_id)
    result = {"table_data": table_data, "component": "singlecell"}
    return JsonResponse(status=200, data=result)

@login_required
def copo_singlecell(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    singlecell_checklists = SinglecellSchemas().get_checklists(checklist_id="")
    profile_checklist_ids = Singlecell().get_collection_handle().distinct("checklist_id", {"profile_id": profile_id})
    checklists = []
    if singlecell_checklists:
        for key, item in singlecell_checklists.items():
            checklist = {"primary_id": key, "name": item.get("name", ""), "description": item.get("description", "")}
            checklists.append(checklist)

    return render(request, 'copo/copo_single_cell.html', {'profile_id': profile_id, 'profile': profile, 'checklists': checklists, "profile_checklist_ids": profile_checklist_ids})


@login_required
def download_manifest(request, profile_id, study_id):

    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "study_id": study_id})
    if not singlecell:
        return HttpResponse(status=404, content="No record found")
    schemas = SinglecellSchemas().get_collection_handle().find_one({"name":"copo"})
    bytesstring = BytesIO()
    SingleCellSchemasHandler().write_manifest(singlecell_schema=schemas, checklist_id=singlecell["checklist_id"], singlecell=singlecell, file_path=bytesstring)
    response = HttpResponse(bytesstring.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Disposition'] = f"attachment; filename=singlecell_manifest_{study_id}.xlsx"
    return response
