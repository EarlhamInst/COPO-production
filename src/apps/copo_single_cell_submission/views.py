from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from common.dal.submission_da import Submission
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from .utils.copo_single_cell import generate_singlecell_record
from .utils.da import SinglecellSchemas, Singlecell, ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
from common.utils.helpers import get_datetime, notify_singlecell_status, get_not_deleted_flag, get_deleted_flag
from .utils.SingleCellSchemasHandler import SinglecellschemasSpreadsheet, SingleCellSchemasHandler
from common.s3.s3Connection import S3Connection as s3
from common.utils.logger import Logger
import pandas as pd
from pymongo import ReturnDocument
from io import BytesIO
from django.conf import settings
from os.path import join
from common.dal.copo_da import  DataFile
import common.ena_utils.FileTransferUtils as tx
from common.dal.mongo_util import cursor_to_list
import common.ena_utils.FileTransferUtils as tx

l = Logger()

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
            
            # check s3 for bucket and files files
            bucket_name = profile_id
            # bucket_name = request.user.username
            file_name_map, msg = singlecell.get_filenames_from_manifest()

            if not file_name_map:
               if msg:
                notify_singlecell_status(data={"profile_id": profile_id},
                            msg=msg, action="error",
                            html_id="singlecell_info")
                return HttpResponse(status=400)
            else: 
                file_names = list(file_name_map.keys())
                if s3obj.check_for_s3_bucket(bucket_name):
                    # get filenames from manifest
                    # check for files
                    result,msg = s3obj.check_s3_bucket_for_files(bucket_name=bucket_name, file_list=file_names)
                    if not result:
                        notify_singlecell_status(data={"profile_id": profile_id},
                            msg=msg, action="error", html_id="singlecell_info")
                        # error message has been sent to frontend by check_s3_bucket_for_files so return so prevent ena.collect() from running
                        return HttpResponse(status=400)
                else:
                    # bucket is missing, therefore create bucket and notify user to upload files
                    notify_singlecell_status(data={"profile_id": profile_id},
                                    msg='s3 bucket not found, creating it', action="info",
                                    html_id="singlecell_info")
                    s3obj.make_s3_bucket(bucket_name=bucket_name)
                    notify_singlecell_status(data={"profile_id": profile_id},
                                    msg='Files not found, please click "Upload Data into COPO" and follow the '
                                        'instructions.', action="error",
                                    html_id="singlecell_info")
                    return HttpResponse(status=400)
                
            notify_singlecell_status(data={"profile_id": profile_id},
                            msg='Spreadsheet is valid', action="info",
                            html_id="singlecell_info")
            notify_singlecell_status(data={"profile_id": profile_id}, msg="", action="close", html_id="upload_controls", checklist_id=checklist_id)
            notify_singlecell_status(data={"profile_id": profile_id}, msg="", action="make_valid", html_id="singlecell_info", checklist_id=checklist_id)

            singlecell.collect()
            return HttpResponse()
        return HttpResponse(status=400)
    return HttpResponse(status=400)



def is_image_file(filename):
    return any(filename.lower().endswith(ext) for ext in settings.IMAGE_FILE_EXTENSIONS)
 
@login_required()
def save_singlecell_records(request):
    # create mongo sample objects from info parsed from manifest and saved to session variable
    singlecell_data = request.session.get("singlecell_data")
    filename_map = request.session.get("filename_map")
    profile_id = request.session["profile_id"]
    profile = Profile().get_record(profile_id)
    schema_name = profile.get("schema_name", "COPO_SINGLE_CELL")

    uid = str(request.user.id)
    username = request.user.username
    checklist_id = request.session["checklist_id"]
    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, target_id=checklist_id)
    identifier_map, _ = SinglecellSchemas().get_key_map(schemas)
    #submission_repository = ["ena", "zenodo"]
    submission_repository = SinglecellSchemas().get_submission_repositiory(schema_name, checklist_id)

    additional_columns_prefix_default_value = ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
    additional_fields_default_value_map = {}
    for repository in submission_repository:
        for prefix in list(additional_columns_prefix_default_value.keys()):
            additional_fields_default_value_map[f"{prefix}_{repository}"] = additional_columns_prefix_default_value[prefix]

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
    condition_for_singlecell_record = {"profile_id": profile_id, "study_id": study_id}

    existing_record = Singlecell().get_collection_handle().find_one(condition_for_singlecell_record)
    
    if existing_record and existing_record["deleted"] != get_deleted_flag():
        if existing_record["schema_name"] != schema_name :
            return HttpResponse(status=400, content=f'Study already exists with different schema: {existing_record["schema_name"]}')
        if existing_record["checklist_id"] != checklist_id:
            return HttpResponse(status=400, content=f'Study already exists with different checklist: {existing_record["checklist_id"]}')
        
        for component_name, existing_component_data in existing_record["components"].items():
            component_data_df = singlecell_record["components"].get(component_name,pd.DataFrame())
            existing_component_data_df = pd.DataFrame.from_records(existing_component_data)
            is_error = False
            if not existing_component_data_df.empty:
                identifier = identifier_map.get(component_name, "")
                if identifier:
                    for respository in submission_repository:
                        if f"{identifier}_{respository}" not in existing_component_data_df.columns:
                            existing_component_data_df[f"{identifier}_{respository}"] = additional_columns_prefix_default_value[identifier]

                    existing_component_data_cannnot_delete_df = existing_component_data_df.drop(existing_component_data_df[
                        #(existing_component_data_df["status"] == "pending") & (existing_component_data_df["accession"] =="")
                        existing_component_data_df.apply(lambda row:  all(row[f"{identifier}_{respository}"] == additional_columns_prefix_default_value[prefix] for prefix in list(additional_columns_prefix_default_value.keys()) for respository in submission_repository ), axis=1)].index)
                    
                    existing_component_data_cannnot_update_df = existing_component_data_df.drop(existing_component_data_df[
                        #(existing_component_data_df["status"] != "processing")
                        existing_component_data_df.apply(lambda row:  all(row[f"status_{respository}"] == "processing" for respository in submission_repository ), axis=1)].index)

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
                            if all(row[f"status_{repository}"] != additional_columns_prefix_default_value["status"] for repository in submission_repository):
                                tmp_data = component_sorted_data_df.loc[component_sorted_data_df[identifier] == row[identifier]].sort_index(axis=1)
                         
                                if not tmp_data.iloc[0].compare(existing_component_common_columns_df.loc[(existing_component_common_columns_df[identifier] == row[identifier])].iloc[0]).empty:
                                    for respository in submission_repository:
                                        existing_component_data_df.loc[(existing_component_data_df[identifier] == row[identifier]), f"status_{repository}"] = additional_columns_prefix_default_value["status"]
                    else:
                        for repository in submission_repository:
                            existing_component_data_df[f"status_{repository}"] = additional_columns_prefix_default_value["status"]


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


    singlecell_record["updated_by"] = uid
    singlecell_record["date_updated"] = now
    singlecell_record["deleted"] = get_not_deleted_flag()


    insert_record = {}
    insert_record["created_by"] = uid
    insert_record["date_created"] = now
    insert_record["profile_id"] = profile_id
    insert_record["study_id"] = study_id
    singlecell_record["checklist_id"] = checklist_id
    singlecell_record["schema_name"] = schema_name


    sub = dict()
    sub["complete"] = "false"
    sub["updated_by"] = uid
    sub["deleted"] = get_not_deleted_flag()
    sub["date_updated"] = now

    insert_sub = dict()
    insert_sub["date_created"] = now
    insert_sub["created_by"] = uid
    insert_sub["repository"] = "ena"
    insert_sub["accessions"] = dict()
    insert_sub["profile_id"] = profile_id
    
    sub = Submission().get_collection_handle().find_one_and_update({"profile_id": profile_id}, {"$set": sub,                                                             
                                                             "$setOnInsert": insert_sub},
                                                             upsert=True,
                                                             return_document=ReturnDocument.AFTER)

    #how about the files?
    bucket_name = profile_id
 
    #create DataFileCollection / transfer records for every file in the filenames 

    filenames = list(filename_map.keys())
    datafiles = cursor_to_list(DataFile().get_collection_handle().find({"profile_id": profile_id, "file_name": { "$in" : filenames } }))
    
    duplicated_files = [datafile for datafile in datafiles if datafile.get("study_id","") != study_id] 
    if duplicated_files:
        return HttpResponse(status=400, content="The following files are already associated with another study: " + ", ".join( [ datafile["file_name"] + " : " + datafile.get("study_id","NULL") for datafile in duplicated_files]))
    
    datafile_map = {datafile["file_location"]: datafile for datafile in datafiles}  

    datafile_list = [] 
    file_location_folder = settings.LOCAL_UPLOAD_PATH
    for filename in filenames:

        file_type = "image" if is_image_file(filename) else "TODO"
        """
        if file_type == "image":
            file_location_folder = settings.UPLOAD_PATH
        else:
            file_location_folder = settings.LOCAL_UPLOAD_PATH
        """
        file_location = join(file_location_folder, profile_id, filename)

        df = dict()
        df["profile_id"] = profile_id
        df["study_id"] = study_id
        df["file_name"] = filename
        df["name"] = filename
        df["file_location"] = file_location
        df["ecs_location"] = bucket_name + "/" + filename
        df["file_hash"] = filename_map.get(filename,"")
        df["file_type"] =  file_type
        df["type"] = "RAW DATA FILE"
        df["bucket_name"] = bucket_name
        df["deleted"] = get_not_deleted_flag()
        datafile = datafile_map.get(file_location, None)
        file_changed = True
        file_id = ""
        if datafile:   
            file_id = str(datafile["_id"])      
            if datafile["file_hash"] == df["file_hash"]:
                file_changed = False

        result = DataFile().get_collection_handle().update_one({"file_location": file_location}, {"$set": df},
                                                                 upsert=True)
        if result.upserted_id:
            file_id = str(result.upserted_id)

        if file_changed:
            datafile_list.append(file_id)

    for f in datafile_list:
        tx.make_transfer_record(file_id=str(f), submission_id=str(sub["_id"]), no_remote_location=True)  #remote_location=f"{profile_id}/{study_id}/"
    

    """

    for file in filenames:
        file_location = join(settings.LOCAL_UPLOAD_PATH, profile_id, file)
        datafile = datafile_map.get(file_location, None)    
        if datafile:
            file_id = str(datafile["_id"])
            file_hash = datafile["file_hash"]
            hash = filename_map[file]
            if hash and file_hash != hash:
                datafile["file_hash"] = hash
                DataFile().get_collection_handle().update_one({"_id": datafile["_id"]}, {"$set": datafile}) 
                file_transfer_list.append(datafile)
        else:
            datafile = dict()
            datafile["file_name"] = file
            datafile["file_location"] = file_location
            datafile["ecs_location"] = bucket_name + "/" + file

        # create transfer record for each image file
        tx.make_transfer_record(file_id=str(inserted["_id"]), submission_id=str(sub_id), no_remote_location=True)

        tx.make_transfer_record(file_id=image_file, submission_id=str(sub["_id"]), remote_location=str(sub["_id"]) + "/images/")
        # create thumbnail for each image file
        tx.make_thumbnail(file_id=image_file, submission_id=str(sub["_id"]), remote_location=str(sub["_id"]) + "/images/")



    file_locations = [join(file_location_folder, filename) for filename in filenames]   
    datafiles = DataFile().get_collection_handle().find({"file_location": { "$in" : file_locations } })
    datafiles = cursor_to_list(datafiles)
    duplicated_files = [datafile for datafile in datafiles if datafile.get("study_id","") != study_id] 
    if duplicated_files:
        return HttpResponse(status=400, content="The following files are already associated with another study: " + ", ".join( [ datafile["file_name"] + " : " + datafile.get("study_id","NULL") for datafile in duplicated_files]))
    
    datafile_map = {datafile["file_location"]: datafile for datafile in cursor_to_list(datafiles)}
    

    datafile_list = [] 
    for filename in filenames:
        df = dict()
        df["profile_id"] = profile_id
        df["study_id"] = study_id
        df["file_name"] = filename
        df["name"] = filename
        df["file_hash"] = "TODO"
        file_location = join(file_location_folder, filename)
        df["file_location"] = file_location
        df["ecs_location"] = bucket_name + "/" + filename
        df["file_type"] = "TODO"
        df["type"] = "RAW DATA FILE"
        df["bucket_name"] = bucket_name
        df["deleted"] = get_not_deleted_flag()
        datafile = datafile_map.get(file_location, None)
        file_changed = True
        if datafile:                
            if datafile["file_hash"] == df["file_hash"]:
                file_changed = False
            file_id = str(datafile["_id"])
        result = DataFile().get_collection_handle().update_one({"file_location": file_location}, {"$set": df},
                                                                        upsert=True)
        if result.upserted_id:
            file_id = str(result.upserted_id)
        if file_changed:
            datafile_list.append(file_id)
    """
            
    """
    for f in datafile_list:
        tx.make_transfer_record(file_id=str(f), submission_id=str(sub["_id"]), remote_location=str(sub["_id"]) + "/reads/")
    """

    singlecell_record = Singlecell().get_collection_handle().find_one_and_update(condition_for_singlecell_record,
                                                            {"$set": singlecell_record, "$setOnInsert": insert_record },
                                                            upsert=True,  return_document=ReturnDocument.AFTER)   

    table_data = generate_singlecell_record(profile_id=profile_id, checklist_id=checklist_id)
    result = {"table_data": table_data, "component": "singlecell"}
    return JsonResponse(status=200, data=result)

@login_required
def copo_singlecell(request, profile_id):
    request.session["profile_id"] = profile_id

    profile = Profile().get_record(profile_id)
    schema_name = profile.get("schema_name", "COPO_SINGLE_CELL")
    singlecell_checklists = SinglecellSchemas().get_checklists(schema_name=schema_name, checklist_id="")
    profile_checklist_ids = Singlecell().get_collection_handle().distinct("checklist_id", {"profile_id": profile_id, "schema_name" : schema_name})

    checklists = []
    if singlecell_checklists:
        for key, item in singlecell_checklists.items():
            checklist = {"primary_id": key, "name": item.get("name", ""), "description": item.get("description", "")}
            checklists.append(checklist)

    return render(request, 'copo/copo_single_cell.html', {'profile_id': profile_id, 'profile': profile, 'schema_name':schema_name, 'checklists': checklists, "profile_checklist_ids": profile_checklist_ids})


@login_required
def download_manifest(request, profile_id, study_id):

    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "study_id": study_id})
    if not singlecell:
        return HttpResponse(status=404, content="No record found")
    schemas = SinglecellSchemas().get_collection_handle().find_one({"name":singlecell["schema_name"]})
    bytesstring = BytesIO()
    SingleCellSchemasHandler().write_manifest(singlecell_schema=schemas, checklist_id=singlecell["checklist_id"], singlecell=singlecell, file_path=bytesstring)
    response = HttpResponse(bytesstring.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Disposition'] = f"attachment; filename=singlecell_manifest_{study_id}.xlsx"
    return response
