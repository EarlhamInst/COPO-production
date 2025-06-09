from common.utils.logger import Logger
from .da import SinglecellSchemas, Singlecell, ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
import pandas as pd
from common.utils.helpers import get_datetime, get_thumbnail_folder
from common.dal.profile_da import Profile
from common.dal.copo_da import  DataFile, EnaFileTransfer
from common.utils.helpers import get_not_deleted_flag
import requests
from django_tools.middlewares import ThreadLocal
from common.dal.submission_da import Submission
from common.dal.mongo_util import cursor_to_list
from django.conf import settings
from bson import regex
import os
import common.ena_utils.FileTransferUtils as tx
from common.utils.helpers import get_env

l = Logger()

pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]
session = requests.Session()
session.auth = (user_token, pass_word)

def _query_ena_file_processing_status(accession_no):
    result = ""
    url = f"{get_env('ENA_ENDPOINT_REPORT')}run-files/{accession_no}?format=json"
    with requests.Session() as session:
        session.auth = (user_token, pass_word)
        headers = {'Accept': '/'}

        try:
            response = session.get(url, headers=headers)
            if response.status_code == requests.codes.ok:
                response_body = response.json()
                for r in response_body:
                    report = r.get("report",{})
                    if report:
                        result += "|"+ report.get("fileName") + " : " + report.get("archiveStatus") + " : " + report.get("releaseStatus")
                if result:
                    result = result[1:].replace("|", "<br/>")

            else:
                result = "Cannot get file processing result from ENA"
                l.error(str(response.status_code) + ":" + response.text)
        except Exception as e:
            l.exception(e)
        return result
    
def generate_singlecell_record(profile_id, checklist_id=str(), study_id=str(), schema_name=str()):

    data_set = {}
    columns = {}
    column_keys = {}
    studies = []
    identifier_map = {}
    submission_repository = {}

    """
    profile = Profile().get_record(profile_id)
    if not profile:
        return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))
    """
    studies = []
    if not schema_name:
        if study_id:
            studies = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"study_id": study_id, "checklist_id": checklist_id}, projection={"study_id": 1, "schema_name": 1})
            if studies:
                schema_name = studies[0].get("schema_name", "")
    if schema_name:
        studies = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"schema_name": schema_name, "checklist_id": checklist_id}, projection={"study_id": 1, "components.study":1})
    else:
        return dict(dataSet=data_set, columns=columns, submission_repository=submission_repository, components=list(columns.keys()))
    
    if not study_id:
        study_id = studies[0]["study_id"]

    if checklist_id:
        schemas = SinglecellSchemas().get_schema(schema_name=schema_name, target_id=checklist_id)

        repositories = set()
        submission_repository_df = SinglecellSchemas().get_submission_repositiory(schema_name)
        submisison_repository_component_map = submission_repository_df.to_dict('index')
        additional_columns_prefix_default_value = ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
        additional_fields_map = {}
        file_df_map = {}
        for component, respositories in submisison_repository_component_map.items():
            submission_repository[component] = [repository for repository, value in respositories.items() if value]
            additional_fields_map[component] = [f"{prefix}_{repository}" for repository, value in respositories.items() if value for prefix in list(additional_columns_prefix_default_value.keys())]
            repositories.update(submission_repository[component])

        for component_name, component_schema in schemas.items():
            columns[component_name] = []
            component_schema_df = pd.DataFrame.from_records(component_schema)
            identifier_df = component_schema_df.loc[component_schema_df['identifier'], 'term_name']
            file_df = component_schema_df.loc[component_schema_df['term_type'] == 'file', 'term_name']

            if not identifier_df.empty:
                identifier_map[component_name]= identifier_df.iloc[0]

            #no details button for component with no submission button
            if submission_repository.get(component_name, []):
                detail_dict = dict(className='summary-details-control detail-hover-message', orderable=False, data=None,
                            title='', defaultContent='', width="5%")
                columns[component_name].insert(0, detail_dict)

            columns[component_name].append(dict(data="record_id", visible=False))
            columns[component_name].append(dict(data="DT_RowId", visible=False))
            columns[component_name].extend([dict(data=item["term_name"],  title=item["term_label"], defaultContent='', 
                                                    render = "render_thumbnail_image_column_function" if item["term_type"] == "file" else None
                                                  ) for item in component_schema])


            column_keys[component_name] = ([item["term_name"] for item in component_schema])

            for name in additional_fields_map.get(component_name, []):  
                    prefix = name.split("_")[0]
                    columns[component_name].append(dict(data=name, title=name.replace("_", " for "), className="ena-accession" if name.lower().endswith("accession_ena") else "", defaultContent= additional_columns_prefix_default_value[prefix])) # render = "render_accession_column_function"
                    column_keys[component_name].append(name)
 
            if not file_df.empty:
                columns[component_name].append(dict(data="file_status", title="file_status", defaultContent=''))
                
                if "ena" in submission_repository.get(component_name,[]):
                    columns[component_name].append(dict(data="ena_file_processing_status", title="ena_file_processing_status", defaultContent='', className="ena_file_processing_status"))
                file_df_map[component_name] = file_df.values.tolist()

        #retriever all components info for first study
        singlecell = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"checklist_id": checklist_id, "study_id": study_id})
        if not singlecell or not singlecell[0]["components"].get("study",[]):
            return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))
        
        if len(studies) > 1:
            #combine all study component info from studies except the first one
            for study in studies:
                if study["study_id"] != study_id:
                    singlecell[0]["components"]["study"].extend(study["components"].get("study",[]))

        files = SinglecellSchemas().get_all_files(singlecell=singlecell[0], schemas=schemas)

        local_path = [regex.Regex(f'{x}$') for x in files]
        projection = {'_id':0, 'local_path':1, 'status':1, "transfer_status":1}
        filter = dict()
        filter['local_path'] = {'$in': local_path}
        filter['profile_id'] = profile_id
    
        enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)

        enaFile_map = {os.path.basename(enafile["local_path"]) : tx.TransferStatusNames[tx.get_transfer_status(enafile)] for enafile in enaFiles}

        submission_status_map = {}
        submission_error_map = {}
        submission_status={}
        submission_error=[]
        for component_name, component_data in singlecell[0]["components"].items():
            if not component_data:
                continue                

            component_data_df = pd.DataFrame.from_records(component_data)
            
            for column in component_data_df.columns:
                if column not in column_keys.get(component_name, []):
                    component_data_df.drop(column, axis=1, inplace=True)

            if component_name != "study":
            #propagate the submission status from components to study
                for repository in submission_repository.get(component_name, []):
                    status_column = f"status_{repository}"
                    error_column = f"error_{repository}"
                    if status_column in component_data_df.columns:
                        status = "rejected" if any(component_data_df[status_column] == "rejected") else "pending" if any(component_data_df[status_column] != "accepted") else "accepted"
                        if submission_status.get(repository, "accepted") != status and status != "accepted":
                            submission_status[repository] = status
                    if error_column in component_data_df.columns:
                        error = component_data_df[error_column].dropna().tolist()
                        if error:
                            submission_error.extend(error)
 
            #set the identifier to DT_RowId
            #set the identifier to record_id

            component_data_df["DT_RowId"] = component_name + ( "_"+ study_id if component_name != 'study' else "")  + "_" + component_data_df.get(identifier_map.get(component_name,""), "")
            component_data_df["record_id"] = component_data_df["DT_RowId"]
            component_data_df.fillna("", inplace=True)

            file_terms = file_df_map.get(component_name, [])
            if file_terms:
                component_data_df["file_status"] = ""
                for term in file_terms:
                    component_data_df["file_status"] = component_data_df["file_status"] + component_data_df[term].apply(lambda x: (x+ " : " + enaFile_map.get(x, "unknown") + "  ") if x else "")
                
                if "ena" in submission_repository.get(component_name,[]):
                    component_data_df["ena_file_processing_status"] = component_data_df["accession_ena"].apply(lambda x: _query_ena_file_processing_status(x) if x else "")
       
            data_set[component_name] = component_data_df.to_dict(orient="records")
                    

        study_component = data_set["study"][0]
        for repository, status in submission_status.items():
            status_for_study = [study_component[ f"status_{repository}"],status=="rejected"]
            study_component[ f"status_{repository}"] = "rejected" if (any(status == "rejected" for status in status_for_study)) else "pending" if (any(status == "pending" for status in status_for_study)) else "accepted"
            
            #"rejected" if (study_component[ f"status_{repository}"] == "rejected" or submission_status["repository"]=="rejected") else "pending" if (study_component[ f"status_{repository}"] == "pending" or submission_status["repository"]=="pending") else "accepted"        
            study_component[f"error_{repository}"] = study_component[f"error_{repository}"] + " " + "<br/>".join(submission_error) if submission_error else ""   


    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       submission_repository=submission_repository,
                       components=list(columns.keys()),
                       study_id = study_id,
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict

def _check_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map):
    
    for child_component_name, foreign_key in child_map.get(component_name, {}).items():
        child_component_data = singlecell_data["components"].get(child_component_name, [])
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        child_identifier_key = identifier_map.get(child_component_name, "")

        if not child_component_data_df.empty:
            children_df = child_component_data_df.loc[child_component_data_df[foreign_key].isin(identifiers)]
            accession_columns = child_component_data_df.columns[child_component_data_df.columns.str.startswith("accession_")].tolist()
            if not accession_columns:
                continue
            
            children_has_accession_df = children_df.loc[children_df.apply(lambda x:  all(x[accession_columns] != ""), axis=1), child_identifier_key]
            #children_has_accession_df = children_df.loc[children_df["accession"]!="" ,    child_identifier_key]
            if not children_has_accession_df.empty:
                return False,  f'{child_component_name}:{children_has_accession_df.tolist()} : record with accession number'
            _check_child_component_data(singlecell_data, child_component_name, children_df[child_identifier_key].tolist(),  identifier_map, child_map)
    return True, ""


def _delete_datafile(profile_id, to_be_delete_component_data_df, schema):
    #delete the datafiles
    schema_df = pd.DataFrame.from_records(schema)
    schema_file_df = schema_df.loc[schema_df["term_type"] == "file", "term_name"]
    if not schema_file_df.empty:
        file_df = to_be_delete_component_data_df[schema_file_df.tolist()]
        file_df = file_df.dropna()
        fileslist = file_df.values.tolist()
        filelist = []
        for files in fileslist:
            filelist.extend(list(filter(None, files))) #remove empty strings

        if filelist:
            fileIdList = cursor_to_list(DataFile().get_collection_handle().find({"profile_id": profile_id, "file_name": {"$in": filelist}}, {"_id": 1}))

            #delete the files
            DataFile().get_collection_handle().delete_many({"profile_id": profile_id, "_id": {"$in": [file["_id"] for file in fileIdList]}})    
            #delete the files transfer records
            EnaFileTransfer().get_collection_handle().delete_many({"profile_id": profile_id, "file_id": {"$in": [str(file["_id"]) for file in fileIdList]}})
            #delete the thumbnail files
            for filename in filelist:
                final_dot = filename.rfind(".")
                if final_dot == -1:
                    continue
                file_ext = filename[final_dot:]
                if file_ext.lower() in settings.IMAGE_FILE_EXTENSIONS:
                    thumbnail_file = get_thumbnail_folder(profile_id) + '/' + filename[:final_dot] + "_thumb" + file_ext
                    if os.path.exists(thumbnail_file):
                        os.remove(thumbnail_file)   

def _delete_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map, schemas):

    for child_component_name, foreign_key in child_map.get(component_name, {}).items():
        child_schema = schemas.get(child_component_name, [])
        child_component_data = singlecell_data["components"].get(child_component_name, [])
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        child_component_identifier_key = identifier_map.get(child_component_name, "")
        
        if not child_component_data_df.empty:
            to_be_delete_child_component_data_df = child_component_data_df.loc[child_component_data_df[foreign_key].isin(identifiers)]
            if not to_be_delete_child_component_data_df.empty: 
                #no identifier key, no child component
                if child_component_identifier_key:
                    _delete_child_component_data(singlecell_data, child_component_name, to_be_delete_child_component_data_df[child_component_identifier_key].tolist(), identifier_map, child_map, schemas)
                #delete the datafiles
                _delete_datafile(singlecell_data["profile_id"], to_be_delete_child_component_data_df, child_schema)
                '''
                child_schema_df = pd.DataFrame.from_records(child_schema)
                child_schema_file_df = child_schema_df.loc[child_schema_df["term_type"] == "file", "term_name"]
                if not child_schema_file_df.empty:
                    filelist = []
                    fileslist = to_be_delete_child_component_data_df[child_schema_file_df.values.tolist()]
                    for files in fileslist:
                        filelist.extend(list(filter(None, files)))
                    if filelist:
                        #delete the files
                        DataFile().get_collection_handle().delete_many({"profile_id":singlecell_data["profile_id"], "file_name": {"$in": filelist}})        
                '''
                child_component_data_df = child_component_data_df.drop(child_component_data_df[child_component_data_df[foreign_key].isin(identifiers)].index)
                if not child_component_data_df.empty:
                    singlecell_data["components"][child_component_name] = child_component_data_df.to_dict(orient="records")
                else:
                    singlecell_data["components"].pop(child_component_name, None)

def delete_singlecell_records(profile_id, checklist_id, target_ids=[],target_id="", study_id="", schema_name=""):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to delete!")
    
    dt = get_datetime()
    study_ids = []
    if study_id:
        is_single_study = True
        study_ids.append(study_id)
    else:
        is_single_study = False

    profile = Profile().get_record(profile_id)
    if not profile:
        return dict(status='error', message="Profile not found!")
    #schema_name = profile.get("schema_name", "COPO_SINGLE_CELL")
    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, target_id=checklist_id)
    identifier_map, foreignkey_map = SinglecellSchemas().get_key_map(schemas)
    child_map = SinglecellSchemas().get_child_map(foreignkey_map)

    identifiers = []
    component_name = ""
    

    #all target_id should come from same component
    #target_id format: component_study_identifier or study_identifier
    for target_id in target_ids:

        tmp = target_id.split("_")
        if len(tmp) >= 3:
            identifier = tmp[len(tmp)-1]  
            tmp_study_id = tmp[len(tmp)-2]          
            tmp_component_name = "_".join(tmp[:len(tmp)-2])
        elif len(tmp) == 2 and tmp[0] == "study":
                tmp_component_name = "study"
                tmp_study_id = tmp[1]
                identifier = tmp[1]
        else:
            return dict(status='error', message="taget id incorrect: " + target_id)

        if not component_name:
            component_name = tmp_component_name
        elif component_name != tmp_component_name:
            return dict(status='error', message="Please select records from the same component!")
         
        if study_id != tmp_study_id and is_single_study:
            return dict(status='error', message="Please select records from the same study!")
        elif not is_single_study:
            study_ids.append(tmp[1])

        identifiers.append(identifier)

    identifier_key = identifier_map.get(component_name, "")
    if not identifier_key:
        return dict(status='error', message="Identifier not found for component: " + component_name)
    
    study_messag_map = {}
    for study_id in study_ids:
        singlecell_data =  Singlecell(profile_id=profile_id).get_collection_handle().find_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id }, {"components": 1, "profile_id":1, "checklist_id":1})
        
        if not singlecell_data:
            message=f"Record not found"
            study_messag_map[study_id] = message
            continue

        #delete the record if both of it and its child records have no accession number
        component_data_df = pd.DataFrame.from_records(singlecell_data["components"][component_name])
        component_data_has_accession_df = component_data_df.loc[component_data_df.apply(lambda x: x[identifier_key] in identifiers and any(x[accession_column] != "" for accession_column in list(component_data_df.columns.values) if accession_column.startswith("accession_")), axis=1), identifier_key]


        #component_data_has_accession_df  = component_data_df.loc[(component_data_df[identifier_key].isin(identifiers)) & (component_data_df["accession"] !=""), identifier_key]
        if not component_data_has_accession_df.empty:
            if component_name == "study":
                message= ' record with accession number'
            else:
                message= f'{component_name}:{component_data_has_accession_df.tolist()}: record with accession number'
            study_messag_map[study_id] = message
            continue

        result, message =  _check_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map)
        if not result:
            study_messag_map[study_id] = message
 
    if study_messag_map:
        message = "Record deleted failed!"
        for key, msg in study_messag_map.items():
            message += f"<br/>study:'{key}'| {msg}"
        return dict(status='error', message=message)

    #get the schema for the file type item
    component_schema = schemas.get(component_name, [])

    for study_id in study_ids: 

        component_data_df = pd.DataFrame.from_records(singlecell_data["components"][component_name])

        #delete the record and the child records
        _delete_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map, schemas)

        to_be_delete_component_data_df = component_data_df.loc[component_data_df[identifier_key].isin(identifiers)]
        _delete_datafile(singlecell_data["profile_id"], to_be_delete_component_data_df, component_schema)
        
        component_data_df = component_data_df.drop(component_data_df[component_data_df[identifier_key].isin(identifiers)].index)

        if not component_data_df.empty:
            singlecell_data["components"][component_name] = component_data_df.to_dict(orient="records")
        else:
            singlecell_data["components"].pop(component_name, None)

        if singlecell_data["components"]:                    
            Singlecell(profile_id=profile_id).get_collection_handle().update_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id}, {"$set": {"components": singlecell_data["components"], "last_modified": dt, "last_update_by": dt}})
        else:
            Singlecell(profile_id=profile_id).get_collection_handle().delete_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id})
 
    return {"status": "success", "message": "Record deleted successfully!"}


"""
def submit_singlecell_ena(profile_id, target_ids, target_id,checklist_id, study_id):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to submit!")

    user = ThreadLocal.get_current_user()
    dt = get_datetime()

    sub = Submission().get_collection_handle().find_one(
        {"profile_id": profile_id, "deleted": get_not_deleted_flag()})

    if not sub:
        return dict(status='error', message="Please contact System Support Error 10211!")
    
    return dict(status='error', message="Not Implement.")        
"""

def submit_singlecell(profile_id, target_ids, target_id, checklist_id, study_id, repository="ena"):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to submit!")
    
    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "deleted": get_not_deleted_flag(), "study_id" : study_id})
    if not singlecell:
        return dict(status='error', message="No record found.")
    #check if the submission is in progress
    studies = singlecell.get("components",{}).get("study",[])
    if studies[0].get(f"status_{repository}", "") == "processing":
        return dict(status='error', message="Submission is in progress, please wait until it is completed!")

    submissions = Submission().execute_query({"profile_id": profile_id, "repository": repository, "deleted": get_not_deleted_flag()})
    schemas = SinglecellSchemas().get_schema(schema_name=singlecell.get("schema_name", singlecell["schema_name"]), target_id=singlecell["checklist_id"])
    files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas)
    if files:
        if repository == "ena":
            datafiles = DataFile().get_all_records_columns(filter_by={"profile_id": profile_id, "file_name": {"$in": files}}, projection={"_id": 1})
            for file in datafiles:
                tx.make_transfer_record(file["_id"], str(submissions[0]["_id"] ))
            
    result =  Submission().make_submission_downloading(profile_id=profile_id, component="study", component_id=study_id, repository=repository)
    if result.get("status","") == "error":
        return result  
    else:
        #update the status of the singlecell record
        Singlecell().update_component_status(singlecell["_id"], component="study", identifier="study_id", identifier_value=study_id, repository=repository, status_column_value={"status":"processing"})
        return dict(status='success', message="Submission has been scheduled.")


def update_submission_pending():
    component = "study"
    subs = Submission().get_submission_downloading(component=component)
    all_downloaded_sub_ids = []
    for sub in subs:
        all_file_downloaded = True
        for study_id in sub[component]:
            singlecell = Singlecell().get_collection_handle().find_one(
                 {"profile_id":sub["profile_id"], "study_id":study_id,"deleted":get_not_deleted_flag()},
                 {"schema_name":1,"checklist_id":1, "components":1})
            if not singlecell:
                Submission().remove_study_from_singlecell_submission(sub_id=str(sub["_id"]), study_id= study_id)
                continue
            schemas = SinglecellSchemas().get_schema(schema_name=singlecell["schema_name"], target_id=singlecell["checklist_id"])
            files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas)

            local_path = [regex.Regex(f'{x}$') for x in files]
            projection = {'_id':0, 'local_path':1, 'status':1}
            filter = dict()
            filter['local_path'] = {'$in': local_path}
            filter['profile_id'] = sub["profile_id"]
        
            enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)
 
            if not files and not enaFiles:
                # No files to download, continue to the next study
                continue

            elif len(files) > len(enaFiles):
                all_file_downloaded = False
                # Log the missing files
                missing_files = set(files) - {os.path.basename(enaFile["local_path"]) for enaFile in enaFiles}
                Logger().error(f"Files not uploaded for submission {sub['_id']} : study {study_id} : {missing_files} ") 
                break

            elif not all( enaFile["status" ] == "complete" for enaFile in enaFiles):
                all_file_downloaded = False
                break

        if all_file_downloaded:
            all_downloaded_sub_ids.append(sub["_id"])

    if all_downloaded_sub_ids:
        Submission().update_submission_pending(all_downloaded_sub_ids, component="study")


"""
class _GET_ENA_FILE_PROCESSING_STATUS(threading.Thread):
    def __init__(self, profile_id, run_accession_number_map, data_map=dict(), columns=dict(), ena_file_transfer_map=dict()):
        self.profile_id = profile_id
        self.run_accession_number_map = run_accession_number_map
        self.data_map = data_map
        self.ena_file_transfer_map = ena_file_transfer_map

        super(_GET_ENA_FILE_PROCESSING_STATUS, self).__init__() 

    def run(self):
        sent_2_frontend_every = 4000
        #data = []
        i = 0
        #data = self.return_dict["dataSet"]
        ecs_file_complete = []

        for run_accession in self.run_accession_number_map.keys():
            i += 1
            file_processing_status = _query_ena_file_processing_status(run_accession)
            if file_processing_status:
               #data.append({"run_accession":run_accession, "msg":file_processing_status})

               row = self.data_map.get(self.run_accession_number_map.get(run_accession), dict())
               row["ena_file_processing_status"] = file_processing_status
               complete_cnt = file_processing_status.count("File archived")
               if complete_cnt > 0:
                   file_ids = row["DT_RowId"][4:].split("_")    #row_data["DT_RowId"] = "row_fileid1_fileid2"
                   if complete_cnt == len(file_ids):
                        for file_id in file_ids:
                            ecs_location = self.ena_file_transfer_map.get(file_id, {}).get("ecs_location","")
                            if ecs_location:
                                ecs_file_complete.append(ecs_location)


            if i == sent_2_frontend_every:                 
               #notify_read_status(data={"profile_id": self.profile_id, "file_processing_status":data},  msg="", action="file_processing_status" )
               notify_read_status(data={"profile_id": self.profile_id, "table_data" : list(self.data_map.values())},
                        msg="Refreshing table for file processing status", action="file_processing_status", html_id="sample_info")
               i = 0
               #data=[]
        if i>0:
            #notify_read_status(data={"profile_id": self.profile_id, "file_processing_status":data},  msg="", action="file_processing_status" )
            notify_read_status(data={"profile_id": self.profile_id, "table_data" : list(self.data_map.values())},
                        msg="Refreshing table for file processing status", action="file_processing_status", html_id="sample_info")
            
        if ecs_file_complete:
            EnaFileTransfer().complete_remote_transfer_status_by_ecs_path( ecs_locations=ecs_file_complete)
"""