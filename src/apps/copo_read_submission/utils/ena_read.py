from django_tools.middlewares import ThreadLocal
from common.utils.logger import Logger
from common.dal.copo_da import DataFile, EnaFileTransfer, EnaChecklist
from common.dal.submission_da import Submission
from common.dal.sample_da import Sample, Source
from .da import SubmissionQueue
from common.utils.helpers import get_datetime, get_not_deleted_flag, get_env, notify_read_status
from common.dal.mongo_util import cursor_to_list
from bson import ObjectId
import requests
import threading

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

def submit_read(profile_id,  target_ids=list(), target_id=None, checklist_id=None):

    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to submit!")

    user = ThreadLocal.get_current_user()
    dt = get_datetime()
    file_ids = [file_id for id in target_ids for file_id in id.split("_")[1].split(",")]
    sample_obj_ids = [ObjectId(id.split("_")[0]) for id in target_ids]
    paired_file_ids = [id.split("_")[1] for id in target_ids]

    sub = Submission().get_collection_handle().find_one(
        {"profile_id": profile_id, "deleted": get_not_deleted_flag()})

    if not sub:
        return dict(status='error', message="Please contact System Support Error 10211!")

    doc = SubmissionQueue(profile_id=profile_id).execute_query({"submission_id": str(sub["_id"])})
    if doc and doc[0].get("processing_status", "pending") != 'pending':
        context = dict(status='error', message='Submission is already in the processing queue. Please try it later')
        return context

    Submission(profile_id=profile_id).get_collection_handle().update_one({"_id": sub["_id"]}, {
        "$addToSet": {"bundle": {"$each": paired_file_ids}},
        "$set": {"complete": "false", "date_modified": dt, "updated_by": str(user.id)}})

    for id in paired_file_ids:
        Sample(profile_id=profile_id).get_collection_handle().update_one(
            {"_id": {"$in": sample_obj_ids}, "read.file_id": id, "read.status": "pending"},
            {"$set": {"read.$.status": "processing", "date_modified": dt, "updated_by": str(user.id)}})

    if not doc:  # submission not in queue, add to queue
        fields = dict(
            submission_id=str(sub["_id"]),
            date_modified=dt,
            date_created=dt,
            repository=sub["repository"],
            processing_status='pending',
            profile_id=profile_id,
        )
        result = SubmissionQueue(profile_id=profile_id).get_collection_handle().insert_one(fields)
    return dict(status='success',
                message="Submission has been added to the processing queue. Status update will be provided.")


def delete_ena_records(profile_id, target_ids=list(), target_id=None):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to delete!")

    dt = get_datetime()
    existing_sample_with_file = []
    delete_samples = []
    delete_sources = []
    delete_sample_accessions_4_external_samples = []

    file_ids = [file_id for id in target_ids for file_id in id.split("_")[1].split(",")]
    sample_obj_ids = [ObjectId(id.split("_")[0]) for id in target_ids]
    file_regex_ids = "|".join([file_id for id in target_ids for file_id in id.split("_")[1].split(",")])

    # check if any of the selected file records have been submitted to ENA
    result = Sample(profile_id=profile_id).get_all_records_columns(
        filter_by={"_id": {"$in": sample_obj_ids}, "read.file_id": {"$regex": file_regex_ids}},
        projection={"status": 1, "biosampleAccession": 1, "read.$": 1, "derivesFrom": 1})
    for r in result:
        for file in r.get("read", []):
            interset = [file_id for file_id in file.get("file_id", "").split(",") if file_id in file_ids]
            if not interset:
                continue
            if file.get("status", "pending") == "accepted":
                return dict(status='error', message="one or more record/s have been submitted to ENA!")
            elif file.get("status", "pending") == "processing":
                return dict(status='error', message="one or more record/s have been scheduled to submit to ENA!")

    # check if any of the selected file records have been used by other samples

    # remove file_id from samples
    Sample().get_collection_handle().update_many({"_id": {"$in": sample_obj_ids}},
                                                 {"$pull": {"read": {"file_id": {"$regex": file_regex_ids}}}})

    # remove datafile records if no sample is using it
    other_samples_with_same_file = cursor_to_list(Sample(profile_id=profile_id).get_collection_handle().find(
        {"_id": {"$nin": sample_obj_ids}, "read.file_id": {"$regex": file_regex_ids}}, {"_id": 1, "read.$": 1}))
    #other_annotation_with_same_file = cursor_to_list(
    #    SequenceAnnotation(profile_id=profile_id).get_collection_handle().find({"files": {"$in": file_ids}},
    #                                                                            {"_id": 1, "files": 1}))
    for s in other_samples_with_same_file:
        for f in s.get("read", []):
            for file_id in f["file_id"].split(","):
                file_ids.remove(file_id) if file_id in file_ids else None

    '''
    for a in other_annotation_with_same_file:
        for f in a.get("files", []):
            file_ids.remove(f) if f in file_ids else None
    '''

    if file_ids:
        DataFile(profile_id=profile_id).get_collection_handle().delete_many(
            {"_id": {"$in": [ObjectId(f) for f in file_ids]}})
        EnaFileTransfer(profile_id=profile_id).get_collection_handle().delete_many({"file_id": {"$in": file_ids}})

    # remove sample records if no file inside

    samples = Sample(profile_id=profile_id).get_all_records_columns(filter_by={"_id": {"$in": sample_obj_ids}},
                                                                    projection={"biosampleAccession": 1, "read": 1,"is_external":1,
                                                                                "derivesFrom": 1})

    for sample in samples:
        if not sample.get("read", []) and ( sample.get("is_external", "0") == "1" or not sample.get("biosampleAccession", "")):
            if  "derivesFrom" in sample:
                delete_sources.append(sample["derivesFrom"])
            delete_samples.append(sample["_id"])

    if delete_sources:
        other_samples_with_same_source = cursor_to_list(Sample(profile_id=profile_id).get_collection_handle().find(
            {"_id": {"$nin": delete_samples}, "derivesFrom": {"$in": delete_sources}}, {"derivesFrom": 1}))
        for s in other_samples_with_same_source:
            delete_sources.remove(s["derivesFrom"]) if s["derivesFrom"] in delete_sources else None
        Source(profile_id=profile_id).get_collection_handle().delete_many(
            {"_id": {"$in": [ObjectId(s) for s in delete_sources]}})

    if delete_samples:
        Sample(profile_id=profile_id).get_collection_handle().delete_many({"_id": {"$in": delete_samples}})
        Submission().get_collection_handle().update_one({"profile_id": profile_id}, {"$pull": {"accessions.sample": {"sample_id": {"$in": [str(id) for id in delete_samples]}}}})

    return dict(status='success', message="Read record/s have been deleted!")



def generate_read_record(profile_id=str(), checklist_id=str()):
    checklist = EnaChecklist().execute_query({"primary_id" : checklist_id})
    run_accession_number = []

    if not checklist:
        return dict(dataSet=[],
                    columns=[],
                    )
    label = []
    default_label = []
    fields = checklist[0]["fields"]
    if checklist_id == 'read':
        label =  [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and fields[x].get("for_dtol", True) ]
        default_label = ["ena_file_upload_status", "study_accession",  "sraAccession", "status",  "run_accession", "experiment_accession", "ena_file_processing_status"]

    else:
        label = [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and not fields[x].get("for_dtol", False) ]
        default_label = ["ena_file_upload_status", "study_accession",  "biosampleAccession", "sraAccession", "status",  "run_accession", "experiment_accession", "ena_file_processing_status"]


    read_label =  [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and fields[x].get("read_field", False) ] 

    #data_set = []
    columns = []
    label_set = set()
    data_map = dict()
    run_accession_number_map = dict()

    detail_dict = dict(className1='summary-details-control detail-hover-message', orderable=False, data=None,
                        title='', defaultContent='', width="5%")   # remove the details 
    columns.insert(0, detail_dict)
    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))
    columns.extend([dict(data=x, title=fields[x]["name"], defaultContent='', className="ena-accession" if x.lower().endswith("accession") else x  if x == "ena_file_processing_status" else ""    ) for x in label  ])
    columns.extend([dict(data=x, title=x.upper().replace("_", " "), defaultContent='', className="ena-accession" if x.lower().endswith("accession") else  x if x == "ena_file_processing_status" else "" ) for x in default_label ])  

    label.extend(default_label)

    submission = Submission().get_all_records_columns(filter_by={"profile_id": profile_id}, projection={"_id": 1, "name": 1, "accessions": 1})
    if not submission:
        return dict(dataSet=[],
                columns=columns
        )
        
    project_accession = submission[0].get("accessions",dict()).get("project",[])
    study_accession = ""
    if project_accession:
        study_accession = project_accession[0].get("accession","")

    samples = Sample(profile_id=profile_id).execute_query({"read.checklist_id" : checklist_id, "profile_id": profile_id, 'deleted': get_not_deleted_flag()})

    for sample in samples:
        for read in sample.get("read", []):
            if read.get("checklist_id","") == checklist_id:
                row_data = dict()
                row_data.update({key : sample.get(key, str()) for key in label})
                row_data["study_accession"] = study_accession
                row_data["record_id"] = f'{str(sample["_id"])}_{read["file_id"]}'
                row_data["file_name"] = read["file_name"]

                file_id_str = read.get("file_id", str())
                file_ids = file_id_str.split(",")
                if file_ids:
                    row_data["DT_RowId"] = "row_" + read["file_id"].replace(",", "_")
                    row_data["status"] = read.get("status", "pending")

                    if submission and row_data["status"] == "accepted":
                        alias = None
                        for accession in submission[0].get("accessions", {}).get("run", []):
                            if set(accession.get("datafiles",[])) == set(file_ids):
                                row_data["run_accession"] = accession.get("accession", str())
                                alias = accession.get("alias", str())
                                break
                        if alias : 
                            for accession in submission[0].get("accessions", {}).get("experiment", []):
                                if accession.get("alias",[]) == alias:
                                    row_data["experiment_accession"] = accession.get("accession", str())
                                    break                        

                    row_data["ena_file_upload_status"] = "unknown"
                    ena_file_transfer = EnaFileTransfer(profile_id=profile_id).execute_query({
                        "file_id": {"$in": file_ids}})
                    if ena_file_transfer:
                        row_data["ena_file_upload_status"] = ena_file_transfer[0].get(
                            "status", str())
                        if len(ena_file_transfer) > 1:
                            row_data["ena_file_upload_status"] = row_data["ena_file_upload_status"] + \
                                " | " + \
                                ena_file_transfer[1].get("status", str())
                                
                    files = DataFile().get_records(file_ids)
                    if files:
                        read_values = files[0].get("description", dict()).get("attributes",dict())
                        row_data.update({key : read_values.get(key, str()) for key in read_label})

                row_data["ena_file_processing_status"] = ""
                
                if row_data["run_accession"]:
                    run_accession_number_map[row_data["run_accession"]] = row_data["record_id"]
                    
                    #row_data["ena_file_processing_status"] = _query_ena_file_processing_status(row_data["run_accession"])
                
                #data_set.append(row_data)
                data_map[row_data["record_id"]] = row_data

    return_dict = dict(dataSet=list(data_map.values()),
                    columns=columns,
                    )

    if run_accession_number_map:
        thread = _GET_ENA_FILE_PROCESSING_STATUS(profile_id=profile_id, run_accession_number_map=run_accession_number_map, data_map=data_map)  
        thread.start()

    return return_dict

class _GET_ENA_FILE_PROCESSING_STATUS(threading.Thread):
    def __init__(self, profile_id, run_accession_number_map, data_map=dict(), columns=dict()):
        self.profile_id = profile_id
        self.run_accession_number_map = run_accession_number_map
        self.data_map = data_map
        super(_GET_ENA_FILE_PROCESSING_STATUS, self).__init__() 

    def run(self):
        sent_2_frontend_every = 4000
        #data = []
        i = 0
        #data = self.return_dict["dataSet"]

        for run_accession in self.run_accession_number_map.keys():
            i += 1
            file_processing_status = _query_ena_file_processing_status(run_accession)
            if file_processing_status:
               #data.append({"run_accession":run_accession, "msg":file_processing_status})
               row = self.data_map.get(self.run_accession_number_map.get(run_accession), dict())
               row["ena_file_processing_status"] = file_processing_status

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