from common.dal.copo_da import EnaChecklist
from common.dal.sample_da import Sample
from common.utils.helpers import get_not_deleted_flag
from django_tools.middlewares import ThreadLocal
from common.utils.helpers import get_datetime, get_not_deleted_flag, get_env, notify_submission_status, json_to_pytype
from bson import ObjectId
from common.dal.submission_da import Submission
from common.utils.logger import Logger
from common.ena_utils import generic_helper as ghlper
from lxml import etree
from common.lookup.lookup import SRA_SAMPLE_TEMPLATE
from common.lookup.lookup import SRA_SETTINGS
import pandas as pd
import subprocess
import os
import tempfile
from common.ena_utils.ena_helper import EnaSubmissionHelper

l = Logger()
ena_service = get_env('ENA_SERVICE')
pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]
webin_user = get_env('WEBIN_USER')
webin_domain = get_env('WEBIN_USER').split("@")[1]

def generate_table_records(profile_id=str(), checklist_id=str()):
    checklist = EnaChecklist().execute_query({"primary_id" : checklist_id})

    if not checklist:
        return dict(dataSet=[],
                    columns=[],
                    )
    label = []
    default_label = ["sraAccession", "biosampleAccession", "status", "error"]

    fields = checklist[0]["fields"]
    label =  [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and fields[x].get("read_field", False)  == False or x in ["sample", "organism"]]

    #data_set = []
    columns = []
    data_map = dict()

    detail_dict = dict(className1='summary-details-control detail-hover-message', orderable=False, data=None,
                        title='', defaultContent='', width="5%")   # remove the details 
    columns.insert(0, detail_dict)
    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))
    columns.extend([dict(data=x, title=fields[x]["label"], defaultContent='', render="render_ena_accession_function" if x.lower().endswith("accession") else ""    ) for x in label  ])
    columns.extend([dict(data=x, title=x.upper().replace("_", " "), defaultContent='', render="render_ena_accession_function" if x.lower().endswith("accession") else "" ) for x in default_label ])  

    label.extend(default_label)

    samples = Sample(profile_id=profile_id).execute_query({"checklist_id" : checklist_id, "profile_id": profile_id, 'deleted': get_not_deleted_flag()})

    for sample in samples:
        row_data = dict()
        row_data.update({key : sample.get(key, str()) for key in label})
        row_data["record_id"] = str(sample["_id"])
        row_data["DT_RowId"] = "row_" +  str(sample["_id"])
        data_map[row_data["record_id"]] = row_data

    return_dict = dict(dataSet=list(data_map.values()),
                    columns=columns,
                    )
    return return_dict


def submit_sample(profile_id,  target_ids=list(), target_id=None, checklist_id=None):

    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to submit!")
    component = "sample"
    user = ThreadLocal.get_current_user()
    dt = get_datetime()
 
    sub = Submission().get_collection_handle().find_one({"profile_id": profile_id, "repository":"ena", "deleted": get_not_deleted_flag()})
    update_info = dict()
    update_info[f"{component}_status"] = "pending"
    update_info["date_modified"] = dt
    update_info["updated_by"] = str(user.id)
    
    if not sub:
            sub = dict()
            sub["date_created"] = dt
            sub["date_modified"] = dt
            sub["repository"] = "ena"
            sub["accessions"] = dict()
            sub["profile_id"] = profile_id
            sub["deleted"] = get_not_deleted_flag()
            sub_id = Submission().get_collection_handle().insert_one(sub).inserted_id
    else :
        sub_id = sub["_id"] 
        if sub.get(f"{component}_status", "pending") == "sending":
            #don't update the submission status
            update_info.pop(f"{component}_status", None)

    Submission(profile_id=profile_id).get_collection_handle().update_one({"_id": sub_id}, {
        "$addToSet": {component: {"$each": target_ids}},
        "$set": update_info})
    
    Sample().get_collection_handle().update_many({"_id": {"$in": [ObjectId(id) for id in target_ids]}},
        {"$set": {"status": "processing", "date_modified": dt, "updated_by": str(user.id), "error": ""}})

    return dict(status='success',
                message="Submission has been added to the processing queue. Status update will be provided.")

def delete_sample_records(profile_id, target_ids=list(), target_id=None):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to delete!")
    
    sample_obj_ids = [ObjectId(id) for id in target_ids]
 
    # check if any of the selected file records have been submitted to ENA
    result = Sample(profile_id=profile_id).get_all_records_columns(
        filter_by={"_id": {"$in": sample_obj_ids}},
        projection={"name":1, "status": 1, "biosampleAccession": 1,"derivesFrom": 1})
    
    sample_with_biosample = [f'{s["name"]}({s["biosampleAccession"]})' for s in result if s.get("biosampleAccession", "")]
    sample_with_processing = [f'{s["name"]}' for s in result if s.get("status", "") == "processing"]
    message = str()
    if sample_with_biosample:
        message= "Sample record/s have been submitted to ENA. Cannot be deleted! : " + ",".join(sample_with_biosample)
    if sample_with_processing:
        message += "\nSample record/s are being processed. Cannot be deleted! : " + ",".join(sample_with_processing)
    if message:
        return dict(status='error', message=message)

    Sample(profile_id=profile_id).get_collection_handle().delete_many({"_id": {"$in": sample_obj_ids}})
    return dict(status='success', message="Read record/s have been deleted!")


def process_pending_submission():
    """
    Process samples that are pending submission to ENA.
    """
    dt = get_datetime()
    
    submissions = Submission().get_pending_submission(repository="ena", component="sample")
    for submission in submissions:
        # Process each submission
        profile_id = submission["profile_id"]
        sample_ids = submission.get("sample", [])
        submission_id = str(submission["_id"])
        ena_submission_helper = EnaSubmissionHelper(submission_id=submission_id, profile_id=profile_id)

        samples = Sample(profile_id=profile_id).get_all_records_columns(filter_by={"_id": {"$in": [ObjectId(id) for id in sample_ids]},
                                                                   "deleted": get_not_deleted_flag()}
                                                              )
        
        sample_processing_ids = {sample["_id"]: sample for sample in samples if sample.get("status", "") == "processing"}
    
        if sample_processing_ids:
            # Update the status of the samples to 'sending'
            #Sample(profile_id=profile_id).get_collection_handle().update_many(
            #    {"_id": {"$in": sample_processing_ids.keys()}},
            #    {"$set": {"status": "sending", "date_modified": dt, "updated_by":  "system"}})

            # Perform the submission to ENA
            output_location = tempfile.mkdtemp()
            context = ena_submission_helper.get_submission_xml(output_location)

            if context['status'] is False:
                notify_submission_status(data={"profile_id": profile_id}, msg=context.get("message", str()), action="error",
                                html_id="submission_info")
                ghlper.update_submission_status(status='error', message=context.get("message", str()),
                                                submission_id=submission_id)
                return context

            submission_xml_path = context['value']

            context = ena_submission_helper.get_edit_submission_xml(output_location, submission_xml_path)
            if context['status'] is False:
                notify_submission_status(data={"profile_id": profile_id}, msg=context.get("message", str()), action="error",
                                html_id="submission_info")
                ghlper.update_submission_status(status='error', message=context.get("message", str()),
                                                submission_id=submission_id)
                return context
            modify_submission_xml_path = context['value']

            context = ena_submission_helper.register_samples(submission_xml_path=submission_xml_path,
                              modify_submission_xml_path=modify_submission_xml_path,
                              samples=list(sample_processing_ids.values()))
            if context['status'] is False:
                notify_submission_status(data={"profile_id": profile_id}, msg=context.get("message", str()), action="error",
                                html_id="submission_info")
                ghlper.update_submission_status(status='error', message=context.get("message", str()),
                                                submission_id=submission_id)
                return context
            else:
                message = "Sample submission has been submitted to ENA."
                notify_submission_status(data={"profile_id": profile_id}, msg=message, action="info",
                                html_id="submission_info")





