from django_tools.middlewares import ThreadLocal
from common.utils.logger import Logger
from common.dal.copo_da import Sample, DataFile, Source, Submission, SubmissionQueue, Sequnece_annotation, EnaFileTransfer, EnaChecklist
from common.utils.helpers import get_datetime, get_not_deleted_flag
from common.dal.mongo_util import cursor_to_list
from bson import ObjectId

l = Logger()

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
    other_annotation_with_same_file = cursor_to_list(
        Sequnece_annotation(profile_id=profile_id).get_collection_handle().find({"files": {"$in": file_ids}},
                                                                                {"_id": 1, "files": 1}))
    for s in other_samples_with_same_file:
        for f in s.get("read", []):
            for file_id in f["file_id"].split(","):
                file_ids.remove(file_id) if file_id in file_ids else None

    for a in other_annotation_with_same_file:
        for f in a.get("files", []):
            file_ids.remove(f) if f in file_ids else None

    if file_ids:
        DataFile(profile_id=profile_id).get_collection_handle().delete_many(
            {"_id": {"$in": [ObjectId(f) for f in file_ids]}})
        EnaFileTransfer(profile_id=profile_id).get_collection_handle().delete_many({"file_id": {"$in": file_ids}})

    # remove sample records if no file inside

    samples = Sample(profile_id=profile_id).get_all_records_columns(filter_by={"_id": {"$in": sample_obj_ids}},
                                                                    projection={"biosampleAccession": 1, "read": 1,
                                                                                "derivesFrom": 1})

    for sample in samples:
        if not sample.get("read", []) and not sample.get("biosampleAccession", ""):
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

    return dict(status='success', message="Read record/s have been deleted!")



def generate_read_record(profile_id=str(), checklist_id=str()):
    checklist = EnaChecklist().execute_query({"primary_id" : checklist_id})
    if not checklist:
        return dict(dataSet=[],
                    columns=[],
                    )
    label = []
    default_label = []
    fields = checklist[0]["fields"]
    if checklist_id == 'read':
        label =  [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and fields[x].get("for_dtol", True) ]
        default_label = ["ena_file_upload_status", "study_accession",  "sraAccession", "status",  "run_accession", "experiment_accession", "error"]

    else:
        label = [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and not fields[x].get("for_dtol", False) ]
        default_label = ["ena_file_upload_status", "study_accession",  "biosampleAccession", "sraAccession", "status",  "run_accession", "experiment_accession", "error"]


    read_label =  [ x for x in fields.keys() if fields[x]["type"] != "TEXT_AREA_FIELD" and fields[x].get("read_field", False) ] 

    data_set = []
    columns = []
    label_set = set()

    detail_dict = dict(className='summary-details-control detail-hover-message', orderable=False, data=None,
                        title='', defaultContent='', width="5%")
    columns.insert(0, detail_dict)
    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))
    columns.extend([dict(data=x, title=fields[x]["name"], defaultContent='') for x in label  ])
    columns.extend([dict(data=x, title=x.upper().replace("_", " "), defaultContent='') for x in default_label ])  

    label.extend(default_label)


    submission = Submission().get_all_records_columns(filter_by={"profile_id": profile_id}, projection={"_id": 1, "name": 1, "accessions": 1})
    if not submission:
        return dict(dataSet=data_set,
                columns=columns,
                )
        
    project_accession = submission[0].get("accessions",dict()).get("project",[])
    study_accession = ""
    if project_accession:
        study_accession = project_accession[0].get("accession","")

    samples = Sample(profile_id=profile_id).execute_query({"checklist_id" : checklist_id, "profile_id": profile_id, 'deleted': get_not_deleted_flag()})

    for sample in samples:
        for read in sample.get("read", []):
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
                    for accession in submission[0].get("accessions", {}).get("run", []):
                        if set(accession.get("datafiles",[])) == set(file_ids):
                            row_data["run_accession"] = accession.get("accession", str())
                            alias = accession.get("alias", str())
                            break
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



            data_set.append(row_data)


    return_dict = dict(dataSet=data_set,
                    columns=columns,
                    )

    return return_dict