from django.shortcuts import render
from django.contrib.auth.decorators import login_required
import json
import subprocess
from common.dal.copo_da import  DataFile, EnaChecklist
from common.dal.submission_da import Submission
from common.dal.profile_da import Profile
from common.dal.sample_da import Sample, Source
from common.utils import helpers 
from django.http import HttpResponse, JsonResponse
import datetime
from common.s3.s3Connection import S3Connection as s3
from pymongo import ReturnDocument
import common.ena_utils.FileTransferUtils as tx
from django.conf import settings
from os.path import join
from pathlib import Path
from bson import json_util, ObjectId
from common.utils.logger import Logger
import jsonpickle
from common.ena_utils import generic_helper as ghlper
import inspect
from common.validators.validator import Validator
from .utils.ena_validator import ena_seq_validators as required_validators
from common.ena_utils.EnaChecklistHandler import EnaCheckListSpreadsheet, write_manifest

from common.utils.helpers import get_datetime, get_not_deleted_flag,map_to_dict
from .utils import ena_read  
from io import BytesIO
from src.apps.copo_core.views import web_page_access_checker

l = Logger()

@login_required()
def ena_read_manifest_validate(request, profile_id):
    request.session["profile_id"] = profile_id
    checklist_id = request.GET.get("checklist_id")
    data = {"profile_id": profile_id}
    if checklist_id:
        checklist = EnaChecklist().execute_query({"primary_id": checklist_id})
        if checklist:
            data["checklist_id"] = checklist_id
            data["checklist_name"] = checklist[0]["name"]
            
    return render(request, "copo/ena_read_manifest_validate.html", data)


@login_required()
def parse_ena_spreadsheet(request):
    profile_id = request.session["profile_id"]
    ghlper.notify_read_status(data={"profile_id": profile_id},
                       msg='', action="info",
                       html_id="sample_info")
    # method called by rest
    file = request.FILES["file"]
    checklist_id = request.POST["checklist_id"]
    name = file.name
    
    required_validators = []
    required = dict(globals().items())["required_validators"]
    for element_name in dir(required):
        element = getattr(required, element_name)
        if inspect.isclass(element) and issubclass(element, Validator) and not element.__name__ == "Validator":
            required_validators.append(element)

    ena = EnaCheckListSpreadsheet(file=file, checklist_id=checklist_id, component="sample", validators=required_validators)
    s3obj = s3()
    if name.endswith("xlsx") or name.endswith("xls"):
        fmt = 'xls'
    else:
        return HttpResponse(status=415, content="Please make sure your manifest is in xlxs format")

    if ena.loadManifest(fmt):
        l.log("Dtol manifest loaded")
        if ena.validate():
            l.log("About to collect Dtol manifest")
            # check s3 for bucket and files files
            bucket_name = str(request.user.id) + "_" + request.user.username
            # bucket_name = request.user.username
            file_names = ena.get_filenames_from_manifest()

            if s3obj.check_for_s3_bucket(bucket_name):
                # get filenames from manifest
                # check for files
                if not s3obj.check_s3_bucket_for_files(bucket_name=bucket_name, file_list=file_names):
                    # error message has been sent to frontend by check_s3_bucket_for_files so return so prevent ena.collect() from running
                    return HttpResponse(status=400)
            else:
                # bucket is missing, therefore create bucket and notify user to upload files
                ghlper.notify_read_status(data={"profile_id": profile_id},
                                   msg='s3 bucket not found, creating it', action="info",
                                   html_id="sample_info")
                s3obj.make_s3_bucket(bucket_name=bucket_name)
                ghlper.notify_read_status(data={"profile_id": profile_id},
                                msg='Files not found, please click "Upload Data into COPO" and follow the '
                                    'instructions.', action="error",
                                html_id="sample_info")
                return HttpResponse(status=400)
            ghlper.notify_read_status(data={"profile_id": profile_id},
                            msg='Spreadsheet is valid', action="info",
                            html_id="sample_info")
            ena.collect()
            return HttpResponse()
        return HttpResponse(status=400)
    return HttpResponse(status=400)

@login_required()
def save_ena_records(request):
    # create mongo sample objects from info parsed from manifest and saved to session variable
    sample_data = request.session.get("sample_data")
    profile_id = request.session["profile_id"]
    #profile_name = Profile().get_name(profile_id)
    uid = str(request.user.id)
    username = request.user.username
    checklist = EnaChecklist().get_collection_handle().find_one({"primary_id": request.session["checklist_id"]})
    column_name_mapping = { field["name"].upper() : key  for key, field in checklist["fields"].items() if not field.get("read_field", False) }
    #checklist_read = EnaChecklist().get_collection_handle().find_one({"primary_id": "read"})
    column_name_mapping_read = { field["name"].upper() : key  for key, field in checklist["fields"].items() if field.get("read_field", False) }
    #bundle = list()
    #alias = str(uuid.uuid4())
    #bundle_meta = list()
    pairing = list()
    datafile_list = list()
    #existing_bundle = list()
    #existing_bundle_meta = list()
    sub = Submission().get_collection_handle().find_one(
        {"profile_id": profile_id, "deleted": get_not_deleted_flag()})
    # override the bundle files for every manifest upload
    # if sub:
    #    existing_bundle = sub["bundle"]
    #    existing_bundle_meta = sub["bundle_meta"]
    dt = get_datetime()
    project_release_date = None
    submission_external_sample_accession=[]

    organism_map = dict()
    source_map = dict()

    for line in range(1, len(sample_data)):
        is_external_sample = False
        # for each row in the manifest

        s = (map_to_dict(sample_data[0], sample_data[line]))

   
        #project_release_date = s["release_date"]
        df = dict()
        p = Profile().get_record(profile_id)
        attributes = dict()
        # attributes["datafiles_pairing"] = list()
        attributes["target_repository"] = {"deposition_context": "ena"}
        # attributes["project_details"] = {
        #    "project_name": p["title"],
        #    "project_title": p["title"],
        #    "project_description": p["description"],
        #    "project_release_date": s["release_date"]
        # }

        '''
        attributes["library_preparation"] = {
            "library_layout": s["library_layout"],
            "library_strategy": s["library_strategy"],
            "library_source": s["library_source"],
            "library_selection": s["library_selection"],
            "library_description": s["library_description"]
        }
        '''

        if "biosampleAccession" in s:
            sample = Sample().get_collection_handle().find_one({"profile_id": profile_id, "biosampleAccession": s["biosampleAccession"]})   
        else:
            # check if sample already exists, if so, add new datafile
            sample = Sample().get_collection_handle().find_one({"name": s["Sample"], "profile_id": profile_id})

        insert_record = {}
        insert_record["created_by"] = uid
        insert_record["time_created"] = get_datetime()
        insert_record["date_created"] = dt

        if "Organism" in s:
            if not sample or sample.get("organism","") != s["Organism"]:
                if not sample:
                    sample = dict()

                source = dict()
                taxinfo = organism_map.get(s["Organism"], None)
                source_id = source_map.get(s["Organism"], None)
                if not taxinfo:
                    curl_cmd = "curl " + \
                            "https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/" + s["Organism"].replace(" ", "%20")
                    receipt = subprocess.check_output(curl_cmd, shell=True)
                    # ToDo - exit if species not found
                    print(receipt)
                    taxinfo = json.loads(receipt.decode("utf-8"))
                    organism_map[s["Organism"]] = taxinfo

                    # create source from organism
                    termAccession = "http://purl.obolibrary.org/obo/NCBITaxon_" + str(taxinfo[0]["taxId"])
                    source["organism"] = \
                        {"annotationValue": s["Organism"], "termSource": "NCBITAXON", "termAccession":
                            termAccession}
                    # source["profile_id"] = request.session["profile_id"]
                    source["date_modified"] = dt
                    source["deleted"] = "0"
                    source["name"] = s["Sample"]
                    source["profile_id"] = profile_id                
                    source_id = str(
                        Source().get_collection_handle().find_one_and_update({"organism.termAccession": termAccession, "profile_id": profile_id},
                                                                            {"$set": source, "$setOnInsert": insert_record},
                                                                            upsert=True, return_document=ReturnDocument.AFTER)["_id"])
                    source_map[s["Organism"]] = source_id
                    

                sample["derivesFrom"] = source_id
                sample["name"] = s["Sample"]
                # create associated sample
                insert_record["sample_type"] = "isasample"
                insert_record["status"] = "pending"
                #sample["derivesFrom"] = source_id
                #sample["read"] = {"file_name": [s["file_name"]] }
        else:
            if not sample:
                sample = dict()
                is_external_sample = True
                insert_record["sraAccession"] = s["sraAccession"]
                insert_record["sample_type"] = "isasample"
                insert_record["status"] = "accepted"
                insert_record["biosampleAccession"] = s["biosampleAccession"]
                insert_record["is_external"] = "1"
                insert_record["TAXON_ID"] = s["TAXON_ID"]
                insert_record["profile_id"] = profile_id    

            sample["name"] = s["biosampleAccession"]


        sample.pop("created_by", None)
        sample.pop("time_created", None)
        sample.pop("date_created", None)
        sample.pop("status", None) 
        sample.pop("profile_id", None)
        sample.pop("sample_type", None)
        sample.pop("TAXON_ID", None)
        sample.pop("biosampleAccession", None)
        sample.pop("is_external", None)
        sample["date_modified"] = dt 
        sample["deleted"] = get_not_deleted_flag()           
        sample["updated_by"] = uid

        #sample["checklist_id"] = request.session["checklist_id"]

            
        for key, value in s.items():
            header = key
            header = header.replace(" (optional)", "", -1)
            upper_key = header.upper()
            if upper_key in column_name_mapping:
                sample[column_name_mapping[upper_key]] = value

        condition = {"profile_id": profile_id}
        if "biosampleAccession" in s:
            condition["biosampleAccession"] = s["biosampleAccession"]
            sample = Sample().get_collection_handle().find_one_and_update(condition,
                                                                    {"$set": sample, "$setOnInsert": insert_record },
                                                                    upsert=True,  return_document=ReturnDocument.AFTER)   


            if is_external_sample:
                sample_accession={"sample_accession":s["sraAccession"], "biosample_accession": s["biosampleAccession"], "sample_id" : str(sample["_id"])}
                submission_external_sample_accession.append(sample_accession)

        else:
            condition["name"] = s["Sample"]
            sample.pop("profile_id", None)
            sample.pop("sample_type", None)

            sample = Sample().get_collection_handle().find_one_and_update(condition,
                                                                    {"$set": sample, "$setOnInsert": insert_record},
                                                                    upsert=True,  return_document=ReturnDocument.AFTER)
 
        sample_id = str(sample["_id"])
       

        for key, value in s.items():
            header = key
            header = header.replace(" (optional)", "", -1)
            upper_key = header.upper()
            if upper_key in column_name_mapping_read:
                attributes[column_name_mapping_read[upper_key]] = value

        #attributes["library_preparation"] = {key: s[key] for key in s.keys() if key.startswith("library_")}
        #attributes["nucleic_acid_sequencing"] = {"sequencing_instrument": s["sequencing_instrument"]}
        attributes["study_samples"] = [sample_id] 

        df["description"] = {"attributes": attributes}
        df["title"] = p["title"]
        # df["date_created"] = dt
        df["profile_id"] = str(p["_id"])
        df["file_type"] = "TODO"
        df["type"] = "RAW DATA FILE"

        df["bucket_name"] = str(request.user.id) + "_" + request.user.username
        # df["bucket_name"] = username

        # create local location
        Path(join(settings.UPLOAD_PATH, username)).mkdir(parents=True, exist_ok=True)
        nserted = None
        f_meta = None
        # check if there are two files or one
        if s["Library layout"] == "SINGLE":
            # create single record
            f_name = s["File name"]
            df["ecs_location"] = uid + "_" + username + "/" + f_name
            # df["ecs_location"] = username + "/" + f_name   #temp-solution
            df["file_name"] = f_name
            file_location = join(settings.UPLOAD_PATH, username, "read", f_name)
            df["file_location"] = file_location
            df["name"] = f_name
            df["file_id"] = "NA"
            df["file_hash"] = s["File checksum"].strip()
            df["deleted"] = get_not_deleted_flag()
            file_changed = True
            datafile = DataFile().get_collection_handle().find_one({"file_location": file_location})
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
            f_meta = {"file_id": file_id, "file_name": f_name, "status": "pending", "checklist_id": request.session["checklist_id"]}
            # Sample(profile_id=profile_id).get_collection_handle().update_one({"_id": ObjectId(sample_id)}, {"$addToSet": {"read": f_meta}})
        else:
            file_id1 = None
            file_id2 = None
            # create record for left
            tmp_pairing = dict()
            file_names = s["File name"].split(",")
            f_name = file_names[0].strip()
            df["file_name"] = f_name
            df["ecs_location"] = uid + "_" + username + "/" + f_name
            # df["ecs_location"] = username + "/" + f_name   #temp-solution
            file_location = join(settings.UPLOAD_PATH, username, "read", f_name)
            df["file_location"] = file_location
            df["name"] = f_name
            df["file_id"] = "NA"
            df["file_hash"] = s["File checksum"].split(",")[0].strip()
            df["deleted"] = get_not_deleted_flag()
            file_changed = True
            datafile = DataFile().get_collection_handle().find_one({"file_location": file_location})
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
            file_id1 = file_id

            # create record for right
            tmp_pairing["_id"] = file_id
            # bundle_meta.append(f_meta)
            # df.pop("_id")
            f_name = file_names[1].strip()
            df["file_name"] = f_name
            df["ecs_location"] = uid + "_" + username + "/" + f_name
            # df["ecs_location"] = request.user.username + "/" + f_name
            file_location = join(settings.UPLOAD_PATH, username, "read", f_name)
            df["file_location"] = file_location
            df["name"] = f_name
            df["file_id"] = "NA"
            df["file_hash"] = s["File checksum"].split(",")[1].strip()
            df["deleted"] = get_not_deleted_flag()
            file_changed = True
            datafile = DataFile().get_collection_handle().find_one({"file_location": file_location})
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

            file_id2 = file_id
            f_meta = {"file_id": f"{file_id1},{file_id2}", "file_name": s["File name"], "status": "pending", "checklist_id": request.session["checklist_id"]}
            tmp_pairing["_id2"] = file_id
            pairing.append(tmp_pairing)
            # Sample(profile_id=profile_id).get_collection_handle().update_one({"_id": ObjectId(sample_id)}, {"$addToSet": {"read": f_meta }} )

        is_found = False
        for read in sample.get("read", []):
            if set(read["file_name"].split(",")) == set(f_meta["file_name"].split(",")):
                is_found = True
                break
        if not is_found:
            Sample(profile_id=profile_id).get_collection_handle().update_one({"_id": ObjectId(sample_id)}, {"$addToSet": {"read": f_meta }} )


    # attributes["datafiles_pairing"] = pairing

    # read_files = [x["file_location"] for x in bundle_meta]

    # if sub and sub["accessions"]:
    #    return HttpResponse(content="", status=400)

    if not sub:
        sub = dict()
        sub["date_created"] = dt
        sub["repository"] = "ena"
        sub["accessions"] = dict()
        sub["profile_id"] = profile_id

    sub["complete"] = "false"
    sub["user_id"] = uid
    # sub["bundle_meta"] = existing_bundle_meta
    # sub["bundle"] = existing_bundle
    sub["manifest_submission"] = 1
    sub["deleted"] = get_not_deleted_flag()
    sub["project_release_date"] = project_release_date
 
    # make description records and submissions record
    # dr = Description().create_description(attributes=attributes, profile_id=profile_id, component='datafile',
    #                                      name=profile_name)
    # sub["description_token"] = dr["_id"]

    if "_id" in sub:
        Submission().get_collection_handle().update_one({"_id": sub["_id"]}, {"$set": sub})
        sub_id = sub["_id"]
    else:
        sub_id = Submission().get_collection_handle().insert_one(sub).inserted_id

    if submission_external_sample_accession:
        Submission().get_collection_handle().update_one({"_id": sub["_id"]},{"$addToSet" : {"accessions.sample":{"$each": submission_external_sample_accession}}})

    for f in datafile_list:
        tx.make_transfer_record(file_id=str(f), submission_id=str(sub_id))

    table_data = ena_read.generate_read_record(profile_id=profile_id, checklist_id=request.session["checklist_id"])
    result = {"table_data": table_data, "component": "read"}
    return JsonResponse(status=200, data=result)




@login_required()
def get_manifest_submission_list(request):
    profile_id = request.session["profile_id"]
    docs = Submission().get_collection_handle().find({"$and": [
        {"profile_id": profile_id},
        {"manifest_submission": {"$exists": True}},
        {"manifest_submission": {"$eq": 1}}
    ]})
    output = list(docs)
    out = json_util.dumps(output)
    return HttpResponse(out)

@login_required()
def init_manifest_submission(request):
    submission_id = request.POST["submission_id"]
    submission_repo = "ena"
    

    context = dict(status=True, message='')

    if not submission_id:
        context = dict(status=False, message='Submission identifier not found!')
        return context

    collection_handle = ghlper.get_submission_queue_handle()
    doc = collection_handle.find_one({"submission_id": submission_id})

    if not doc:  # submission not in queue, add to queue
        fields = dict(
            submission_id=submission_id,
            date_modified=helpers.get_datetime(),
            repository=submission_repo,
            processing_status='pending'
        )

        collection_handle.insert(fields)
        context['message'] = 'Submission has been added to the processing queue. Status update will be provided.'
    else:
        context['message'] = 'Submission is already in the processing queue.'

    ghlper.update_submission_status(status='info', message=context['message'], submission_id=submission_id)
    ghlper.logging_info(context['message'], submission_id)
    return HttpResponse()

@login_required()
def get_manifest_submission_list(request):
    profile_id = request.session["profile_id"]
    docs = Submission().get_collection_handle().find({"$and": [
        {"profile_id": profile_id},
        {"manifest_submission": {"$exists": True}},
        {"manifest_submission": {"$eq": 1}}
    ]})
    output = list(docs)
    out = json_util.dumps(output)
    return HttpResponse(out)

@login_required()
def get_submission_status(request):
    """
    function returns the status of a submission record
    :param request:
    :return:
    """

    context = dict()
    submission_ids = json.loads(request.POST.get("submission_ids", "[]"))
    submission_ids = [ObjectId(x) for x in submission_ids]

    # get completed submissions
    submission_records = Submission().get_collection_handle().find(
        {"_id": {"$in": submission_ids}},
        {'_id': 1, 'complete': 1, 'transcript': 1, 'manifest_submission': 1})

    for rec in submission_records:

        record_id = str(rec['_id'])
        new_data = dict(record_id=record_id)
        if rec.get("manifest_submission", 0):
            new_data["manifest_submission"] = 1
        context[new_data["record_id"]] = new_data

        new_data["complete"] = str(rec.get("complete", False)).lower()

        # get any transcript to provide a clearer picture
        status = rec.get("transcript", dict()).get('status', dict())
        new_data['transcript_status'] = status.get('type', str())  # status type is: 'info', 'error', or 'success'
        new_data['transcript_message'] = status.get('message', str())

        # completed submissions
        if new_data["complete"] == 'true':  # this is where we part ways
            continue

        # processing or queued for processing
        submission_queue_handle = ghlper.get_submission_queue_handle()

        if submission_queue_handle.find_one({"submission_id": record_id}):
            new_data["complete"] = "processing"
            continue

        # not submitted, not in processing queue, must be unsubmitted or in error state
        # we turn to transcript for answer: reason why recording status in transcript is important!
        # see: update_submission_status() in generic_helper.py
        new_data["complete"] = "error" if new_data['transcript_status'] == 'error' else "pending"

    return HttpResponse(jsonpickle.encode(context), content_type='application/json')

@login_required()
def get_read_accessions(request, sample_accession): 
    samples = Sample().get_all_records_columns(filter_by={"sraAccession": sample_accession}, projection={"profile_id":1, "read":1})
    run_accessions = []
    experiment_accessions = []
    if samples:
        sample = samples[0]
        submission = Submission().get_all_records_columns(filter_by={"profile_id": sample["profile_id"]}, projection={"accessions":1})
        for read in sample.get("read", []):
            file_id_str = read.get("file_id", str())
            file_ids = file_id_str.split(",")
            if file_ids:
                if read.get("status", "pending") == "accepted":
                        for accession in submission[0].get("accessions", {}).get("run", []):
                            if set(accession.get("datafiles",[])) == set(file_ids):
                                run_accessions.append(accession.get("accession", str()))
                                alias = accession.get("alias", str())
                                break
                        for accession in submission[0].get("accessions", {}).get("experiment", []):
                            if accession.get("alias",[]) == alias:
                                experiment_accessions.append(accession.get("accession", str()))
                                break       
    result = dict(run_accessions=run_accessions, experiment_accessions=experiment_accessions)                                                     
    return JsonResponse(status=200,  data=result)


@web_page_access_checker
@login_required
def copo_reads(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    checklists = EnaChecklist().get_sample_checklists_no_fields()
    return render(request, 'copo/copo_read.html', {'profile_id': profile_id, 'profile': profile, 'checklists': checklists})


@login_required
def download_initial_read_manifest(request, profile_id):
    request.session["profile_id"] = profile_id
    samples = Sample().get_all_records_columns(filter_by={"profile_id": profile_id}, projection={"_id":0, "biosampleAccession":1, "TAXON_ID":1, "SPECIMEN_ID":1})
    checklist = EnaChecklist().get_collection_handle().find_one({"primary_id": "read"})
    bytesstring = BytesIO()
    write_manifest(checklist=checklist, samples=samples, for_dtol=True, file_path=bytesstring)
    response = HttpResponse(bytesstring.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Disposition'] = f"attachment; filename=read_manifest_{profile_id}.xlsx"
    return response
