from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from common.s3.s3Connection import S3Connection
from django.shortcuts import render
import jsonpickle
from django.http import HttpResponse
from .utils.copo_single_cell import generate_singlecell_record
import json
from .utils.da import SinglecellSchemas
import inspect
from common.validators.validator import Validator
from common.utils.helpers import get_datetime, get_not_deleted_flag, notify_singlecell_status
from .utils.SingleCellSchemasHandler import SinglecellschemasSpreadsheet
from common.s3.s3Connection import S3Connection as s3
from common.utils.logger import Logger

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
        l.log("Dtol manifest loaded")
        if singlecell.validate():
            l.log("About to collect Dtol manifest")
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
            singlecell.collect()
            return HttpResponse()
        return HttpResponse(status=400)
    return HttpResponse(status=400)



@login_required()
def save_singlecell_records(request):
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
        #Path(join(settings.UPLOAD_PATH, username)).mkdir(parents=True, exist_ok=True)
        nserted = None
        f_meta = None
        # check if there are two files or one
        if s["Library layout"] == "SINGLE":
            # create single record
            f_name = s["File name"]
            df["ecs_location"] = uid + "_" + username + "/" + f_name
            # df["ecs_location"] = username + "/" + f_name   #temp-solution
            df["file_name"] = f_name
            file_location = join(settings.LOCAL_UPLOAD_PATH, username, "read", f_name)
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
            file_location = join(settings.LOCAL_UPLOAD_PATH, username, "read", f_name)
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
            file_location = join(settings.LOCAL_UPLOAD_PATH, username, "read", f_name)
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


@login_required
def download_initial_singlecell_manifest(request, profile_id):
    request.session["profile_id"] = profile_id
    samples = Sample().get_all_records_columns(filter_by={"profile_id": profile_id}, projection={"_id":0, "biosampleAccession":1, "TAXON_ID":1, "SPECIMEN_ID":1})
    checklist = EnaChecklist().get_collection_handle().find_one({"primary_id": "read"})
    bytesstring = BytesIO()
    write_manifest(checklist=checklist, samples=samples, for_dtol=True, file_path=bytesstring)
    response = HttpResponse(bytesstring.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Disposition'] = f"attachment; filename=read_manifest_{profile_id}.xlsx"
    return response
