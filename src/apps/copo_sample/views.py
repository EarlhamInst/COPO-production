from django.shortcuts import render
from common.dal.profile_da import Profile
from django.contrib.auth.decorators import login_required
from common.utils.helpers import get_group_membership_asString
from src.apps.copo_core.views import web_page_access_checker
from common.dal.copo_da import  EnaChecklist
from common.dal.sample_da import Sample
from django.http import HttpResponse, JsonResponse
from common.ena_utils.EnaChecklistHandler import EnaCheckListSpreadsheet, write_manifest
from common.ena_utils import generic_helper as ghlper
from common.utils.helpers import get_datetime, get_not_deleted_flag,map_to_dict
from .utils import copo_sample  
from io import BytesIO
from common.utils.logger import Logger
from common.ena_utils import generic_helper as ghlper
import json
import subprocess
from pymongo import ReturnDocument

l = Logger()

@web_page_access_checker
@login_required
def copo_samples(request, profile_id, ui_component):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    groups = get_group_membership_asString()
    return render(request, 'copo/copo_sample.html', {'profile_id': profile_id, 'profile': profile, 'groups': groups, 'ui_component': ui_component})

@web_page_access_checker
@login_required
def copo_general_samples(request, profile_id, ui_component):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    checklists = EnaChecklist().get_general_sample_checklists_no_fields()
    profile_checklist_ids = Sample().get_distinct_checklist(profile_id)
    if not profile_checklist_ids:
        profile_checklist_ids = []
    return render(request, 'copo/copo_general_sample.html', {'profile_id': profile_id, 'profile': profile, 'checklists': checklists, "profile_checklist_ids": profile_checklist_ids, 'ui_component': ui_component})

@web_page_access_checker
@login_required()
def sample_manifest_validate(request, profile_id):
    request.session["profile_id"] = profile_id
    checklist_id = request.GET.get("checklist_id")
    data = {"profile_id": profile_id}
    if checklist_id:
        checklist = EnaChecklist().execute_query({"primary_id": checklist_id})
        if checklist:
            data["checklist_id"] = checklist_id
            data["checklist_name"] = checklist[0]["name"]
            
    return render(request, "copo/sample_manifest_validate.html", data)

@web_page_access_checker
@login_required()
def parse_sample_spreadsheet(request):
    profile_id = request.session["profile_id"]
    #ghlper.notify_read_status(data={"profile_id": profile_id}, msg="Loading", action="info",
    #                        max_ellipsis_length=3, html_id="sample_info")
    
    
    # method called by rest
    file = request.FILES["file"]
    checklist_id = request.POST["checklist_id"]
    name = file.name
    
    required_validators = []
    """
    required = dict(globals().items())["required_validators"]
    for element_name in dir(required):
        element = getattr(required, element_name)
        if inspect.isclass(element) and issubclass(element, Validator) and not element.__name__ == "Validator":
            required_validators.append(element)
    """

    ena = EnaCheckListSpreadsheet(file=file, checklist_id=checklist_id, component="sample", validators=required_validators, with_read=False)
    #s3obj = s3()
    if name.endswith("xlsx") or name.endswith("xls"):
        fmt = 'xls'
    else:
        return HttpResponse(status=415, content="Please make sure your manifest is in xlxs format")

    if ena.loadManifest(fmt):
        l.log("Sample manifest loaded")
        if ena.validate():
            ghlper.notify_read_status(data={"profile_id": profile_id}, msg="Spreadsheet is valid. Please click <b>Finish</b> to complete the upload.", 
                action="success", html_id="sample_info")
            ena.collect()
            return HttpResponse()
        else:
            return HttpResponse(status=400)
    else:
        return HttpResponse(status=400)

@web_page_access_checker
@login_required()
def save_sample_records(request):
    # create mongo sample objects from info parsed from manifest and saved to session variable
    sample_data = request.session.get("sample_data")
    profile_id = request.session["profile_id"]
    #profile_name = Profile().get_name(profile_id)
    uid = str(request.user.id)
    checklist = EnaChecklist().get_collection_handle().find_one({"primary_id": request.session["checklist_id"]})
    column_name_mapping = { field["label"].upper() : key  for key, field in checklist["fields"].items()}
    
    #add taxon_id column
    column_name_mapping["TAXON_ID"] = "taxon_id"
    column_name_mapping["SCIENTIFIC_NAME"] = "scientific_name"
    dt = get_datetime()

    organism_map = dict()
    source_map = dict()
    sample_map = dict()
 
    sample_names = []
    for line in range(1, len(sample_data)):
        # for each row in the manifest
        s = (map_to_dict(sample_data[0], sample_data[line]))
        sample_names.append(s["Sample"])
        samples = Sample().get_collection_handle().find({"name": {"$in": sample_names}, "profile_id": profile_id})
        sample_map = {sample["name"]: sample for sample in samples}

    for line in range(1, len(sample_data)):
        # for each row in the manifest

        s = (map_to_dict(sample_data[0], sample_data[line]))

        sample = sample_map.get(s["Sample"], None)
        if sample and sample.get("status","pending") == "sending":
            ghlper.notify_read_status(data={"profile_id": profile_id}, msg=f'sample ({sample["name"]} ) is being submitted, please update it later', action="error",
                             html_id="sample_info")
            return HttpResponse(status=400)
 
        insert_record = {}
        if not sample:
            sample = dict()
            insert_record["created_by"] = uid
            insert_record["time_created"] = get_datetime()
            insert_record["date_created"] = dt
            insert_record["profile_id"] = profile_id
            insert_record["sample_type"] = "isasample"
            insert_record["status"] = "pending"
            insert_record["deleted"] = get_not_deleted_flag()
        else:
            if sample["status"] in ["accepted", "rejected"]:
                sample["status"] = "pending"
            sample.pop("created_by", None)
            sample.pop("time_created", None)
            sample.pop("date_created", None)
            sample.pop("profile_id", None)
            sample.pop("sample_type", None)
        sample["checklist_id"] = request.session["checklist_id"]
        sample["date_modified"] = dt 
        sample["updated_by"] = uid
        sample["error"] = ""

        """
        if not sample or sample.get("organism","") != s["Organism"]:
            source = dict()
            tax_id = organism_map.get(s["Organism"], None)
            source_id = source_map.get(s["Organism"], None)
            if not tax_id:
                curl_cmd = "curl " + \
                        "https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/" + s["Organism"].replace(" ", "%20")
                receipt = subprocess.check_output(curl_cmd, shell=True)
                # ToDo - exit if species not found
                print(receipt)
                taxinfo = json.loads(receipt.decode("utf-8"))
                tax_id = taxinfo[0]["taxId"]
                organism_map[s["Organism"]] = tax_id
                
            sample["taxon_id"] = tax_id
            # create associated sample
        """

        #sample["checklist_id"] = request.session["checklist_id"]
            
        for key, value in s.items():
            header = key
            header = header.replace(" (optional)", "", -1)
            upper_key = header.upper()
            if upper_key in column_name_mapping:
                sample[column_name_mapping[upper_key]] = value

        condition = {"profile_id": profile_id, "deleted": get_not_deleted_flag(), "name": s["Sample"]}

        sample = Sample().get_collection_handle().find_one_and_update(condition,
                                                                {"$set": sample, "$setOnInsert": insert_record},
                                                                upsert=True,  return_document=ReturnDocument.AFTER)       

    table_data = copo_sample.generate_table_records(profile_id=profile_id, checklist_id=request.session["checklist_id"])
    result = {"table_data": table_data, "component": "general_sample"}
    return JsonResponse(status=200, data=result)


@login_required
@web_page_access_checker
def download_manifest(request, profile_id, sample_checklist_id):

    samples = Sample(profile_id=profile_id).execute_query({"checklist_id" : sample_checklist_id, "profile_id": profile_id, 'deleted': get_not_deleted_flag()})
    if not samples:
        return HttpResponse(status=404, content="No record found")
    bytesstring = BytesIO()
    checklist = EnaChecklist().get_checklist(sample_checklist_id, with_read=False)
    write_manifest(checklist=checklist, with_read=False, samples=samples, file_path=bytesstring)
    response = HttpResponse(bytesstring.getvalue(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Disposition'] = f"attachment; filename={sample_checklist_id}_sample_manifest.xlsx"
    return response
