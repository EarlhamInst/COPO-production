__author__ = 'felix.shaw@tgac.ac.uk - 20/01/2016'

import datetime
import importlib
import sys
import requests
import dateutil.parser as parser
from bson.errors import InvalidId
from django.conf import settings
from django.http import HttpResponse
import json
from bson.errors import InvalidId
from src.apps.api.utils import generate_csv_response, generate_wrapper_response, get_return_template, extract_to_template, finish_request, sort_dict_list_by_priority
from src.apps.api.views.mapping import get_mapped_field_by_standard
from common.dal.copo_da import APIValidationReport
from common.dal.sample_da import Sample, Source
from common.dal.submission_da import Submission
from common.dal.profile_da import Profile
from common.schema_versions.lookup import dtol_lookups as lookup
from common.lookup.lookup import API_ERRORS
from rest_framework.views import APIView
from rest_framework.response import Response
from common.utils.helpers import get_excluded_associated_projects
from common.utils.logger import Logger
from src.apps.copo_dtol_upload.utils.Dtol_Spreadsheet import DtolSpreadsheet
from src.apps.copo_dtol_upload.utils.da import ValidationQueue
from io import BytesIO
import bson.json_util as jsonb
import common.schemas.utils.data_utils as d_utils

def get(request, id):
    """
    Method to handle a request for a single sample object from the API
    :param request: a Django HTTPRequest object
    :param id: the id of the Sample object (can be string or ObjectID)
    :return: an HttpResponse object embedded with the completed return template for Sample
    """

    # farm request to appropriate sample type handler
    try:
        ss = Sample().get_record(id)
        source = ss['source']
        sample = ss['sample']

        # get template for return type
        t_source = get_return_template('SOURCE')
        t_sample = get_return_template('SAMPLE')

        # extract fields for both source and sample
        tmp_source = extract_to_template(object=source, template=t_source)
        tmp_sample = extract_to_template(object=sample, template=t_sample)
        tmp_sample['source'] = tmp_source

        out_list = []
        out_list.append(tmp_sample)

        return finish_request(out_list)
    except TypeError as e:
        print(e)
        return finish_request(error=API_ERRORS['NOT_FOUND'])
    except InvalidId as e:
        print(e)
        return finish_request(error=API_ERRORS['INVALID_PARAMETER'])
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


def format_date(input_date):
    try:
        # Check if 'input_date' is an empty string
        if input_date == '':
            return ''

        # Convert input_date from string to datetime if it's not already
        if isinstance(input_date, str):
            input_date = datetime.datetime.fromisoformat(input_date)

        # Format of date fields exported to STS
        return input_date.replace(tzinfo=datetime.timezone.utc).isoformat()
    except Exception as e:
        Logger().exception(f'An error occurred while formatting the date: {e}')
        return None


def filter_for_API(sample_list, add_all_fields=False):
    # add field(s) here which should be time formatted
    time_fields = ["time_created", "time_updated", "last_bioimage_submitted"]
    profile_type = None
    profile_title_map = dict()

    if len(sample_list) > 0:
        profile_type = sample_list[0].get("tol_project", "dtol").lower()
    if not profile_type:
        profile_type = "dtol"
    export = d_utils.get_export_fields(component='sample', project=profile_type)
    out = list()
    rights_to_lookup = list()
    notices = dict()
    for s in sample_list:
        # ERGA samples may be subject to traditional knowledge labels
        if s.get("tol_project", "") in ["erga", "ERGA"]:
            # check for rights applicable
            if s.get("ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE") in ["Y", "y"]:
                # if applicable save project id
                rights_to_lookup.append(s.get("ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID", ""))
    # now we have a list of project ids which pertain to a protected sample, so unique to get only one copy of each project
    rights_to_lookup = list(set(rights_to_lookup))
    for r in rights_to_lookup:
        notices[r] = query_local_contexts_hub(r)

    for s in sample_list:
        embargoed = False
        if isinstance(s, InvalidId):
            break
        species_list = s.pop("species_list", "")
        if species_list:
            s = {**s, **species_list[0]}
        s_out = dict()
        
        # Always export COPO ID i.e. sample ID
        s_out["copo_id"] = str(s.get("_id", ""))

        # Create a map of profile ID and profile title to avoid multiple queries
        profile_id = s.get("profile_id", "")

        if profile_id:
            if profile_id not in profile_title_map:
                profile = Profile().get_record(profile_id)
                if profile:
                    profile_title_map[profile_id] = profile.get("title", "")
        
            # Export profile title        
            profile_title = profile_title_map.get(profile_id, "")
            s_out["copo_profile_title"] = profile_title

        # handle corner cases
        for k, v in s.items():
            if k == "SPECIMEN_ID_RISK":
                # this to account for old manifests before name change
                k = "SPECIMEN_IDENTITY_RISK"
            # check if there is a traditional right embargo
            if k == "ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE":
                if v in ["N", "n", False, ""] or s.get("tol_project") not in ["ERGA", "erga"]:
                    # we need not do anything, since no rights apply
                    s_out[k] = v
                else:
                    # ToDo - check local context hub
                    project_id = s.get("ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID", "")
                    if not project_id:
                        # rights are applicable, but no contexts id provided, therefore embargo
                        s_out = {"status": "embargoed"}
                        out.append(s_out)
                        embargoed = True
                        break
                    else:
                        q = notices[project_id]
                        if q.get("project_privacy", "").lower() == "public":
                            s_out[k] = v
                        else:
                            s_out = {"status": "embargoed"}
                            out.append(s_out)
                            embargoed = True
                            break
           
            # check if field is listed to be exported to STS
            # print(k)
            if k in export:
                if k in time_fields:
                    s_out[k] = format_date(v)
                else:
                    s_out[k] = v
            if k == "changelog":
                s_out["latest_update"] = format_date(v[-1].get("date"))

        # create list of fields and defaults for fields which are not present in earlier versions of the manifest
        defaults_list = {
            "MIXED_SAMPLE_RISK": "NOT_PROVIDED",
            "BARCODING_STATUS": "DNA_BARCODE_EXEMPT"
        }
        # iterate through fields to be exported and add them in blank if not present in the sample object
        if not embargoed:
            if add_all_fields:
                for k in export:
                    if k not in s_out.keys():
                        if k in defaults_list.keys():
                            s_out[k] = defaults_list[k]
                        else:
                            s_out[k] = ""
                out.append(s_out)
            else:
                filtered_s_out = {key: value for key, value in s_out.items() if key in export}
                out.append(filtered_s_out)
    
    # Sort data with uppercase keys preserved at the top, 
    # followed by sorted lowercase camel case keys
    out = sort_dict_list_by_priority(out)
    return out


def get_manifests(request):
    # get all manifests of dtol samples
    manifest_ids = Sample().get_manifests()
    return finish_request(manifest_ids)

def get_manifests_by_sequencing_centre(request):
    sequencing_centre = request.GET.get('sequencing_centre', str())
    manifest_ids = Sample().get_by_sequencing_centre(sequencing_centre, isQueryByManifestLevel=True)
    return finish_request(manifest_ids)

def get_current_manifest_version(request):
    manifest_type = request.GET.get('manifest_type', str()).upper()
    out = list()

    if manifest_type:
        manifest_version = {manifest_type: settings.MANIFEST_VERSION.get(manifest_type, str())}
        out.append(manifest_version)
    else:
        manifest_versions = {i.upper(): settings.MANIFEST_VERSION.get(i.upper(), str())for i in lookup.TOL_PROFILE_TYPES}
        out.append(manifest_versions)

    return HttpResponse(json.dumps(out, indent=2))

def query_local_contexts_hub(project_id):
    lch_url = "https://localcontextshub.org/api/v1/projects/" + project_id
    resp = requests.get(lch_url)
    j_resp = json.loads(resp.content)
    print(j_resp)
    return j_resp


def get_all_manifest_between_dates(request, d_from, d_to):
    # get all manifests between d_from and d_to
    # dates must be ISO 8601 formatted
    d_from = parser.parse(d_from)
    d_to = parser.parse(d_to)
    if d_from > d_to:
        return HttpResponse(status=400, content="'from' must be earlier than'to'")
    manifest_ids = Sample().get_manifests_by_date(d_from, d_to)
    return finish_request(manifest_ids)


def get_project_manifests_between_dates(request, project, d_from, d_to):
    # get project manifests between d_from and d_to
    # dates must be ISO 8601 formatted
    d_from = parser.parse(d_from)
    d_to = parser.parse(d_to)
    if d_from > d_to:
        return HttpResponse(status=400, content="'from' must be earlier than'to'")
    manifest_ids = Sample().get_manifests_by_date_and_project(project, d_from, d_to)
    return finish_request(manifest_ids)

def get_specimens_with_submitted_images(request):
    # Fetch all specimens i.e. sources with submitted 
    # sample images by sample type/project type and
    # between 'from' date and 'to' date if provided
    # Dates must be ISO 8601 formatted
    project = request.GET.get('project', str()).lower()
    
    try:                
        d_from = parser.parse(request.GET.get('d_from', None))
    except TypeError: 
        d_from = None

    try:                
        d_to = parser.parse(request.GET.get('d_to', None))
    except TypeError: 
        d_to = None

    if d_from and d_to is None:
        return HttpResponse(status=400, content=f'\'to date\' is required when \'from date\' is entered')

    if d_from is None and d_to:
        return HttpResponse(status=400, content=f'\'from date\' is required when \'to date\' is entered')

    if d_from and d_to and d_from > d_to:
        return HttpResponse(status=400, content=f'\'from date\' must be earlier than \'to date\'')
    
    specimens = Source().get_specimens_with_submitted_images(project, d_from, d_to)
    
    out = list()
    if specimens:
        out = filter_for_API(specimens, add_all_fields=False)
        # Remove 'copo_id' from the output
        for item in out:
            item.pop('copo_id', None) 
    return finish_request(out)

def get_all_samples_between_dates(request, d_from, d_to):
    # get all samples between d_from and d_to
    # dates must be ISO 8601 formatted
    d_from = parser.parse(d_from)
    d_to = parser.parse(d_to)

    if d_from > d_to:
        return HttpResponse(status=400, content="'from date' must be earlier than 'to date'")

    samples = Sample().get_samples_by_date(d_from, d_to)
    out = list()
    
    if samples:
        out = filter_for_API(samples, add_all_fields=True)
    return finish_request(out)

def get_samples_in_manifest(request, manifest_id):
    # get all samples tagged with the given manifest_id
    sample_list = Sample().get_by_manifest_id(manifest_id)
    out = filter_for_API(sample_list, add_all_fields=True)
    return finish_request(out)


def get_sample_status_for_manifest(request, manifest_id):
    sample_list = Sample().get_status_by_manifest_id(manifest_id)
    out = filter_for_API(sample_list, add_all_fields=False)
    return finish_request(out)


def get_by_biosampleAccessions(request, biosampleAccessions):
    # Get sample associated with given biosampleAccession
    # This will return nothing if ENA submission has not yet occurred
    accessions = biosampleAccessions.split(",")
    # strip white space
    accessions = list(map(lambda x: x.strip(), accessions))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    accessions[:] = [x for x in accessions if x]
    sample = Sample().get_by_biosampleAccessions(accessions)
    out = list()
    if sample:
        out = filter_for_API(sample)
    return finish_request(out)


def get_num_dtol_samples(request):
    samples = Sample().get_all_dtol_samples()
    number = len(samples)
    return HttpResponse(str(number))


def get_project_samples(request, project):
    projectlist = project.split(",")
    projectlist = list(map(lambda x: x.strip().lower(), projectlist))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    projectlist[:] = [x for x in projectlist if x]
    samples = Sample().get_project_samples(projectlist)
    out = list()
    if samples:
        out = filter_for_API(samples)
    return finish_request(out)

def get_samples_by_sequencing_centre(request):
    sequencing_centre = request.GET.get('sequencing_centre', str())
    samples = Sample().get_by_sequencing_centre(sequencing_centre, isQueryByManifestLevel=False)
    
    out = list()
    if samples:
        out = filter_for_API(samples)
    return finish_request(out)

def get_updatable_fields_by_project(request):
    project = request.GET.get('project', str()).lower()
    non_compliant_fields = d_utils.get_non_compliant_fields(component='sample', project=project)

    # Return non-compliant fields i.e. fields that user can update
    non_compliant_fields.sort()
    return finish_request(non_compliant_fields)

def get_fields_by_manifest_version(request):
    return_type = request.GET.get('return_type', 'json').lower()
    standard = request.GET.get('standard', 'tol').lower()
    project = request.GET.get('project', str())
    manifest_version = request.GET.get('manifest_version', str())

    manifest_versions = Sample().get_available_manifest_versions(project)
    template = None

    if manifest_version in manifest_versions:
        # Get fields based on standard
        mapped_field_dict = get_mapped_field_by_standard(standard=standard, project=project, manifest_version=manifest_version)
        template = list(mapped_field_dict.values())    
    else:
        # Show error if manifest version does not exist
        error= f'No fields exist for the manifest version, {manifest_version}. Available manifest versions are {d_utils.join_list_with_and_as_last_entry(manifest_versions)}.'
        return HttpResponse(status=400, content=error)
   
    if return_type not in ['json', 'csv']:
         # Show error if return type is not 'json' or 'csv'
        error = 'Invalid return type provided. Please provide either "json" or "csv".'
        return HttpResponse(status=400, content=error)
    
    if  return_type == 'csv':
        return generate_csv_response(standard, template)
    
    # Generate JSON response
    output = generate_wrapper_response(template=template)
    output = jsonb.dumps([output])

    return  HttpResponse(output, content_type='application/json')

def get_project_samples_by_associated_project_type(request, value):
    excluded_associated_projects = get_excluded_associated_projects()
    associated_project_type = value.upper().strip()

    if associated_project_type in excluded_associated_projects:
        return HttpResponse(status=400, content=f'Invalid input! {value.strip()} is not allowed.')
    
    samples = Sample().get_project_samples_by_associated_project_type(associated_project_type)
    out = list()
    if samples:
        out = filter_for_API(samples, add_all_fields=True)
    return finish_request(out)


def get_by_copo_ids(request, copo_ids):
    # get sample by COPO id if known
    ids = copo_ids.split(",")
    # strip white space
    ids = list(map(lambda x: x.strip(), ids))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    ids[:] = [x for x in ids if x]
    samples = Sample().get_records(ids)
    out = list()
    if samples:
        if not type(samples) == InvalidId:
            out = filter_for_API(samples, add_all_fields=True)
        else:
            return HttpResponse(status=400, content="InvalidId found in request")
    return finish_request(out)


def get_by_field(request, dtol_field, value):
    # generic method to return all samples where given "dtol_field" matches "value"
    vals = value.split(",")
    # strip white space
    vals = list(map(lambda x: x.strip(), vals))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    vals[:] = [x for x in vals if x]
    out = list()
    sample_list = Sample().get_by_field(dtol_field, vals)
    if sample_list:
        out = filter_for_API(sample_list, add_all_fields=True)
    return finish_request(out)

def get_samples_by_taxon_id(request, taxon_ids):
    out = list()
    # Get sample by taxon id
    sample_taxon_ids = taxon_ids.split(',')
    # Strip white space
    sample_taxon_ids = list(map(lambda x: x.strip(), sample_taxon_ids))
    # Remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    sample_taxon_ids[:] = [x for x in sample_taxon_ids if x]
    sample_taxon_ids = list(set(sample_taxon_ids))  # Remove duplicates
    
    samples = Sample().get_samples_by_taxon_id(sample_taxon_ids)
    if samples:
        out = filter_for_API(samples, add_all_fields=True)
    return finish_request(out)

def get_study_from_sample_accession(request, accessions):
    ids = accessions.split(",")
    # strip white space
    ids = list(map(lambda x: x.strip(), ids))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    ids[:] = [x for x in ids if x]
    # try to get sample from either sra or biosample id
    samples = Sample().get_by_field(dtol_field="sraAccession", value=ids)
    if not samples:
        samples = Sample().get_by_field(dtol_field="biosampleAccession", value=ids)
        if not samples:
            return finish_request([])
    # if record found, find associated submission record
    out = []
    for s in samples:
        sub = Submission().get_submission_from_sample_id(str(s["_id"]))
        d = sub[0]["accessions"]["study_accessions"]
        d["sample_biosampleId"] = s["biosampleAccession"]
        out.append(d)
    return finish_request(out)


def get_samples_from_study_accessions(request, accessions):
    ids = accessions.split(",")
    # strip white space
    ids = list(map(lambda x: x.strip(), ids))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    ids[:] = [x for x in ids if x]
    subs = Submission().get_dtol_samples_in_biostudy(ids)
    to_finish = list()
    sample_count = 0
    for s in subs:
        out = dict()
        out["study_accessions"] = s["accessions"]["study_accessions"]
        out["sample_accessions"] = []
        for sa in s["accessions"]["sample_accessions"]:
            sample_count += 1
            smpl_accessions = s["accessions"]["sample_accessions"][sa]
            smpl_accessions["copo_sample_id"] = sa
            out["sample_accessions"].append(smpl_accessions)
        to_finish.append(out)
    return finish_request(to_finish, num_found=sample_count)


def query_local_contexts_hub(project_id):
    lch_url = "https://localcontextshub.org/api/v1/projects/" + project_id
    resp = requests.get(lch_url)
    j_resp = json.loads(resp.content)
    print(j_resp)
    return j_resp


def get_all(request):
    """
    Method to handle a request for all
    :param request: a Django HttpRequest object
    :return: A dictionary containing all samples in COPO
    """

    out_list = []

    # get sample and source objects
    try:
        sample_list = Sample().get_samples_across_profiles()
    except TypeError as e:
        # print(e)
        return finish_request(error=API_ERRORS['NOT_FOUND'])
    except InvalidId as e:
        # print(e)
        return finish_request(error=API_ERRORS['INVALID_PARAMETER'])
    except:
        # print("Unexpected error:", sys.exc_info()[0])
        raise

    for s in sample_list:
        # get template for return type
        t_source = get_return_template('SOURCE')
        t_sample = get_return_template('SAMPLE')

        # get source for sample
        source = Source().GET(s['source_id'])
        # extract fields for both source and sample
        tmp_source = extract_to_template(object=source, template=t_source)
        tmp_sample = extract_to_template(object=s, template=t_sample)
        tmp_sample['source'] = tmp_source

        out_list.append(tmp_sample)

    return finish_request(out_list)


class APIValidateManifest(APIView):

    def sample_spreadsheet(request, report_id=""):
        file = request.FILES["file"]
        name = file.name
        if "profile_id" in request.POST:
            p_id = request.POST["profile_id"]
        else:
            p_id = request.session["profile_id"]
        dtol = DtolSpreadsheet(file=file, p_id=p_id)
        if name.endswith("xlsx") or name.endswith("xls"):
            fmt = 'xls'
        elif name.endswith("csv"):
            fmt = 'csv'
        else:
            msg = "Unrecognised file format for spreadsheet. " \
                "File format should be either <strong>.xls</strong>, <strong>.xlsx</strong> or <strong>.csv</strong>."
            Logger().error("Unrecognised file format for sample spreadsheet")
            return HttpResponse(status=400, content=msg)

        if dtol.loadManifest(m_format=fmt):
            bytesstring = BytesIO()
            dtol.data.to_pickle(bytesstring)
             
            if "profile_id" in request.POST:
                p_id = request.POST["profile_id"]
            else:
                p_id = request.session["profile_id"]
            r = {"$set": {"manifest_data": bytesstring.getvalue(), "profile_id": p_id, "schema_validation_status": "pending",
                        "taxon_validation_status": "pending", "err_msg": [],
                        "time_added": datetime.utcnow(),
                        "file_name": name,
                        "isupdate": False,
                        "report_id": report_id
                        }}
            ValidationQueue().get_collection_handle().update_one({"profile_id": p_id}, r, upsert=True)

        return HttpResponse()


    def post(self, request):
        id = APIValidationReport().get_collection_handle().insert_one(
            {"profile_id": request.POST["profile_id"], "status": "pending", "content": "",
             "submitted": datetime.datetime.utcnow(), "user_id": request.user.id})
        self.sample_spreadsheet(request, report_id=id.inserted_id)

        out = {"validation_report_id": str(id.inserted_id)}
        return Response(out)


class APIGetManifestValidationReport(APIView):
    def post(self, request):
        uid = request.user.id
        validation_id = request.POST.get("validation_report_id")
        v_record = APIValidationReport().get_record(validation_id)
        profile_record = Profile().get_record(v_record["profile_id"])
        if profile_record["user_id"] == uid:
            out = {"status": v_record["status"], "content": v_record["content"], "submitted": v_record["submitted"]}
        else:
            out = {"content": "User not permitted to view resource"}
        return Response(out)


class APIGetUserValidations(APIView):
    def post(self, request):
        uid = request.user.id
        v_records = APIValidationReport().get_collection_handle().find({"user_id": uid}, {"_id": 0})
        return Response(list(v_records))
