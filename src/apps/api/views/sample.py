import bson.json_util as jsonb
import datetime
import importlib
import json
import requests
import sys

from bson.errors import InvalidId
from django.conf import settings
from django.http import HttpResponse
from dateutil.parser import parse as parse_date
from io import BytesIO
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

import common.schemas.utils.data_utils as d_utils
from common.dal.copo_da import APIValidationReport
from common.dal.profile_da import Profile
from common.dal.sample_da import Sample, Source
from common.dal.submission_da import Submission
from common.lookup.lookup import API_ERRORS
from common.schema_versions.lookup import dtol_lookups as lookup
from common.utils.logger import Logger
from src.apps.api.utils import (
    extract_to_template,
    finish_request,
    generate_csv_response,
    generate_wrapper_response,
    get_return_template,
    sort_dict_list_by_priority,
)
from src.apps.api.views.mapping import get_mapped_field_by_standard
from src.apps.copo_dtol_upload.utils.da import ValidationQueue
from src.apps.copo_dtol_upload.utils.Dtol_Spreadsheet import DtolSpreadsheet

from ..enums import (
    AssociatedProjectEnum,
    SampleFieldsEnum,
    SequencingCentreEnum,
)
from ..utils import validate_date_from_api, validate_project, validate_return_type


def get(request, id):
    '''
    Method to handle a request for a single sample object from the API
    :param request: a Django HTTPRequest object
    :param id: the id of the Sample object (can be string or ObjectID)
    :return: an HttpResponse object embedded with the completed return template for Sample
    '''

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
        print('Unexpected error:', sys.exc_info()[0])
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
    # Add field(s) here that should be datetime formatted
    time_fields = ['time_created', 'time_updated', 'last_bioimage_submitted']
    excluded_export_fields = ['copo_audit_type']
    profile_type = None
    profile_title_map = dict()

    if len(sample_list) > 0:
        profile_type = sample_list[0].get('tol_project', 'dtol').lower()
    if not profile_type:
        profile_type = 'dtol'

    export = d_utils.get_export_fields(component='sample', project=profile_type)
    # Remove fields which are not to be exported
    export = [field for field in export if field not in excluded_export_fields]
    out = list()
    rights_to_lookup = list()
    notices = dict()

    for s in sample_list:
        # ERGA samples may be subject to traditional knowledge labels
        if s.get('tol_project', '') in ['erga', 'ERGA']:
            # check for rights applicable
            if s.get(
                'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE'
            ) in ['Y', 'y']:
                # if applicable save project id
                rights_to_lookup.append(
                    s.get(
                        'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID', ''
                    )
                )
    # now we have a list of project ids which pertain to a protected sample, so unique to get only one copy of each project
    rights_to_lookup = list(set(rights_to_lookup))
    for r in rights_to_lookup:
        notices[r] = query_local_contexts_hub(r)

    for s in sample_list:
        embargoed = False
        if isinstance(s, InvalidId):
            break
        species_list = s.pop('species_list', '')
        if species_list:
            s = {**s, **species_list[0]}
        s_out = dict()

        # Always export COPO ID i.e. sample ID
        s_out['copo_id'] = str(s.get('_id', ''))

        # Create a map of profile ID and profile title to avoid multiple queries
        profile_id = s.get('profile_id', '')

        if profile_id:
            if profile_id not in profile_title_map:
                profile = Profile().get_record(profile_id)
                if profile:
                    profile_title_map[profile_id] = profile.get('title', '')

            # Export profile title
            profile_title = profile_title_map.get(profile_id, '')
            s_out['copo_profile_title'] = profile_title

        # handle corner cases
        for k, v in s.items():
            if k == 'SPECIMEN_ID_RISK':
                # this to account for old manifests before name change
                k = 'SPECIMEN_IDENTITY_RISK'
            # check if there is a traditional right embargo
            if k == 'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE':
                if v in ['N', 'n', False, ''] or s.get('tol_project') not in [
                    'ERGA',
                    'erga',
                ]:
                    # we need not do anything, since no rights apply
                    s_out[k] = v
                else:
                    # ToDo - check local context hub
                    project_id = s.get(
                        'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID', ''
                    )
                    if not project_id:
                        # rights are applicable, but no contexts id provided, therefore embargo
                        s_out = {'status': 'embargoed'}
                        out.append(s_out)
                        embargoed = True
                        break
                    else:
                        q = notices[project_id]
                        if q.get('project_privacy', '').lower() == 'public':
                            s_out[k] = v
                        else:
                            s_out = {'status': 'embargoed'}
                            out.append(s_out)
                            embargoed = True
                            break

            # check if field is listed to be exported to STS
            if k in export:
                if k in time_fields:
                    s_out[k] = format_date(v)
                elif k == 'tol_project':
                    # Ensure that 'tol_project' is always exported as uppercase
                    s_out['tol_project'] = v.upper()
                else:
                    s_out[k] = v

        # create list of fields and defaults for fields which are not present in earlier versions of the manifest
        defaults_list = {
            'MIXED_SAMPLE_RISK': 'NOT_PROVIDED',
            'BARCODING_STATUS': 'DNA_BARCODE_EXEMPT',
        }
        # iterate through fields to be exported and add them in blank if not present in the sample object
        if not embargoed:
            if add_all_fields:
                for k in export:
                    if k not in s_out.keys():
                        if k in defaults_list.keys():
                            s_out[k] = defaults_list[k]
                        else:
                            s_out[k] = ''
                out.append(s_out)
            else:
                filtered_s_out = {
                    key: value for key, value in s_out.items() if key in export
                }
                out.append(filtered_s_out)

    # Sort data with uppercase keys preserved at the top,
    # followed by sorted lowercase camel case keys
    out = sort_dict_list_by_priority(out, export)
    return out


def get_manifests(request):
    # get all manifests of dtol samples
    manifest_ids = Sample().get_manifests()
    return finish_request(manifest_ids)


def get_manifests_by_sequencing_centre(request):
    sequencing_centre = request.GET.get('sequencing_centre', str())

    valid_sequencing_centres = SequencingCentreEnum.values()
    if sequencing_centre not in valid_sequencing_centres:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f"Invalid value for 'sequencing_centre'. Accepted values are: {d_utils.join_with_and(valid_sequencing_centres, conjunction='and')}",
        )

    manifest_ids = Sample().get_by_sequencing_centre(
        sequencing_centre, isQueryByManifestLevel=True
    )
    return finish_request(manifest_ids)


def get_current_manifest_version(request):
    manifest_type = request.GET.get('manifest_type', str()).upper()
    out = list()

    # Validate optional manifest type field
    issues = validate_project(manifest_type, field_name='manifest_type', optional=True)
    if issues:
        return issues

    if manifest_type:
        manifest_version = {
            manifest_type: settings.MANIFEST_VERSION.get(manifest_type, str())
        }
        out.append(manifest_version)
    else:
        manifest_versions = {
            i.upper(): settings.MANIFEST_VERSION.get(i.upper(), str())
            for i in lookup.TOL_PROFILE_TYPES
        }
        out.append(manifest_versions)

    return HttpResponse(json.dumps(out, indent=2))


def query_local_contexts_hub(project_id):
    lch_url = 'https://localcontextshub.org/api/v1/projects/' + project_id
    resp = requests.get(lch_url)
    j_resp = json.loads(resp.content)
    print(j_resp)
    return j_resp


def get_all_manifest_between_dates(request, d_from, d_to):
    # Validate required date fields
    result = validate_date_from_api(d_from, d_to)

    # Return response if result is an error
    if isinstance(result, HttpResponse):
        return result

    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    manifest_ids = Sample().get_manifests_by_date(d_from_parsed, d_to_parsed)
    return finish_request(manifest_ids)


def get_project_manifests_between_dates(request, project, d_from, d_to):
    # Validate required project field
    project_issues = validate_project(project)
    if project_issues:
        return project_issues

    # Validate required date fields
    result = validate_date_from_api(d_from, d_to)

    # Return response if result is an error
    if isinstance(result, HttpResponse):
        return result

    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    lst = project.split(',')
    projects = list(map(lambda x: x.strip(), lst))
    # Remove any empty elements in the list (e.g. where 2 or more comas have been typed in error)
    projects[:] = [x for x in projects if x]

    manifest_ids = Sample().get_manifests_by_date_and_project(
        projects, d_from_parsed, d_to_parsed
    )

    return finish_request(manifest_ids)


def get_specimens_with_submitted_images(request):
    # Fetch all specimens i.e. sources with submitted
    # sample images by sample type/project type and
    # between 'from' date and 'to' date if provided

    # Validate required project field
    project = request.GET.get('project', str()).lower()
    project_issues = validate_project(project)
    if project_issues:
        return project_issues

    # Validate optional date fields
    d_from = request.GET.get('d_from', None)
    d_to = request.GET.get('d_to', None)
    result = validate_date_from_api(d_from, d_to, optional=True)

    # Return response if result is an error
    if isinstance(result, HttpResponse):
        return result

    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    specimens = Source().get_specimens_with_submitted_images(
        project, d_from_parsed, d_to_parsed
    )

    out = list()
    if specimens:
        out = filter_for_API(specimens, add_all_fields=False)
        # Remove 'copo_id' from the output
        for item in out:
            item.pop('copo_id', None)
    return finish_request(out)


def get_all_samples_between_dates(request, d_from, d_to):
    # Validate required date fields
    result = validate_date_from_api(d_from, d_to)

    # Return response if result is an error
    if isinstance(result, HttpResponse):
        return result

    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    samples = Sample().get_samples_by_date(d_from_parsed, d_to_parsed)
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
    accessions = biosampleAccessions.split(',')
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
    projectlist = project.split(',')
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
    samples = Sample().get_by_sequencing_centre(
        sequencing_centre, isQueryByManifestLevel=False
    )

    out = list()
    if samples:
        out = filter_for_API(samples)
    return finish_request(out)


def get_updatable_fields_by_project(request):
    project = request.GET.get('project', str()).lower()
    # Validate required project field
    project_issues = validate_project(project)
    if project_issues:
        return project_issues

    non_compliant_fields = d_utils.get_non_compliant_fields(
        component='sample', project=project
    )

    # Return non-compliant fields i.e. fields that user can update
    non_compliant_fields.sort()
    return finish_request(non_compliant_fields)


def get_fields_by_manifest_version(request):
    return_type = request.GET.get('return_type', 'json').lower()
    standard = request.GET.get('standard', 'tol').lower()
    project = request.GET.get('project', str())
    manifest_version = request.GET.get('manifest_version', str())
    template = None

    # Validate required project field
    project_issues = validate_project(project)
    if project_issues:
        return project_issues

    manifest_versions = Sample().get_available_manifest_versions(project)
    if manifest_version in manifest_versions:
        # Get fields based on standard
        mapped_field_dict = get_mapped_field_by_standard(
            standard=standard, project=project, manifest_version=manifest_version
        )
        template = list(mapped_field_dict.values())
    else:
        # Show error if manifest version does not exist
        error = f'No fields exist for the manifest version, {manifest_version}. Available manifest versions are {d_utils.join_with_and(manifest_versions)}.'
        return HttpResponse(status=status.HTTP_400_BAD_REQUEST, content=error)

    # Determine output response
    return_type_issues = validate_return_type(return_type)
    if return_type_issues:
        # Show error if return type is not 'json' or 'csv'
        return return_type_issues
    elif return_type == 'csv':
        return generate_csv_response(standard, template)
    else:
        # Generate JSON response
        output = generate_wrapper_response(template=template)
        output = jsonb.dumps([output])

        return HttpResponse(output, content_type='application/json')


def get_by_associated_project_type(request):
    valid_associated_types = AssociatedProjectEnum.values()
    # Gets multiple selected values and convert to uppercase
    associated_project_type_list = {
        x.strip().upper() for x in request.GET.getlist('values', []) if x.strip()
    }

    if not associated_project_type_list or not all(
        x in valid_associated_types for x in associated_project_type_list
    ):
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f'Invalid associated project type(s) provided! Accepted values are {d_utils.join_with_and(valid_associated_types)}.',
        )

    samples = Sample().get_project_samples_by_associated_project(
        associated_project_type_list
    )

    return finish_request(
        filter_for_API(samples, add_all_fields=True) if samples else []
    )


def get_by_copo_ids(request, copo_ids):
    # get sample by COPO id if known
    ids = copo_ids.split(',')
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
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content="Invalid 'copo_id' found in request",
            )
    return finish_request(out)


def get_by_field(request, field, values):
    valid_sample_fields = SampleFieldsEnum.values()
    if field not in valid_sample_fields:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f"Invalid field provided. Please provide either {d_utils.join_with_and(valid_sample_fields, conjunction='or')}.",
        )

    # generic method to return all samples where given 'field' matches 'value'
    vals = values.split(',')
    # strip white space
    vals = list(map(lambda x: x.strip(), vals))
    # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
    vals[:] = [x for x in vals if x]

    if field in lookup.COPO_DATE_FIELDS:
        # Convert date fields from string to datetime
        try:
            vals = [parse_date(x) for x in vals]
        except Exception as e:
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content=f'Invalid date format provided. Error: {e}',
            )

    out = list()
    sample_list = Sample().get_by_field(field, vals)
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


# def get_study_from_sample_accession(request, accessions):
#     ids = accessions.split(',')
#     # strip white space
#     ids = list(map(lambda x: x.strip(), ids))
#     # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
#     ids[:] = [x for x in ids if x]
#     # try to get sample from either sra or biosample id
#     samples = Sample().get_by_field(field='sraAccession', value=ids)
#     if not samples:
#         samples = Sample().get_by_field(field='biosampleAccession', value=ids)
#         if not samples:
#             return finish_request([])
#     # if record found, find associated submission record
#     out = []
#     for s in samples:
#         sub = Submission().get_submission_from_sample_id(str(s['_id']))
#         if not sub:
#             continue
#         d = sub[0]['accessions']['project']
#         d['sample_biosampleId'] = s['biosampleAccession']
#         out.append(d)
#     return finish_request(out)


# def get_samples_from_study_accessions(request, accessions):
#     ids = accessions.split(',')
#     # strip white space
#     ids = list(map(lambda x: x.strip(), ids))
#     # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
#     ids[:] = [x for x in ids if x]
#     subs = Submission().get_dtol_samples_in_biostudy(ids)
#     to_finish = list()
#     sample_count = 0
#     for s in subs:
#         out = dict()
#         out['study_accessions'] = s['accessions']['study_accessions']
#         out['sample_accessions'] = []
#         for sa in s['accessions']['sample_accessions']:
#             sample_count += 1
#             smpl_accessions = s['accessions']['sample_accessions'][sa]
#             smpl_accessions['copo_sample_id'] = sa
#             out['sample_accessions'].append(smpl_accessions)
#         to_finish.append(out)
#     return finish_request(to_finish, num_found=sample_count)


def query_local_contexts_hub(project_id):
    lch_url = 'https://localcontextshub.org/api/v1/projects/' + project_id
    resp = requests.get(lch_url)
    j_resp = json.loads(resp.content)
    print(j_resp)
    return j_resp


def get_all(request):
    '''
    Method to handle a request for all
    :param request: a Django HttpRequest object
    :return: A dictionary containing all samples in COPO
    '''

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
        # print('Unexpected error:', sys.exc_info()[0])
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

    def sample_spreadsheet(request, report_id=''):
        file = request.FILES['file']
        name = file.name
        if 'profile_id' in request.POST:
            p_id = request.POST['profile_id']
        else:
            p_id = request.session['profile_id']
        dtol = DtolSpreadsheet(file=file, p_id=p_id)
        if name.endswith('xlsx') or name.endswith('xls'):
            fmt = 'xls'
        elif name.endswith('csv'):
            fmt = 'csv'
        else:
            msg = (
                'Unrecognised file format for spreadsheet. '
                'File format should be either <strong>.xls</strong>, <strong>.xlsx</strong> or <strong>.csv</strong>.'
            )
            Logger().error('Unrecognised file format for sample spreadsheet')
            return HttpResponse(status=status.HTTP_400_BAD_REQUEST, content=msg)

        if dtol.loadManifest(m_format=fmt):
            bytesstring = BytesIO()
            dtol.data.to_pickle(bytesstring)

            if 'profile_id' in request.POST:
                p_id = request.POST['profile_id']
            else:
                p_id = request.session['profile_id']
            r = {
                '$set': {
                    'manifest_data': bytesstring.getvalue(),
                    'profile_id': p_id,
                    'schema_validation_status': 'pending',
                    'taxon_validation_status': 'pending',
                    'err_msg': [],
                    'time_added': datetime.utcnow(),
                    'file_name': name,
                    'isupdate': False,
                    'report_id': report_id,
                }
            }
            ValidationQueue().get_collection_handle().update_one(
                {'profile_id': p_id}, r, upsert=True
            )

        return HttpResponse()

    def post(self, request):
        id = (
            APIValidationReport()
            .get_collection_handle()
            .insert_one(
                {
                    'profile_id': request.POST['profile_id'],
                    'status': 'pending',
                    'content': '',
                    'submitted': datetime.datetime.utcnow(),
                    'user_id': request.user.id,
                }
            )
        )
        self.sample_spreadsheet(request, report_id=id.inserted_id)

        out = {'validation_report_id': str(id.inserted_id)}
        return Response(out)


class APIGetManifestValidationReport(APIView):
    def post(self, request):
        uid = request.user.id
        validation_id = request.POST.get('validation_report_id')
        v_record = APIValidationReport().get_record(validation_id)
        profile_record = Profile().get_record(v_record['profile_id'])
        if profile_record['user_id'] == uid:
            out = {
                'status': v_record['status'],
                'content': v_record['content'],
                'submitted': v_record['submitted'],
            }
        else:
            out = {'content': 'User not permitted to view resource'}
        return Response(out)


class APIGetUserValidations(APIView):
    def post(self, request):
        uid = request.user.id
        v_records = (
            APIValidationReport()
            .get_collection_handle()
            .find({'user_id': uid}, {'_id': 0})
        )
        return Response(list(v_records))
