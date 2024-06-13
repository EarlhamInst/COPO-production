import common.schemas.utils.data_utils as d_utils
import dateutil.parser as parser

from .sample import format_date
from bson import ObjectId
from common.dal.copo_da import Audit
from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES
from django.http import HttpResponse
from src.apps.api.utils import finish_request

def filter_audits_for_API(audits):
    time_fields = ['time_updated']
    email_fields = ['updated_by']
    audit_log_types = ['update_log', 'removal_log', 'truncated_log']   
    out = list()

    for audit in audits:
        for log_type in audit_log_types:
            if log_type in audit:
                audit_log = audit.get(log_type, list())

                for element in audit_log:
                    data = dict()

                    for k, v in element.items():
                        if k in time_fields:
                            data[k] = format_date(v)
                        elif k in email_fields:
                            data[k] = '*****@' + \
                                (v.split('@')[1] if '@' in v else '')
                        elif k == 'copo_id':
                            # Convert the 'copo_id' to a string so that it can be displayed
                            data[k] = str(element[k])
                        else:
                            data[k] = v

                    out.append(data)
    return out

def filtered_audits_by_updatable_field(sample_id, updatable_field, sample_type):
    # Split the string into a list
    sample_id_list = sample_id.split(',')
    sample_id_list = list(map(lambda x: x.strip().lower(), sample_id_list))

    # Remove any empty elements in the list e.g.
    # where 2 or more commas have been typed in error
    sample_id_list[:] = [x for x in sample_id_list if x]

    sample_updates = Audit().get_sample_update_audits_by_field_updated(
        sample_type, sample_id_list, updatable_field)

    return filter_audits_for_API(sample_updates)

def get_sample_updates_by_sample_field_and_value(request,field, field_value):
    '''
    Get sample updates by one of the following fields and their respective value:
        'RACK_OR_PLATE_ID' field 
        'SPECIMEN_ID' field
        'TUBE_OR_WELL_ID' field
        'biosampleAccession' field
        'public_name' field
        'sraAccession' field
    '''

    out = list()

    sample_updates = Audit().get_sample_update_audits_by_field_and_value(
        field, field_value)

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)

def get_sample_updates_by_manifest_id(request, manifest_id):
    # Split the string into a list
    manifest_id_list = manifest_id.split(',')
    manifest_id_list = list(map(lambda x: x.strip(), manifest_id_list))

    # Remove any empty elements in the list (e.g.
    # where 2 or more commas have been typed in error
    manifest_id_list[:] = [x for x in manifest_id_list if x]

    # Remove duplicates
    manifest_id_list = list(set(manifest_id_list))

    # Check if the 'manifest_id' provided is valid
    if manifest_id_list and not all(d_utils.is_valid_uuid(x) for x in manifest_id_list):
        return HttpResponse(status=400, content=f'Invalid \'manifest_id\'(s) provided!')

    out = list()

    sample_updates = Audit().get_sample_update_audits_field_value_lst(
        manifest_id_list, key='manifest_id',)

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)

def get_sample_updates_by_copo_id(request, copo_id):
    # NB: 'sample_id' is the 'copo_id' key in DB

    # Split the string into a list
    sample_id_list = copo_id.split(',')
    sample_id_list = list(map(lambda x: x.strip(), sample_id_list))

    # Remove any empty elements in the list (e.g.
    # where 2 or more commas have been typed in error
    sample_id_list[:] = [x for x in sample_id_list if x]

    # Check if the 'sample_id' provided is valid
    if sample_id_list and not all(d_utils.is_valid_ObjectId(x) for x in sample_id_list):
        return HttpResponse(status=400, content=f'Invalid \'copo_id\'(s) provided!')

    # Convert each string sample id to ObjectId sample id
    sample_id_list = [ObjectId(x) for x in sample_id_list]

    out = list()

    sample_updates = Audit().get_sample_update_audits_field_value_lst(
        sample_id_list, key='copo_id',)

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)

def get_asg_sample_updates_by_updatable_field(request):
    sample_id = request.GET.get('copo_id', str())
    updatable_field = request.GET.get('updatable_field', str())
    out = filtered_audits_by_updatable_field(
        sample_id, updatable_field, sample_type='asg',)

    return finish_request(out)

def get_dtol_sample_updates_by_updatable_field(request):
    sample_id = request.GET.get('copo_id', str())
    updatable_field = request.GET.get('updatable_field', str())
    out = filtered_audits_by_updatable_field(
        sample_id, updatable_field, sample_type='dtol',)

    return finish_request(out)

def get_erga_sample_updates_by_updatable_field(request):
    sample_id = request.GET.get('copo_id', str())
    updatable_field = request.GET.get('updatable_field', str())
    out = filtered_audits_by_updatable_field(
        sample_id, updatable_field, sample_type='erga',)

    return finish_request(out)

def get_sample_updates_by_update_type(request, update_type):
    # Get all sample updates by 'sample_type' and 'update_type'
    # 'update_type' can be 'system' or 'user'
    sample_type = request.GET.get('sample_type', str())
    sample_type_list = sample_type.split(',')
    sample_type_list = list(map(lambda x: x.strip().lower(), sample_type_list))

    # Remove any empty elements in the list (e.g.
    # where 2 or more commas have been typed in error
    sample_type_list[:] = [x for x in sample_type_list if x]

    if len(sample_type_list) > 1 and not all(x in TOL_PROFILE_TYPES for x in sample_type_list) or len(sample_type_list) == 1 and sample_type_list[0] not in TOL_PROFILE_TYPES:
        return HttpResponse(status=400, content=f'Invalid sample type provided! COPO does not support the sample type(s) provided.')

    out = list()

    sample_updates = Audit().get_sample_update_audits_by_update_type(
        sample_type_list, update_type)

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)

def get_sample_updates_between_dates(request, d_from, d_to):
    # Get all sample updates between d_from and d_to
    # Dates must be ISO 8601 formatted
    d_from = parser.parse(d_from)
    d_to = parser.parse(d_to)

    if d_from > d_to:
        return HttpResponse(status=400, content=f'\'from date\' must be earlier than \'to date\'')

    sample_updates = Audit().get_sample_update_audits_by_date(d_from, d_to)

    out = list()

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)
