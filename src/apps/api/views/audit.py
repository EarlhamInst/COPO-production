import common.schemas.utils.data_utils as d_utils
import dateutil.parser as parser

from .sample import format_date
from bson import ObjectId
from common.dal.copo_da import Audit
from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES
from django.http import HttpResponse
from src.apps.api.utils import finish_request

def filter_audits_for_API(audits=list()):
    default_fields = ['copo_id', 'field', 'outdated_value', 'sample_type', 'update_type', 'updated_value']
    time_fields = ['time_updated']
    audit_log_types = ['update_log', 'removal_log', 'truncated_log']   
    out = list()
    export_fields_map = dict()
    
    for audit in audits:
         export_fields = dict()
         sample_type = audit.get('sample_type','').lower()
         if not sample_type:
             continue
         if sample_type not in export_fields_map():
              export_fields = d_utils.get_export_fields(component='sample', project=sample_type)
              export_fields_map[sample_type] = export_fields
         else: 
              export_fields = export_fields_map[sample_type]
         audit_data = dict()
         audit_log_list = list()
         # Convert the 'copo_id' to a string so that it can be displayed
         for key, value in audit.items():
            if key == "copo_id":
                audit_data['copo_id'] = str(audit.get('copo_id',ObjectId()))

            elif key in audit_log_types:
                for element in value:
                    if element['field'] in export_fields:
                        audit_log_data = dict()
                        audit_log_data["audit_type"] = key
                        for k, v in element.items():
                             if k in default_fields:
                                if k in time_fields:
                                    audit_log_data[k] = format_date(v)
                                else:
                                    audit_log_data[k] = v
                        audit_log_list.append(audit_log_data)
            else:
                audit_data[key] = value
         for audit_log_data in audit_log_list():
            audit_log_data.update(audit_data)
            out.append(audit_log_data)
    return out

def filtered_audits_by_updatable_field(sample_id, updatable_field, sample_type):
    sample_id_list = d_utils.convertStringToList(sample_id)

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
    manifest_id_list = d_utils.convertStringToList(manifest_id)

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
    sample_id_list = d_utils.convertStringToList(copo_id)

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
    sample_type_list = d_utils.convertStringToList(sample_type)

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
