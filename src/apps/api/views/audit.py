from bson import ObjectId
from collections import OrderedDict
from datetime import datetime
from django.http import HttpResponse
from rest_framework import status

import common.schemas.utils.data_utils as d_utils
from common.dal.copo_da import Audit
from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES
from src.apps.api.utils import finish_request
from .sample import format_date
from ..enums import UpdateTypeEnum, UpdateAuditFieldEnum
from ..utils import validate_date_from_api, validate_project


def sort_audit_output(audit_entry, default_fields):
    # Separate default fields and remaining fields
    default_part = {k: audit_entry[k] for k in default_fields if k in audit_entry}
    remaining_part = {
        k: audit_entry[k] for k in sorted(audit_entry.keys()) if k not in default_fields
    }

    # Merge into one dictionary with correct order
    sorted_audit_log_data = OrderedDict(**default_part, **remaining_part)
    return sorted_audit_log_data


def filter_audits_for_API(audits=[]):
    default_fields = [
        'field',
        'outdated_value',
        'updated_value',
        'update_type',
        'time_updated',
    ]
    audit_log_types = ['update_log', 'removal_log', 'truncated_log']
    out = []
    export_fields_map = {}

    for audit in audits:
        export_fields = {}
        sample_type = audit.pop('sample_type', None)
        if not sample_type:
            continue
        if sample_type not in export_fields_map:
            export_fields = d_utils.get_export_fields(
                component='sample', project=sample_type
            )
            export_fields.extend(default_fields)
            export_fields_map[sample_type] = list(
                set(export_fields)
            )  # Remove duplicates
        else:
            export_fields = export_fields_map[sample_type]

        audit_data = {}
        audit_log_list = []
        for key, value in audit.items():
            if key in audit_log_types:
                for element in value:
                    if element.get('field', '') in export_fields:
                        audit_log_data = {}
                        audit_log_data['copo_audit_type'] = key

                        # Replace 'sample_type' with 'tol_project'
                        element.pop('sample_type', None)
                        element['tol_project'] = sample_type.upper()

                        # Process remaining fields
                        for k, v in element.items():
                            if k in export_fields:
                                if k == 'copo_id':
                                    audit_log_data[k] = str(v)
                                elif isinstance(v, datetime):
                                    audit_log_data[k] = format_date(v)
                                else:
                                    audit_log_data[k] = v

                        audit_log_data = sort_audit_output(
                            audit_log_data, default_fields
                        )
                        audit_log_list.append(audit_log_data)
            else:
                audit_data[key] = value
        for audit_log_data in audit_log_list:
            audit_log_data.update(audit_data)
            out.append(audit_log_data)
    return out


def get_sample_updates_by_sample_field_and_value(request, field, field_value):
    '''
    Get sample updates by one of the following fields and their respective value:
        'RACK_OR_PLATE_ID' field
        'SPECIMEN_ID' field
        'TUBE_OR_WELL_ID' field
        'biosampleAccession' field
        'public_name' field
        'sraAccession' field
    '''
    # Check if the 'field' provided is valid
    valid_update_filter_fields = UpdateAuditFieldEnum.values()
    if field not in valid_update_filter_fields:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f'Invalid \'field\' provided! Must be one of: {d_utils.join_with_and(valid_update_filter_fields, conjunction="or")}',
        )

    sample_updates = Audit().get_sample_update_audits_by_field_and_value(
        field, field_value
    )

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)


def get_sample_updates_by_manifest_id(request, manifest_id):
    manifest_id_list = d_utils.convertStringToList(manifest_id)

    # Remove duplicates
    manifest_id_list = list(set(manifest_id_list))

    # Check if the 'manifest_id' provided is valid
    if manifest_id_list and not all(d_utils.is_valid_uuid(x) for x in manifest_id_list):
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f'Invalid \'manifest_id\'(s) provided!',
        )

    sample_updates = Audit().get_sample_update_audits_field_value_lst(
        manifest_id_list, key='manifest_id'
    )

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)


def get_sample_updates(request):
    # NB: 'sample_id' is the 'copo_id' key in DB
    sample_id = request.GET.get('copo_id', str())
    sample_id_list = []
    updatable_field = request.GET.get('updatable_field', str())
    project = request.GET.get('project', str()).lower()

    # Validate optional project field
    issues = validate_project(project, optional=True)
    if issues:
        return issues

    if sample_id:
        sample_id_list = d_utils.convertStringToList(sample_id)

        # Check if the 'sample_id' provided is valid
        if sample_id_list and not all(
            d_utils.is_valid_ObjectId(x) for x in sample_id_list
        ):
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content=f'Invalid \'copo_id\'(s) provided!',
            )

        # Convert each string sample ID to ObjectId sample ID
        sample_id_list = [ObjectId(x) for x in sample_id_list]

    sample_updates = Audit().get_sample_update_audits(
        sample_id_list, updatable_field, project
    )

    out = filter_audits_for_API(sample_updates)

    return finish_request(out)


def get_sample_updates_by_update_type(request, update_type):
    # Get all sample updates by 'sample_type' and 'update_type'
    # 'update_type' can be 'system' or 'user'
    sample_type_list = request.GET.getlist(
        'project', []
    )  # Gets multiple selected values
    sample_type_list = [x.lower() for x in sample_type_list]  # Convert to lowercase

    # Validate optional project field
    issues = validate_project(sample_type_list, optional=True)
    if issues:
        return issues

    # Validate required update_type
    valid_update_types = UpdateTypeEnum.values()
    if update_type not in valid_update_types:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f"Invalid value for 'update_type'. Must be one of: {d_utils.join_with_and(valid_update_types, conjunction='or')}",
        )

    sample_updates = Audit().get_sample_update_audits_by_update_type(
        sample_type_list, update_type
    )
    out = filter_audits_for_API(sample_updates)

    return finish_request(out)


def get_sample_updates_between_dates(request, d_from, d_to):
    # Validate required date fields
    result = validate_date_from_api(d_from, d_to)

    # Return response if result is an error
    if isinstance(result, HttpResponse):
        return result
   
    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    sample_updates = Audit().get_sample_update_audits_by_date(
        d_from_parsed, d_to_parsed
    )
    out = filter_audits_for_API(sample_updates)

    return finish_request(out)
