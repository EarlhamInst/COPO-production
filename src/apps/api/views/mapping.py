import bson.json_util as jsonb

from datetime import datetime, timezone
from django.conf import settings
from django.http import HttpResponse

from common.dal.sample_da import Sample
from common.schemas.utils.data_utils import get_copo_schema


def get_mapped_field_by_standard(
    standard, project, component='sample', manifest_version=None
):
    schema = get_copo_schema(component)

    if not manifest_version:
        manifest_version = settings.MANIFEST_VERSION.get(project.upper(), str())

    # Return the dictionary of the mapped field and the tol field based on the standard for a specific project
    mapped_fields_dict = {
        x['id'].split('.')[-1]: x['standards'][standard]
        for x in schema
        if project.lower() in x.get('specifications', list())
        and x.get('standards', dict()).get(standard, '')
        and manifest_version in x.get('manifest_version', list())
    }
    return mapped_fields_dict


def get_mapped_data_from_sample_data(standard, sample_data):
    # COPO defined fields are fields that are primarily lowercase or camel case
    copo_defined_fields = Sample().get_custom_sample_fields()
    output_list = list()

    for sample in sample_data:
        output_dict = dict()
        sample_type = sample.get('tol_project', str()).lower()
        manifest_version = sample.get('manifest_version', str())
        mapped_field_dict = get_mapped_field_by_standard(
            standard=standard, project=sample_type, manifest_version=manifest_version
        )

        for field, value in sample.items():
            mapped_field = mapped_field_dict.get(field, str())

            if mapped_field:
                output_dict[mapped_field] = value
            else:
                if field in copo_defined_fields:
                    if field.startswith('copo'):
                        output_dict[field] = value
                    else:
                        output_dict[f'copo_{field}'] = value
        output_list.append(output_dict)
    return output_list


def get_mapped_result(standard, template, project):
    if template:
        return get_mapped_data_from_sample_data(standard=standard, sample_data=template)
    else:
        mapped_field_dict = get_mapped_field_by_standard(
            standard=standard, project=project
        )
        return list(mapped_field_dict.values())


def get_mapping(request):
    from src.apps.api.utils import (
        generate_wrapper_response,
        validate_project,
        validate_standard,
    )

    standard = request.GET.get('standard', 'tol')
    project = request.GET.get('project', '')

    # Validate required project field
    issues = validate_project(project)
    if issues:
        return issues

    # Validate standard field
    standard_issues = validate_standard(standard)
    if isinstance(standard_issues, HttpResponse):
        return standard_issues

    template = get_mapped_result(
        standard=standard, template=None, project=project.lower()
    )
    output = generate_wrapper_response(template=template)
    output = jsonb.dumps([output])

    return HttpResponse(output, content_type='application/json')
