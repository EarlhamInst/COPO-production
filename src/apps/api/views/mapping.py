from common.dal.sample_da import Sample
from common.schema_versions.lookup.dtol_lookups import STANDARDS
from common.schemas.utils.data_utils import get_copo_schema
from datetime import datetime, timezone
from django.conf import settings
from django.http import HttpResponse

import bson.json_util as jsonb

def get_mapped_field_by_standard(standard, project, component='sample', field=None, manifest_version=None):
    schema = get_copo_schema(component)

    if not manifest_version:  
        manifest_version = settings.MANIFEST_VERSION.get(project.upper(), str()) 
    
    if field:
        # Return the field that is mapped to the standard for a specific project
        standards = [x.get('standards', dict()) for x in schema if x['id'].split('.')[-1] == field and project.lower() in x.get('specifications', list()) and standard in x.get('standards', dict()) and manifest_version in x.get('manifest_version', list())]
        mapped_field = standards[0].get(standard, str()) if standards else str()

        # If manifest version and/project are not found, return the first field that is mapped to the standard
        if not mapped_field and not manifest_version:
            standards = [x.get('standards', dict()) for x in schema if x['id'].split('.')[-1] == field and standard in x.get('standards', dict())]
            mapped_field = standards[0].get(standard, str()) if standards else str()
        return mapped_field
    else:
        # Return mapped fields based on manifest version
        standards = [x.get('standards', str()) for x in schema if project.lower() in x.get('specifications', list()) and standard in x.get('standards', dict()) and manifest_version in x.get('manifest_version', list())]
        mapped_fields = [x.get(standard, str()) for x in standards if x.get(standard, str()) ]
        mapped_fields = list(set(mapped_fields)) # Remove duplicates

        if isinstance(mapped_fields, dict):
            mapped_fields = mapped_fields.get('data','')

        mapped_fields.sort()
        return mapped_fields

def get_mapped_data_from_sample_data(standard, sample_data):
    # COPO defined fields are fields that are primarily lowercase or camel case
    copo_defined_fields =  Sample().get_custom_sample_fields()
    output_list = list()

    for sample in sample_data:
        output_dict = dict()
        sample_type = sample.get('tol_project', str()).lower()
        manifest_version = sample.get('manifest_version', str())

        for field, value in sample.items():
            mapped_field = get_mapped_field_by_standard(standard=standard, project=sample_type, field=field, manifest_version=manifest_version)

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
        return get_mapped_field_by_standard(standard=standard, project=project)

def get_mapping(request):
    from src.apps.api.utils import generate_wrapper_response

    standard = request.GET.get('standard', 'tol')
    project = request.GET.get('project', '')
    
    template = get_mapped_result(standard=standard, template=None, project=project.lower())
    output = generate_wrapper_response(template=template)
    output = jsonb.dumps([output])

    return  HttpResponse(output, content_type='application/json')