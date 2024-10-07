from common.dal.sample_da import Sample
from common.schema_versions.lookup.dtol_lookups import DTOL_EXPORT_TO_STS_FIELDS, STANDARDS
from django.conf import settings
from django.http import HttpResponse

import bson.json_util as jsonb
import json


def get_mapped_fields_for_project(standard, project):
    if standard not in STANDARDS:
        return None

    with open(settings.STANDARDS_MAP_FILE_PATH) as f:
        standard_mapped_data = json.load(f)
        
        if standard and not project:
            output_list = list()

            for tol_field, mapped_data in standard_mapped_data.items():
                if standard != 'tol':
                    field = mapped_data.get(standard, dict()).get('field', str())
                else:
                    field = tol_field
                
                # Ignore if 'field' is nonexistent
                if field:
                    output_list.append(field)

            output_list.sort()
            return output_list
        
        elif standard and project:
            output_list = list()
            project_fields = DTOL_EXPORT_TO_STS_FIELDS.get(project, str())

            for tol_field in project_fields:
                if standard == 'tol':  
                    field = tol_field
                else:  
                    field =  standard_mapped_data.get(tol_field, dict()).get(standard, dict()).get('field', str())

                # Ignore if 'field' is nonexistent
                if field:
                    output_list.append(field)
                            
            return output_list
        else:
            return list()

def get_mapped_data_from_sample_data(standard, sample_data):
    # COPO defined fields are fields that primarily lowercase or camel case
    copo_defined_fields =  Sample().get_custom_sample_fields()

    if standard not in STANDARDS:
        return None
    
    with open(settings.STANDARDS_MAP_FILE_PATH) as f:
        output_list = list()
        standard_mapped_data = json.load(f)
       
        for sample in sample_data:
            output_dict = dict()

            for tol_field, value in sample.items():
                if standard == 'tol':  
                    field = tol_field
                else:  
                    field =  standard_mapped_data.get(tol_field, dict()).get(standard, dict()).get('field', str())
               
                # If field is defined by COPO, add it to 'output_dict'
                if tol_field.lower().startswith('copo'):
                    field = tol_field
                elif tol_field in copo_defined_fields:
                    field = f'copo_{tol_field}'

                # Proceed if 'field' is not an empty string
                if field: 
                    output_dict[field] = value

            output_list.append(output_dict)

    return output_list 

def get_mapped_result(standard, template, project):
    if template:
        return get_mapped_data_from_sample_data(standard=standard, sample_data=template)
    else:
        return get_mapped_fields_for_project(standard=standard, project=project)

def get_mapping(request):
    from src.apps.api.utils import generate_wrapper_response

    standard = request.GET.get('standard', 'tol')
    project = request.GET.get('project', '')
    is_query_by_project = True if project else False
    
    template = get_mapped_result(standard=standard, template=None, project=project.lower())
    output = generate_wrapper_response(error=None, num_found=len(template), template=template)
    output = jsonb.dumps([output])

    return  HttpResponse(output, content_type='application/json')