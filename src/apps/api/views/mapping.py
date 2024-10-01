from common.dal.sample_da import Sample
from common.schema_versions.lookup.dtol_lookups import DTOL_EXPORT_TO_STS_FIELDS, STANDARDS, STANDARDS_MAPPING_FILE_PATH
from django.http import HttpResponse

import bson.json_util as jsonb
import json

def get_standard_fields(standard, standards_data):
    fields = list()
    
    for key in standards_data:
        # Retrieve the field for the standard if it exists
        field = standards_data[key].get(standard, {}).get('field', '')
        
        if field:
            fields.append(field)

    return fields

def get_mapped_fields_for_project(standard, project, is_query_by_project):
    if standard not in STANDARDS:
        return None

    with open(STANDARDS_MAPPING_FILE_PATH) as f:
        standard_mapped_data = json.load(f)
        standard_mapped_data = standard_mapped_data[0]
        standard_fields = get_standard_fields(standard, standard_mapped_data)
        
        if standard and not is_query_by_project:
            output_list = list()

            for tol_field, mapped_data in standard_mapped_data.items():
                if standard != 'tol':
                    field = mapped_data.get(standard, str()).get('field', str())
                else:
                    field = tol_field if standard == 'tol' else str()
                
                # Ignore if 'field' is an empty string
                if field in standard_fields:
                    output_list.append(field)

            output_list.sort()
            return output_list
        
        elif standard and is_query_by_project and project:
            output_list = list()
            project_fields = DTOL_EXPORT_TO_STS_FIELDS.get(project, str())

            for tol_field in project_fields:
                if standard != 'tol' and tol_field in list(standard_mapped_data.keys()):
                   field = standard_mapped_data.get(tol_field, str()).get(standard, str()).get('field', str())
                else:
                    field = tol_field if standard == 'tol' else str()

                # Ignore if 'field' is an empty string
                if field:
                    output_list.append(field)
                            
            return output_list
        else:
            return list()

def get_mapped_data_from_sample_data(standard, sample_data):
    # COPO defined fields are fields that are lowercase or camel case
    copo_defined_fields =  Sample().get_all_sample_fields(has_copo_defined_fields=True, has_project_defined_fields=False)

    if standard not in STANDARDS:
        return None
    
    with open(STANDARDS_MAPPING_FILE_PATH) as f:
        output_list = list()
        standard_mapped_data = json.load(f)
        standard_mapped_data = standard_mapped_data[0]
        standard_fields = get_standard_fields(standard, standard_mapped_data)
       
        for sample in sample_data:
            output_dict = dict()

            for tol_field, value in sample.items():
                if standard != 'tol' and tol_field in list(standard_mapped_data.keys()):
                    field = standard_mapped_data[tol_field].get(standard, str()).get('field', str())
                else:
                    field = tol_field if standard == 'tol' else str()
               
                if field in standard_fields:
                    # Proceed if the 'field' is a standard field
                    pass
                else:
                    # If field is defined by COPO, add it to 'output_dict'
                    if tol_field in copo_defined_fields or tol_field.lower().startswith('copo'):
                        field = tol_field if tol_field.lower().startswith('copo') else f'copo_{tol_field}'

                # Check if both 'field' and 'value' are empty strings
                is_both_empty = field == '' and value == ''

                # Check if 'field' is an empty string and 'value' is not an empty string
                is_field_empty_value_non_empty = field == '' and value != ''

                # Ignore if both 'field' and 'value' are empty strings
                # Ignore if 'field' is an empty string and 'value' is not an empty string
                # Do not ignore if 'field' is not an empty string and 'value' is an empty string
                if is_both_empty or is_field_empty_value_non_empty:
                    continue

                output_dict[field] = value

            output_list.append(output_dict)

    return output_list 

def get_mapped_result(are_fields_required, standard, template, project, is_query_by_project):
    if are_fields_required:
        return get_mapped_fields_for_project(standard=standard, project=project, is_query_by_project=is_query_by_project)
    else:
        return get_mapped_data_from_sample_data(standard=standard, sample_data=template)

def get_mapping(request):
    from src.apps.api.utils import generate_wrapper_response

    standard = request.GET.get('standard', 'tol')
    project = request.GET.get('project', '')
    is_query_by_project = True if project else False
    
    template = get_mapped_result(are_fields_required=True, standard=standard, template=None, project=project.lower(), is_query_by_project=is_query_by_project)
    output = generate_wrapper_response(error=None, num_found=len(template), template=template)
    output = jsonb.dumps([output])

    return  HttpResponse(output, content_type='application/json')