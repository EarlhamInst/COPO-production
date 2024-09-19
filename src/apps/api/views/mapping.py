from common.schema_versions.lookup.dtol_lookups import DTOL_EXPORT_TO_STS_FIELDS, STANDARDS, STANDARDS_MAPPING_FILE_PATH
from django.http import HttpResponse
from src.apps.api.utils import finish_request

import bson.json_util as jsonb
import common.schemas.utils.data_utils as d_utils
import json

def get_standard_data(standard_list=STANDARDS, manifest_type=str(), queryByManifestType=False):
    with open(STANDARDS_MAPPING_FILE_PATH) as f:
        standards_file_data = json.load(f)
        standards_file_data = standards_file_data[0]
        output = list()
        output_dict = dict()

        if standard_list and not queryByManifestType:
            for tol_column, standard_data in standards_file_data.items():
                tol_column_dict = dict()
                tol_column_dict[tol_column] = dict()
                
                for standard in standard_list:
                    if standard_data.get(standard, str()):
                        tol_column_dict[tol_column] |= {standard: {'key': standard_data.get(standard,str()).get('field', str())}}
                            
                        # Merge dictionaries
                        output_dict |= tol_column_dict
                            
            output.append(output_dict)

            result = finish_request(template=output, error=None, num_found=len(output[0]), return_http_response=False)

        elif any(x in standard_list for x in STANDARDS) and queryByManifestType and manifest_type:
            manifest_type_columns = DTOL_EXPORT_TO_STS_FIELDS.get(manifest_type, str())

            for tol_column, standard_data in standards_file_data.items():
                tol_column_dict = dict()
                tol_column_dict[tol_column] = dict()

                for standard in standard_list:
                    if standard_data.get(standard, str()) and tol_column in manifest_type_columns:
                        tol_column_dict[tol_column] |= {standard: {'key': standard_data.get(standard,str()).get('field', str())}}
                        
                        # Merge dictionaries
                        output_dict |= tol_column_dict
                            
            output.append(output_dict)

            result = finish_request(template=output, error=None, num_found=len(output[0]),return_http_response=False)
        else:
            result =  finish_request(template=None, error=None, num_found=None, return_http_response=False)

    # Removes slashes from output
    result = json.loads(result)
    return result

def get_mapping(request):
    return_type = request.GET.get('return_type', "json").lower()
    
    default_value = d_utils.convertListToString(STANDARDS)
    standard = request.GET.get('standard', default_value)
    standard_list = d_utils.convertStringToList(standard)

    if return_type == "csv":
        return HttpResponse(content="Not Implemented")
    
    standard_data = get_standard_data(standard_list=standard_list)

    return HttpResponse(jsonb.dumps(standard_data), content_type="application/json")


def get_mapping_for_manifest_type(request, manifest_type):
    return_type = request.GET.get('return_type', "json").lower()

    default_value = d_utils.convertListToString(STANDARDS)
    standard = request.GET.get('standard', default_value)
    standard_list = d_utils.convertStringToList(standard) # Split the 'standard' string into a list

    if return_type == 'csv':
        return HttpResponse(content='Not Implemented')

    standard_data = get_standard_data(standard_list=standard_list, manifest_type=manifest_type.lower(), queryByManifestType=True)

    return HttpResponse(jsonb.dumps(standard_data), content_type='application/json')