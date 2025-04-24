import bson.json_util as jsonb
import json
import pandas as pd
import shortuuid
import uuid

from django.http import HttpResponse
from datetime import date, datetime
from dateutil.parser import parse as parse_date
from django_tools.middlewares import ThreadLocal
from rest_framework import status

import common.schemas.utils.data_utils as d_utils
from common.dal.profile_da import Profile
from common.lookup.lookup import API_RETURN_TEMPLATES
from common.schemas.utils.data_utils import get_export_fields
from .enums import AssociatedProjectEnum, ProjectEnum, StandardEnum, ReturnTypeEnum
from .views.mapping import get_mapped_result


# Helpers related to template generation
def get_return_template(type):
    '''
    Method to return a python object representation of the given api template return type
    :param type: a string naming the template type
    :return: an python object representation of the json contained in the template
    '''
    path = API_RETURN_TEMPLATES[type.upper()]
    with open(path) as data_file:
        data = json.load(data_file)
    return data


def extract_to_template(object=None, template=None):
    '''
    Method to examine fields in object and extract those which match the field names in template along with their values
    :param object: the object to search
    :param template: the fields to look for
    :return: the template with the values completed
    '''
    for f in object:
        for t in template:
            if f == t:
                template[t] = object[t]

    return template


def generate_rocrate_person_object(
    df, personlist, person_field_name, prefix, person_map
):
    # items = []
    for people in personlist:
        person_affiliations = (
            df[df[person_field_name] == people][f'{prefix}_AFFILIATION'].unique()[0]
            if f'{prefix}_AFFILIATION' in df.columns
            else str()
        )
        orcid_ids = (
            df[df[person_field_name] == people][f'{prefix}_ORCID_ID'].unique()[0]
            if f'{prefix}_ORCID_ID' in df.columns
            else str()
        )

        person_list = people.split('|')
        person_affiliation_list = person_affiliations.split('|')
        orcid_id_list = orcid_ids.split('|')
        affiliation = ''

        for index, person in enumerate(person_list):
            name = person.strip()
            item = person_map.get(name, dict())
            person_map[name] = item

            if orcid_ids and len(orcid_id_list) > index:
                item['@id'] = 'https://orcid.org/' + orcid_id_list[index].strip()
            else:
                item['@id'] = uuid.uuid4().hex
            item['name'] = name
            item['@type'] = 'Person'

            affiliation_list = item.get('affiliation', list())
            item['affiliation'] = affiliation_list

            # item['role'] = person_field_name
            if person_affiliation_list:
                if len(person_affiliation_list) > index:
                    affiliation = person_affiliation_list[index].strip()
                    if affiliation not in item['affiliation']:
                        item['affiliation'].append(affiliation)
                elif affiliation:
                    if affiliation not in item['affiliation']:
                        item['affiliation'].append(affiliation)

            # items.append(item)
    return person_map


def generate_rocrate_object(request, samples):
    rocrate_json = {}
    manifest_id = samples[0].get('manifest_id', '')
    profile_title = samples[0].get('copo_profile_title', '')
    profile_description = Profile().get_description_by_title(profile_title)
    uri = request.build_absolute_uri('/')

    if not manifest_id:
        rocrate_json['error'] = 'Not Implemented'
    else:
        # @type: context
        context = [
            'https://w3id.org/ro/crate/1.1/context',
            'https://w3id.org/ro/terms/sample',
            'https://w3id.org/ro/terms/copo',
        ]

        info_dict = {
            'BioSample': 'https://bioschemas.org/BioSample',
            'collector': 'https://bioschemas.org/BioSample#collector',
            'custodian': 'https://bioschemas.org/BioSample#custodian',
            'dwc': 'https://rs.tdwg.org/dwc/terms/',
            'dwciri': 'https://rs.tdwg.org/dwc/iri/',
            'isControl': 'https://bioschemas.org/BioSample#isControl',
            'parentTaxon': 'https://schema.org/parentTaxon',
            'samplingAge': 'https://bioschemas.org/BioSample#samplingAge',
            'taxonomicRange': 'https://schema.org/taxonomicRange',
        }

        context.append(info_dict)
        rocrate_json['@context'] = context
        graph_list = list()

        # @type: CreativeWork
        creativeWork = {'@id': 'ro-crate-metadata.json', '@type': 'CreativeWork'}
        creativeWork['conformsTo'] = {'@id': 'https://w3id.org/ro/crate/1.1'}
        creativeWork['about'] = {'@id': f'{uri}api/manifest/{manifest_id}/'}
        graph_list.append(creativeWork)

        df = pd.DataFrame(samples)
        dateCreated = df['time_created'].min()
        dateModified = df['time_updated'].max()

        # @type: Person
        # updatedby = df['updated_by'].unique()
        # author = df['created_by'].unique()
        collectedby = df['COLLECTED_BY'].unique()
        coordinator = (
            df['SAMPLE_COORDINATOR'].unique()
            if 'SAMPLE_COORDINATOR' in df.columns
            else []
        )
        perservedby = (
            df['PRESERVED_BY'].unique() if 'PRESERVED_BY' in df.columns else []
        )
        identifiedby = (
            df['IDENTIFIED_BY'].unique() if 'IDENTIFIED_BY' in df.columns else []
        )

        rocrate_person = dict()
        rocrate_person = generate_rocrate_person_object(
            df, collectedby, 'COLLECTED_BY', 'COLLECTOR', rocrate_person
        )
        rocrate_person = generate_rocrate_person_object(
            df, coordinator, 'SAMPLE_COORDINATOR', 'SAMPLE_COORDINATOR', rocrate_person
        )
        rocrate_person = generate_rocrate_person_object(
            df, perservedby, 'PRESERVED_BY', 'PRESERVER', rocrate_person
        )
        rocrate_person = generate_rocrate_person_object(
            df, identifiedby, 'IDENTIFIED_BY', 'IDENTIFIER', rocrate_person
        )

        # @type: Dataset
        manifest_item = {
            '@id': f'{uri}api/manifest/{manifest_id}/',
            '@type': 'Dataset',
            'datePublished': dateCreated,
            'name': profile_title,
            'description': profile_description,
            'dateModified': dateModified,
            'license': {'@id': 'https://creativecommons.org/publicdomain/zero/1.0'},
        }
        manifest_item['contributor'] = []
        manifest_item['contributor'].extend(
            {'@id': p['@id']} for p in rocrate_person.values()
        )

        manifest_item['taxonomicRange'] = [
            {'@id': f'https://identifiers.org/taxonomy:{x}'}
            for x in df['TAXON_ID'].unique()
        ]
        location_mapping = [
            {f'#place{str(shortuuid.ShortUUID().random(length=10))}': x}
            for x in df['COLLECTION_LOCATION'].unique()
        ]
        manifest_item['location'] = [
            {'@id': k} for element in location_mapping for k, v in element.items()
        ]

        manifest_item['additionalProperty'] = [
            {
                '@id': (
                    f"https://identifiers.org/biosample:{x['biosampleAccession']}"
                    if x.get('biosampleAccession', '')
                    else f"{uri}api/sample/copo_id/{x['copo_id']}"
                )
            }
            for x in samples
        ]

        graph_list.append(manifest_item)
        graph_list.extend(rocrate_person.values())

        # @type: Taxon
        for x in df['TAXON_ID'].unique():
            item = {'@id': f'https://identifiers.org/taxonomy:{x}'}
            item['@type'] = 'TAXON'
            item['name'] = (
                df[df['TAXON_ID'] == x]['SCIENTIFIC_NAME'].unique()[0]
                if 'SCIENTIFIC_NAME' in df.columns
                else ''
            )
            item['parentTaxon'] = {
                '@id': f"{uri}api/sample_field/ORDER_OR_GROUP/{df[df['TAXON_ID']== x ]['ORDER_OR_GROUP'].unique()[0]}"
            }
            graph_list.append(item)

        # @type: Place
        for element in location_mapping:
            for key, value in element.items():
                item = {'@id': key}
                item['@type'] = 'Place'
                item['name'] = value
                item['latitude'] = (
                    df[df['COLLECTION_LOCATION'] == value]['DECIMAL_LATITUDE'].unique()[
                        0
                    ]
                    if 'DECIMAL_LATITUDE' in df.columns
                    else ''
                )
                item['longitude'] = (
                    df[df['COLLECTION_LOCATION'] == value][
                        'DECIMAL_LONGITUDE'
                    ].unique()[0]
                    if 'DECIMAL_LONGITUDE' in df.columns
                    else ''
                )

                graph_list.append(item)

        # @type: PropertyValue
        for x in samples:
            sample_type = x.get('tol_project', '').upper()
            sample_id = (
                f"https://identifiers.org/biosample:{x.get('biosampleAccession', '')}"
                if x.get('biosampleAccession', '')
                else f"{uri}api/sample/copo_id/{x['copo_id']}"
            )

            # Collectors
            collector_dict = {}
            for p in (
                'COLLECTOR:COLLECTED_BY',
                'SAMPLE_COORDINATOR:SAMPLE_COORDINATOR',
                'PRESERVER:PRESERVED_BY',
                'IDENTIFIER:IDENTIFIED_BY',
            ):
                pp = p.split(':')
                collectors = x.get(pp[1], '')
                if collectors:
                    field = (
                        'custodian' if pp[0].lower() != 'collector' else pp[0].lower()
                    )

                    for c in collectors.split('|'):
                        collector = rocrate_person.get(c.strip(), dict())
                        if collector:
                            collector_dict.setdefault(pp[1], []).append(
                                {'biosample_field': field, 'id': collector['@id']}
                            )

            # Filter the data to only include fields that are exportable
            export_fields = get_export_fields(component='sample', project=sample_type)
            filtered_x = {
                key: value for key, value in x.items() if key in export_fields
            }

            # Set the rest of the key-value pair data as additional properties
            for key, value in filtered_x.items():
                sample_item = {
                    '@id': sample_id,
                    '@type': 'PropertyValue',
                    'name': key,
                    'value': value,
                }

                # Add additional properties
                if key == 'biosampleAccession' and value:
                    sample_item['sameAs'] = f"{uri}api/sample/copo_id/{x['copo_id']}"

                if key == 'TAXON_ID' and value:
                    sample_item['sameAs'] = (
                        f"https://identifiers.org/taxonomy:{x['TAXON_ID']}"
                    )

                if key in collector_dict:
                    for collector in collector_dict[key]:
                        sample_item[collector['biosample_field']] = {
                            '@id': collector['id']
                        }

                graph_list.append(sample_item)

        rocrate_json['@graph'] = graph_list
    return rocrate_json


# Output responses
def generate_rocrate_response(request, template):
    '''
    Run RO-Crate validation using the command line.

    1. Generate the RO-Crate:
        - Replace <manifest_id> with the actual manifest ID to query sample records.
        - Download the RO-Crate from:
            https://copo-project.org/api/manifest/<manifest_id>?return_type=rocrate
        - Save it as `ro-crate-metadata.json` inside a directory named `rocrate`.

    2. Install the RO-Crate validator:
        $ pip3 install roc-validator

    3. Validate the RO-Crate:
        $ rocrate-validator validate <path_to_rocrate_directory>

        Example:
        $ rocrate-validator validate --profile-identifier='ro-crate-1.1' \
            --requirement-severity='REQUIRED' \
            --output-file=/rocrate/validation_log.txt /rocrate

        NB: Ensure that `/rocrate` is the correct path to your RO-Crate directory.
    '''
    df = pd.DataFrame(template)
    manifest_ids = df['manifest_id'].unique() if 'manifest_id' in df.columns else []

    if len(manifest_ids) == 1:
        # Generate the rocrate_objs
        rocrate_objs = generate_rocrate_object(request, template)

        return HttpResponse(
            content=json.dumps(rocrate_objs), content_type='application/json'
        )
    return HttpResponse(content='Not Implemented')


def generate_csv_response(standard, template):
    sanitised_standard = standard.replace(' ', '_').replace('/', '-')

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = (
        f'attachment; filename={sanitised_standard}_export.csv'
    )

    df = pd.DataFrame(template)
    df.to_csv(response, index=False)

    return response


def generate_wrapper_response(error=None, num_found=None, template=None):
    wrapper = get_return_template('WRAPPER')

    if error is None:
        if num_found == None:
            wrapper['number_found'] = 0
            if template == None:
                wrapper['number_found'] = 0
            if type(template) == type(list()):
                wrapper['number_found'] = len(template)
            # else:
            #     wrapper['number_found'] = 1
        else:
            wrapper['number_found'] = num_found
        wrapper['data'] = template
        wrapper['status'] = 'OK'
    else:
        wrapper['status'] = 'Error'
        wrapper['error_details'] = error
        wrapper['number_found'] = None
        wrapper['data'] = None

    return wrapper


# Main method for API return
def finish_request(
    template=None, error=None, num_found=None, return_http_response=True
):
    '''
    Method to tidy up data before returning API caller
    :param template: completed template of resource data
    :param error_info: error created if any
    :return: the complete API return
    '''
    request = ThreadLocal.get_current_request()
    return_type = request.GET.get('return_type', 'json').lower()
    standard = request.GET.get('standard', 'tol').lower()

    # Validate optional return_type
    return_type_issues = validate_return_type(return_type)
    if return_type_issues:
        return return_type_issues

    # Validate optional standard
    standard_issues = validate_standard(standard)
    if standard_issues:
        return standard_issues

    # Set template with data based on the standard if there is no error and template exists
    if not error and template:
        template = (
            template
            if standard == 'tol'
            else get_mapped_result(standard=standard, template=template, project=str())
        )

    wrapper = generate_wrapper_response(error, num_found, template)
    output = jsonb.dumps(wrapper)

    if return_http_response:
        if return_type == 'csv':
            return generate_csv_response(standard, template)
        elif return_type == 'rocrate':
            return generate_rocrate_response(request, template)
        else:
            return HttpResponse(output, content_type='application/json')
    else:
        return output


# Helper for sorting dictionaries
def sort_dict_list_by_priority(dict_list, export_fields):
    '''
    Sorts a list of dictionaries so that:
    - Uppercase keys remain at the top in their original order
    - Lowercase camel case keys are alphabetically sorted and placed below

    Args:
        dict_list (list[dict]): A list of dictionaries to be sorted.
        export_fields (list[str]): A list specifying the priority order of uppercase keys.

    Returns:
        list[dict]: A new list of sorted dictionaries.
    '''
    # Retrieve fields that are uppercase
    export_fields_uppercase = [field for field in export_fields if field.isupper()]

    def sort_fields_prioritised(data):
        # Preserve uppercase key order based on export_fields
        uppercase_keys = [
            k for k in export_fields_uppercase if k in data and k.isupper()
        ]
        # uppercase_keys = [k for k in data if k.isupper()]
        lowercase_keys = sorted(k for k in data if not k.isupper())

        sorted_keys = uppercase_keys + lowercase_keys
        return {key: data[key] for key in sorted_keys}

        # Apply sorting to each dictionary in the list

    return [sort_fields_prioritised(d) for d in dict_list]


def validate_project(project, field_name='project', optional=False):
    '''
    Validate the project against a list of valid project names.
    '''
    # If the project is blank and optional, no validation is needed
    if not project:
        if optional:
            return None  # No error for blank value if optional
        else:
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content=f"'{field_name}' is required but not provided.",
            )

    # Validate when project is not blank
    # Lowercase project names
    valid_projects = [x.lower() for x in ProjectEnum.values()]
    response = HttpResponse(
        status=status.HTTP_400_BAD_REQUEST,
        content=f"Invalid value for '{field_name}'. Must be one of: {d_utils.join_with_and([x.upper() for x in valid_projects], conjunction='or')}",
    )

    if isinstance(project, list):
        if not all(x.lower() in valid_projects for x in project):
            return response
    elif isinstance(project, str):
        if project.lower() not in valid_projects:
            return response

    return None  # No issues, validation passed


def validate_associated_project(associated_project, optional=False):
    '''
    Validate the associated project against a list of valid associated project names.
    '''
    # Uppercase associated project names
    valid_associated_projects = [x.upper() for x in AssociatedProjectEnum.values()]

    # If the associated project is blank and optional, no validation is needed
    if not associated_project:
        if optional:
            return None  # No error for blank value if optional
        else:
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content="Associated project is required but not provided.",
            )

    # Validate required associated project (when associated project is not blank)
    if associated_project.upper() not in valid_associated_projects:
        valid_associated_projects = [x.upper() for x in valid_associated_projects]
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f"Invalid value for associated project. Must be one of: {d_utils.join_with_and(valid_associated_projects, conjunction='or')}",
        )

    return None  # No issues, validation passed


def validate_date_from_api(d_from, d_to, optional=False):
    '''
    Validate that both dates are present (if required), in ISO format,
    and that 'from' is not after 'to'.
    '''
    # If both dates are blank
    if not d_from and not d_to:
        if optional:
            return None, None  # No error for blank value if optional
        else:
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content=f"'from' and 'to' date values are required but not provided.",
            )

    # If one date is missing, the other shouldn't be allowed
    if not d_from or not d_to:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content="Missing date. Both 'from' and 'to' date values must be provided together.",
        )

    # Parse dates. Date format must be ISO 8601 format
    if isinstance(d_from, str):
        d_from_parsed = parse_date(d_from)
    elif isinstance(d_from, (date, datetime)):
        d_from_parsed = d_from
    else:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST, content="Invalid type for d_from"
        )

    if isinstance(d_to, str):
        d_to_parsed = parse_date(d_to)
    elif isinstance(d_to, (date, datetime)):
        d_to_parsed = d_to
    else:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST, content="Invalid type for d_to"
        )

    # If parse_date returns None (bad string format)
    if d_from_parsed is None or d_to_parsed is None:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content="Failed to parse one of the dates",
        )

    # Check if 'from' date is 'before' date
    if d_from_parsed > d_to_parsed:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content="Invalid date. 'from' date must be earlier than 'to' date.",
        )

    # No issues, validation has passed. Return parsed dates.
    return d_from_parsed, d_to_parsed


def validate_standard(standard, optional=True):
    # If the standard is blank and optional, no validation is needed
    if not standard:
        if optional:
            return None  # No error for blank value if optional
        else:
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content=f"'standard' is required but not provided.",
            )

    valid_standards = StandardEnum.values()
    if standard not in valid_standards:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f"Invalid value for 'standard'. Accepted values are: {', '.join(d_utils.join_with_and(valid_standards, conjunction='and'))}",
        )
    return None  # No issues, validation passed


def validate_return_type(return_type, optional=True):
    # If the return_type is blank and optional, no validation is needed
    if not return_type:
        if optional:
            return None  # No error for blank value if optional
        else:
            return HttpResponse(
                status=status.HTTP_400_BAD_REQUEST,
                content=f"'return_type' is required but not provided.",
            )

    valid_return_types = ReturnTypeEnum.values()
    if return_type not in valid_return_types:
        return HttpResponse(
            status=status.HTTP_400_BAD_REQUEST,
            content=f"Invalid value for 'return_type'. Accepted values are: {', '.join(d_utils.join_with_and(valid_return_types, conjunction='and'))}",
        )
    return None  # No issues, validation passed
