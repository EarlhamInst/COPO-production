'''
To generate the 'standards_map.json' file:
    Run the command below in the 'shared_tools/scripts' directory.
    The command will save the file to the '/copo/common/schema_versions/isa_mappings/' directory

    NB: Modify the database credentials in get_tol_project_defined_fields() function 
    as required to get an accurate account of the TOL defined fields

    $  python generate_standards_map.py

    Alternatively, run the VSCode configuration, 'Python: Generate standards map' in the 
    'launch.json' file to generate the file
'''
from io import BytesIO, StringIO
from bs4 import BeautifulSoup
from django.core.management import call_command

import django
import json
import os
import pandas as pd
import pymongo
import re
import requests
import sys
import urllib.parse

# Set project root directory, Django environment variables and initialise Django
sys.path.append('/copo') # Add the project root directory to the system path
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'src.main_config.settings.all')
django.setup()

#_______________________

# Helpers: General

def get_django_command_output(command_name, argument):
    output = StringIO()
    call_command(command_name, argument, stdout=output)
    output_value = output.getvalue()
    return json.loads(output_value) # Convert the JSON string to a dictionary

def generate_json_file(file_name, data):
    file_path = f'/copo/common/schema_versions/isa_mappings/{file_name}'

    with open(file_path, 'w+') as f:
        print(json.dumps(data, indent=4, sort_keys=False, default=str), file=f)
    
    # Display message after file generation
    print(f'Successfully created the file: {file_name}')

#_______________________

# Helpers: ENA field mappings
# https://www.ebi.ac.uk/ena/submit/report/checklists/xml/*?type=sample

# Execute the Django management command to get the specified ENA mapping
ENA_FIELD_NAMES_MAPPING = get_django_command_output('get_ena_map', 'name')
ENA_AND_TOL_RULES = get_django_command_output('get_ena_map', 'rules')
ENA_UNITS_MAPPING = get_django_command_output('get_ena_map', 'unit')

#_______________________

# Helpers: TOL field mappings
def get_tol_project_defined_fields():
    schema = get_django_command_output('get_schema', 'sample')

    # Get builtin field names i.e. project defined field names
    # These are usually field names that are uppercase or uppercase with underscores
    # Retrieving fields based on manifest version is not considered for this task 
    # because the fields not pertaining to the latest version can be fetched from the API
    tol_fields = [x['id'].split('.')[-1] for x in schema if x.get('is_builtin', False) and x.get('standard', str()) == 'tol']
    tol_fields = list(set(tol_fields)) # Remove duplicates
    tol_fields.sort() # Sort the list

    return tol_fields

tol_fields_lst = get_tol_project_defined_fields()

#_______________________

# Helpers: MIxS field mappings
MIXS_MAPPING = {
    'COLLECTOR_SAMPLE_ID':{
        'mixs': 'samp_name'
    },
    'COMMON_NAME': {
        'mixs': 'host_common_name'
    },
    'CULTURE_OR_STRAIN_ID':{
        'mixs':'microb_start_taxID'
    },
    'DATE_OF_COLLECTION': {
        'mixs': 'collection_date'
    },
    'DECIMAL_LATITUDE': {
        'mixs': 'lat_lon'
    },
    'DECIMAL_LONGITUDE': {
        'mixs': 'lat_lon'
    },
    'DEPTH': {
        'mixs': 'depth'
    },
    'DESCRIPTION_OF_COLLECTION_METHOD': {
        'mixs': 'samp_collect_method'
    },
    'DISSOLVED_OXYGEN': {
        'mixs': 'diss_oxygen'
    },
    'ELEVATION': {
        'mixs': 'elev'
    },
    'HABITAT':{
        'mixs': 'host_body_habitat'
    },
    'LIGHT_INTENSITY':{
        'mixs': 'light_intensity'
    },
    'ORIGINAL_GEOGRAPHIC_LOCATION': {
        'mixs': 'geo_loc_name'
    },
    'OTHER_INFORMATION': {
        'mixs': 'additional_info'
    },
    'PRESERVATION_APPROACH': {
        'mixs': 'samp_preserv'
    },
    'PRESERVATIVE_SOLUTION': {
        'mixs': 'samp_store_sol'
    },
    'PH': {
        'mixs': 'ph'
    },
    'PURPOSE_OF_SPECIMEN': {
        'mixs': 'samp_purpose'
    },
    'RELATIONSHIP':{
        'mixs': 'biotic_relationship'
    },
    'SCIENTIFIC_NAME':{
        'mixs': 'specific_host'
    },
    'SIZE_OF_TISSUE_IN_TUBE': {
        'mixs': 'size_frac'
    },
    'SPECIMEN_ID': {
        'mixs':'source_mat_id'
    },
    'SEX': {
        'mixs':'urobiom_sex'
    },
    'SYMBIONT':{
        'mixs':'host_symbiont'
    },
    'TAXON_ID':
    {
        'mixs':'samp_taxon_id'
    },
    'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION': {
        'mixs': 'timepoint'
    },
    'TEMPERATURE': {
        'mixs': 'samp_store_temp'
    },
    'TUBE_OR_WELL_ID': {
        'mixs': 'samp_well_name'
    },
    'tol_project': {
        'mixs': 'project_name'
    },
}

def fetch_partial_mixs_data_from_excel():
    # URL to the remote Excel file
    terms_version = 'v6'
    url = f'https://github.com/GenomicsStandardsConsortium/mixs/raw/issue-610-temp-mixs-xlsx-home/mixs/excel/mixs_{terms_version}.xlsx'
    response = requests.get(url)

    if response.status_code == 200:
        # Read the Excel file content from the specified worksheet
        excel_data = pd.read_excel(BytesIO(response.content), sheet_name='MIxS')

        # Columns to extract
        columns_to_extract = ['Structured comment name', 'Item (rdfs:label)', 'Definition', 'MIXS ID']
        
        # Check if all the necessary columns are present
        if not all(col in excel_data.columns for col in columns_to_extract):
            print(f'Missing required columns: {set(columns_to_extract) - set(excel_data.columns)}')
            return list()

        # Initialise list to store the processed terms
        terms = list()

        # Iterate over the rows of the Excel file
        for _, row in excel_data.iterrows():
            output_dict = dict()

            # Extract relevant values
            structured_comment_name = row['Structured comment name']
            label = row['Item (rdfs:label)']
            definition = row['Definition']
            mixs_id = row['MIXS ID']

            if pd.notnull(structured_comment_name) and pd.notnull(definition) and pd.notnull(mixs_id):
                uri = f'https://genomicsstandardsconsortium.github.io/mixs/{mixs_id.split(":")[-1]}'
                
                output_dict[structured_comment_name] = {
                    'description': definition,
                    'label': label,
                    'uri': uri
                }
                terms.append(output_dict)
                
        # Uncomment the following line to generate the partial MIxS data to a file
        # in the '/copo/common/schema_versions/isa_mappings/' directory
        # generate_json_file(file_name='mixs_partial_data.json', data=terms)

        return terms
    else:
        print(f'Failed to retrieve the Excel file. Status code: {response.status_code}')
        return None

def generate_mixs_json():
    out = list()

    # MIxS GitHub json schema
    mixs_json_schema_url =  'https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/project/jsonschema/mixs.schema.json'
    
    # Get MIxS field names, description and URIs from the terms table
    mixs_partial_data = fetch_partial_mixs_data_from_excel()

    try:
        # Read the json schema from the URL and load the json file
        response = requests.get(mixs_json_schema_url)
        mixs_json_file = json.loads(response.text)

        # Remove the $defs key from the dictionary
        # and create a list of dictionaries
        mixs_schema = mixs_json_file.get('$defs','')

        if not mixs_schema:
            print('No MIxS field definitions found')
            return out
        
        # Rearrange the list of dictionaries and retrieve only the 
        # field 'type' and 'pattern' from the MIxS json schema
        # NB: 'pattern' in the json schema is known as 'regex' in the TOL schema
        
        added_fields = set() # Track which fields have been added

        for key, value_dict in mixs_schema.items():
            def add_data(field):
                # Create a tuple that uniquely identifies the data entry (could use just 'field' if only that needs to be unique)
                unique_identifier = (field, value_dict.get('type', ''), value_dict.get('pattern', ''))

                if unique_identifier not in added_fields:
                    added_fields.add(unique_identifier)
                    data = next((d for d in mixs_partial_data if field in d), None)

                    if data:
                        description = data[field].get('description', '') if data else value_dict.get('description', '')
                        uri = data[field].get('uri', '') if data else value_dict.get('uri', '')
                    else:
                        description = value_dict.get('description', '')
                        uri = ''

                    return {
                        'field': field,
                        'type': value_dict.get('type', ''),
                        'regex': value_dict.get('pattern', ''),
                        'description': description,
                        'uri': uri
                    }

            if not value_dict.get('properties', ''):
                data = add_data(key)
                if data:
                    out.append(data)
            else:
                for k in value_dict['properties']:
                    data = add_data(k)
                    if data:
                        out.append(data)

        # Uncomment the following line to generate the MIxS json schema to a file
        # in the '/copo/common/schema_versions/isa_mappings/' directory
        # generate_json_file(file_name='mixs_fields.json', data=out)

    except Exception as e:
        print(f'Error: {e}')

    return out

'''
Uncomment the following line to generate the MIxS json schema 
from the 'generate_mixs_json' function if the code to generate file has been
uncommented in the 'generate_mixs_json' function
Then, comment the following line, 'MIXS_FIELDS = generate_mixs_json()'
'''

# MIXS_FIELDS = json.load(open('/copo/common/schema_versions/isa_mappings/mixs_fields.json'))
MIXS_FIELDS = generate_mixs_json()

# print(f'\MIXS_FIELDS:\n {json.dumps(MIXS_FIELDS, indent=4, sort_keys=False,default=str)}\n')

# Create a mapping between the TOL and MIxS fields 
def create_tol_mixs_mapping():
    out = dict()

    for tol_field in tol_fields_lst:
        # Check if the TOL field is in the MIxS field names mapping
        if tol_field in MIXS_MAPPING:
            mixs_field = MIXS_MAPPING[tol_field]['mixs']

            # Use next with a default value of an empty dict to find the first matching entry
            mixs_field_info = next((x for x in MIXS_FIELDS if x.get('field', '') == mixs_field), None)

            if mixs_field_info:
                out[tol_field] = {
                    'field': mixs_field_info['field'],
                    'type': mixs_field_info['type'],
                    'regex': mixs_field_info['regex'],
                    'description': mixs_field_info['description'],
                    'uri': mixs_field_info['uri']
                }

        # If the TOL field is not in the MIXS mapping
        mixs_field_info = next((x for x in MIXS_FIELDS if tol_field.lower().replace('_', '') in x['field'].lower() or tol_field.lower() in x['field'].lower()), 
                               None)

        # Only mapped items are recorded
        if mixs_field_info:
            out[tol_field] = {
                'field': mixs_field_info['field'],
                'type': mixs_field_info['type'],
                'regex': mixs_field_info['regex'],
                'description': mixs_field_info['description'],
                'uri': mixs_field_info['uri']
            }

    # print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

    return out

MIXS_MAPPING = create_tol_mixs_mapping()

# print(f'\nMIXS_MAPPING:\n {json.dumps(MIXS_MAPPING, indent=4, sort_keys=False,default=str)}\n')

#_______________________

# Helpers: Darwin Core field mappings
dwc_fields_uri_lst ={
    'license': {
        'uri': 'http://purl.org/dc/terms/license'
    }
}

DWC_FIELD_NAMES_MAPPING = {
    'COLLECTION_LOCATION': {
        'dwc': 'higherGeography'
    },
    'COLLECTED_BY': {
        'dwc': 'recordedBy'
    },
    'COLLECTOR_AFFILIATION': {
        'dwc': 'institutionCode'
    },
    'COLLECTOR_ORCID_ID': {
        'dwc': 'recordedByID'
    },
    'COLLECTOR_SAMPLE_ID':{
        'dwc': 'materialSampleID'
    },
    'COMMON_NAME': {
        'dwc': 'vernacularName'
    },
    'CULTURE_OR_STRAIN_ID':{
        'dwc':'occurrenceID'
    },
    'DATE_OF_COLLECTION':{
        'dwc':'eventDate'
    },
    'DEPTH':{
        'dwc':'verbatimDepth'
    },
    'ELEVATION':{
        'dwc':'verbatimElevation'
    },
    'ETHICS_PERMITS_DEF':{
        'dwc': 'license'
    },
    'FAMILY': {
        'dwc': 'family'
    },
    'GAL_SAMPLE_ID:': {
        'dwc': 'institutionID'
    },
    'GENUS': {
        'dwc': 'genus'
    },
    'GRID_REFERENCE': {
        'dwc': 'georeferenceRemarks'
    },
    'IDENTIFIED_BY': {
        'dwc': 'identifiedBy'
    },
    'ORDER_OR_GROUP': {
        'dwc': 'order'
    },
    'ORGANISM_PART': {
        'dwc': 'organismScope'
    },
    'ORIGINAL_COLLECTION_DATE': {
        'dwc': 'eventDate'
    },
    'PRESERVATION_APPROACH': {
        'dwc': 'preparations'
    },
    'RELATIONSHIP': {
        'dwc': 'relationshipRemarks'
    },
    'SAMPLE_COORDINATOR_AFFILIATION': {
        'dwc': 'institutionCode'
    },
    'SCIENTIFIC_NAME': {
        'dwc': 'scientificName'
    },
    'SERIES': {
        'dwc': 'fieldNumber'
    },
    'SIZE_OF_TISSUE_IN_TUBE':{
        'dwc': 'sampleSizeValue'
    },
    'SPECIMEN_ID': {
        'dwc':'materialSampleID'
    },
    'TIME_OF_COLLECTION': {
        'dwc': 'eventTime'
    },
}

def generate_dwc_fields_json():
    out = list()

    # Get DWC csv schema link from GitHub
    url =  'https://raw.githubusercontent.com/tdwg/dwc/master/vocabulary/term_versions.csv'
    # Latest dwc term version (2023-09-18): https://rs.tdwg.org/dwc/version/terms/2023-09-18.json

    try:
        # Read the csv schema from the URL
        df = pd.read_csv(url)

        ''' 
        Get DWC term versions based on the following criteria:
         - latest term versions (based on the 'issued' date column)
         - terms that are not deprecated (i.e. 'status' != 'deprecated)
         - terms' uri that are from the DWC namespace (i.e. 'term_iri'which start with 'http://rs.tdwg.org/dwc/terms')
        
        Then, convert the DWC dataframe to json
        '''

        # Select only the required columns: term_localName, definition, term_iri
        df = df[['term_localName', 'definition', 'term_iri', 'issued', 'status']]
        
        # Rename the columns
        df = df.rename(columns={
            'term_localName': 'field',
            'definition': 'description',
            'term_iri': 'uri'
        })

        # Apply the filtering criteria
        dwc_uri_prefix = 'http://rs.tdwg.org/dwc/terms/'
        df_latest_term_versions = df.groupby('field')['issued'].transform('max')

        # Filter the dataframe based on latest versions, non-deprecated, and valid DWC term URIs
        df_filtered = df[(df['issued'] == df_latest_term_versions) & 
                        (df['status'] != 'deprecated') & 
                        (df['uri'].str.startswith(dwc_uri_prefix, na=False))]
        
        # Convert the filtered dataframe to JSON format
        dwc_json_latest_non_deprecated_term_versions = df_filtered[['field', 'description', 'uri']].to_json(orient='records', indent=4)

        # Load the json file to remove unwanted characters (e.g. escape characters)
        dwc_json = json.loads(dwc_json_latest_non_deprecated_term_versions)

        # print(json.dumps(dwc_json, indent=4, sort_keys=False, default=str))
                
        # Append custom items to the dwc_json
        for custom in dwc_fields_uri_lst:
            if custom not in [d.get('field','') for d in dwc_json]:
                field = custom
                uri = dwc_fields_uri_lst[custom]['uri']
                dwc_json.append({'field': field, 'description': '', 'uri': uri})

        # Uncomment the following lines to write the DWC json schema to a file
        # in the '/copo/common/schema_versions/isa_mappings' directory
        # generate_json_file(file_name='dwc_fields.json', data=dwc_json)

        out = dwc_json

    except Exception as e:
        print(f'Error: {e}')
    
    return out

'''
Uncomment the following line to load the DWC json schema 
from the 'generate_dwc_fields_json' function if the code to generate file has been 
uncommented in the 'generate_dwc_fields_json' function
Then, comment the succeeding line, 'DWC_FIELDS = generate_dwc_fields_json()'
'''

# DWC_FIELDS = json.load(open('/copo/common/schema_versions/isa_mappings/dwc_fields.json'))
DWC_FIELDS = generate_dwc_fields_json()

# print(f'\DWC_FIELDS:\n {json.dumps(DWC_FIELDS, indent=4, sort_keys=False,default=str)}\n')

# Create a mapping between the TOL and DWC fields 
def create_tol_dwc_mapping():
    out = dict()

    for tol_field in tol_fields_lst:
        # Check if the TOL field is in the DWC field names mapping
        if tol_field in DWC_FIELD_NAMES_MAPPING:
            dwc_field = DWC_FIELD_NAMES_MAPPING[tol_field]['dwc']

            dwc_field_info = [dwc_field_dict for dwc_field_dict in DWC_FIELDS if dwc_field_dict.get('field','') == dwc_field]
        else:
            # If the TOL field is not in the DWC field names mapping,
            dwc_field_info = [dwc_field_dict for dwc_field_dict in DWC_FIELDS if tol_field.lower().replace('_','') in dwc_field_dict['field'].lower() or tol_field.lower() in dwc_field_dict['field'].lower()]
            
        # The length of the 'dwc_field_info' list should be 1, 
        # to signify that there are no duplicates but still 
        # iterate through the list to get the dictionary
        if dwc_field_info:
            item = dwc_field_info[0] # Get first item in the list
            dwc_field = item['field']
            out.update({tol_field: {'field': dwc_field, 'description': item['description'], 'uri': item['uri']}})

    # print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

    return out

DWC_MAPPING = create_tol_dwc_mapping()

# print(f'\nDWC_MAPPING:\n {json.dumps(DWC_MAPPING, indent=4, sort_keys=False,default=str)}\n')

#_______________________________

# Generate 'standards_map.json' file
def extract_field_mapping(field, mapping):
    output = dict()

    # Only mapped items are recorded
    if field in mapping:
        if mapping[field].get('field'):
            output['field'] = mapping[field]['field']
        
        if mapping[field].get('type'):
            output['type'] = mapping[field]['type']
        
        if mapping[field].get('uri'):
            output['uri'] = mapping[field]['uri']
        
        if mapping[field].get('regex'):
            output['regex'] = mapping[field]['regex']
        
        if mapping[field].get('description'):
            output['description'] = mapping[field]['description']

    return output

def filter_empty_dictionaries(dwc, ena, mixs, tol):
    # Create a dictionary only with non-empty dictionaries
    output_dicts = {}

    if dwc:
        output_dicts['dwc'] = dwc

    if ena:
        output_dicts['ena'] = ena

    if mixs:
        output_dicts['mixs'] = mixs

    if tol:
        output_dicts['tol'] = tol

    return output_dicts

def generate_standards_map():
    output = dict()

    for tol_field in tol_fields_lst:
        # European Nucleotide Archive (ENA)
        ena = dict()

        # Only mapped items are recorded
        if tol_field in ENA_FIELD_NAMES_MAPPING:
            ena['field'] = ENA_FIELD_NAMES_MAPPING.get(tol_field, str()).get('ena', str())
            
            if tol_field in ENA_UNITS_MAPPING:
                ena['unit'] = ENA_UNITS_MAPPING[tol_field]['ena_unit']

            if tol_field in ENA_AND_TOL_RULES and 'ena_regex' in ENA_AND_TOL_RULES[tol_field]:
                ena['regex'] = ENA_AND_TOL_RULES[tol_field]['ena_regex']
            
            if tol_field in ENA_AND_TOL_RULES and 'human_readable' in ENA_AND_TOL_RULES[tol_field]:
                ena['description'] = ENA_AND_TOL_RULES[tol_field]['human_readable']

        #_______________________________

        # Minimum Information about any (x) Sequence (MIxS)
        # Terms website:  https://genomicsstandardsconsortium.github.io/mixs/term_list
        mixs = extract_field_mapping(tol_field, MIXS_MAPPING)
        #_______________________________
            
        # Darwin Core (dwc)
        # Terms website: https://dwc.tdwg.org/list/#4-vocabulary
        dwc = extract_field_mapping(tol_field, DWC_MAPPING)

        #_______________________________
            
        # Tree of Life (TOL)
        tol = dict()
        tol['field'] = tol_field
            
        if tol_field in ENA_AND_TOL_RULES and 'strict_regex' in list(ENA_AND_TOL_RULES[tol_field].keys()):
            ena['regex'] = ENA_AND_TOL_RULES[tol_field]['strict_regex']
            
        if tol_field in ENA_AND_TOL_RULES and 'human_readable' in list(ENA_AND_TOL_RULES[tol_field].keys()):
            ena['description'] = ENA_AND_TOL_RULES[tol_field]['human_readable']

        #_______________________________

        # Combine the three dictionaries into one and map it to the TOL field
        # Only mapped items are recorded i.e. empty dictionaries are omitted
        filtered_output = filter_empty_dictionaries(dwc, ena, mixs, tol)

        if filtered_output:
            output[tol_field] = filtered_output

    # print(f'\nStandards_map:\n {json.dumps(output, indent=4, sort_keys=False, default=str)}\n')

    # Return the nested dictionary as a .json file in
    # the '/copo/common/schema_versions/isa_mappings/' directory
    generate_json_file(file_name='standards_map.json', data=output)

if __name__ == '__main__':
    generate_standards_map()