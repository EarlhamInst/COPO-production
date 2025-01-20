'''
    To generate the 'standards_map.json' file:
       Run the Django command: $ python manage.py generate_standards_map

       Alternatively, run the VSCode configuration, 'Python: Generate standards map',
       in the 'launch.json' file to generate the file

       The 'standards_map.json' file will be written to the 
       '/common/schema_versions/isa_mappings/' directory
'''
from common.lookup import lookup as lk
from common.schemas.utils.data_utils import get_copo_schema
from common.schema_versions.lookup.dtol_lookups import  DTOL_ENA_MAPPINGS, DTOL_RULES, DTOL_UNITS
from django.conf import settings
from django.core.management import BaseCommand
from io import BytesIO

import json
import os
import pandas as pd
import requests

# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = "Command to generate the 'standards_map.json' file and update the respective standards mappings in the sample attributes json files"

    def handle(self, *args, **options):
        self.stdout.write('Generating the standards map...')
        
        self.load_data()

        self.tol_fields_lst = self.get_tol_project_defined_fields()
        
        self.mixs_fields = self.generate_mixs_json()
        self.mixs_mapping = self.create_tol_mixs_mapping()

        self.dwc_fields = self.generate_dwc_fields_json()
        self.dwc_mapping = self.create_tol_dwc_mapping()

        self.generate_standards_map()
    
    #_______________________

    # Helpers: General
    def load_data(self):
        # ENA data
        self.ena_field_names_mapping = DTOL_ENA_MAPPINGS

        # Add additional ENA field names mapping: TAXON_ID and SCIENTIFIC_NAME
        self.ena_field_names_mapping.update({
            'TAXON_ID': {
                'ena': 'TAXON_ID'
            },
            'SCIENTIFIC_NAME': {
                'ena': 'SCIENTIFIC_NAME'
            }
        })
        self.ena_and_tol_rules = DTOL_RULES
        self.ena_units_mapping = DTOL_UNITS

        # MIxS data
        self.mixs_mapping = {
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
        
        # DWC data
        self.DWC_FIELD_NAMES_MAPPING = {
            'DESCRIPTION_OF_COLLECTION_METHOD': {
                'dwc': 'samplingProtocol'
            },
            'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE': {
                'dwc': 'samplingEffort'
            },
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
            'HABITAT': {
                'dwc': 'locationRemarks'
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
            }
        }

        # Sample attributes json schema
        sample_json_schema = self.read_json_file(lk.WIZARD_FILES['sample_details'])
        self.sample_attributes_file_paths = [os.path.join(settings.SCHEMA_VERSIONS_DIR, prop['$ref']) for prop in sample_json_schema.get('properties', []) if '$ref' in prop]

    def generate_json_file(self, file_name, data, directory_path=os.path.join(settings.SCHEMA_VERSIONS_DIR, 'isa_mappings')):
        file_path = os.path.join(directory_path, file_name)

        with open(file_path, 'w+') as f:
            f.write(json.dumps(data, indent=4, sort_keys=False, default=str))
        
        self.stdout.write(self.style.SUCCESS(f'Successfully generated the file: {file_name}'))

    def read_json_file(self, file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    
    #_______________________

    # Helpers: TOL field mappings
    def get_tol_project_defined_fields(self):
        schema = get_copo_schema(component='sample')

        # Get project defined field names
        # These are usually field names that are uppercase or uppercase with underscores
        # Retrieving fields based on manifest version is not considered for this task 
        # because the fields not pertaining to the latest version can be fetched from the API
        tol_fields = [x['id'].split('.')[-1] for x in schema if x.get('specifications', list()) and 'tol' in x.get('standards', dict())]
        tol_fields = list(set(tol_fields)) # Remove duplicates
        tol_fields.sort() # Sort the list
        return tol_fields

    #_______________________

    # Helpers: MIxS field mappings
    def fetch_partial_mixs_data_from_excel(self):
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
                    
            '''
             Uncomment the following line to generate the partial MIxS data to a file
             in the '/copo/common/schema_versions/isa_mappings/' directory
            '''
            # self.generate_json_file(file_name='mixs_partial_data.json', data=terms)

            return terms
        else:
            self.stdout.write(self.style.ERROR(f'Failed to retrieve the Excel file. Status code: {response.status_code}'))
            return None

    def generate_mixs_json(self):
        out = list()

        # MIxS GitHub json schema
        mixs_json_schema_url =  'https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/project/jsonschema/mixs.schema.json'
        
        # Get MIxS field names, description and URIs from the terms table
        mixs_partial_data = self.fetch_partial_mixs_data_from_excel()

        try:
            # Read the json schema from the URL and load the json file
            response = requests.get(mixs_json_schema_url)
            mixs_json_file = json.loads(response.text)

            # Remove the $defs key from the dictionary
            # and create a list of dictionaries
            mixs_schema = mixs_json_file.get('$defs','')

            if not mixs_schema:
                self.stdout.write(self.style.ERROR('No MIxS field definitions found'))
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

            '''
             Uncomment the following line to generate the MIxS json schema to a file
             in the '/copo/common/schema_versions/isa_mappings/' directory
            '''
            # self.generate_json_file(file_name='mixs_fields.json', data=out)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))

        return out

    def create_tol_mixs_mapping(self):
        # Create a mapping between the TOL and MIxS fields 
        out = dict()

        for tol_field in self.tol_fields_lst:
            # Check if the TOL field is in the MIxS field names mapping
            if tol_field in self.mixs_mapping:
                mixs_field = self.mixs_mapping[tol_field]['mixs']

                # Use next with a default value of an empty dict to find the first matching entry
                mixs_field_info = next((x for x in self.mixs_fields if x.get('field', '') == mixs_field), None)

                if mixs_field_info:
                    out[tol_field] = {
                        'field': mixs_field_info['field'],
                        'type': mixs_field_info['type'],
                        'regex': mixs_field_info['regex'],
                        'description': mixs_field_info['description'],
                        'uri': mixs_field_info['uri']
                    }

            # If the TOL field is not in the MIXS mapping
            mixs_field_info = next((x for x in self.mixs_fields if tol_field.lower().replace('_', '') in x['field'].lower() or tol_field.lower() in x['field'].lower()), 
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

    #_______________________

    # Helpers: DWC field mappings
    def generate_dwc_fields_json(self):
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
            df_latest_term_versions = df.groupby('field')['issued'].transform('max')

            # Filter the dataframe based on latest versions, non-deprecated, and valid DWC term URIs
            df_filtered = df[(df['issued'] == df_latest_term_versions) & 
                            (df['status'] == 'recommended')]
            
            # Convert the filtered dataframe to JSON format
            dwc_json_latest_non_deprecated_term_versions = df_filtered[['field', 'description', 'uri']].to_json(orient='records', indent=4)

            # Load the json file to remove unwanted characters (e.g. escape characters)
            dwc_json = json.loads(dwc_json_latest_non_deprecated_term_versions)

            # print(json.dumps(dwc_json, indent=4, sort_keys=False, default=str))

            ''' 
             Uncomment the following line to write the DWC json schema to a file
             in the '/copo/common/schema_versions/isa_mappings' directory
            '''
            # self.generate_json_file(file_name='dwc_fields.json', data=dwc_json)

            out = dwc_json

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        
        return out

    def create_tol_dwc_mapping(self):
        # Create a mapping between the TOL and DWC fields 
        out = dict()

        for tol_field in self.tol_fields_lst:
            # Check if the TOL field is in the DWC field names mapping
            if tol_field in self.DWC_FIELD_NAMES_MAPPING:
                dwc_field = self.DWC_FIELD_NAMES_MAPPING[tol_field]['dwc']

                dwc_field_info = [dwc_field_dict for dwc_field_dict in self.dwc_fields if dwc_field_dict.get('field','') == dwc_field]
            else:
                # If the TOL field is not in the DWC field names mapping,
                dwc_field_info = [dwc_field_dict for dwc_field_dict in self.dwc_fields if tol_field.lower().replace('_','') in dwc_field_dict['field'].lower() or tol_field.lower() in dwc_field_dict['field'].lower()]
                
            # The length of the 'dwc_field_info' list should be 1, 
            # to signify that there are no duplicates but still 
            # iterate through the list to get the dictionary
            if dwc_field_info:
                item = dwc_field_info[0] # Get first item in the list
                dwc_field = item['field']
                out.update({tol_field: {'field': dwc_field, 'description': item['description'], 'uri': item['uri']}})

        # print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

        return out

    #_______________________________

    # Generate 'standards_map.json' file
    def extract_field_mapping(self, field, mapping):
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

    def filter_empty_dictionaries(self, dwc, ena, mixs, tol):
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

    def generate_standards_map(self):
        output = dict()

        for tol_field in self.tol_fields_lst:
            # European Nucleotide Archive (ENA)
            ena = dict()

            # Only mapped items are recorded
            if tol_field in self.ena_field_names_mapping:
                ena['field'] = self.ena_field_names_mapping.get(tol_field, str()).get('ena', str())
                
                if tol_field in self.ena_units_mapping:
                    ena['unit'] = self.ena_units_mapping[tol_field]['ena_unit']

                if tol_field in self.ena_and_tol_rules and 'ena_regex' in self.ena_and_tol_rules[tol_field]:
                    ena['regex'] = self.ena_and_tol_rules[tol_field]['ena_regex']
                
                if tol_field in self.ena_and_tol_rules and 'human_readable' in self.ena_and_tol_rules[tol_field]:
                    ena['description'] = self.ena_and_tol_rules[tol_field]['human_readable']

            #_______________________________

            # Minimum Information about any (x) Sequence (MIxS)
            mixs = self.extract_field_mapping(tol_field, self.mixs_mapping)

            #_______________________________
                
            # Darwin Core (DWC)
            # Terms website: https://dwc.tdwg.org/list/#4-vocabulary
            dwc = self.extract_field_mapping(tol_field, self.dwc_mapping)

            #_______________________________
                
            # Tree of Life (TOL)
            tol = dict()
            tol['field'] = tol_field
                
            if tol_field in self.ena_and_tol_rules and 'strict_regex' in list(self.ena_and_tol_rules[tol_field].keys()):
                ena['regex'] = self.ena_and_tol_rules[tol_field]['strict_regex']
                
            if tol_field in self.ena_and_tol_rules and 'human_readable' in list(self.ena_and_tol_rules[tol_field].keys()):
                ena['description'] = self.ena_and_tol_rules[tol_field]['human_readable']

            #_______________________________

            # Combine the three dictionaries into one and map it to the TOL field
            # Only mapped items are recorded i.e. empty dictionaries are omitted
            filtered_output = self.filter_empty_dictionaries(dwc, ena, mixs, tol)

            if filtered_output:
                output[tol_field] = filtered_output

        # print(f'\nStandards_map:\n {json.dumps(output, indent=4, sort_keys=False, default=str)}\n')

        # Return the nested dictionary as a .json file in
        # the '/common/schema_versions/isa_mappings/' directory
        self.generate_json_file(file_name='standards_map.json', data=output)

        # Update the sample attributes schema based on the standards
        self.update_sample_schema_based_on_standards(standards_map_data=output)

    #_______________________________

    # Update sample attributes schema based on standards
    def update_sample_schema_based_on_standards(self, standards_map_data):
        for schema_file_path in self.sample_attributes_file_paths:
            updated = False
            directory_path, file_name = os.path.split(schema_file_path)

            # Read JSON file
            schema_data = self.read_json_file(schema_file_path)

            # Update schema standards using schema data
            for entry in schema_data:
                # Extract the key (i.e. field name) from the 'id' field
                key = entry['id'].split('.')[-1]
                    
                # Check if the key from 'standards_map_data' matches with 'schema_data'
                if key in standards_map_data:
                    # Update the 'standards' dictionary
                    for standard in entry['standards']:
                        if standard in standards_map_data[key]:
                            new_value = standards_map_data[key][standard].get('field','')
                            current_value = entry['standards'][standard]

                            if current_value != new_value:
                                entry['standards'][standard] = new_value
                                updated = True

            # Only generate the file if an update was made
            if updated:
                self.generate_json_file(file_name=file_name, data=schema_data, directory_path=directory_path)