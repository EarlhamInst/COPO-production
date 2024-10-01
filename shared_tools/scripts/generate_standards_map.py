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
from bs4 import BeautifulSoup

import json
import pandas as pd
import pymongo
import requests
import urllib.parse

# Helpers for ENA field mappings
# https://www.ebi.ac.uk/ena/submit/report/checklists/xml/*?type=sample

ENA_FIELD_NAMES_MAPPING = {
    'ASSOCIATED_BIOGENOME_PROJECTS': {
        'ena': 'associated biogenome projects'
    },
    'BARCODE_HUB': {
        'ena': 'barcoding center'
    },
    'COLLECTED_BY': {
        'ena': 'collected_by'
    },
    'COLLECTION_LOCATION_1': {
        'info': 'split COLLECTION_LOCATION on first "|" and put left hand side here (should be country)',
        'ena': 'geographic location (country and/or sea)',
    },
    'COLLECTION_LOCATION_2': {
        'info': 'split COLLECTION_LOCATION on first "|" and put right hand side here (should be a list of "|" separated locations)',
        'ena': 'geographic location (region and locality)',
    },
    'COLLECTOR_AFFILIATION': {
        'ena': 'collecting institution'
    },
    'COLLECTOR_ORCID_ID': {
        'ena': 'collector ORCID ID'
    },
    'CULTURE_OR_STRAIN_ID': {
        'ena': 'culture_or_strain_id'
    },
    'DATE_OF_COLLECTION': {
        'ena': 'collection date'
    },
    'DECIMAL_LATITUDE': {
        'ena': 'geographic location (latitude)'
    },
    'DECIMAL_LONGITUDE': {
        'ena': 'geographic location (longitude)'
    },
    'DEPTH': {
        'ena': 'geographic location (depth)'
    },
    'DESCRIPTION_OF_COLLECTION_METHOD': {
        'ena': 'sample collection device or method'
    },
    'DNA_VOUCHER_ID_FOR_BIOBANKING': {
        'ena': 'bio_material'
    },
    'ELEVATION': {
        'ena': 'geographic location (elevation)'
    },
    'GAL': {
        'ena': 'GAL'
    },
    'GAL_SAMPLE_ID': {
        'ena': 'GAL_sample_id'
    },
    'HABITAT': {
        'ena': 'habitat'
    },
    'IDENTIFIED_BY': {
        'ena': 'identified_by'
    },
    'IDENTIFIER_AFFILIATION': {
        'ena': 'identifier_affiliation'
    },
    'LATITUDE_END': {
        'ena': 'geographic location end (latitude_end)'
    },
    'LATITUDE_START': {
        'ena': 'geographic location start (latitude_start)'
    },
    'LIFESTAGE': {
        'ena': 'lifestage'
    },
    'LONGITUDE_END': {
        'ena': 'geographic location end (longitude_end)'
    },
    'LONGITUDE_START': {
        'ena': 'geographic location start (longitude_start)'
    },
    'ORGANISM_PART': {
        'ena': 'organism part'
    },
    'ORIGINAL_COLLECTION_DATE': {
        'ena': 'original collection date'
    },
    'ORIGINAL_DECIMAL_LATITUDE': {
        'ena': 'original geographic location (latitude)'
    },
    'ORIGINAL_DECIMAL_LONGITUDE': {
        'ena': 'original geographic location (longitude)'
    },
    'ORIGINAL_GEOGRAPHIC_LOCATION': {
        'ena': 'original collection location'
    },
    'PARTNER': {
        'ena': 'GAL'
    },
    'PARTNER_SAMPLE_ID': {
        'ena': 'GAL_sample_id'
    },
    'PROXY_TISSUE_VOUCHER_ID_FOR_BIOBANKING': {
        'ena': 'proxy biomaterial'
    },
    'PROXY_VOUCHER_ID': {
        'ena': 'proxy voucher'
    },
    'PROXY_VOUCHER_LINK': {
        'ena': 'proxy voucher url'
    },
    'RELATIONSHIP': {
        'ena': 'relationship'
    },
    'SAMPLE_COORDINATOR': {
        'ena': 'sample coordinator'
    },
    'SAMPLE_COORDINATOR_AFFILIATION': {
        'ena': 'sample coordinator affiliation'
    },
    'SAMPLE_COORDINATOR_ORCID_ID': {
        'ena': 'sample coordinator ORCID ID'
    },
    'SEX': {
        'ena': 'sex'
    },
    'SCIENTIFIC_NAME': {
        'ena': 'SCIENTIFIC_NAME'
    } ,
    'SPECIMEN_ID': {
        'ena': 'specimen_id'
    },
    'TAXON_ID': {
        'ena': 'TAXON_ID'
    }  ,
    'TISSUE_VOUCHER_ID_FOR_BIOBANKING': {
        'ena': 'bio_material'
    },
    'VOUCHER_ID': {
        'ena': 'specimen_voucher'
    },
    'VOUCHER_INSTITUTION': {
        'ena': 'voucher institution url'
    },
    'VOUCHER_LINK': {
        'ena': 'specimen voucher url'
    },
    'public_name': {
        'ena': 'tolid'
    },
    'sampleDerivedFrom': {
        'ena': 'sample derived from'
    },
    'sampleSameAs': {
        'ena': 'sample same as'
    },
    'sampleSymbiontOf': {
        'ena': 'sample symbiont of'
    }
}

ENA_AND_TOL_RULES = {
    'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID':
        {
            'strict_regex': '^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12}$',
            'human_readable': '[ID provided by the local context hub]'
        },
    'CHLOROPHYL_A':
        {
            'strict_regex': '^\d+$',
            'human_readable': 'integer'
        },
    'COLLECTOR_ORCID_ID':
        {
            'strict_regex': '^((\d{4}-){3}\d{3}(\d|X))(\|(\d{4}-){3}\d{3}(\d|X))*|(^not provided$)|(^not applicable$)',
            'human_readable': '16-digit number that is compatible with the ISO Standard (ISO 27729),  if multiple IDs separate with a | and no spaces'
        },
    'DATE_OF_COLLECTION':
        {
            'ena_regex': '(^[12][0-9]{3}(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01])(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?'
                         '([+-][0-9]{1,2})?)?)?)?(/[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?'
                         '([+-][0-9]{1,2})?)?)?)?)?$)|(^not collected$)|(^not provided$)|(^restricted access$) ',
            'human_readable': 'YYYY-MM-DD, YYYY-MM, YYYY, NOT_COLLECTED or NOT_PROVIDED'
        },
    'DECIMAL_LATITUDE':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)',
            'human_readable': 'numeric, NOT_COLLECTED or NOT_PROVIDED'
        },
    'DECIMAL_LATITUDE_ERGA':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)',
            'human_readable': 'numeric, or NOT_COLLECTED'

        },
    'DECIMAL_LONGITUDE':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)',
            'human_readable': 'numeric, NOT_COLLECTED or NOT_PROVIDED'

        },
    'DECIMAL_LONGITUDE_ERGA':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)',
            'human_readable': 'numeric, or NOT_COLLECTED'

        },
    'DEPTH':
        {
            'ena_regex': '(0|((0\.)|([1-9][0-9]*\.?))[0-9]*)([Ee][+-]?[0-9]+)?',
            'human_readable': 'numeric, or empty string'
        },
    'DISSOLVED_OXYGEN':
        {
            'strict_regex': '^\d+$',
            'human_readable': 'integer'
        },
    'ELEVATION':
        {
            'ena_regex': '[+-]?(0|((0\.)|([1-9][0-9]*\.?))[0-9]*)([Ee][+-]?[0-9]+)?',
            'human_readable': 'numeric, or empty string'
        },
    'ETHICS_PERMITS_FILENAME':
        {
            'optional_regex': '(^.+\.pdf$)|(^.+\.PDF$)',
            'human_readable': 'filename (including ".pdf" extension) if permit is required or NOT_APPLICABLE if permit is not required'
        },
    'LATITUDE_END':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)',
            'human_readable': 'numeric, NOT_COLLECTED or NOT_PROVIDED'
        },
    'LATITUDE_END_ERGA':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)',
            'human_readable': 'numeric, or NOT_COLLECTED'

        },
    'LATITUDE_START':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)',
            'human_readable': 'numeric, NOT_COLLECTED or NOT_PROVIDED'

        },
    'LATITUDE_START_ERGA':
        {

            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)',
            'human_readable': 'numeric, or NOT_COLLECTED'

        },
    'LONGITUDE_END':
        {

            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)',
            'human_readable': 'numeric, NOT_COLLECTED or NOT_PROVIDED'

        },
    'LONGITUDE_END_ERGA':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)',
            'human_readable': 'numeric, or NOT_COLLECTED'

        },
    'LONGITUDE_START':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)',
            'human_readable': 'numeric, NOT_COLLECTED or NOT_PROVIDED'
        },
    'LONGITUDE_START_ERGA':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)',
            'human_readable': 'numeric, or NOT_COLLECTED'

        },
    'NAGOYA_PERMITS_FILENAME':
        {
            'optional_regex': '(^.+\.pdf$)|(^.+\.PDF$)',
            'human_readable': 'filename (including ".pdf" extension) if permit is required or NOT_APPLICABLE if permit is not required'
        },
    'ORIGINAL_COLLECTION_DATE':
        {
            'ena_regex': '^[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?(/[0-9]{'
                         '4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?)?$',
            'human_readable': 'Date as YYYY, YYYY-MM or YYYY-MM-DD'
        },
    'ORIGINAL_DECIMAL_LATITUDE':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]{0,8}$)',
            'human_readable': 'numeric with 8 decimal places'
        },
    'ORIGINAL_DECIMAL_LONGITUDE':
        {
            'ena_regex': '(^[+-]?[0-9]+.?[0-9]{0,8}$)',
            'human_readable': 'numeric with 8 decimal places'
        },
    'RACK_OR_PLATE_ID':
        {
            'optional_regex': '^[a-zA-Z]{2}\d{8}$'
        },
    'SALINITY':
        {
            'strict_regex': '^\d+$',
            'human_readable': 'integer'
        },
    'SAMPLE_COORDINATOR_ORCID_ID':
        {
            'strict_regex': '^((\d{4}-){3}\d{3}(\d|X))(\|(\d{4}-){3}\d{3}(\d|X))*$',
            'human_readable': '16-digit number that is compatible with the ISO Standard (ISO 27729), if multiple IDs separate with a | and no spaces'
        },
    'SAMPLE_DERIVED_FROM':
        {
            'ena_regex': '(^[ESD]R[SR]\d{6,}(,[ESD]R[SR]\d{6,})*$)|(^SAM[END][AG]?\d+(,SAM[END][AG]?\d+)*$)|(^EGA[NR]\d{'
                         '11}(,EGA[NR]\d{11})*$)|(^[ESD]R[SR]\d{6,}-[ESD]R[SR]\d{6,}$)|(^SAM[END][AG]?\d+-SAM[END]['
                         'AG]?\d+$)|(^EGA[NR]\d{11}-EGA[NR]\d{11}$)',
            'human_readable': 'Specimen accession'
        },
    'SAMPLE_SAME_AS':
        {
            'ena_regex': '(^[ESD]R[SR]\d{6,}(,[ESD]R[SR]\d{6,})*$)|(^SAM[END][AG]?\d+(,SAM[END][AG]?\d+)*$)|(^EGA[NR]\d{'
                         '11}(,EGA[NR]\d{11})*$)|(^[ESD]R[SR]\d{6,}-[ESD]R[SR]\d{6,}$)|(^SAM[END][AG]?\d+-SAM[END]['
                         'AG]?\d+$)|(^EGA[NR]\d{11}-EGA[NR]\d{11}$)',
            'human_readable': 'Specimen accession'
        },
    'SAMPLE_SYMBIONT_OF':
        {
            'ena_regex': '(^[ESD]R[SR]\d{6,}(,[ESD]R[SR]\d{6,})*$)|(^SAM[END][AG]?\d+(,SAM[END][AG]?\d+)*$)|(^EGA[NR]\d{'
                         '11}(,EGA[NR]\d{11})*$)|(^[ESD]R[SR]\d{6,}-[ESD]R[SR]\d{6,}$)|(^SAM[END][AG]?\d+-SAM[END]['
                         'AG]?\d+$)|(^EGA[NR]\d{11}-EGA[NR]\d{11}$)',
            'human_readable': 'Specimen accession'
        },
    'SAMPLING_PERMITS_FILENAME':
        {
            'optional_regex': '(^.+\.pdf$)|(^.+\.PDF$)',
            'human_readable': 'filename (including ".pdf" extension) if permit is required or NOT_APPLICABLE if permit is not required'
        },
    'SAMPLING_WATER_BODY_DEPTH':
        {
            'strict_regex': '^\d+$',
            'human_readable': 'integer'
        },
    'TEMPERATURE':
        {
            'strict_regex': '^\d+$',
            'human_readable': 'integer'
        },
    'TIME_OF_COLLECTION':
        {
            'strict_regex': '^([0-1][0-9]|2[0-4]):[0-5]\d$',
            'human_readable': '24-hour format with hours and minutes separated by colon'
        },
    'TUBE_OR_WELL_ID':
        {
            'optional_regex': '^[a-zA-Z]{2}\d{8}$'
        },
    'WATER_SPEED':
        {
            'strict_regex': '^\d+$',
            'human_readable': 'integer'
        },
    'tmp_TISSUE_VOUCHER_ID_FOR_BIOBANKING':
        {
            'strict_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)|(^not applicable$)|(^not provided$)|^$', 
            'biocollection_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)',
            'biocollection_qualifier_type': 'specimen_voucher',
            #every id should be in the format of 'institute code:collection code:id' and separated by '|'. it can aslo be 'Not_applicable, not provided or empty'
            'human_readable': 'The ID should be in the format of institute unique name:collection code:id or institute unique name:id and separated by \'|\' and the ID should be registered already.'
        },
    'tmp_PROXY_TISSUE_VOUCHER_ID_FOR_BIOBANKING':
        {
            'strict_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)|(^not applicable$)|(^not provided$)|^$', 
            'biocollection_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)',
            'biocollection_qualifier_type': 'specimen_voucher',
            #every id should be in the format of 'institute code:collection code:id' and separated by '|'. it can aslo be 'Not_applicable, not provided or empty'
            'human_readable': 'The ID should be in the format of institute unique name:collection code:id or institute unique name:id and separated by \'|\' and the ID should be registered already.'
        },
    'tmp_DNA_VOUCHER_ID_FOR_BIOBANKING':
        {
            'strict_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)|(^not applicable$)|(^not provided$)|^$', 
            'biocollection_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)',
            'biocollection_qualifier_type': 'bio_material',
            #every id should be in the format of 'institute code:collection code:id' and separated by '|'. it can aslo be 'Not_applicable, not provided or empty'
            'human_readable': 'The ID should be in the format of institute unique name:collection code:id or institute unique name:id and separated by \'|\' and the ID should be registered already.'
        },
    'tmp_PROXY_VOUCHER_ID':
        {
            'strict_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)|(^not applicable$)|(^not provided$)|^$', 
            'biocollection_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)',
            'biocollection_qualifier_type': 'specimen_voucher',
            #every id should be in the format of 'institute code:collection code:id' and separated by '|'. it can aslo be 'Not_applicable, not provided or empty'
            'human_readable': 'The ID should be in the format of institute unique name:collection code:id or institute unique name:id and separated by \'|\' and the ID should be registered already.'
        },
    'tmp_VOUCHER_ID':
        {
            'strict_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)|(^not applicable$)|(^not provided$)|^$', 
            #every id should be in the format of 'institute code:collection code:id' and separated by '|'. it can aslo be 'Not_applicable, not provided or empty'
            'biocollection_regex': '((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+$)|(((^([^\|:])+)(:(([^\|:])+))?:[^\|:]+)(\|(([^\|:])+)(:(([^\|:])+))?:([^\|:])+)+$)',
            'biocollection_qualifier_type': 'specimen_voucher',
            'human_readable': 'The ID should be in the format of institute unique name:collection code:id or institute unique name:id and separated by \'|\' and the ID should be registered already.'
        }        
}

ENA_UNITS_MAPPING = {
    'DECIMAL_LATITUDE': {'ena_unit': 'DD'},
    'DECIMAL_LONGITUDE': {'ena_unit': 'DD'},
    'DEPTH': {'ena_unit': 'm'},
    'ELEVATION': {'ena_unit': 'm'},
    'LATITUDE_END': {'ena_unit': 'DD'},
    'LATITUDE_START': {'ena_unit': 'DD'},
    'LONGITUDE_END': {'ena_unit': 'DD'},
    'LONGITUDE_START': {'ena_unit': 'DD'},
    'ORIGINAL_DECIMAL_LATITUDE': {'ena_unit': 'DD'},
    'ORIGINAL_DECIMAL_LONGITUDE': {'ena_unit': 'DD'}
}

#_______________________

# Helpers for TOL field mappings
def get_tol_project_defined_fields():
    # Get distinct TOL defined field names i.e. field names that 
    # are uppercase or uppercase with underscores
    username = urllib.parse.quote_plus('copo_user')
    password = urllib.parse.quote_plus('password')
    myclient = pymongo.MongoClient('mongodb://%s:%s@copo_mongo:27017/' % (username, password))
    mydb = myclient['copo_mongo']

    sample_collection = mydb['SampleCollection']

    cursor = sample_collection.aggregate([
        {
            '$project': {
            'fieldNames': { '$objectToArray': '$$ROOT' }
            }
        },
        {
            '$unwind': '$fieldNames'
        },
        {
            '$group': {
            '_id': None,
            'distinctFields': { '$addToSet': '$fieldNames.k' }
            }
        },
        {
            '$project': {
            '_id': 0,
            'distinctFields': {
                '$filter': {
                'input': '$distinctFields',
                'as': 'field',
                'cond': {
                    '$or': [
                    { '$regexMatch': { 'input': '$$field', 'regex': '^[A-Z]+$' } },
                    { '$regexMatch': { 'input': '$$field', 'regex': '^[A-Z_]+$' } }
                    ]
                }
                }
            }
            }
        }
    ])

    tol_fields_lst = list(cursor)[0]['distinctFields']
    tol_fields_lst.sort()

    return tol_fields_lst

tol_fields_lst = get_tol_project_defined_fields()

#_______________________

# Helpers for MIxS field mappings
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

def get_mixs_partial_data_from_terms_table():
    # Get MIxS terms
    url = 'https://genomicsstandardsconsortium.github.io/mixs/term_list'
    response = requests.get(url)

    if response.status_code == 200:
        terms = []
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table that contains the terms
        # Assume the table is the first <table> tag on the page
        table = soup.find('table')
        rows = table.find_all('tr') # Extract table rows

        # Iterate over the rows (skip the header)
        for row in rows[1:]:
            output_dict = dict()

            # Extract the columns in each row
            columns = row.find_all('td')
            
            # Get the term, uri and description from the columns
            if len(columns) >= 2:
                # Term description
                description = columns[1].text.strip() or ''

                # Term name
                a_tag = columns[0].find_all('a')

                if len(a_tag) != 1:
                    print(f'More than one "a" tag found in the first column')
                    return []

                # Assume that the number of 'a' tags/links in the first column is 1
                a_tag=a_tag[0]
                data= dict()
                href= a_tag['href'].strip()
                term = a_tag.text.strip()
                suffix = href[href.find('../')+1:href.find('/">')][2::]

                if suffix.isdigit():
                    uri = f'https://genomicsstandardsconsortium.github.io/mixs/{suffix}'
                    data[term] = {'description': description, 'uri': uri}

                    output_dict.update(data)

                    terms.append(output_dict)
            else:
                print(f'Failed to extract the term, uri and description from the columns')
                return []
        return terms
    else:
        print(f'Failed to retrieve the web page. Status code: {response.status_code}')
        return None

def generate_mixs_json():
    out = list()

    # MIxS GitHub json schema
    mixs_json_schema_url =  'https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/project/jsonschema/mixs.schema.json'
    
    # Get MIxS field names, description and URIs from the terms table
    mixs_partial_data = get_mixs_partial_data_from_terms_table()

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

        # Uncomment the following lines to generate the MIxS json schema to a file
        # in the '/copo/common/schema_versions/isa_mappings/' directory
        '''
        file_name = 'mixs_fields.json'
        file_directory = '/copo/common/schema_versions/isa_mappings/'
        file_path = file_directory + file_name

        with open(file_path, 'w+') as f:
            print(json.dumps(out, indent=4, sort_keys=False,default=str), file=f)
            f.close()
        '''
        
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

        if mixs_field_info:
            out[tol_field] = {
                'field': mixs_field_info['field'],
                'type': mixs_field_info['type'],
                'regex': mixs_field_info['regex'],
                'description': mixs_field_info['description'],
                'uri': mixs_field_info['uri']
            }
        else:
            out[tol_field] = {'field': '', 'type': '', 'regex': '', 'description': ''}

    # print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

    return out

MIXS_MAPPING = create_tol_mixs_mapping()

#print(f'\nMIXS_MAPPING:\n {json.dumps(MIXS_MAPPING, indent=4, sort_keys=False,default=str)}\n')

#___________________________________________________________________

# Helpers for Darwin Core field mappings
dwc_fields_uri_lst ={
    'license': {
        'uri': 'http://purl.org/dc/terms/license'
    },
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
    dwc_csv_schema_url =  'https://raw.githubusercontent.com/tdwg/dwc/master/vocabulary/term_versions.csv'
    # Latest dwc term version (2023-09-18): https://rs.tdwg.org/dwc/version/terms/2023-09-18.json

    try:
        # Read the csv schema from the URL
        df = pd.read_csv(dwc_csv_schema_url)

        ''' 
        Get DWC term versions based on the following criteria:
         - latest term versions (based on the 'issued' date column)
         - terms that are not deprecated ('status' != 'deprecated)
         - terms' uri that are from the DWC namespace ('term_iri' starts with 'http://rs.tdwg.org/dwc/terms')
        
        Then, convert the DWC dataframe to json
        '''
        dwc_uri_prefix = 'http://rs.tdwg.org/dwc/terms/'
        df_latest_term_versions = df.groupby('term_localName')['issued'].transform('max')
        dwc_json_latest_non_deprecated_term_versions = df[(df['issued'] == df_latest_term_versions) & (df['status'] != 'deprecated') & (df['term_iri'].str.startswith(dwc_uri_prefix, na=False))].to_json(orient='records', indent=4)

        # Load the json file to avoid forwarded slashes in the 'term_uri' urls
        dwc_json = json.loads(dwc_json_latest_non_deprecated_term_versions)

        #print(json.dumps(dwc_json, indent=4, sort_keys=False,default=str))
                
        # Rearrange the list of dictionaries and retrieve only the 
        #'term_localName', 'definition', and 'term_iri' keys from the DWC json
        dwc_json = [{'field': d.get('term_localName',''), 'description': d.get('definition',''), 'uri': d.get('term_iri','')}  for d in dwc_json]

        # Append custom items to the dwc_json
        for custom in dwc_fields_uri_lst:
            if custom not in [d.get('field','') for d in dwc_json]:
                field = custom
                uri = dwc_fields_uri_lst[custom]['uri']
                dwc_json.append({'field':field, 'description': '', 'uri': uri})

        # Uncomment the following lines to write the DWC json schema to a file
        # in the '/copo/common/schema_versions/isa_mappings' directory
        '''
        file_name = 'dwc_fields.json'
        file_directory = '/copo/common/schema_versions/isa_mappings/'
        file_path = file_directory + file_name

        with open(file_path, 'w+') as f:
            print(json.dumps(dwc_json, indent=4, sort_keys=False,default=str), file=f)
            f.close()
        '''

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
#DWC_FIELDS = json.load(open('/copo/common/schema_versions/isa_mappings/dwc_fields.json'))
DWC_FIELDS = generate_dwc_fields_json()

#print(f'\DWC_FIELDS:\n {json.dumps(DWC_FIELDS, indent=4, sort_keys=False,default=str)}\n')

# Create a mapping between the TOL and DWC fields 
def create_tol_dwc_mapping():
    data = dict()
    out = dict()

    for tol_field in tol_fields_lst:
        # Check if the TOL field is in the DWC field names mapping
        if tol_field in DWC_FIELD_NAMES_MAPPING:
            dwc_field = DWC_FIELD_NAMES_MAPPING[tol_field]['dwc']

            dwc_field_info = [dwc_field_dict for dwc_field_dict in DWC_FIELDS if dwc_field_dict.get('field','') == dwc_field]
        
            for x in dwc_field_info:               
                data[tol_field] = {'field':dwc_field, 'description': x['description'], 'uri': x['uri']}
                out.update(data)   
          
            continue

        # If the TOL field is not in the DWC field names mapping,
        dwc_field_info = [dwc_field_dict for dwc_field_dict in DWC_FIELDS if tol_field.lower().replace('_','') in dwc_field_dict['field'].lower() or tol_field.lower() in dwc_field_dict['field'].lower()]
        
        if dwc_field_info:
            # The length of the 'dwc_field_info' list should be 1, 
            # to signify that there are no duplicates but still 
            # iterate through the list to get the dictionary
            for x in dwc_field_info:
                data[tol_field] = {'field':x['field'], 'description': x['description'], 'uri':x['uri']}
                out.update(data)
            continue        
        else:
            data[tol_field] = {'field':'', 'description': '', 'uri':''}
            out.update(data)
            continue

    #print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

    return out

DWC_MAPPING = create_tol_dwc_mapping()

#print(f'\nDWC_MAPPING:\n {json.dumps(DWC_MAPPING, indent=4, sort_keys=False,default=str)}\n')

#_______________________________

# Generate 'standards_map.json' file
def generate_standards_map():
    output =list()
    tol_field_dict = dict()

    for tol_field in tol_fields_lst:
        # European Nucleotide Archive (ENA)
        ena = dict()
    
        if tol_field in ENA_FIELD_NAMES_MAPPING:
            ena['field'] = ENA_FIELD_NAMES_MAPPING.get(tol_field, str()).get('ena', str())
            
            ena['type'] = ''
            
            if tol_field in ENA_UNITS_MAPPING:
                ena['unit'] = ENA_UNITS_MAPPING[tol_field]['ena_unit']
            
            ena['uri'] = ''

            if tol_field in ENA_AND_TOL_RULES and 'ena_regex' in list(ENA_AND_TOL_RULES[tol_field].keys()):
                ena['regex'] = ENA_AND_TOL_RULES[tol_field]['ena_regex']
            
            if tol_field in ENA_AND_TOL_RULES and 'human_readable' in list(ENA_AND_TOL_RULES[tol_field].keys()):
                ena['description'] = ENA_AND_TOL_RULES[tol_field]['human_readable']
            else:
                ena['description'] = ''
        else:
            ena = {'field':'', 'uri':'', 'description':''}
        #_____________________
        
        # Minimum Information about any (x) Sequence (MIxS)
        # Terms website:  https://genomicsstandardsconsortium.github.io/mixs/term_list
        mixs = dict()

        if tol_field in list(MIXS_MAPPING.keys()):
            mixs['field'] = MIXS_MAPPING[tol_field]['field'] or str()
            mixs['type'] = MIXS_MAPPING[tol_field]['type'] or str()
            
            if 'uri' in list(MIXS_MAPPING[tol_field].keys()):
                mixs['uri'] = MIXS_MAPPING[tol_field]['uri']
            else:
                mixs['uri'] = ''
            
            if 'regex' in list(MIXS_MAPPING[tol_field].keys()):
                mixs['regex'] = MIXS_MAPPING[tol_field]['regex']

            mixs['description'] = MIXS_MAPPING[tol_field]['description'] or str()
        else:
            mixs = {'field':'','type':'', 'uri':'', 'regex': '', 'description':''}
        
        #_______________________
            
        # Darwin Core (dwc)
        # Terms website: https://dwc.tdwg.org/list/#4-vocabulary
        dwc = dict()

        if tol_field in list(DWC_MAPPING.keys()):
            dwc['field'] = DWC_MAPPING[tol_field]['field'] or str()
        
            if 'type' in list(DWC_MAPPING[tol_field].keys()):
                dwc['type'] = DWC_MAPPING[tol_field]['type']
            
            if 'uri' in list(DWC_MAPPING[tol_field].keys()):
                dwc['uri'] = DWC_MAPPING[tol_field]['uri']
            else:
                dwc['uri'] = ''
            
            if 'regex' in list(DWC_MAPPING[tol_field].keys()):
                dwc['regex'] = DWC_MAPPING[tol_field]['regex']
            
            dwc['description'] = DWC_MAPPING[tol_field]['description'] or str()
        else:
            dwc = {'field':'','type':'', 'uri':'', 'regex': '', 'description':''}

        #_______________________
            
        # Tree of Life (TOL)
        tol = dict()
        tol['field'] = tol_field
        tol['type'] = ''
        tol['uri'] = ''
            
        if tol_field in ENA_AND_TOL_RULES and 'strict_regex' in list(ENA_AND_TOL_RULES[tol_field].keys()):
            ena['regex'] = ENA_AND_TOL_RULES[tol_field]['strict_regex']
            
        if tol_field in ENA_AND_TOL_RULES and 'human_readable' in list(ENA_AND_TOL_RULES[tol_field].keys()):
            ena['description'] = ENA_AND_TOL_RULES[tol_field]['human_readable']
        else:
            ena['description'] = ''  

        # Combine the three dictionaries into one and map it to the TOL field
        tol_field_dict[tol_field] = {'dwc': dwc,'ena': ena, 'mixs': mixs,'tol':tol}
        
    # Avoid duplication by appending outside the for loop        
    output.append(tol_field_dict)

    #print(f'\nStandards_map:\n {json.dumps(output, indent=4, sort_keys=False,default=str)}\n')

    # Return the list of dictionaries i.e. a .json file in
    # the '/copo/common/schema_versions/isa_mappings/' directory
    file_name = 'standards_map.json'
    file_directory = '/copo/common/schema_versions/isa_mappings/'
    file_path = file_directory + file_name

    with open(file_path, 'w+') as f:
        print(json.dumps(output, indent=4, sort_keys=False,default=str), file=f)
        f.close()


if __name__ == '__main__':
    generate_standards_map()