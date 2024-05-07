'''
To generate the 'generate_dwc_ena_mixs_tol_fields_mapping.json' file:
    Run the command below in the 'shared_tools/scripts' directory.
    The command will generate the 'generate_dwc_ena_mixs_tol_fields_mapping.json' file  
    and save it to the '/copo/common/schema_versions/isa_mappings/' directory.

    $  python generate_dwc_ena_mixs_tol_fields_mapping.py
'''
import json
import pandas as pd
import requests

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
        'info': "split COLLECTION_LOCATION on first '|' and put left hand side here (should be country)",
        'ena': 'geographic location (country and/or sea)',
    },
    'COLLECTION_LOCATION_2': {
        'info': "split COLLECTION_LOCATION on first '|' and put right hand side here (should be a list of '|' separated locations)",
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
    'SPECIMEN_ID': {
        'ena': 'specimen_id'
    },
    'TIME_OF_COLLECTION': {
        'ena': 'time of collection'
    },
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
            "strict_regex": "^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12}$",
            "human_readable": "[ID provided by the local context hub]"
        },
    'CHLOROPHYL_A':
        {
            "strict_regex": "^\d+$",
            "human_readable": "integer"
        },
    'COLLECTOR_ORCID_ID':
        {
            "strict_regex": "^((\d{4}-){3}\d{3}(\d|X))(\|(\d{4}-){3}\d{3}(\d|X))*|(^not provided$)|(^not applicable$)",
            "human_readable": "16-digit number that is compatible with the ISO Standard (ISO 27729),  if multiple IDs separate with a | and no spaces"
        },
    'DATE_OF_COLLECTION':
        {
            "ena_regex": "(^[12][0-9]{3}(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01])(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?"
                         "([+-][0-9]{1,2})?)?)?)?(/[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?"
                         "([+-][0-9]{1,2})?)?)?)?)?$)|(^not collected$)|(^not provided$)|(^restricted access$) ",
            "human_readable": "YYYY-MM-DD, YYYY-MM, YYYY, NOT_COLLECTED or NOT_PROVIDED"
        },
    'DECIMAL_LATITUDE':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)",
            "human_readable": "numeric, NOT_COLLECTED or NOT_PROVIDED"
        },
    'DECIMAL_LATITUDE_ERGA':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)",
            "human_readable": "numeric, or NOT_COLLECTED"

        },
    'DECIMAL_LONGITUDE':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)",
            "human_readable": "numeric, NOT_COLLECTED or NOT_PROVIDED"

        },
    'DECIMAL_LONGITUDE_ERGA':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)",
            "human_readable": "numeric, or NOT_COLLECTED"

        },
    'DEPTH':
        {
            "ena_regex": "(0|((0\.)|([1-9][0-9]*\.?))[0-9]*)([Ee][+-]?[0-9]+)?",
            "human_readable": "numeric, or empty string"
        },
    'DISSOLVED_OXYGEN':
        {
            "strict_regex": "^\d+$",
            "human_readable": "integer"
        },
    'ELEVATION':
        {
            "ena_regex": "[+-]?(0|((0\.)|([1-9][0-9]*\.?))[0-9]*)([Ee][+-]?[0-9]+)?",
            "human_readable": "numeric, or empty string"
        },
    'ETHICS_PERMITS_FILENAME':
        {
            "optional_regex": "^.+\.pdf$",
            "human_readable": "filename (including '.pdf' extension) if permit is required or NOT_APPLICABLE if permit is not required"
        },
    'LATITUDE_END':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)",
            "human_readable": "numeric, NOT_COLLECTED or NOT_PROVIDED"
        },
    'LATITUDE_END_ERGA':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)",
            "human_readable": "numeric, or NOT_COLLECTED"

        },
    'LATITUDE_START':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)",
            "human_readable": "numeric, NOT_COLLECTED or NOT_PROVIDED"

        },
    'LATITUDE_START_ERGA':
        {

            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)",
            "human_readable": "numeric, or NOT_COLLECTED"

        },
    'LONGITUDE_END':
        {

            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)",
            "human_readable": "numeric, NOT_COLLECTED or NOT_PROVIDED"

        },
    'LONGITUDE_END_ERGA':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)",
            "human_readable": "numeric, or NOT_COLLECTED"

        },
    'LONGITUDE_START':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)|(^not provided$)|(^restricted access$)",
            "human_readable": "numeric, NOT_COLLECTED or NOT_PROVIDED"
        },
    'LONGITUDE_START_ERGA':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]*$)|(^not collected$)",
            "human_readable": "numeric, or NOT_COLLECTED"

        },
    'NAGOYA_PERMITS_FILENAME':
        {
            "optional_regex": "^.+\.pdf$",
            "human_readable": "filename (including '.pdf' extension) if permit is required or NOT_APPLICABLE if permit is not required"
        },
    'ORIGINAL_COLLECTION_DATE':
        {
            "ena_regex": "^[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?(/[0-9]{"
                         "4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?)?$",
            "human_readable": "Date as YYYY, YYYY-MM or YYYY-MM-DD"
        },
    'ORIGINAL_DECIMAL_LATITUDE':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]{0,8}$)",
            "human_readable": "numeric with 8 decimal places"
        },
    'ORIGINAL_DECIMAL_LONGITUDE':
        {
            "ena_regex": "(^[+-]?[0-9]+.?[0-9]{0,8}$)",
            "human_readable": "numeric with 8 decimal places"
        },
    'RACK_OR_PLATE_ID':
        {
            "optional_regex": "^[a-zA-Z]{2}\d{8}$"
        },
    'SALINITY':
        {
            "strict_regex": "^\d+$",
            "human_readable": "integer"
        },
    'SAMPLE_COORDINATOR_ORCID_ID':
        {
            "strict_regex": "^((\d{4}-){3}\d{3}(\d|X))(\|(\d{4}-){3}\d{3}(\d|X))*$",
            "human_readable": "16-digit number that is compatible with the ISO Standard (ISO 27729), if multiple IDs separate with a | and no spaces"
        },
    'SAMPLE_DERIVED_FROM':
        {
            "ena_regex": "(^[ESD]R[SR]\d{6,}(,[ESD]R[SR]\d{6,})*$)|(^SAM[END][AG]?\d+(,SAM[END][AG]?\d+)*$)|(^EGA[NR]\d{"
                         "11}(,EGA[NR]\d{11})*$)|(^[ESD]R[SR]\d{6,}-[ESD]R[SR]\d{6,}$)|(^SAM[END][AG]?\d+-SAM[END]["
                         "AG]?\d+$)|(^EGA[NR]\d{11}-EGA[NR]\d{11}$)",
            "human_readable": "Specimen accession"
        },
    'SAMPLE_SAME_AS':
        {
            "ena_regex": "(^[ESD]R[SR]\d{6,}(,[ESD]R[SR]\d{6,})*$)|(^SAM[END][AG]?\d+(,SAM[END][AG]?\d+)*$)|(^EGA[NR]\d{"
                         "11}(,EGA[NR]\d{11})*$)|(^[ESD]R[SR]\d{6,}-[ESD]R[SR]\d{6,}$)|(^SAM[END][AG]?\d+-SAM[END]["
                         "AG]?\d+$)|(^EGA[NR]\d{11}-EGA[NR]\d{11}$)",
            "human_readable": "Specimen accession"
        },
    'SAMPLE_SYMBIONT_OF':
        {
            "ena_regex": "(^[ESD]R[SR]\d{6,}(,[ESD]R[SR]\d{6,})*$)|(^SAM[END][AG]?\d+(,SAM[END][AG]?\d+)*$)|(^EGA[NR]\d{"
                         "11}(,EGA[NR]\d{11})*$)|(^[ESD]R[SR]\d{6,}-[ESD]R[SR]\d{6,}$)|(^SAM[END][AG]?\d+-SAM[END]["
                         "AG]?\d+$)|(^EGA[NR]\d{11}-EGA[NR]\d{11}$)",
            "human_readable": "Specimen accession"
        },
    'SAMPLING_PERMITS_FILENAME':
        {
            "optional_regex": "^.+\.pdf$",
            "human_readable": "filename (including '.pdf' extension) if permit is required or NOT_APPLICABLE if permit is not required"
        },
    'SAMPLING_WATER_BODY_DEPTH':
        {
            "strict_regex": "^\d+$",
            "human_readable": "integer"
        },
    'TEMPERATURE':
        {
            "strict_regex": "^\d+$",
            "human_readable": "integer"
        },
    'TIME_OF_COLLECTION':
        {
            "strict_regex": "^([0-1][0-9]|2[0-4]):[0-5]\d$",
            "human_readable": "24-hour format with hours and minutes separated by colon"
        },
    'TUBE_OR_WELL_ID':
        {
            "optional_regex": "^[a-zA-Z]{2}\d{8}$"
        },
    'WATER_SPEED':
        {
            "strict_regex": "^\d+$",
            "human_readable": "integer"
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
tol_fields_lst = [
    "ASSOCIATED_BIOGENOME_PROJECTS",
    "ASSOCIATED_PROJECT_ACCESSIONS",
    "ASSOCIATED_TRADITIONAL_KNOWLEDGE_CONTACT",
    "ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID",
    "ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE",
    "BARCODE_HUB",
    "BARCODE_PLATE_PRESERVATIVE",
    "BARCODING_STATUS",
    "BIOBANKED_TISSUE_PRESERVATIVE",
    "CHLA",
    "COLLECTED_BY",
    "COLLECTION_LOCATION",
    "COLLECTOR_AFFILIATION",
    "COLLECTOR_ORCID_ID",
    "COLLECTOR_SAMPLE_ID",
    "COMMON_NAME",
    "CULTURE_OR_STRAIN_ID",
    "DATE_OF_COLLECTION",
    "DATE_OF_PRESERVATION",
    "DECIMAL_LATITUDE",
    "DECIMAL_LONGITUDE",
    "DEPTH",
    "DESCRIPTION_OF_COLLECTION_METHOD",
    "DIFFICULT_OR_HIGH_PRIORITY_SAMPLE",
    "DISOLVED_OXYGEN",
    "DNA_REMOVED_FOR_BIOBANKING",
    "DNA_VOUCHER_ID_FOR_BIOBANKING",
    "ELEVATION",
    "ETHICS_PERMITS_DEF",
    "ETHICS_PERMITS_FILENAME",
    "ETHICS_PERMITS_REQUIRED",
    "FAMILY",
    "GAL",
    "GAL_SAMPLE_ID",
    "GENUS",
    "GRID_REFERENCE",
    "HABITAT",
    "HAZARD_GROUP",
    "IDENTIFIED_BY",
    "IDENTIFIED_HOW",
    "IDENTIFIER_AFFILIATION",
    "INDIGENOUS_RIGHTS_DEF",
    "INFRASPECIFIC_EPITHET",
    "LATITUDE_END",
    "LATITUDE_START",
    "LIFESTAGE",
    "LIGHT_INTENSITY",
    "LONGITUDE_END",
    "LONGITUDE_START",
    "MIXED_SAMPLE_RISK",
    "NAGOYA_PERMITS_DEF",
    "NAGOYA_PERMITS_FILENAME",
    "NAGOYA_PERMITS_REQUIRED",
    "ORDER_OR_GROUP",
    "ORGANISM_PART",
    "ORIGINAL_COLLECTION_DATE",
    "ORIGINAL_DECIMAL_LATITUDE",
    "ORIGINAL_DECIMAL_LONGITUDE",
    "ORIGINAL_GEOGRAPHIC_LOCATION",
    "OTHER_INFORMATION",
    "PH",
    "PLATE_ID_FOR_BARCODING",
    "PRESERVATION_APPROACH",
    "PRESERVATIVE_SOLUTION",
    "PRESERVED_BY",
    "PRESERVER_AFFILIATION",
    "PRIMARY_BIOGENOME_PROJECT",
    "PROXY_TISSUE_VOUCHER_ID_FOR_BIOBANKING",
    "PROXY_VOUCHER_ID",
    "PROXY_VOUCHER_LINK",
    "PURPOSE_OF_SPECIMEN",
    "RACK_OR_PLATE_ID",
    "REGULATORY_COMPLIANCE",
    "RELATIONSHIP",
    "SALINITY",
    "SAMPLE_COORDINATOR",
    "SAMPLE_COORDINATOR_AFFILIATION",
    "SAMPLE_COORDINATOR_ORCID_ID",
    "SAMPLE_FORMAT",
    "SAMPLING_PERMITS_DEF",
    "SAMPLING_PERMITS_FILENAME",
    "SAMPLING_PERMITS_REQUIRED",
    "SCIENTIFIC_NAME",
    "SERIES",
    "SEX",
    "SIZE_OF_TISSUE_IN_TUBE",
    "SPECIES_RARITY",
    "SPECIMEN_ID",
    "SPECIMEN_IDENTITY_RISK",
    "SYMBIONT",
    "TAXON_ID",
    "TAXON_REMARKS",
    "TEMPERATURE",
    "TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION",
    "TIME_OF_COLLECTION",
    "TISSUE_FOR_BARCODING",
    "TISSUE_FOR_BIOBANKING",
    "TISSUE_REMOVED_FOR_BARCODING",
    "TISSUE_REMOVED_FOR_BIOBANKING",
    "TISSUE_VOUCHER_ID_FOR_BIOBANKING",
    "TUBE_OR_WELL_ID",
    "TUBE_OR_WELL_ID_FOR_BARCODING",
    "VOUCHER_ID",
    "VOUCHER_INSTITUTION",
    "VOUCHER_LINK",

    # "associated_tol_project",
    # "biosampleAccession",
    # "date_created",
    # "date_modified",
    # "manifest_id",
    # "manifest_version",
    # "profile_id",
    # "public_name",
    # "rack_tube",
    # "sampleDerivedFrom",
    # "sample_type",
    # "species_list",
    # "sraAccession",
    # "status",
    # "submissionAccession",
    # "time_created",
    # "time_updated",
    # "tol_project",
    # "update_type",
    # "updated_by",
]

#_______________________

# Helpers for MIxS field mappings
MIXS_FIELD_NAMES_MAPPING = {
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
    'ELEVATION ': {
        'mixs': 'elev'
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
    'SIZE_OF_TISSUE_IN_TUBE': {
        'mixs': 'size_frac'
    },
    'SEX': {
        'mixs':'urobiom_sex'
    },
    'TAXON_ID':
    {
        'mixs':'samp_taxon_id'
    },
    'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION': {
        'mixs': 'timepoint' #samp_transport_cond #samp_store_dur
    },
    'TEMPERATURE': {
        'mixs': 'samp_store_temp'
    },
    'TUBE_OR_WELL_ID': {
        'mixs': 'samp_well_name'
    },
}

mixs_fields_uri_lst ={
    "HACCP_term": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "IFSAC_category": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "abs_air_humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "adapters": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "add_recov_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "additional_info": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "address": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "adj_room": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "adjacent_environment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "aero_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "agrochem_addition": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "air_PM_concen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "air_flow_impede": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "air_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "air_temp_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "al_sat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "al_sat_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "alkalinity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "alkalinity_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "alkyl_diethers": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "alt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "aminopept_act": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ammonium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "amniotic_fluid_color": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "amount_light": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ances_data": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "anim_water_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_am": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_am_dur": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_am_freq": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_am_route": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_am_use": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_body_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_diet": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_feed_equip": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_group_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_housing": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_intrusion": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "animal_sex": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "annot": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "annual_precpt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "annual_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "antibiotic_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "api": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "arch_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "area_samp_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "aromatics_pc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "asphaltenes_pc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "assembly_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "assembly_qual": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "assembly_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "associated_resource": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "association_duration": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "atmospheric_data": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "avg_dew_point": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "avg_occup": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "avg_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bac_prod": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bac_resp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bacteria_carb_prod": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bacterial_density": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "barometric_press": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "basin": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bathroom_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bedroom_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "benzene": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bin_param": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bin_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biochem_oxygen_dem": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biocide": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biocide_admin_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biocide_used": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biol_stat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biomass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biotic_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "biotic_relationship": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "birth_control": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bishomohopanol": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "blood_blood_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "blood_press_diast": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "blood_press_syst": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "bromide": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "build_docs": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "build_occup_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "building_setting": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "built_struc_age": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "built_struc_set": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "built_struc_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "calcium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "carb_dioxide": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "carb_monoxide": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "carb_nitro_ratio": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_area": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_finish_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_texture": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_thermal_mass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ceil_water_mold": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chem_administration": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chem_mutagen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chem_oxygen_dem": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chem_treat_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chem_treatment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chimera_check": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chloride": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "chlorophyll": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "climate_environment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "coll_site_geo_feat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "collection_date": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "compl_appr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "compl_score": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "compl_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "conduc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cons_food_stor_dur": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cons_food_stor_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cons_purch_date": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cons_qty_purchased": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "contam_score": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "contam_screen_input": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "contam_screen_param": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cool_syst_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "crop_rotation": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "crop_yield": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cult_isol_date": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cult_result": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cult_result_org": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cult_root_med": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cult_target": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cur_land_use": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cur_vegetation": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "cur_vegetation_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "date_extr_weath": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "date_last_rain": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "decontam_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "density": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "depos_env": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "depth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "dermatology_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "detec_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "dew_point": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diet_last_six_month": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "dietary_claim_use": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diether_lipids": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_carb_dioxide": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_hydrogen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_inorg_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_inorg_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_inorg_phosp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_iron": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_org_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_org_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_oxygen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "diss_oxygen_fluid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "dominant_hand": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_comp_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_direct": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_move": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_type_metal": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_type_wood": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "door_water_mold": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "douche": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "down_par": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "drainage_class": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "drawings": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "drug_usage": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "efficiency_percent": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "elev": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "elevator": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "emulsions": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "encoded_traits": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "enrichment_protocol": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "env_broad_scale": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "env_local_scale": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "env_medium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "env_monitoring_zone": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "escalator": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "estimated_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ethnicity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ethylbenzene": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "exp_duct": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "exp_pipe": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "experimental_factor": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ext_door": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ext_wall_orient": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ext_window_orient": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "extr_weather_event": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "extrachrom_elements": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "extreme_event": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "facility_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fao_class": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "farm_equip": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "farm_equip_san": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "farm_equip_san_freq": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "farm_equip_shared": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "farm_water_source": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "feat_pred": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_chem_add": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_chem_add_perc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_headspace_oxy": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_medium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_pH": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_rel_humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_time": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ferm_vessel": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fertilizer_admin": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fertilizer_date": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fertilizer_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "field": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "filter_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fire": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fireplace_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "flooding": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_age": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_area": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_finish_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_thermal_mass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "floor_water_mold": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fluor": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "foetal_health_stat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_additive": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_allergen_label": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_clean_proc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_contact_surf": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_contain_wrap": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_cooking_proc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_dis_point": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_dis_point_city": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_harvest_proc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_ingredient": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_name_status": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_origin": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_pack_capacity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_pack_integrity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_pack_medium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_preserv_proc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_prior_contact": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_prod": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_prod_char": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_prod_synonym": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_product_qual": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_product_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_quality_date": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_source": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_source_age": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_trace_list": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_trav_mode": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_trav_vehic": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "food_treat_proc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "freq_clean": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "freq_cook": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "fungicide_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "furniture": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gaseous_environment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gaseous_substances": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gastrointest_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gender_restroom": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "genetic_mod": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "geo_loc_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gestation_state": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "glucosidase_act": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gravidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gravity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "growth_facil": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "growth_habit": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "growth_hormone_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "growth_medium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "gynecologic_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hall_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "handidness": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hc_produced": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hcr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hcr_fw_salinity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hcr_geol_age": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hcr_pressure": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hcr_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "heat_cool_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "heat_deliv_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "heat_sys_deliv_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "heat_system_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "heavy_metals": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "heavy_metals_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "height_carper_fiber": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "herbicide_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "horizon_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_age": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_body_habitat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_body_mass_index": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_body_product": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_body_site": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_body_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_cellular_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_color": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_common_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_dependence": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_diet": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_disease_stat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_dry_mass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_fam_rel": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_genotype": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_growth_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_height": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_hiv_stat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_infra_spec_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_infra_spec_rank": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_last_meal": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_length": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_life_stage": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_number": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_occupation": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_coinf": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_disease": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_env_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_env_med": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_fam_rel": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_geno": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_gravid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_infname": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_infrank": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_pheno": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_sub_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_taxid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_of_host_totmass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_phenotype": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_pred_appr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_pred_est_acc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_pulse": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_sex": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_shape": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_spec_range": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_specificity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_subject_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_subspecf_genlin": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_substrate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_symbiont": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_taxid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_tot_mass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "host_wet_mass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hrt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "humidity_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hygienic_area": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "hysterectomy": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ihmc_medication_code": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "indoor_space": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "indoor_surf": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "indust_eff_percent": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "inorg_particles": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "inside_lux": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "int_wall_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "intended_consumer": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "isol_growth_condt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "iw_bt_date_well": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "iwf": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "kidney_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "last_clean": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lat_lon": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lib_layout": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lib_reads_seqd": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lib_screen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lib_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lib_vector": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "library_prep_kit": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "light_intensity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "light_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "light_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "link_addit_analys": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "link_class_info": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "link_climate_info": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lithology": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "liver_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "local_class": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "local_class_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "lot_number": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mag_cov_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "magnesium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "maternal_health_stat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "max_occup": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mean_frict_vel": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mean_peak_frict_vel": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mech_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mechanical_damage": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "medic_hist_perform": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "menarche": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "menopause": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "methane": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "micro_biomass_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_cult_med": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_start": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_start_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_start_inoc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_start_prep": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_start_source": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microb_start_taxID": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "microbial_biomass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mineral_nutr_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "misc_param": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "mode_transmission": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "n_alkanes": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "neg_cont_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nitrate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nitrite": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "non_min_nutr_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nose_mouth_teeth_throat_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nose_throat_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nucl_acid_amp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nucl_acid_ext": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "nucl_acid_ext_kit": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "num_replicons": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "num_samp_collect": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "number_contig": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "number_pets": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "number_plants": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "number_resident": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "occup_density_samp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "occup_document": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "occup_samp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "org_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "org_count_qpcr_info": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "org_matter": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "org_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "org_particles": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "organism_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "otu_class_appr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "otu_db": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "otu_seq_comp_appr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "owc_tvdss": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "oxy_stat_samp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "oxygen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "part_org_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "part_org_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "part_plant_animal": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "particle_class": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pathogenicity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pcr_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pcr_primers": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "permeability": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "perturbation": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pesticide_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pet_farm_animal": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "petroleum_hydrocarb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ph": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ph_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ph_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "phaeopigments": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "phosphate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "phosplipid_fatt_acid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "photon_flux": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "photosynt_activ": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "photosynt_activ_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_growth_med": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_part_maturity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_product": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_reprod_crop": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_sex": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "plant_water_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ploidy": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pollutants": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pool_dna_extracts": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "porosity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pos_cont_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "potassium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pour_point": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pre_treatment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pred_genome_struc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pred_genome_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pregnancy": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pres_animal_insect": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pressure": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "prev_land_use_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "previous_land_use": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "primary_prod": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "primary_treatment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "prod_label_claims": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "prod_rate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "prod_start_date": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "profile_position": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "project_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "propagation": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "pulmonary_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "quad_pos": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "radiation_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "rainfall_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "reactor_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "reassembly_bin": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "redox_potential": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ref_biomaterial": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ref_db": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "rel_air_humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "rel_humidity_out": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "rel_location": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "rel_samp_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "rel_to_oxygen": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "repository_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "reservoir": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "resins_pc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_air_exch_rate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_architec_elem": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_condt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_connected": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_dim": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_door_dist": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_door_share": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_hallway": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_moist_dam_hist": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_net_area": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_occup": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_samp_pos": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_vol": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_wall_share": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "room_window_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_carbon": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_macronutr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_micronutr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_ph": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_regl": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_solid": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "root_med_suppl": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "route_transmission": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "salinity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "salt_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_capt_status": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_collect_device": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_collect_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_collect_point": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_dis_stage": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_floor": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_loc_condition": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_loc_corr_rate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_mat_process": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_md": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_pooling": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_preserv": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_purpose": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_rep_biol": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_rep_tech": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_room_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_sort_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_source_mat_cat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_stor_device": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_stor_media": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_store_dur": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_store_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_store_sol": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_store_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_subtype": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_surf_moisture": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_taxon_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_time_out": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_transport_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_transport_cont": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_transport_dur": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_transport_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_tvdss": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_vol_we_dna_ext": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_weather": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "samp_well_name": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "saturates_pc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sc_lysis_approach": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sc_lysis_method": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "season": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "season_environment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "season_humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "season_precpt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "season_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "season_use": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "secondary_treatment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sediment_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "seq_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "seq_quality_check": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sequencing_kit": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sequencing_location": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "serovar_or_serotype": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sewage_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sexual_act": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "shad_dev_water_mold": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "shading_device_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "shading_device_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "shading_device_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "shading_device_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sieving": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "silicate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sim_search_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "size_frac": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "size_frac_low": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "size_frac_up": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "slope_aspect": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "slope_gradient": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sludge_retent_time": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "smoker": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sodium": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_conductivity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_cover": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_horizon": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_pH": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_porosity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_texture": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_texture_class": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_texture_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soil_type_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "solar_irradiance": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soluble_inorg_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soluble_org_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "soluble_react_phosp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sop": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sort_tech": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "source_mat_id": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "source_uvig": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "space_typ_state": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spec_intended_cons": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "special_diet": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "specific": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "specific_host": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "specific_humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_AMR": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_antibiotic": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_count": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_growth_med": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_metal": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_org": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_serovar": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "spikein_strain": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sr_dep_env": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sr_geol_age": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sr_kerog_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sr_lithology": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "standing_water_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ster_meth_samp_room": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "store_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "study_complt_stat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "study_design": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "study_inc_dur": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "study_inc_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "study_timecourse": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "study_tmnt": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "subspecf_gen_lin": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "substructure_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sulfate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sulfate_fw": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sulfide": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "surf_air_cont": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "surf_humidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "surf_material": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "surf_moisture": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "surf_moisture_ph": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "surf_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "suspend_part_matter": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "suspend_solids": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "sym_life_cycle_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "symbiont_host_role": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tan": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "target_gene": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "target_subfragment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tax_class": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tax_ident": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "temp_out": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tertiary_treatment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tidal_stage": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tillage": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "time_last_toothbrush": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "time_since_last_wash": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "timepoint": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tiss_cult_growth_med": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "toluene": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_depth_water_col": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_diss_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_inorg_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_iron": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_nitro": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_nitro_cont_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_nitro_content": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_org_c_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_org_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_part_carb": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_phosp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_phosphate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tot_sulfur": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "train_line": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "train_stat_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "train_stop_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "travel_out_six_month": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "trna_ext_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "trnas": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "trophic_level": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "turbidity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tvdss_of_hcr_press": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "tvdss_of_hcr_temp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "twin_sibling": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "typ_occup_density": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "type_of_symbiosis": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "urine_collect_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "urobiom_sex": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "urogenit_disord": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "urogenit_tract_disor": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ventilation_rate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "ventilation_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "vfa": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "vfa_fw": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "vir_ident_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "virus_enrich_appr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "vis_media": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "viscosity": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "volatile_org_comp": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_area": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_const_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_finish_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_height": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_surf_treatment": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_texture": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_thermal_mass": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wall_water_mold": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wastewater_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_cont_soil_meth": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_content": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_current": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_cut": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_feat_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_feat_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_frequency": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_pH": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_prod_rate": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_source_adjac": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_source_shared": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "water_temp_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "watering_regm": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "weekday": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "weight_loss_3_month": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wga_amp_appr": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wga_amp_kit": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "win": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wind_direction": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "wind_speed": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_cond": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_cover": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_horiz_pos": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_loc": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_mat": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_open_freq": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_size": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_status": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_type": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_vert_pos": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "window_water_mold": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "x16s_recover": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "x16s_recover_software": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    },
    "xylene": {
        "uri": "https://genomicsstandardsconsortium.github.io/mixs/0001215"
    }
}

def remove_duplicates_from_json(json_list):
    out = list()
    for i in json_list:
        if i not in out:
            out.append(i)
    return out

def generate_mixs_fields_json():
    out = list()
    mixs_json = list()

    # Get MiXS json schema from GitHub
    mixs_json_schema_url =  "https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/project/jsonschema/mixs.schema.json"

    try:
        # Read the json schema from the URL and load the json file
        resp = requests.get(mixs_json_schema_url)
        mixs_json_file = json.loads(resp.text)

        # Remove the $defs key from the dictionary
        # and create a list of dictionaries
        mixs_dict_def = mixs_json_file.get('$defs','')
        
        # Rearrange the list of dictionaries and retrieve only the 
        # field name, 'description', 'type' and 'pattern' keys from the MIxS json
        # NB: 'pattern' in the json schema is known as 'regex' in the TOL schema
        data = dict()
        #mixs_json = [{'field':key, 'type': value_dict.get('type',''), 'regex': value_dict.get('pattern',''), 'description': value_dict.get('description','')} for key, value_dict in mixs_dict_def.items()]
        
        for key, value_dict in mixs_dict_def.items():
            if not value_dict.get('properties',''):
                data = {'field':key, 'type': value_dict.get('type',''), 'regex': value_dict.get('pattern',''), 'description': value_dict.get('description','')}
                
                mixs_json.append(data)

            if value_dict.get('properties',''):
                for k, v in value_dict.get('properties','').items():
                    data = {'field':k, 'type': v.get('type',''), 'regex': v.get('pattern',''), 'description': v.get('description','')}
            
                    mixs_json.append(data)
        
        # Remove duplicates from the json file
        mixs_json = list(remove_duplicates_from_json(mixs_json))

        # Uncomment the following lines to generate the MIxS json schema to a file
        # in the '/copo/common/schema_versions/isa_mappings/' directory
        '''
        file_name = "mixs_fields.json"
        file_directory = "/copo/common/schema_versions/isa_mappings/"
        file_path = file_directory + file_name

        with open(file_path, 'w+') as f:
            print(json.dumps(mixs_json, indent=4, sort_keys=False,default=str), file=f)
            f.close()
        '''

        out = mixs_json
        
    except Exception as e:
        print(f'Error: {e}')  


    return out

'''
Uncomment the following line to generate the MIxS json schema 
from the 'generate_mixs_fields_json' function if the code to generate file has been
uncommented in the 'generate_mixs_fields_json' function
Then, comment the succeeding line, 'MIXS_FIELDS = generate_mixs_fields_json()'
'''
# MIXS_FIELDS = json.load(open('/copo/common/schema_versions/isa_mappings/mixs_fields.json'))
MIXS_FIELDS = generate_mixs_fields_json()

# print(f'\MIXS_FIELDS:\n {json.dumps(MIXS_FIELDS, indent=4, sort_keys=False,default=str)}\n')

# Create a mapping between the TOL and MIxS fields 
# MIXS_MAPPING = {tol_field: {'field':mixs_field['name'], 'description': mixs_field['name'], 'uri':mixs_field['uri']} for mixs_field in MIXS_FIELDS for tol_field in tol_fields_lst if tol_field.lower() in mixs_field['name']}
def create_tol_mixs_mapping():
    data = dict()
    out = dict()

    for tol_field in tol_fields_lst:
        # Check if the TOL field is in the MIxS field names mapping
        if tol_field in MIXS_FIELD_NAMES_MAPPING:
            mixs_field = MIXS_FIELD_NAMES_MAPPING[tol_field]['mixs']
            mixs_field_uri = mixs_fields_uri_lst.get(mixs_field,str()).get('uri', str())

            mixs_field_info = [mixs_field_dict for mixs_field_dict in MIXS_FIELDS if mixs_field_dict.get('field','') == mixs_field]
        
            for x in mixs_field_info:
                data[tol_field] = {'field':mixs_field, 'type': x['type'], 'regex': x['regex'], 'description': x['description'], 'uri': mixs_field_uri}
                out.update(data)   
          
            continue

        # If the TOL field is not in the MIXS field names mapping,
        mixs_field_info = [mixs_field_dict for mixs_field_dict in MIXS_FIELDS if tol_field.lower().replace('_','') in mixs_field_dict['field'].lower() or tol_field.lower() in mixs_field_dict['field'].lower()]
        
        if mixs_field_info:
            # The length of the 'mixs_field_info' list should be 1, 
            # to signify that there are no duplicates but still 
            # iterate through the list to get the dictionary
            for x in mixs_field_info:
                data[tol_field] = {'field':x['field'], 'type': x['type'], 'regex': x['regex'], 'description': x['description']}
                out.update(data)
            continue         
        else:
            data[tol_field] = {'field':"", 'type': '', 'regex': '', 'description': ""}
            out.update(data)
            continue

    #print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

    return out

MIXS_MAPPING = create_tol_mixs_mapping()

#print(f'\nMIXS_MAPPING:\n {json.dumps(MIXS_MAPPING, indent=4, sort_keys=False,default=str)}\n')

#___________________________________________________________________

# Helpers for Darwin Core field mappings
DWC_FIELD_NAMES_MAPPING = {
    'COLLECTION_LOCATION': {
        'dwc': 'higherGeography'
    },
        'COLLECTED_BY': {
        'dwc': 'recordedBy'
    },
     'COLLECTOR_AFFILIATION': {
        'dwc': 'collectionCode'
    },
    'COLLECTOR_ORCID_ID': {
        'dwc': 'recordedByID'
    },
    'COMMON_NAME': {
        'dwc': 'vernacularName'
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
        'dwc': 'Organism'
    },
    'ORIGINAL_COLLECTION_DATE': {
        'dwc': 'eventDate'
    },
    'PRESERVATION_APPROACH': {
        'dwc': 'Preparations'
    },
    'SAMPLE_COORDINATOR_AFFILIATION': {
        'dwc': 'institutionCode'
    },
    'SCIENTIFIC_NAME': {
        'dwc': 'scientificName'
    },
    'SERIES': {
        'dwc': 'FieldNumber'
    },
    'TIME_OF_COLLECTION': {
        'dwc': 'eventTime'
    },
}

def generate_dwc_fields_json():
    out = list()

    # Get DWC csv schema link from GitHub
    dwc_csv_schema_url =  "https://raw.githubusercontent.com/tdwg/dwc/master/vocabulary/term_versions.csv"
    # Latest dwc term version (2023-09-18): https://rs.tdwg.org/dwc/version/terms/2023-09-18.json

    try:
        # Read the csv schema from the URL
        df = pd.read_csv(dwc_csv_schema_url)

        ''' 
        Get DWC term versions based on the following criteria:
         - latest term versions (based on the 'issued' date column)
         - terms that are not deprecated ('status' != 'deprecated)
         - terms' uri that are from the DWC namespace ('term_iri' starts with 'http://rs.tdwg.org/dwc/terms/')
        
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
        
        # Uncomment the following lines to write the DWC json schema to a file
        # in the '/copo/common/schema_versions/isa_mappings/' directory
        '''
        file_name = "dwc_fields.json"
        file_directory = "/copo/common/schema_versions/isa_mappings/"
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
Uncomment the following line to generate the DWC json schema 
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
                data[tol_field] = {'field':dwc_field, 'description': x['description'], 'uri':x['uri']}
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
            data[tol_field] = {'field':"", 'description': "", 'uri':""}
            out.update(data)
            continue

    #print(f'\out:\n {json.dumps(out, indent=4, sort_keys=False,default=str)}\n')

    return out

DWC_MAPPING = create_tol_dwc_mapping()

#print(f'\nDWC_MAPPING:\n {json.dumps(DWC_MAPPING, indent=4, sort_keys=False,default=str)}\n')

#_______________________________

# Generate 'dwc_ena_mixs_tol_fields_mapping.json' file
output =list()

for tol_field in tol_fields_lst:
    tol_field_dict = dict()
    # European Nucleotide Archive (ENA)
    ena = dict()
  
    if tol_field in ENA_FIELD_NAMES_MAPPING:
        ena["field"] = ENA_FIELD_NAMES_MAPPING.get(tol_field, str()).get("ena", str())
        
        ena["type"] = ""
        
        if tol_field in ENA_UNITS_MAPPING:
            ena["unit"] = ENA_UNITS_MAPPING[tol_field]["ena_unit"]
        
        ena["uri"] = ""

        if tol_field in ENA_AND_TOL_RULES and "ena_regex" in list(ENA_AND_TOL_RULES[tol_field].keys()):
            ena["regex"] = ENA_AND_TOL_RULES[tol_field]["ena_regex"]
        
        if tol_field in ENA_AND_TOL_RULES and "human_readable" in list(ENA_AND_TOL_RULES[tol_field].keys()):
            ena["description"] = ENA_AND_TOL_RULES[tol_field]["human_readable"]
        else:
            ena["description"] = ""

    #_____________________
    
    # Minimum Information about any (X) Sequence (MIxs)
    # Terms website:  https://genomicsstandardsconsortium.github.io/mixs/term_list/
    mixs = dict()

    if tol_field in list(MIXS_MAPPING.keys()):
        mixs["field"] = MIXS_MAPPING[tol_field]["field"] or str()
        mixs["type"] = MIXS_MAPPING[tol_field]["type"] or str()
        
        if "uri" in list(MIXS_MAPPING[tol_field].keys()):
            mixs["uri"] = MIXS_MAPPING[tol_field]["uri"]
        else:
            mixs["uri"] = ""
        
        if "regex" in list(MIXS_MAPPING[tol_field].keys()):
            mixs["regex"] = MIXS_MAPPING[tol_field]["regex"]

        mixs["description"] = MIXS_MAPPING[tol_field]["description"] or str()
    else:
        mixs = {"field":"","type":"", "uri":"", "regex": "", "description":""}
    
    #_______________________
        
    # Darwin Core (dwc)
    # Terms website: https://dwc.tdwg.org/list/#4-vocabulary
    dwc = dict()

    if tol_field in list(DWC_MAPPING.keys()):
        dwc["field"] = DWC_MAPPING[tol_field]["field"] or str()
      
        if "type" in list(DWC_MAPPING[tol_field].keys()):
            dwc["type"] = DWC_MAPPING[tol_field]["type"]
        
        if "uri" in list(DWC_MAPPING[tol_field].keys()):
            dwc["uri"] = DWC_MAPPING[tol_field]["uri"]
        else:
            dwc["uri"] = ""
        
        if "regex" in list(DWC_MAPPING[tol_field].keys()):
            dwc["regex"] = DWC_MAPPING[tol_field]["regex"]
        
        dwc["description"] = DWC_MAPPING[tol_field]["description"] or str()
    else:
        dwc = {"field":"","type":"", "uri":"", "regex": "", "description":""}

    #_______________________
        
    # Tree of Life (TOL)
    tol = dict()
    tol["field"] = tol_field
    tol["type"] = ""
    tol["uri"] = ""
        
    if tol_field in ENA_AND_TOL_RULES and "strict_regex" in list(ENA_AND_TOL_RULES[tol_field].keys()):
        ena["regex"] = ENA_AND_TOL_RULES[tol_field]["strict_regex"]
        
    if tol_field in ENA_AND_TOL_RULES and "human_readable" in list(ENA_AND_TOL_RULES[tol_field].keys()):
        ena["description"] = ENA_AND_TOL_RULES[tol_field]["human_readable"]
    else:
        ena["description"] = ""  


    # Combine the three dictionaries into one and map it to the TOL field
    tol_field_dict[tol_field] = {"dwc": dwc,"ena": ena,"mixs":mixs,"tol":tol}
        
    output.append(tol_field_dict)

#print(f'\ndwc_ena_mixs_tol_fields_mapping:\n {json.dumps(output, indent=4, sort_keys=False,default=str)}\n')

# Return the list of dictionaries i.e. a .json file in
# the '/copo/common/schema_versions/isa_mappings/' directory
file_name = "dwc_ena_mixs_tol_fields_mapping.json"
file_directory = "/copo/common/schema_versions/isa_mappings/"
file_path = file_directory + file_name

with open(file_path, 'w+') as f:
    print(json.dumps(output, indent=4, sort_keys=False,default=str), file=f)
    f.close()