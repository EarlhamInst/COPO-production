'''
To generate the 'generate_manifest_type_fields.json' file:
    Run the command below in the 'shared_tools/scripts' directory.
    The command will generate the 'generate_manifest_type_fields.json' file  
    and save it to the '/copo/common/schema_versions/isa_mappings/' directory.

    $  python generate_manifest_type_fields.py
'''
from openpyxl.utils.cell import get_column_letter, column_index_from_string
from operator import itemgetter
from itertools import groupby
import json
import pandas as pd
import warnings

# Ignore the warning: UserWarning: Unknown extension is not supported and will be removed warn(msg)
warnings.simplefilter("ignore")

# Helpers
DTOL_ENUMS = {
    'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE':
        [
            'Y',
            'N'
        ],
    'BARCODE_HUB': [
        'MARINE BIOLOGICAL ASSOCIATION',
        'NATURAL HISTORY MUSEUM',
        'ROYAL BOTANIC GARDEN EDINBURGH',
        'ROYAL BOTANIC GARDENS KEW/NATURAL HISTORY MUSEUM',
        'UNIVERSITY OF OXFORD',
        'NOT_COLLECTED',
        'NOT_PROVIDED'
        ],
    'BARCODING_STATUS': [
        'DNA_BARCODING_COMPLETED',
        'DNA_BARCODE_EXEMPT',
        'DNA_BARCODING_FAILED',
        'DNA_BARCODING_TO_BE_PERFORMED_GAL',
        'DNA_BARCODING_VIA_WSI_PROCESS'
        ],
    'CELL_NUMBER': [
        '1',
        '2-10',
        '11-50',
        '51-100',
        '101-10000',
        '10001-50000',
        '50001-100000',
        '100001-500000',
        '500001-1000000',
        '1000000+'
        ],
    'COLLECTION_LOCATION':
        [
            'AFGHANISTAN',
            'ALBANIA',
            'ALGERIA',
            'AMERICAN SAMOA',
            'ANDORRA',
            'ANGOLA',
            'ANGUILLA',
            'ANTARCTICA',
            'ANTIGUA AND BARBUDA',
            'ARCTIC OCEAN',
            'ARGENTINA',
            'ARMENIA',
            'ARUBA',
            'ASHMORE AND CARTIER ISLANDS',
            'ATLANTIC OCEAN',
            'AUSTRALIA',
            'AUSTRIA',
            'AZERBAIJAN',
            'BAHAMAS',
            'BAHRAIN',
            'BAKER ISLAND',
            'BALTIC SEA',
            'BANGLADESH',
            'BARBADOS',
            'BASSAS DA INDIA',
            'BELARUS',
            'BELGIUM',
            'BELIZE',
            'BENIN',
            'BERMUDA',
            'BHUTAN',
            'BOLIVIA',
            'BORNEO',
            'BOSNIA AND HERZEGOVINA',
            'BOTSWANA',
            'BOUVET ISLAND',
            'BRAZIL',
            'BRITISH VIRGIN ISLANDS',
            'BRUNEI',
            'BULGARIA',
            'BURKINA FASO',
            'BURUNDI',
            'CAMBODIA',
            'CAMEROON',
            'CANADA',
            'CAPE VERDE',
            'CAYMAN ISLANDS',
            'CENTRAL AFRICAN REPUBLIC',
            'CHAD',
            'CHILE',
            'CHINA',
            'CHRISTMAS ISLAND',
            'CLIPPERTON ISLAND',
            'COCOS ISLANDS',
            'COLOMBIA',
            'COMOROS',
            'COOK ISLANDS',
            'CORAL SEA ISLANDS',
            'COSTA RICA',
            "COTE D'IVOIRE",
            'CROATIA',
            'CUBA',
            'CURACAO',
            'CYPRUS',
            'CZECH REPUBLIC',
            'DEMOCRATIC REPUBLIC OF THE CONGO',
            'DENMARK',
            'DJIBOUTI',
            'DOMINICA',
            'DOMINICAN REPUBLIC',
            'EAST TIMOR',
            'ECUADOR',
            'EGYPT',
            'EL SALVADOR',
            'EQUATORIAL GUINEA',
            'ERITREA',
            'ESTONIA',
            'ETHIOPIA',
            'EUROPA ISLAND',
            'FALKLAND ISLANDS (ISLAS MALVINAS)',
            'FAROE ISLANDS',
            'FIJI',
            'FINLAND',
            'FRANCE',
            'FRENCH GUIANA',
            'FRENCH POLYNESIA',
            'FRENCH SOUTHERN AND ANTARCTIC LANDS',
            'GABON',
            'GAMBIA',
            'GAZA STRIP',
            'GEORGIA',
            'GERMANY',
            'GHANA',
            'GIBRALTAR',
            'GLORIOSO ISLANDS',
            'GREECE',
            'GREENLAND',
            'GRENADA',
            'GUADELOUPE',
            'GUAM',
            'GUATEMALA',
            'GUERNSEY',
            'GUINEA',
            'GUINEA-BISSAU',
            'GUYANA',
            'HAITI',
            'HEARD ISLAND AND MCDONALD ISLANDS',
            'HONDURAS',
            'HONG KONG',
            'HOWLAND ISLAND',
            'HUNGARY',
            'ICELAND',
            'INDIA',
            'INDIAN OCEAN',
            'INDONESIA',
            'IRAN',
            'IRAQ',
            'IRELAND',
            'ISLE OF MAN',
            'ISRAEL',
            'ITALY',
            'JAMAICA',
            'JAN MAYEN',
            'JAPAN',
            'JARVIS ISLAND',
            'JERSEY',
            'JOHNSTON ATOLL',
            'JORDAN',
            'JUAN DE NOVA ISLAND',
            'KAZAKHSTAN',
            'KENYA',
            'KERGUELEN ARCHIPELAGO',
            'KINGMAN REEF',
            'KIRIBATI',
            'KOSOVO',
            'KUWAIT',
            'KYRGYZSTAN',
            'LAOS',
            'LATVIA',
            'LEBANON',
            'LESOTHO',
            'LIBERIA',
            'LIBYA',
            'LIECHTENSTEIN',
            'LITHUANIA',
            'LUXEMBOURG',
            'MACAU',
            'MACEDONIA',
            'MADAGASCAR',
            'MALAWI',
            'MALAYSIA',
            'MALDIVES',
            'MALI',
            'MALTA',
            'MARSHALL ISLANDS',
            'MARTINIQUE',
            'MAURITANIA',
            'MAURITIUS',
            'MAYOTTE',
            'MEDITERRANEAN SEA',
            'MEXICO',
            'MICRONESIA',
            'MIDWAY ISLANDS',
            'MOLDOVA',
            'MONACO',
            'MONGOLIA',
            'MONTENEGRO',
            'MONTSERRAT',
            'MOROCCO',
            'MOZAMBIQUE',
            'MYANMAR',
            'NAMIBIA',
            'NAURU',
            'NAVASSA ISLAND',
            'NEPAL',
            'NETHERLANDS',
            'NEW CALEDONIA',
            'NEW ZEALAND',
            'NICARAGUA',
            'NIGER',
            'NIGERIA',
            'NIUE',
            'NORFOLK ISLAND',
            'NORTH KOREA',
            'NORTH SEA',
            'NORTHERN MARIANA ISLANDS',
            'NORWAY',
            'OMAN',
            'PACIFIC OCEAN',
            'PAKISTAN',
            'PALAU',
            'PALMYRA ATOLL',
            'PANAMA',
            'PAPUA NEW GUINEA',
            'PARACEL ISLANDS',
            'PARAGUAY',
            'PERU',
            'PHILIPPINES',
            'PITCAIRN ISLANDS',
            'POLAND',
            'PORTUGAL',
            'PUERTO RICO',
            'QATAR',
            'REPUBLIC OF THE CONGO',
            'REUNION',
            'ROMANIA',
            'ROSS SEA',
            'RUSSIA',
            'RWANDA',
            'SAINT HELENA',
            'SAINT KITTS AND NEVIS',
            'SAINT LUCIA',
            'SAINT PIERRE AND MIQUELON',
            'SAINT VINCENT AND THE GRENADINES',
            'SAMOA',
            'SAN MARINO',
            'SAO TOME AND PRINCIPE',
            'SAUDI ARABIA',
            'SENEGAL',
            'SERBIA',
            'SEYCHELLES',
            'SIERRA LEONE',
            'SINGAPORE',
            'SINT MAARTEN',
            'SLOVAKIA',
            'SLOVENIA',
            'SOLOMON ISLANDS',
            'SOMALIA',
            'SOUTH AFRICA',
            'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS',
            'SOUTH KOREA',
            'SOUTHERN OCEAN',
            'SPAIN',
            'SPRATLY ISLANDS',
            'SRI LANKA',
            'SUDAN',
            'SURINAME',
            'SVALBARD',
            'SWAZILAND',
            'SWEDEN',
            'SWITZERLAND',
            'SYRIA',
            'TAIWAN',
            'TAJIKISTAN',
            'TANZANIA',
            'TASMAN SEA',
            'THAILAND',
            'TOGO',
            'TOKELAU',
            'TONGA',
            'TRINIDAD AND TOBAGO',
            'TROMELIN ISLAND',
            'TUNISIA',
            'TURKEY',
            'TURKMENISTAN',
            'TURKS AND CAICOS ISLANDS',
            'TUVALU',
            'UGANDA',
            'UKRAINE',
            'UNITED ARAB EMIRATES',
            'UNITED KINGDOM',
            'URUGUAY',
            'USA',
            'UZBEKISTAN',
            'VANUATU',
            'VENEZUELA',
            'VIETNAM',
            'VIRGIN ISLANDS',
            'WAKE ISLAND',
            'WALLIS AND FUTUNA',
            'WEST BANK',
            'WESTERN SAHARA',
            'YEMEN',
            'ZAMBIA',
            'ZIMBABWE',
            'NOT APPLICABLE',
            'NOT COLLECTED',
            'NOT PROVIDED'
            ],
    'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE':
        [
            'DIFFICULT',
            'FULL_CURATION',
            'HIGH_PRIORITY',
            'NOT_APPLICABLE',
            'NOT_COLLECTED',
            'NOT_PROVIDED'
            ],
    'DNA_REMOVED_FOR_BIOBANKING':
        [
            'Y',
            'N'
            ],
    'ETHICS_PERMIT_REQUIRED':
        [
            'Y',
            'N'
            ],
    'GAL': {
        'DTOL': [
            'EARLHAM INSTITUTE',
            'MARINE BIOLOGICAL ASSOCIATION',
            'NATURAL HISTORY MUSEUM',
            'ROYAL BOTANIC GARDEN EDINBURGH',
            'ROYAL BOTANIC GARDENS KEW',
            'SANGER INSTITUTE',
            'UNIVERSITY OF OXFORD'
        ],
        'DTOLENV': [
            'EARLHAM INSTITUTE',
            'MARINE BIOLOGICAL ASSOCIATION',
            'NATURAL HISTORY MUSEUM',
            'ROYAL BOTANIC GARDEN EDINBURGH',
            'ROYAL BOTANIC GARDENS KEW',
            'SANGER INSTITUTE',
            'UNIVERSITY OF OXFORD'
        ],
        'ERGA':
            [
                'CENTRO NACIONAL DE ANÁLISIS GENÓMICO',
                'DNA SEQUENCING AND GENOMICS LABORATORY, HELSINKI GENOMICS CORE FACILITY',
                'DRESDEN-CONCEPT',
                'EARLHAM INSTITUTE',
                'FUNCTIONAL GENOMIC CENTER ZURICH',
                'GENOSCOPE',
                'GIGA-GENOMICS CORE FACILITY UNIVERSITY OF LIEGE',
                'HANSEN LAB, DENMARK',
                'INDUSTRY PARTNER',
                'LAUSANNE GENOMIC TECHNOLOGIES FACILITY',
                'LEIBNIZ INSTITUTE FOR THE ANALYSIS OF BIODIVERSITY CHANGE, MUSEUM KOENIG, BONN',
                'NEUROMICS SUPPORT FACILITY, UANTWERP, VIB',
                'NGS BERN',
                'NGS COMPETENCE CENTER TÜBINGEN',
                'NORWEGIAN SEQUENCING CENTRE',
                'SANGER INSTITUTE',
                'SCILIFELAB',
                'SVARDAL LAB, ANTWERP',
                'UNIVERSITY OF BARI',
                'UNIVERSITY OF FLORENCE',
                'WEST GERMAN GENOME CENTRE',
                'Other_ERGA_Associated_GAL'
        ]
        },
    'HAZARD_GROUP': {
        'DTOL':
            [
                'HG1',
                'HG2',
                'HG3'
            ],
        'ASG':
            [
                'HG1',
                'HG2',
                'HG3'],
        'ERGA':
            [
                '1',
                '2',
                '3',
                '4'
            ]
        },
    'LIFESTAGE':
        [
            'ADULT',
            'EGG',
            'EMBRYO',
            'GAMETOPHYTE',
            'JUVENILE',
            'LARVA',
            'PUPA',
            'SPORE_BEARING_STRUCTURE',
            'SPOROPHYTE',
            'VEGETATIVE_CELL',
            'VEGETATIVE_STRUCTURE',
            'ZYGOTE',
            'NOT_APPLICABLE',
            'NOT_COLLECTED',
            'NOT_PROVIDED'
            ],
    'MIXED_SAMPLE_RISK':
        [
            'Y',
            'N'
            ],
    'NAGOYA_PERMITS_REQUIRED':
        [
            'Y',
            'N'
            ],
    'ORGANISM_PART':
        [
            '**OTHER_FUNGAL_TISSUE**',
            '**OTHER_PLANT_TISSUE**',
            '**OTHER_REPRODUCTIVE_ANIMAL_TISSUE**',
            '**OTHER_SOMATIC_ANIMAL_TISSUE**',
            'ABDOMEN',
            'ANTERIOR_BODY',
            'BLADE',
            'BLOOD',
            'BODYWALL',
            'BRACT',
            'BRAIN',
            'BUD',
            'CAP',
            'CEPHALOTHORAX',
            'EGG',
            'EGGSHELL',
            'ENDOCRINE_TISSUE',
            'EYE',
            'FAT_BODY',
            'FIN',
            'FLOWER',
            'GILL_ANIMAL',
            'GILL_FUNGI',
            'GONAD',
            'HAIR',
            'HEAD',
            'HEART',
            'HEPATOPANCREAS',
            'HOLDFAST_FUNGI',
            'INTESTINE',
            'KIDNEY',
            'LEAF',
            'LEG',
            'LIVER',
            'LUNG',
            'MID_BODY',
            'MODULAR_COLONY',
            'MOLLUSC_FOOT',
            'MULTICELLULAR_ORGANISMS_IN_CULTURE',
            'MUSCLE',
            'MYCELIUM',
            'MYCORRHIZA',
            'OVARY_ANIMAL',
            'OVIDUCT',
            'PANCREAS',
            'PETIOLE',
            'POSTERIOR_BODY',
            'ROOT',
            'SCALES',
            'SCAT',
            'SEED',
            'SEEDLING',
            'SHOOT',
            'SKIN',
            'SPERM_SEMINAL_FLUID',
            'SPLEEN',
            'SPORE',
            'SPORE_BEARING_STRUCTURE',
            'STEM',
            'STIPE',
            'STOMACH',
            'TENTACLE',
            'TERMINAL_BODY',
            'TESTIS',
            'THALLUS_FUNGI',
            'THALLUS_PLANT',
            'THORAX',
            'UNICELLULAR_ORGANISMS_IN_CULTURE',
            'WHOLE_ORGANISM',
            'WHOLE_PLANT',
            'NOT_APPLICABLE',
            'NOT_COLLECTED',
            'NOT_PROVIDED'
            ],
    'PARTNER':
        [
            'DALHOUSIE UNIVERSITY',
            'GEOMAR HELMHOLTZ CENTRE',
            'NOVA SOUTHEASTERN UNIVERSITY',
            'PORTLAND STATE UNIVERSITY',
            'QUEEN MARY UNIVERSITY OF LONDON',
            'SENCKENBERG RESEARCH INSTITUTE',
            'THE SAINSBURY LABORATORY',
            'UNIVERSITY OF BRITISH COLUMBIA',
            'UNIVERSITY OF CALIFORNIA',
            'UNIVERSITY OF DERBY',
            'UNIVERSITY OF ORGEON',
            'UNIVERSITY OF RHODE ISLAND',
            'UNIVERSITY OF VIENNA (CEPHALOPOD)',
            'UNIVERSITY OF VIENNA (MOLLUSC)'
            ],
    'PRIMARY_BIOGENOME_PROJECT': [
        'ERGA-associated',
        'ERGA-BGE',
        'ERGA-Pilot',
        'ERGA-Community',
        ],
    'PURPOSE_OF_SPECIMEN': {
        'ASG':
            [
                'REFERENCE_GENOME',
                'RESEQUENCING',
                'DNA_BARCODING_ONLY',
                'RNA_SEQUENCING',
                'R&D',
                'NOT_PROVIDED'
            ],
        'DTOL':
            [
                'REFERENCE_GENOME',
                'RESEQUENCING',
                'DNA_BARCODING_ONLY',
                'RNA_SEQUENCING',
                'R&D',
                'NOT_PROVIDED'
            ],

        'ERGA':
            [
                'REFERENCE_GENOME',
                'RESEQUENCING',
                'DNA_BARCODING_ONLY',
                'RNA_SEQUENCING',
                'R&D',
                'NOT_PROVIDED'
            ]
        },
    'REGULATORY_COMPLIANCE':
        [
            'Y',
            'N',
            'NOT_APPLICABLE'
            ],
    'SAMPLE_FORMAT': [
        'DNA',
        'RNA',
        'biological sample/tissue from non-infectious organism',
        'inactivated biological sample from infectious organism'
        'live biological sample from infectious organism'
        ],
    'SAMPLING_PERMITS_REQUIRED':
        [
            'Y',
            'N'
            ],
    'SEQUENCING_CENTRE':
        [
            'EARLHAM INSTITUTE',
            'SANGER INSTITUTE'
        ],
    'SEX':
        [
            'ASEXUAL_MORPH',
            'HERMAPHRODITE_MONOECIOUS',
            'FEMALE',
            'MALE',
            'NOT_APPLICABLE',
            'NOT_COLLECTED',
            'NOT_PROVIDED',
            'SEXUAL_MORPH'
            ],
    'SIZE_OF_TISSUE_IN_TUBE':
        [
            'VS',
            'S',
            'M',
            'L',
            'SINGLE_CELL',
            'NOT_APPLICABLE',
            'NOT_COLLECTED',
            'NOT_PROVIDED'
            ],
    'SORTER_AFFILIATION':
        [
            'EARLHAM INSTITUTE',
            'UNIVERSITY OF OXFORD'
            ],
    'SPECIMEN_IDENTITY_RISK':
        [
            'Y',
            'N'
            ],
    'SYMBIONT':
        [
            'TARGET',
            'SYMBIONT'
            ],
    'TISSUE_FOR_BARCODING':
        [
            '**OTHER_FUNGAL_TISSUE**',
            '**OTHER_PLANT_TISSUE**',
            '**OTHER_REPRODUCTIVE_ANIMAL_TISSUE**',
            '**OTHER_SOMATIC_ANIMAL_TISSUE**',
            'ABDOMEN',
            'ANTERIOR_BODY',
            'BLADE',
            'BLOOD',
            'BODYWALL',
            'BRACT',
            'BRAIN',
            'BUD',
            'CAP',
            'CEPHALOTHORAX',
            'DNA_EXTRACT',
            'EGG',
            'EGGSHELL',
            'ENDOCRINE_TISSUE',
            'EYE',
            'FAT_BODY',
            'FIN',
            'FLOWER',
            'GILL_ANIMAL',
            'GILL_FUNGI',
            'GONAD',
            'HAIR',
            'HEAD',
            'HEART',
            'HEPATOPANCREAS',
            'HOLDFAST_FUNGI',
            'INTESTINE',
            'KIDNEY',
            'LEAF',
            'LEG',
            'LIVER',
            'LUNG',
            'MID_BODY',
            'MODULAR_COLONY',
            'MOLLUSC_FOOT',
            'MULTICELLULAR_ORGANISMS_IN_CULTURE',
            'MUSCLE',
            'MYCELIUM',
            'MYCORRHIZA',
            'NOT_APPLICABLE',
            'NOT_COLLECTED',
            'NOT_PROVIDED',
            'OVARY_ANIMAL',
            'OVIDUCT',
            'PANCREAS',
            'PETIOLE',
            'POSTERIOR_BODY',
            'ROOT',
            'SCALES',
            'SCAT',
            'SEED',
            'SEEDLING',
            'SHOOT',
            'SKIN',
            'SPERM_SEMINAL_FLUID',
            'SPLEEN',
            'SPORE',
            'SPORE_BEARING_STRUCTURE',
            'STEM',
            'STIPE',
            'STOMACH',
            'TENTACLE',
            'TERMINAL_BODY',
            'TESTIS',
            'THALLUS_FUNGI',
            'THALLUS_PLANT',
            'THORAX',
            'UNICELLULAR_ORGANISMS_IN_CULTURE',
            'WHOLE_ORGANISM',
            'WHOLE_PLANT'
            ],
    'TISSUE_FOR_BIOBANKING': [
        '**OTHER_FUNGAL_TISSUE**',
        '**OTHER_PLANT_TISSUE**',
        '**OTHER_REPRODUCTIVE_ANIMAL_TISSUE**',
        '**OTHER_SOMATIC_ANIMAL_TISSUE**',
        'ABDOMEN',
        'ANTERIOR_BODY',
        'BLADE',
        'BLOOD',
        'BODYWALL',
        'BRACT',
        'BRAIN',
        'BUD',
        'CAP',
        'CEPHALOTHORAX',
        'EGG',
        'EGGSHELL',
        'ENDOCRINE_TISSUE',
        'EYE',
        'FAT_BODY',
        'FIN',
        'FLOWER',
        'GILL_ANIMAL',
        'GILL_FUNGI',
        'GONAD',
        'HAIR',
        'HEAD',
        'HEART',
        'HEPATOPANCREAS',
        'HOLDFAST_FUNGI',
        'INTESTINE',
        'KIDNEY',
        'LEAF',
        'LEG',
        'LIVER',
        'LUNG',
        'MID_BODY',
        'MODULAR_COLONY',
        'MOLLUSC_FOOT',
        'MULTICELLULAR_ORGANISMS_IN_CULTURE',
        'MUSCLE',
        'MYCELIUM',
        'MYCORRHIZA',
        'OVARY_ANIMAL',
        'OVIDUCT',
        'PANCREAS',
        'PETIOLE',
        'POSTERIOR_BODY',
        'ROOT',
        'SCALES',
        'SCAT',
        'SEED',
        'SEEDLING',
        'SHOOT',
        'SKIN',
        'SPERM_SEMINAL_FLUID',
        'SPLEEN',
        'SPORE',
        'SPORE_BEARING_STRUCTURE',
        'STEM',
        'STIPE',
        'STOMACH',
        'TENTACLE',
        'TERMINAL_BODY',
        'TESTIS',
        'THALLUS_FUNGI',
        'THALLUS_PLANT',
        'THORAX',
        'UNICELLULAR_ORGANISMS_IN_CULTURE',
        'WHOLE_ORGANISM',
        'WHOLE_PLANT',
        'NOT_APPLICABLE',
        'NOT_COLLECTED',
        'NOT_PROVIDED',
        ],
    'TISSUE_REMOVED_FOR_BARCODING':
        [
            'Y',
            'N'
            ],
    'TISSUE_REMOVED_FOR_BIOBANKING':
        [
            'Y',
            'N'
            ],
    'TO_BE_USED_FOR':
        [
            'BARCODING ONLY',
            'REFERENCE GENOME',
            'RESEQUENCING(POPGEN)',
            'RNAseq'
            ],
    'WATER_BODY_TYPE':
        [
            'COASTAL',
            'ESTUARY',
            'LAKE',
            'OPEN SEA',
            'POND',
            'RIVER',
            'STREAM'
            ],
    'WATER_TYPE':
        [
            'BRACKISH_WATER',
            'FRESH_WATER',
            'SALT_WATER'
            ]
}

DTOL_UNITS = {
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

DTOL_EXPORT_TO_STS_FIELDS = {
    'asg': [
        'SERIES',
        'RACK_OR_PLATE_ID',
        'TUBE_OR_WELL_ID',
        'SPECIMEN_ID',
        'ORDER_OR_GROUP',
        'FAMILY',
        'GENUS',
        'TAXON_ID',
        'SCIENTIFIC_NAME',
        'TAXON_REMARKS',
        'INFRASPECIFIC_EPITHET',
        'CULTURE_OR_STRAIN_ID',
        'COMMON_NAME',
        'LIFESTAGE',
        'SEX',
        'ORGANISM_PART',
        'SYMBIONT',
        'RELATIONSHIP',
        'PARTNER',
        'PARTNER_SAMPLE_ID',
        'COLLECTOR_SAMPLE_ID',
        'COLLECTED_BY',
        'COLLECTOR_AFFILIATION',
        'DATE_OF_COLLECTION',
        'COLLECTION_LOCATION',
        'DECIMAL_LATITUDE',
        'DECIMAL_LONGITUDE',
        'GRID_REFERENCE',
        'HABITAT',
        'DEPTH',
        'ELEVATION',
        'ORIGINAL_COLLECTION_DATE',
        'ORIGINAL_GEOGRAPHIC_LOCATION',
        'ORIGINAL_DECIMAL_LATITUDE',
        'ORIGINAL_DECIMAL_LONGITUDE',
        'TIME_OF_COLLECTION',
        'DESCRIPTION_OF_COLLECTION_METHOD',
        'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE',
        'IDENTIFIED_BY',
        'IDENTIFIER_AFFILIATION',
        'IDENTIFIED_HOW',
        'SPECIMEN_IDENTITY_RISK',
        'MIXED_SAMPLE_RISK',
        'PRESERVED_BY',
        'PRESERVER_AFFILIATION',
        'PRESERVATION_APPROACH',
        'PRESERVATIVE_SOLUTION',
        'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION',
        'DATE_OF_PRESERVATION',
        'SIZE_OF_TISSUE_IN_TUBE',
        'BARCODE_HUB',
        'TISSUE_REMOVED_FOR_BARCODING',
        'TISSUE_REMOVED_FROM_BARCODING',
        'PLATE_ID_FOR_BARCODING',
        'TUBE_OR_WELL_ID_FOR_BARCODING',
        'TISSUE_FOR_BARCODING',
        'BARCODE_PLATE_PRESERVATIVE',
        'BARCODING_STATUS',
        'PURPOSE_OF_SPECIMEN',
        'SAMPLE_FORMAT',
        'HAZARD_GROUP',
        'REGULATORY_COMPLIANCE',
        'VOUCHER_ID',
        'PROXY_VOUCHER_ID',
        'VOUCHER_LINK',
        'PROXY_VOUCHER_LINK',
        'VOUCHER_INSTITUTION',
        'OTHER_INFORMATION',
        'associated_tol_project',
        'biosampleAccession',
        'copo_profile_title',
        'created_by',
        'manifest_id',
        'public_name',
        'sampleDerivedFrom',
        'sampleSameAs',
        'sampleSymbiontOf',
        'sraAccession',
        'status',
        'submissionAccession',
        'time_created',
        'time_updated',
        'tol_project',
        'updated_by'
    ],
    'dtol': [
        'SERIES',
        'RACK_OR_PLATE_ID',
        'TUBE_OR_WELL_ID',
        'SPECIMEN_ID',
        'ORDER_OR_GROUP',
        'FAMILY',
        'GENUS',
        'TAXON_ID',
        'SCIENTIFIC_NAME',
        'TAXON_REMARKS',
        'INFRASPECIFIC_EPITHET',
        'CULTURE_OR_STRAIN_ID',
        'COMMON_NAME',
        'LIFESTAGE',
        'SEX',
        'ORGANISM_PART',
        'SYMBIONT',
        'RELATIONSHIP',
        'GAL',
        'GAL_SAMPLE_ID',
        'COLLECTOR_SAMPLE_ID',
        'COLLECTED_BY',
        'COLLECTOR_AFFILIATION',
        'DATE_OF_COLLECTION',
        'COLLECTION_LOCATION',
        'DECIMAL_LATITUDE',
        'DECIMAL_LONGITUDE',
        'GRID_REFERENCE',
        'HABITAT',
        'DEPTH',
        'ELEVATION',
        'ORIGINAL_COLLECTION_DATE',
        'ORIGINAL_GEOGRAPHIC_LOCATION',
        'ORIGINAL_DECIMAL_LATITUDE',
        'ORIGINAL_DECIMAL_LONGITUDE',
        'TIME_OF_COLLECTION',
        'DESCRIPTION_OF_COLLECTION_METHOD',
        'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE',
        'IDENTIFIED_BY',
        'IDENTIFIER_AFFILIATION',
        'IDENTIFIED_HOW',
        'SPECIMEN_IDENTITY_RISK',
        'MIXED_SAMPLE_RISK',
        'PRESERVED_BY',
        'PRESERVER_AFFILIATION',
        'PRESERVATION_APPROACH',
        'PRESERVATIVE_SOLUTION',
        'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION',
        'DATE_OF_PRESERVATION',
        'SIZE_OF_TISSUE_IN_TUBE',
        'BARCODE_HUB',
        'TISSUE_REMOVED_FOR_BARCODING',
        'TISSUE_REMOVED_FROM_BARCODING',
        'PLATE_ID_FOR_BARCODING',
        'TUBE_OR_WELL_ID_FOR_BARCODING',
        'TISSUE_FOR_BARCODING',
        'BARCODE_PLATE_PRESERVATIVE',
        'BARCODING_STATUS',
        'BOLD_ACCESSION_NUMBER',
        'PURPOSE_OF_SPECIMEN',
        'SAMPLE_FORMAT',
        'HAZARD_GROUP',
        'REGULATORY_COMPLIANCE',
        'VOUCHER_ID',
        'PROXY_VOUCHER_ID',
        'VOUCHER_LINK',
        'PROXY_VOUCHER_LINK',
        'VOUCHER_INSTITUTION',
        'OTHER_INFORMATION',
        'associated_tol_project',
        'biosampleAccession',
        'copo_profile_title',
        'created_by',
        'manifest_id',
        'public_name',
        'sampleDerivedFrom',
        'sampleSameAs',
        'sampleSymbiontOf',
        'sraAccession',
        'status',
        'submissionAccession',
        'time_created',
        'time_updated',
        'tol_project',
        'updated_by'
    ],
    'env': [],
    'erga': [
        'TUBE_OR_WELL_ID',
        'SPECIMEN_ID',
        'PURPOSE_OF_SPECIMEN',
        'SAMPLE_COORDINATOR',
        'SAMPLE_COORDINATOR_AFFILIATION',
        'SAMPLE_COORDINATOR_ORCID_ID',
        'ORDER_OR_GROUP',
        'FAMILY',
        'GENUS',
        'TAXON_ID',
        'SCIENTIFIC_NAME',
        'TAXON_REMARKS',
        'INFRASPECIFIC_EPITHET',
        'CULTURE_OR_STRAIN_ID',
        'COMMON_NAME',
        'LIFESTAGE',
        'SEX',
        'ORGANISM_PART',
        'SYMBIONT',
        'RELATIONSHIP',
        'GAL',
        'GAL_SAMPLE_ID',
        'COLLECTOR_SAMPLE_ID',
        'COLLECTED_BY',
        'COLLECTOR_AFFILIATION',
        'COLLECTOR_ORCID_ID',
        'DATE_OF_COLLECTION',
        'TIME_OF_COLLECTION',
        'COLLECTION_LOCATION',
        'DECIMAL_LATITUDE',
        'DECIMAL_LONGITUDE',
        'LATITUDE_START',
        'LONGITUDE_START',
        'LATITUDE_END',
        'LONGITUDE_END',
        'HABITAT',
        'DEPTH',
        'ELEVATION',
        'ORIGINAL_COLLECTION_DATE',
        'ORIGINAL_GEOGRAPHIC_LOCATION',
        'DESCRIPTION_OF_COLLECTION_METHOD',
        'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE',
        'IDENTIFIED_BY',
        'IDENTIFIER_AFFILIATION',
        'IDENTIFIED_HOW',
        'SPECIMEN_IDENTITY_RISK',
        'PRESERVED_BY',
        'PRESERVER_AFFILIATION',
        'PRESERVATION_APPROACH',
        'PRESERVATIVE_SOLUTION',
        'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION',
        'DATE_OF_PRESERVATION',
        'SIZE_OF_TISSUE_IN_TUBE',
        'TISSUE_REMOVED_FOR_BARCODING',
        'TUBE_OR_WELL_ID_FOR_BARCODING',
        'TISSUE_FOR_BARCODING',
        'BARCODE_PLATE_PRESERVATIVE',
        'TISSUE_REMOVED_FOR_BIOBANKING',
        'TISSUE_VOUCHER_ID_FOR_BIOBANKING',
        'TISSUE_FOR_BIOBANKING',
        'DNA_REMOVED_FOR_BIOBANKING',
        'DNA_VOUCHER_ID_FOR_BIOBANKING',
        'VOUCHER_ID',
        'PROXY_VOUCHER_ID',
        'VOUCHER_LINK',
        'PROXY_VOUCHER_LINK',
        'VOUCHER_INSTITUTION',
        'REGULATORY_COMPLIANCE',
        'ASSOCIATED_TRADITIONAL_KNOWLEDGE_APPLICABLE',
        'INDIGENOUS_RIGHTS_DEF',
        'ASSOCIATED_TRADITIONAL_KNOWLEDGE_CONTACT',
        'ASSOCIATED_TRADITIONAL_KNOWLEDGE_LABEL',
        'ETHICS_PERMITS_MANDATORY',
        'ETHICS_PERMITS_DEF',
        'ETHICS_PERMITS_FILENAME',
        'SAMPLING_PERMITS_MANDATORY',
        'SAMPLING_PERMITS_DEF',
        'SAMPLING_PERMITS_FILENAME'
        'NAGOYA_PERMITS_MANDATORY',
        'NAGOYA_PERMITS_DEF',
        'NAGOYA_PERMITS_FILENAME',
        'HAZARD_GROUP',
        'OTHER_INFORMATION',
        'BARCODE_HUB',
        'INDIGENOUS_RIGHTS_APPLICABLE',
        'PLATE_ID_FOR_BARCODING',
        'RACK_OR_PLATE_ID',
        'SERIES',
        'TISSUE_REMOVED_FROM_BARCODING',
        'BIOBANKED_TISSUE_PRESERVATIVE',
        'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID',
        'NAGOYA_PERMITS_REQUIRED',
        'PROXY_TISSUE_VOUCHER_ID_FOR_BIOBANKING',
        'BARCODING_STATUS',
        'ORIGINAL_DECIMAL_LONGITUDE',
        'PRIMARY_BIOGENOME_PROJECT',
        'SAMPLING_PERMITS_REQUIRED',
        'ETHICS_PERMITS_REQUIRED',
        'ORIGINAL_DECIMAL_LATITUDE',
        'MIXED_SAMPLE_RISK',
        'ASSOCIATED_PROJECT_ACCESSIONS',
        'associated_tol_project',
        'biosampleAccession',
        'copo_profile_title',
        'created_by',
        'manifest_id',
        'public_name',
        'sampleDerivedFrom',
        'sampleSameAs',
        'sampleSymbiontOf',
        'sraAccession',
        'status',
        'submissionAccession',
        'time_created',
        'time_updated',
        'tol_project',
        'updated_by'
    ]}

DTOL_RULES = {
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

# Mapping
asg_fields_mapping= {
    'SERIES': {'required' : True, 'type': 'integer'},
    'RACK_OR_PLATE_ID': {'required' : True},
    'TUBE_OR_WELL_ID': {'required' : True},
    'SPECIMEN_ID': {'required' : True},
    'ORDER_OR_GROUP': {'required' : True},
    'FAMILY': {'required' : True},
    'GENUS': {'required' : True},
    'TAXON_ID': {'required' : True, 'type': 'integer'},
    'SCIENTIFIC_NAME': {'required' : True},
    'TAXON_REMARKS': {'required' : False},
    'INFRASPECIFIC_EPITHET': {'required' : False},
    'CULTURE_OR_STRAIN_ID': {'required' : False},
    'COMMON_NAME': {'required' : False},
    'LIFESTAGE': {'required' : True},
    'SEX': {'required' : True},
    'ORGANISM_PART': {'required' : True},
    'SYMBIONT': {'required' : True},
    'RELATIONSHIP': {'required' : False},
    'PARTNER': {'required' : True},
    'PARTNER_SAMPLE_ID': {'required' : True},
    'COLLECTOR_SAMPLE_ID': {'required' : True},
    'COLLECTED_BY': {'required' : True},
    'COLLECTOR_AFFILIATION': {'required' : True},
    'DATE_OF_COLLECTION': {'required' : True, 'type': 'date'},
    'TIME_OF_COLLECTION': {'required' : False},
    'COLLECTION_LOCATION': {'required' : True},
    'DECIMAL_LATITUDE': {'required' : True},
    'DECIMAL_LONGITUDE': {'required' : True},
    'GRID_REFERENCE': {'required' : False},
    'HABITAT': {'required' : True},
    'DEPTH': {'required' : False},
    'ELEVATION': {'required' : False},
    'ORIGINAL_COLLECTION_DATE': {'required' : False, 'type': 'date'},
    'ORIGINAL_GEOGRAPHIC_LOCATION': {'required' : False},
    'ORIGINAL_DECIMAL_LATITUDE': {'required' : False},
    'ORIGINAL_DECIMAL_LONGITUDE': {'required' : False},
    'DESCRIPTION_OF_COLLECTION_METHOD': {'required' : True},
    'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE': {'required' : True},
    'IDENTIFIED_BY': {'required' : True},
    'IDENTIFIER_AFFILIATION': {'required' : True},
    'IDENTIFIED_HOW': {'required' : True},
    'SPECIMEN_IDENTITY_RISK': {'required' : True},
    'MIXED_SAMPLE_RISK': {'required' : True},
    'PRESERVED_BY': {'required' : True},
    'PRESERVER_AFFILIATION': {'required' : True},
    'PRESERVATION_APPROACH': {'required' : True},
    'PRESERVATIVE_SOLUTION': {'required' : False},
    'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION': {'required' : True},
    'DATE_OF_PRESERVATION': {'required' : True, 'type': 'date'},
    'SIZE_OF_TISSUE_IN_TUBE': {'required' : True},
    'BARCODE_HUB': {'required' : True},
    'TISSUE_REMOVED_FOR_BARCODING': {'required' : True},
    'PLATE_ID_FOR_BARCODING': {'required' : True},
    'TUBE_OR_WELL_ID_FOR_BARCODING': {'required' : True},
    'TISSUE_FOR_BARCODING': {'required' : True},
    'BARCODE_PLATE_PRESERVATIVE': {'required' : True},
    'BARCODING_STATUS': {'required' : True},
    'BOLD_ACCESSION_NUMBER': {'required' : False},
    'VOUCHER_ID': {'required' : True},
    'PROXY_VOUCHER_ID': {'required' : False},
    'VOUCHER_LINK': {'required' : False},
    'PROXY_VOUCHER_LINK': {'required' : False},
    'VOUCHER_INSTITUTION': {'required' : False},
    'PURPOSE_OF_SPECIMEN': {'required' : True},
    'SAMPLE_FORMAT': {'required' : False},
    'HAZARD_GROUP': {'required' : True},
    'REGULATORY_COMPLIANCE': {'required' : True},
    'OTHER_INFORMATION': {'required' : False}
}

dtol_fields_mapping=  {
    'SERIES': {'required' : True, 'type': 'integer'},
    'RACK_OR_PLATE_ID': {'required' : True},
    'TUBE_OR_WELL_ID': {'required' : True},
    'SPECIMEN_ID': {'required' : True},
    'ORDER_OR_GROUP': {'required' : True},
    'FAMILY': {'required' : True},
    'GENUS': {'required' : True},
    'TAXON_ID': {'required' : True, 'type': 'integer'},
    'SCIENTIFIC_NAME': {'required' : True},
    'TAXON_REMARKS': {'required' : False},
    'INFRASPECIFIC_EPITHET': {'required' : False},
    'CULTURE_OR_STRAIN_ID': {'required' : False},
    'COMMON_NAME': {'required' : False},
    'LIFESTAGE': {'required' : True},
    'SEX': {'required' : True},
    'ORGANISM_PART': {'required' : True},
    'SYMBIONT': {'required' : True},
    'RELATIONSHIP': {'required' : False},
    'GAL': {'required' : True},
    'GAL_SAMPLE_ID': {'required' : True},
    'COLLECTOR_SAMPLE_ID': {'required' : True},
    'COLLECTED_BY': {'required' : True},
    'COLLECTOR_AFFILIATION': {'required' : True},
    'DATE_OF_COLLECTION': {'required' : True, 'type': 'date'},
    'TIME_OF_COLLECTION': {'required' : False},
    'COLLECTION_LOCATION': {'required' : True},
    'DECIMAL_LATITUDE': {'required' : True},
    'DECIMAL_LONGITUDE': {'required' : True},
    'GRID_REFERENCE': {'required' : False},
    'HABITAT': {'required' : True},
    'DEPTH': {'required' : False},
    'ELEVATION': {'required' : False},
    'ORIGINAL_COLLECTION_DATE': {'required' : False},
    'ORIGINAL_GEOGRAPHIC_LOCATION': {'required' : False},
    'ORIGINAL_DECIMAL_LATITUDE': {'required' : False},
    'ORIGINAL_DECIMAL_LONGITUDE': {'required' : False},
    'DESCRIPTION_OF_COLLECTION_METHOD': {'required' : True},
    'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE': {'required' : True},
    'IDENTIFIED_BY': {'required' : True},
    'IDENTIFIER_AFFILIATION': {'required' : True},
    'IDENTIFIED_HOW': {'required' : True},
    'SPECIMEN_IDENTITY_RISK': {'required' : True},
    'MIXED_SAMPLE_RISK': {'required' : True},
    'PRESERVED_BY': {'required' : True},
    'PRESERVER_AFFILIATION': {'required' : True},
    'PRESERVATION_APPROACH': {'required' : True},
    'PRESERVATIVE_SOLUTION': {'required' : False},
    'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION': {'required' : True},
    'DATE_OF_PRESERVATION': {'required' : True, 'type': 'date'},
    'SIZE_OF_TISSUE_IN_TUBE': {'required' : True},
    'BARCODE_HUB': {'required' : True},
    'TISSUE_REMOVED_FOR_BARCODING': {'required' : True},
    'PLATE_ID_FOR_BARCODING': {'required' : True},
    'TUBE_OR_WELL_ID_FOR_BARCODING': {'required' : True},
    'TISSUE_FOR_BARCODING': {'required' : True},
    'BARCODE_PLATE_PRESERVATIVE': {'required' : True},
    'BARCODING_STATUS': {'required' : True},
    'BOLD_ACCESSION_NUMBER': {'required' : False},
    'VOUCHER_ID': {'required' : True},
    'PROXY_VOUCHER_ID': {'required' : False},
    'VOUCHER_LINK': {'required' : False},
    'PROXY_VOUCHER_LINK': {'required' : False},
    'VOUCHER_INSTITUTION': {'required' : False},
    'PURPOSE_OF_SPECIMEN': {'required' : True},
    'SAMPLE_FORMAT': {'required' : True},
    'HAZARD_GROUP': {'required' : True},
    'REGULATORY_COMPLIANCE': {'required' : True},
    'OTHER_INFORMATION': {'required' : False}
}

erga_fields_mapping= {
    'TUBE_OR_WELL_ID': {'required': True},
    'SPECIMEN_ID': {'required': True},
    'PURPOSE_OF_SPECIMEN': {'required': True},
    'SAMPLE_COORDINATOR': {'required': True},
    'SAMPLE_COORDINATOR_AFFILIATION': {'required': True},
    'SAMPLE_COORDINATOR_ORCID_ID': {'required': True},
    'ORDER_OR_GROUP': {'required': True},
    'FAMILY': {'required': True},
    'GENUS': {'required': True},
    'TAXON_ID': {'required': True, 'type': 'integer'},
    'SCIENTIFIC_NAME': {'description': ""},
    'TAXON_REMARKS': {'required': False},
    'INFRASPECIFIC_EPITHET': {'required': False},
    'CULTURE_OR_STRAIN_ID': {'required': False},
    'COMMON_NAME': {'required': False},
    'LIFESTAGE': {'required': True},
    'SEX': {'required': True},
    'ORGANISM_PART': {'required': True},
    'SYMBIONT': {'required': False},
    'RELATIONSHIP': {'required': False},
    'GAL': {'required': True},
    'GAL_SAMPLE_ID': {'required': True},
    'COLLECTOR_SAMPLE_ID': {'required': True},
    'COLLECTED_BY': {'required': True},
    'COLLECTOR_AFFILIATION': {'required': True},
    'COLLECTOR_ORCID_ID': {'required': True},
    'DATE_OF_COLLECTION': {'required': True, 'type': 'date'},
    'TIME_OF_COLLECTION': {'required': False},
    'COLLECTION_LOCATION': {'required': True},
    'DECIMAL_LATITUDE': {'required': True},
    'DECIMAL_LONGITUDE': {'required': True},
    'LATITUDE_START': {'required': False},
    'LONGITUDE_START': {'required': False},
    'LATITUDE_END': {'required': False},
    'LONGITUDE_END': {'required': False},
    'HABITAT': {'required': True},
    'DEPTH': {'required': False},
    'ELEVATION': {'required': False},
    'ORIGINAL_COLLECTION_DATE': {'required': False, 'type': 'date'},
    'ORIGINAL_GEOGRAPHIC_LOCATION': {'required': False},
    'ORIGINAL_DECIMAL_LATITUDE': {'required': False},
    'ORIGINAL_DECIMAL_LONGITUDE': {'required': False},
    'DESCRIPTION_OF_COLLECTION_METHOD': {'required': True},
    'DIFFICULT_OR_HIGH_PRIORITY_SAMPLE': {'required': True},
    'IDENTIFIED_BY': {'required': True},
    'IDENTIFIER_AFFILIATION': {'required': True},
    'IDENTIFIED_HOW': {'required': True},
    'SPECIMEN_IDENTITY_RISK': {'required': True},
    'MIXED_SAMPLE_RISK': {'required': True},
    'PRESERVED_BY': {'required': True},
    'PRESERVER_AFFILIATION': {'required': True},
    'PRESERVATION_APPROACH': {'required': True},
    'PRESERVATIVE_SOLUTION': {'required': True},
    'TIME_ELAPSED_FROM_COLLECTION_TO_PRESERVATION': {'required': True},
    'DATE_OF_PRESERVATION': {'required': True},
    'SIZE_OF_TISSUE_IN_TUBE': {'required': True},
    'TISSUE_REMOVED_FOR_BARCODING': {'required': True},
    'TUBE_OR_WELL_ID_FOR_BARCODING': {'required': True},
    'TISSUE_FOR_BARCODING': {'required': True},
    'BARCODE_PLATE_PRESERVATIVE': {'required': True},
    'BARCODING_STATUS': {'required': True},
    'TISSUE_REMOVED_FOR_BIOBANKING': {'required': True},
    'TISSUE_VOUCHER_ID_FOR_BIOBANKING': {'required': True},
    'PROXY_TISSUE_VOUCHER_ID_FOR_BIOBANKING': {'required': False},
    'TISSUE_FOR_BIOBANKING': {'required': True},
    'BIOBANKED_TISSUE_PRESERVATIVE': {'required': True},
    'DNA_REMOVED_FOR_BIOBANKING': {'required': True},
    'DNA_VOUCHER_ID_FOR_BIOBANKING': {'required': True},
    'VOUCHER_ID': {'required': True},
    'PROXY_VOUCHER_ID': {'required': False},
    'VOUCHER_LINK': {'required': False},
    'PROXY_VOUCHER_LINK': {'required': False},
    'VOUCHER_INSTITUTION': {'required': False},
    'REGULATORY_COMPLIANCE': {'required': False},
    'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_RIGHTS_APPLICABLE': {'required': True},
    'INDIGENOUS_RIGHTS_DEF': {'required': False},
    'ASSOCIATED_TRADITIONAL_KNOWLEDGE_OR_BIOCULTURAL_PROJECT_ID': {'required': False},
    'ASSOCIATED_TRADITIONAL_KNOWLEDGE_CONTACT': {'required': False},
    'ETHICS_PERMITS_REQUIRED': {'required': True},
    'ETHICS_PERMITS_DEF': {'required': True},
    'ETHICS_PERMITS_FILENAME': {'required': True},
    'SAMPLING_PERMITS_REQUIRED': {'required': True},
    'SAMPLING_PERMITS_DEF': {'required': True},
    'SAMPLING_PERMITS_FILENAME': {'required': True},
    'NAGOYA_PERMITS_REQUIRED': {'required': True},
    'NAGOYA_PERMITS_DEF': {'required': True},
    'NAGOYA_PERMITS_FILENAME': {'required': True},
    'HAZARD_GROUP': {'required': True, 'type': 'integer'},
    'PRIMARY_BIOGENOME_PROJECT': {'required': False},
    'ASSOCIATED_PROJECT_ACCESSIONS': {'required': False},
    'OTHER_INFORMATION': {'required': False}
}

# Blocks
asg_manifest_blocks = [
    {'start_letter': 'A', 'end_letter':'D', "column_colour": "green", "block": "1","block_description":"Sample submission information including specimen identifier and tube/well identifiers"},
    {'start_letter': 'E', 'end_letter':'M', "column_colour": "yellow", "block": "2","block_description":"Taxonomic information including species name, family and common name"},
    {'start_letter': 'N', 'end_letter':'R', "column_colour": "purple", "block": "3","block_description":"Biological information of the sample including lifestage, sex, and organism part"},
    {'start_letter': 'S', 'end_letter':'T', "column_colour": "grey", "block": "4","block_description":"Details of the submitting GAL and the associated organisational codes"},
    {'start_letter': 'U', 'end_letter':'AL', "column_colour": "blue", "block": "5","block_description":"Data on the collector, collection event, and collection localities"},
    {'start_letter': 'AM', 'end_letter':'AQ', "column_colour": "limegreen", "block": "6","block_description":"Information on taxonomic identification, taxonomic uncertainty and risks"},
    {'start_letter': 'AR', 'end_letter':'AX', "column_colour": "brown", "block": "7","block_description":"Details of the tissue preservation event"},
    {'start_letter': 'AY', 'end_letter':'BF', "column_colour": "teal", "block": "8","block_description":"Information on DNA barcoding"},
    {'start_letter': 'BG', 'end_letter':'BK', "column_colour": "red", "block": "9","block_description":"Information on vouchering and biobanking"},
    {'start_letter': 'BL', 'end_letter':'BP', "column_colour": "skyblue", "block": "10","block_description":"IAdditional information fields including free text field for other important sample notes"},
    ]

dtol_manifest_blocks = [
    {'start_letter': 'A', 'end_letter':'D', "column_colour": "green", "block": "1","block_description":"Sample submission information including specimen identifier and tube/well identifiers"},
    {'start_letter': 'E', 'end_letter':'M', "column_colour": "yellow", "block": "2","block_description":"Taxonomic information including species name, family and common name"},
    {'start_letter': 'N', 'end_letter':'R', "column_colour": "purple", "block": "3","block_description":"Biological information of the sample including lifestage, sex, and organism part"},
    {'start_letter': 'S', 'end_letter':'T', "column_colour": "grey", "block": "4","block_description":"Details of the submitting GAL and the associated organisational codes"},
    {'start_letter': 'U', 'end_letter':'AL', "column_colour": "blue", "block": "5","block_description":"Data on the collector, collection event, and collection localities"},
    {'start_letter': 'AM', 'end_letter':'AQ', "column_colour": "limegreen", "block": "6","block_description":"Information on taxonomic identification, taxonomic uncertainty and risks"},
    {'start_letter': 'AR', 'end_letter':'AX', "column_colour": "brown", "block": "7","block_description":"Details of the tissue preservation event"},
    {'start_letter': 'AY', 'end_letter':'BF', "column_colour": "teal", "block": "8","block_description":"Information on DNA barcoding"},
    {'start_letter': 'BG', 'end_letter':'BK', "column_colour": "red", "block": "9","block_description":"Information on vouchering and biobanking"},
    {'start_letter': 'BL', 'end_letter':'BP', "column_colour": "skyblue", "block": "10","block_description":"IAdditional information fields including free text field for other important sample notes"},
    ]

erga_manifest_blocks = [
    {'start_letter': 'A', 'end_letter':'F', "column_colour": "green", "block": "1","block_description":"Sample submission information including specimen identifier and tube/well identifiers,as well as information on the sample coordinator"},
    {'start_letter': 'G', 'end_letter':'O', "column_colour": "yellow", "block": "2","block_description":"Taxonomic information including species name, family and common name"},
    {'start_letter': 'P', 'end_letter':'T', "column_colour": "purple", "block": "3","block_description":"Biological information of the sample including lifestage, sex, and organism part"},
    {'start_letter': 'U', 'end_letter':'V', "column_colour": "grey", "block": "4","block_description":"Details of the submitting GAL and the associated organisational codes"},
    {'start_letter': 'W', 'end_letter':'AR', "column_colour": "blue", "block": "5","block_description":"Data on the collector, collection event, and collection localities"},
    {'start_letter': 'AS', 'end_letter':'AW', "column_colour": "limegreen", "block": "6","block_description":"Information on taxonomic identification, taxonomic uncertainty and risks"},
    {'start_letter': 'AX', 'end_letter':'BD', "column_colour": "brown", "block": "7","block_description":"Details of the tissue preservation event"},
    {'start_letter': 'BE', 'end_letter':'BI', "column_colour": "teal", "block": "8","block_description":"Information on DNA barcoding"},
    {'start_letter': 'BJ', 'end_letter':'BU', "column_colour": "red", "block": "9","block_description":"Information on Biobanking and Vouchering"},
    {'start_letter': 'BV', 'end_letter':'CI', "column_colour": "orange", "block": "10","block_description":"Information on regulatory compliances, Indigenous rights, traditional knowledge and permits"},
    {'start_letter': 'CJ', 'end_letter':'CM', "column_colour": "green", "block": "11","block_description":"Additional information including a free text field to house other important sample notes"}
    ]

def generate_manifest_json(manifest_type, manifest_file_path):
    output = list()
    
    columns = pd.read_excel(manifest_file_path).columns.tolist()

    column_letters = list()
    column_blocks = list()

    if manifest_type == "erga":
        manifest_blocks = erga_manifest_blocks
        manifest_mapping= erga_fields_mapping
    elif manifest_type == "asg":
        manifest_blocks = asg_manifest_blocks
        manifest_mapping= asg_fields_mapping
    elif manifest_type == "dtol":
        manifest_blocks = dtol_manifest_blocks
        manifest_mapping= dtol_fields_mapping

    for i in range(len(columns)):
        column_letter = get_column_letter(i + 1)
        column_letters.append({columns[i]: column_letter})
        column_block = [colour_range["block"] for colour_range in manifest_blocks if column_index_from_string(colour_range["start_letter"]) <= column_index_from_string(column_letter) <= column_index_from_string(colour_range["end_letter"])]
        
        column_blocks.append({'field': columns[i], 'block': column_block[0]})

    column_blocks_grouped = [{key: [g['field'] for g in group]} for key, group in groupby(column_blocks, lambda x: x['block'])]
    
    for d in manifest_blocks:
        fields_lst = list()
        output_dict = dict()
        output_dict["block"] = d["block"]
        output_dict["block_description"] = d["block_description"]
        output_dict["block_colour"] = d["column_colour"]
        
        columns = [v for d in column_blocks_grouped for k, v in d.items() if k == output_dict["block"]]

        for column_name in columns[0]:
            fields_dict = dict()

            fields_dict["name"] = column_name.strip()
            fields_dict["label"] = column_name.strip().replace("_", " ").replace("ID", "Identifier").replace("GAL","Genome Acquisition Lab").title().replace(" Or ", " or ")
        
            try:
                fields_dict["column"] = [value for d in column_letters for key, value in d.items() if key == column_name ][0]
            except IndexError:
                fields_dict["column"] = ""
            
            fields_dict["type"] = manifest_mapping.get(column_name,str()).get("type",str()) if column_name in manifest_mapping and manifest_mapping.get(column_name,str()).get("type",str()) else "string"
            fields_dict["required"] = manifest_mapping.get(column_name,str()).get("required",str()) if column_name in manifest_mapping and manifest_mapping.get(column_name,str()).get("required",str()) else False

            if column_name in DTOL_RULES:
                for key, value in DTOL_RULES[column_name].items():
                    if key.endswith("regex"):
                        fields_dict["regex"] = value            

            if column_name in DTOL_ENUMS:
                if manifest_type.upper() in DTOL_ENUMS[column_name]:
                    fields_dict["enum"] = DTOL_ENUMS[column_name][manifest_type.upper()]
                else:
                    fields_dict["enum"] = DTOL_ENUMS[column_name]

            if column_name in DTOL_UNITS:
                fields_dict["unit"] = DTOL_UNITS[column_name]["ena_unit"]
            
            fields_lst.append(fields_dict)

            output_dict["fields"] = fields_lst
        
        output.append(output_dict)
            
    # Uncomment for the following code to write the json to a file
    #'''
    file_name = f"{manifest_type}_manifest_fields.json"
    file_directory = "/copo/common/schema_versions/isa_mappings/"
    file_path = file_directory + file_name

    with open(file_path, 'w+') as f:
        print(json.dumps(output, indent=4, sort_keys=False,default=str), file=f)
        f.close()
    #'''


    return output

# Generate a json file for each manifest type
generate_manifest_json("asg", "/copo/media/assets/manifests/ASG_MANIFEST_TEMPLATE_v2.5.xlsx")
generate_manifest_json("dtol", "/copo/media/assets/manifests/DTOL_MANIFEST_TEMPLATE_v2.5.xlsx")
generate_manifest_json("erga", "/copo/media/assets/manifests/ERGA_MANIFEST_TEMPLATE_v2.5.xlsx")

