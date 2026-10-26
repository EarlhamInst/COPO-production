# settings for services e.g. postgres, mongo, redis, irods...

import os
import sys
from urllib.parse import urlsplit

from pymongo import MongoClient

from common.utils.helpers import get_env

from . import base

# this value tells COPO whether we are in Development or Production environment
ENVIRONMENT_TYPE = get_env('ENVIRONMENT_TYPE')
if ENVIRONMENT_TYPE == "":
    sys.exit(
        'ENVIRONMENT_TYPE environment variable not set. Value should be either "prod" or "dev"'
    )

# The Playwright test container reaches this service over the Docker Compose
# network via the "copo-web" alias (hyphen, not the "copo_web" service/container
# name — Django's Host-header parser rejects underscores outright, per RFC
# 1034/1035, regardless of ALLOWED_HOSTS). Allowed only outside production,
# same allowlist create_test_user.py uses for its own guard.
if ENVIRONMENT_TYPE in ('test', 'local', 'demo', 'dev'):
    base.ALLOWED_HOSTS.append('copo-web')

# settings for postgres
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': get_env('POSTGRES_DB'),
        'USER': get_env('POSTGRES_USER'),
        'PASSWORD': get_env('POSTGRES_PASSWORD'),
        'HOST': get_env('POSTGRES_SERVICE'),
        'PORT': get_env('POSTGRES_PORT'),
    }
}

# settings for mongodb
MONGO_DB = get_env('MONGO_DB')
MONGO_HOST = get_env('MONGO_HOST')
MONGO_USER = get_env('MONGO_USER')
MONGO_USER_PASSWORD = get_env('MONGO_USER_PASSWORD')
MONGO_PORT = int(get_env('MONGO_PORT'))
MONGO_MAX_POOL_SIZE = int(get_env('MONGO_MAX_POOL_SIZE'))
MONGO_DB_TEST = "test_copo_mongo"

# this is the global DB connection, either use get_collection_ref in dal.mongo_util.py or refer to this setting
# If unit testing is being done use the Mongo test database instead of the production/actual Mongo database

MONGO_CLIENT = MongoClient(
    host=MONGO_HOST,
    username=MONGO_USER,
    password=MONGO_USER_PASSWORD,
    maxPoolSize=MONGO_MAX_POOL_SIZE,
)[MONGO_DB]

# MONGO_CLIENT.authenticate(MONGO_USER, MONGO_USER_PASSWORD, source='admin')

# settings for redis
SESSION_ENGINE = 'redis_sessions.session'
SESSION_REDIS_HOST = get_env('REDIS_HOST')
SESSION_REDIS_PORT = int(get_env('REDIS_PORT'))

SESSION_REDIS = {
    'host': SESSION_REDIS_HOST,
    'port': SESSION_REDIS_PORT,
    'db': 0,
    #'password': 'password',
    'prefix': 'session',
    'socket_timeout': 1,
    'retry_on_timeout': False,
}


# django channels settings
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [(SESSION_REDIS_HOST, SESSION_REDIS_PORT)],
            'capacity': 1500,
            'expiry': 10,
        },
    },
}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': f'redis://{SESSION_REDIS_HOST}:{SESSION_REDIS_PORT}',
    }
}

# settings for object stores
SAMPLE_OBJECT_STORE = os.path.join(base.MEDIA_ROOT, 'object_store', 'samples.h5')
DATAFILE_OBJECT_STORE = os.path.join(base.MEDIA_ROOT, 'object_store', 'datafiles.h5')
SAMPLE_OBJECT_PREFIX = "samples_"
DATAFILE_OBJECT_PREFIX = "datafiles_"
DESCRIPTION_GRACE_PERIOD = 10  # no of days after which pending descriptions are deleted

# settings for ECS
ECS_ACCESS_KEY_ID = get_env('ECS_ACCESS_KEY_ID')
ECS_SECRET_KEY = get_env('ECS_SECRET_KEY')
ECS_ENDPOINT = get_env('ECS_ENDPOINT')

# Settings for Sapio
SAPIO_API_URL = get_env('SAPIO_API_URL')
SAPIO_WEB_URL = f'{urlsplit(SAPIO_API_URL) .scheme}://{urlsplit(SAPIO_API_URL) .netloc}'
SAPIO_PROJECT_DEPARTMENT = 'Technical Genomics'

# Settings for ENA
ENA_URL_HOSTNAME = get_env('ENA_URL_HOSTNAME') or 'www.ebi.ac.uk'

# Accession links based on a repository like ENA, Zenodo and Sapio
# Columns that end with 'field_suffix' are rendered in the UI as a link
# to 'url_template' with '{accession}' replaced by the accession
# NB: '{accession}' is escaped as '{{accession}}' within f-strings.
# It is exposed to JavaScript as 'REPOSITORY_ACCESSION_LINKS' via
# the 'custom_context_template.py' file
REPOSITORY_ACCESSION_LINKS = {
    'ena': {
        'field_suffix': 'accession_ena',
        'url_template': f'https://{ENA_URL_HOSTNAME}/ena/browser/view/{{accession}}',
    },
    'sapio': {
        'field_suffix': 'accession_sapio',
        'url_template': f'{SAPIO_WEB_URL}/veloxClient/#dataType=Project;recordId={{accession}};view=dataRecord',
    },
    'zenodo': {
        'field_suffix': 'accession_zenodo',
        'url_template': 'https://zenodo.org/records/{accession}',
    },
}

# settings for manifest
MANIFEST_VERSION = {
    "ASG": "2.5",
    "DTOL": "2.5",
    "DTOLENV": "2.4",
    "DTOL_EI": "2.4",
    "ERGA": "2.5.1",
    "DTOL_BARCODE": "",
    "COPO_SINGLE_CELL": "0.4",
    "COPO_IMAGE_REMBI": "0.1",
    "COPO_IMAGE_STX_FISH": "0.1",
    "COPO_READ": "0.1",
    "EI_EDP": "0.1",
}

BARCODING_CHECKLIST = ["ERT000002", "ERT000020"]

ENA_CHECKLIST_CONFIG = {
    "ERT000002": {"skip": ["STRAIN", "LAB_HOST"]},
    "ERT000020": {"skip": ["STRAIN", "SPECVOUCH", "VARIETY", "IDBY"]},
}

ENA_BROWSER_API_URL = {'xml': f'https://{ENA_URL_HOSTNAME}/ena/browser/api/xml'}

ENA_CHECKLIST_URL = [
    f"https://{ENA_URL_HOSTNAME}/ena/submit/report/checklists/xml/ERT000002?type=sequence",
    f"https://{ENA_URL_HOSTNAME}/ena/submit/report/checklists/xml/ERT000020?type=sequence",
    f"https://{ENA_URL_HOSTNAME}/ena/submit/report/checklists/xml/*?type=sample",
]

COPO_SAMPLE_CHECKLIST_URL = [
    "https://raw.githubusercontent.com/EarlhamInst/COPO-schemas/refs/heads/main/samples/sample_checklist_dwc.xml",
    "https://raw.githubusercontent.com/EarlhamInst/COPO-schemas/refs/heads/main/samples/sample_checklist_faang.xml",
    "https://raw.githubusercontent.com/EarlhamInst/COPO-schemas/refs/heads/main/samples/sample_checklist_faang_mouse_brain.xml"
]

SINGLE_CELL_SCHEMAS_URL = {
    # "COPO_SINGLE_CELL": f"singlecell_schema_main_v{MANIFEST_VERSION['COPO_SINGLE_CELL']}.xlsx",
    # "COPO_IMAGE_REMBI": f"rembi_image_schema_main_v{MANIFEST_VERSION['COPO_IMAGE_REMBI']}.xlsx",
    # "COPO_IMAGE_STX_FISH": f"stx_fish_image_schema_main_v{MANIFEST_VERSION['COPO_IMAGE_STX_FISH']}.xlsx",
    # "COPO_READ" : f"reads_schema_main_v{MANIFEST_VERSION['COPO_READ']}.xlsx"
    "COPO_SINGLE_CELL": f"https://github.com/EarlhamInst/COPO-schemas/raw/refs/heads/main/single_cell/singlecell_schema_main_v{MANIFEST_VERSION['COPO_SINGLE_CELL']}.xlsx",
    "COPO_IMAGE_REMBI": f"https://github.com/EarlhamInst/COPO-schemas/raw/refs/heads/main/images/rembi_image_schema_main_v{MANIFEST_VERSION['COPO_IMAGE_REMBI']}.xlsx",
    "COPO_IMAGE_STX_FISH": f"https://github.com/EarlhamInst/COPO-schemas/raw/refs/heads/main/images/stx_fish_image_schema_main_v{MANIFEST_VERSION['COPO_IMAGE_STX_FISH']}.xlsx",
    "COPO_READ": f"https://github.com/EarlhamInst/COPO-schemas/raw/refs/heads/main/reads/reads_schema_main_v{MANIFEST_VERSION['COPO_READ']}.xlsx",
    "EI_EDP": f"https://github.com/EarlhamInst/COPO-schemas/raw/refs/heads/main/edp/edp_schema_main_v{MANIFEST_VERSION['EI_EDP']}.xlsx",
}

SINGLE_CELL_SCHEMAS_TEMPLATE_URL = {
    # "EI_EDP" : f"https://github.com/EarlhamInst/COPO-schemas/raw/refs/heads/main/edp/edp_schema_template_v{MANIFEST_VERSION['EI_EDP']}.xlsx"
    "EI_EDP": f"edp_schema_template_v{MANIFEST_VERSION['EI_EDP']}.xlsx"
}

IMAGE_FILE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.tiff', '.bmp']

# number of days to keep local uploads before deleting them
LOCAL_UPLOAD_HOUSEKEEPING_DAYS = 3
