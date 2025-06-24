# settings for services e.g. postgres, mongo, redis, irods...

# from django.conf import settings
from . import base
from pymongo import MongoClient
from common.utils.helpers import get_env
import sys
import os

# this value tells COPO whether we are in Development or Production environment
ENVIRONMENT_TYPE = get_env('ENVIRONMENT_TYPE')
if ENVIRONMENT_TYPE == "":
    sys.exit(
        'ENVIRONMENT_TYPE environment variable not set. Value should be either "prod" or "dev"'
    )

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

# settings for manifest
MANIFEST_VERSION = {
    "ASG": "2.5",
    "DTOL": "2.5",
    "DTOLENV": "2.4",
    "DTOL_EI": "2.4",
    "ERGA": "2.5.1",
    "DTOL_BARCODE": "",
    "COPO_SINGLE_CELL": "0.2",
}

BARCODING_CHECKLIST = ["ERT000002", "ERT000020"]

ENA_CHECKLIST_CONFIG = {
    "ERT000002": {"skip": ["STRAIN", "LAB_HOST"]},
    "ERT000020": {"skip": ["STRAIN", "SPECVOUCH", "VARIETY", "IDBY"]},
}

ENA_CHECKLIST_URL = [
    "https://www.ebi.ac.uk/ena/submit/report/checklists/xml/ERT000002?type=sequence",
    "https://www.ebi.ac.uk/ena/submit/report/checklists/xml/ERT000020?type=sequence",
    "https://www.ebi.ac.uk/ena/submit/report/checklists/xml/*?type=sample",
]
COPO_SAMPLE_CHECKLIST_URL = [
    "https://raw.githubusercontent.com/TGAC/COPO-schemas/refs/heads/main/samples/sample_checklist_dwc.xml",
    "https://raw.githubusercontent.com/TGAC/COPO-schemas/refs/heads/main/samples/sample_checklist_faang.xml"
]


SINGLE_CELL_SCHEMAS_URL = {
   "COPO_SINGLE_CELL" : f"https://github.com/TGAC/COPO-schemas/raw/refs/heads/main/single_cell/singlecell_schema_main_v{MANIFEST_VERSION['COPO_SINGLE_CELL']}.xlsx"
   #"COPO_SINGLE_CELL" : f"singlecell_schema_main_v{MANIFEST_VERSION['COPO_SINGLE_CELL']}.xlsx"

}

IMAGE_FILE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.tiff', '.bmp']
