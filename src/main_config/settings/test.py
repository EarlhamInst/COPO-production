"""Settings for the unit test suite.

Unit tests check pure logic (date formats, string cleaning, manifest validation
rules) and must not need Mongo, Postgres, Redis or the network. Importing
``settings.data`` normally makes that impossible: it calls ``sys.exit()`` when
ENVIRONMENT_TYPE is unset, and raises ValueError converting empty strings to
ints for the Mongo/Redis ports.

So this module seeds placeholder values for what ``data`` insists on, imports
the real settings so nothing drifts out of sync with production config, then
replaces every external service with an in-memory or unreachable equivalent.

Use with: DJANGO_SETTINGS_MODULE=src.main_config.settings.test
"""

import os

# Seeded before importing .all, because settings.data reads these at import
# time. Hostnames use .invalid (RFC 2606) which is reserved and can never
# resolve, so a missed patch fails instead of reaching a real host. setdefault,
# not assignment, so a caller can still override any of these.
_PLACEHOLDER_ENV = {
    'ENVIRONMENT_TYPE': 'test',
    'MONGO_DB': 'unittest_placeholder_db',
    'MONGO_HOST': 'mongo.invalid',
    'MONGO_USER': 'unittest-placeholder-user',
    'MONGO_USER_PASSWORD': 'unittest-placeholder-not-a-real-password',
    'MONGO_PORT': '27017',
    'MONGO_MAX_POOL_SIZE': '1',
    'REDIS_HOST': 'redis.invalid',
    'REDIS_PORT': '6379',
    'SECRET_KEY': 'unittest-placeholder-secret-key-not-for-any-deployment',
}

for _key, _value in _PLACEHOLDER_ENV.items():
    os.environ.setdefault(_key, _value)

from .all import *  # noqa: E402,F401,F403

from pymongo import MongoClient  # noqa: E402

DEBUG = False

# In-memory stand-ins for the real services.
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
    }
}

SESSION_ENGINE = 'django.contrib.sessions.backends.signed_cookies'

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels.layers.InMemoryChannelLayer',
    }
}

# pymongo connects lazily, so this is safe to build with no server running.
# The 1ms timeouts mean a test that accidentally hits the database fails in
# about a second rather than hanging for pymongo's 30 second default.
MONGO_CLIENT = MongoClient(
    host='mongo.invalid',
    serverSelectionTimeoutMS=1,
    connectTimeoutMS=1,
)[MONGO_DB_TEST]  # noqa: F405
