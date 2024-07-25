import os
#from django.conf import settings
from . import base

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

TEMPLATES = [

    {

        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            # insert your TEMPLATE_DIRS here
            #
            os.path.join( BASE_DIR, 'src', 'apps', 'copo_landing_page', 'templates'),
            os.path.join( BASE_DIR, 'src', 'apps', 'copo_core', 'templates', 'copo'),
            os.path.join( BASE_DIR, 'static', 'swagger'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.contrib.auth.context_processors.auth',
                'django.template.context_processors.debug',
                'django.template.context_processors.i18n',
                'django.template.context_processors.media',
                'django.template.context_processors.static',
                'django.template.context_processors.tz',
                'django.contrib.messages.context_processors.messages',
                'django.template.context_processors.csrf',
                'django.template.context_processors.request',
                'django.template.context_processors.static',
                'django.contrib.auth.context_processors.auth',
                'src.apps.copo_core.copo_user_status_messages.latest_message',
                'src.apps.copo_core.copo.copo_context_processors.copo_context'
            ],
            'debug':  base.DEBUG,
        },
    },
]

ROOT_URLCONF = 'src.main_config.urls'
