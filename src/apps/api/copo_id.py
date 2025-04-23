import requests

from django.conf import settings


def get_uid():
    r = requests.get(settings['WEB_SERVICES']['COPO']['get_id'])
    # print(r.text)
    return r.text
