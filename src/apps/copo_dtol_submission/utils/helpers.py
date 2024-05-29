import os
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django_tools.middlewares import ThreadLocal
from common.schema_versions.lookup.dtol_lookups import API_KEY
from urllib.parse import urljoin
import requests
import json
from common.utils.logger import Logger
from common.utils import helpers

public_name_service = helpers.get_env('PUBLIC_NAME_SERVICE')

def query_public_name_service(sample_list):
    headers = {"api-key": API_KEY}
    url = urljoin(public_name_service, 'tol-ids')  # public-name
    Logger().log("name service urls: " + url)
    try:
        r = requests.post(url=url, json=sample_list, headers=headers, verify=False)
        if r.status_code == 200:
            resp = json.loads(r.content)
            print(resp)
        else:
            # in the case there is a network issue, just return an empty dict
            resp = {}
            Logger().error('Name service response status code: ' + str(r.status_code) + ' ' + r.text)

        Logger().log("name service response: " + str(resp))
        return resp
    except Exception as e:
        Logger().log(msg="PUBLIC NAME SERVER ERROR: " + str(e))
        return {}
 
