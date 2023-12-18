import json
from urllib.parse import urljoin
import requests
from common.utils.logger import Logger
from common.schema_versions.lookup.dtol_lookups import API_KEY
from common.lookup.copo_enums import *
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django_tools.middlewares import ThreadLocal
from common.utils import helpers


def make_tax_from_sample(s):
    out = dict()
    out["SYMBIONT"] = "symbiont"
    out["TAXON_ID"] = s["TAXON_ID"]
    out["ORDER_OR_GROUP"] = s["ORDER_OR_GROUP"]
    out["FAMILY"] = s["FAMILY"]
    out["GENUS"] = s["GENUS"]
    out["SCIENTIFIC_NAME"] = s["SCIENTIFIC_NAME"]
    out["INFRASPECIFIC_EPITHET"] = s["INFRASPECIFIC_EPITHET"]
    out["CULTURE_OR_STRAIN_ID"] = s["CULTURE_OR_STRAIN_ID"]
    out["COMMON_NAME"] = s["COMMON_NAME"]
    out["TAXON_REMARKS"] = s["TAXON_REMARKS"]
    out["RACK_OR_PLATE_ID"] = s["RACK_OR_PLATE_ID"]
    out["TUBE_OR_WELL_ID"] = s["TUBE_OR_WELL_ID"]
    for el in out:
        out[el] = str(out[el]).strip()
    return out


public_name_service = helpers.get_env('PUBLIC_NAME_SERVICE')
l = Logger()


def query_public_name_service(sample_list):
    headers = {"api-key": API_KEY}
    url = urljoin(public_name_service, 'tol-ids')  # public-name
    l.log("name service urls: " + url, type=Logtype.FILE)
    try:
        r = requests.post(url=url, json=sample_list, headers=headers, verify=False)
        if r.status_code == 200:
            resp = json.loads(r.content)
            print(resp)
        else:
            # in the case there is a network issue, just return an empty dict
            resp = {}
            l.error('Name service response status code: ' + str(r.status_code) + ' ' + r.text)
        l.log("name service response: " + str(resp), type=Logtype.FILE)
        return resp
    except Exception as e:
        l.log("PUBLIC NAME SERVER ERROR: " + str(e), type=Logtype.FILE)
        l.exception(e)
        return {}

