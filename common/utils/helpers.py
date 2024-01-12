import os
 
from .logger import Logger
from common.lookup.copo_enums import *
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django_tools.middlewares import ThreadLocal
import jsonref
import json
from django.conf import settings
import datetime

def get_group_membership_asString():
    r = ThreadLocal.get_current_request()
    gps = r.user.groups.all()
    gps = [str(g) for g in gps]
    return gps

def get_env(env_key):
    env_value = str()
    if env_key in os.environ:
        env_value = os.getenv(env_key)

    # resolve for file assignment
    file_env = os.environ.get(env_key + "_FILE", str())
    if len(file_env) > 0:
        try:
            with open(file_env, 'r') as mysecret:
                data = mysecret.read().replace('\n', '')
                env_value = data
        except:
            pass

    return env_value

public_name_service = get_env('PUBLIC_NAME_SERVICE')
l = Logger()


def notify_frontend(action="message", msg=str(), data={}, html_id="", max_ellipsis_length=100, profile_id="", group_name='dtol_status'):
    """
        function notifies client changes in Sample creation status
        :param profile_id:
        :param action:
        :param msg:
        :return:
    """
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id, "max_ellipsis_length": max_ellipsis_length}
    channel_layer = get_channel_layer()

    # The following line sometimes causes a RuntimeError - 
    # 'you cannot use AsyncToSync in the same thread as an async event loop'
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )

    return True

def notify_assembly_status(action="message", msg=str(), data={}, html_id="", profile_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id}
    channel_layer = get_channel_layer()
    group_name = 'assembly_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )
    return True

def notify_annotation_status(action="message", msg=str(), data={}, html_id="", profile_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id}
    channel_layer = get_channel_layer()
    group_name = 'annotation_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )
    return True

def notify_read_status(action="message", msg=str(), data={}, html_id="", profile_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id}
    channel_layer = get_channel_layer()
    group_name = 'read_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )
    return True

def notify_tagged_seq_status(action="message", msg=str(), data={}, html_id="", profile_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id}
    channel_layer = get_channel_layer()
    group_name = 'tagged_seq_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )
    return True


def notify_ena_object_status(action="message", msg=str(), data={}, html_id="", profile_id="", checklist_id=str()):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    if checklist_id.startswith("ERC") or checklist_id == "read":
        group_name = 'read_status_%s' % data["profile_id"]
    else:
        group_name = 'tagged_seq_status_%s' % data["profile_id"]
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id}
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )
    return True

def json_to_pytype(path_to_json, compatibility_mode=True):
    # use compatability mode if jsonref is causing problems
    with open(path_to_json, encoding='utf-8') as data_file:
        f = data_file.read()
        if compatibility_mode:
            data = json.loads(f)
        else:
            data = jsonref.loads(f, base_uri="file:" + settings.SCHEMA_VERSIONS_DIR + "/", jsonschema=True)
        if "properties" in data and isinstance(data["properties"], list):
            cp = list(data["properties"])
            idxes = list()
            # expand references
            tmp = list()
            for idx, el in enumerate(data["properties"]):
                if type(el) == jsonref.JsonRef:
                    tmp = tmp + data["properties"][idx]
                else:
                    tmp.append(el)
                    # data["properties"] = data["properties"] + data["properties"][idx]
            data["properties"] = tmp

    return data


def get_datetime():
    """
    provides a consistent way of storing fields of this type across modules
    :return:
    """
    return datetime.datetime.utcnow()


def get_not_deleted_flag():
    """
    provides a consistent way of setting records as not deleted
    :return:
    """
    return "0"


def get_deleted_flag():
    """
    provides a consistent way of setting records as not deleted
    :return:
    """
    return "1"

def default_jsontype(type):
    d_type = str()

    if type == "object":
        d_type = dict()
    elif type == "array":
        d_type = list()
    elif type == "boolean":
        d_type = False

    return d_type        

def get_user_id():
    return ThreadLocal.get_current_user().id


def get_current_user():
    return ThreadLocal.get_current_user()


def get_current_request():
    return ThreadLocal.get_current_request()


def get_base_url():
    r = ThreadLocal.get_current_request()
    scheme = r.scheme
    domain = r.get_host()
    return scheme + "://" + domain    

def get_copo_id():
    # todo: remove this and uncomment the below try block!!!
    import uuid
    u = uuid.uuid4()
    return int(str(u.time_low) + str(u.time_mid))
    # make unique copo id
    # try:
    #     return get_uid()
    # except ConnectionError:
    #     return "0" * 13    

def get_ena_remote_path(submission_token):
    """
    defines the path for datafiles uploaded to ENA Dropbox
    :param submission_token: the submission id
    :return:
    """
    remote_path = os.path.join(submission_token, str(get_current_user()))

    return remote_path    

def trim_parameter_value_label(label):
    if "Parameter Value" in label:
        return str.capitalize(label[label.index('[') + 1:label.index(']')])
    else:
        return label
    
def map_to_dict(x, y):
    # method to make output dict using keys from array x and values from array y
    out = dict()
    for idx, el in enumerate(x):
        out[el] = y[idx]
    return out

def get_users_seq_centres():
    from src.apps.copo_core.models import SequencingCentre
    user = ThreadLocal.get_current_user()
    seq_centres = SequencingCentre.objects.filter(users=user)
    return seq_centres
