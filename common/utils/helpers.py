import os
import requests
import jsonref
import json
import datetime

from .logger import Logger
from common.lookup.copo_enums import *
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django_tools.middlewares import ThreadLocal
from django.conf import settings
from functools import wraps
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import re
from typing import List

def get_class(kls):
    parts = kls.split('.')
    # module = ".".join(parts[:-1])
    m = __import__(kls)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m


def post_interceptor(func=None, provider=None, call_back_function=None, parameter=None):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if provider:
            obj = get_class(provider)
            getattr(obj, call_back_function)(**parameter)
        return result

    return wrapper


def pre_interceptor(func=None, provider=None, call_back_function=None, parameter=None):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if provider:
            obj = get_class(provider)
            if getattr(obj, call_back_function)(**parameter):
                return func(*args, **kwargs)
        else:
            return func(*args, **kwargs)

    return wrapper


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


def notify_frontend(
    action="message",
    msg=str(),
    data={},
    html_id="",
    max_ellipsis_length=100,
    profile_id="",
    group_name='dtol_status',
):
    """
    function notifies client changes in Sample creation status
    :param profile_id:
    :param action:
    :param msg:
    :return:
    """
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
        "max_ellipsis_length": max_ellipsis_length,
    }
    channel_layer = get_channel_layer()

    # The following line sometimes causes a RuntimeError -
    # 'you cannot use AsyncToSync in the same thread as an async event loop'
    try:
        async_to_sync(channel_layer.group_send)(group_name, event)
    except RuntimeError:
        pass
    return True


def notify_assembly_status(
    action="message", msg=str(), data={}, html_id="", profile_id=""
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'assembly_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_annotation_status(
    action="message", msg=str(), data={}, html_id="", profile_id=""
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'annotation_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_read_status(action="message", msg=str(), data={}, html_id="", profile_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'read_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_tagged_seq_status(
    action="message", msg=str(), data={}, html_id="", profile_id=""
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'tagged_seq_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_ena_object_status(
    action="message", msg=str(), data={}, html_id="", profile_id="", checklist_id=str()
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    if checklist_id.startswith("ERT"):
        group_name = 'tagged_seq_status_%s' % data["profile_id"]
    else:
        group_name = 'read_status_%s' % data["profile_id"]
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_singlecell_status(
    action="message", msg=str(), data={}, html_id="", profile_id="", checklist_id=str()
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'singlecell_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_submission_status(action="message", msg=str(), data={}, html_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'submission_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def json_to_pytype(path_to_json, compatibility_mode=True):
    # use compatability mode if jsonref is causing problems
    with open(path_to_json, encoding='utf-8') as data_file:
        f = data_file.read()
        if compatibility_mode:
            data = json.loads(f)
        else:
            data = jsonref.loads(
                f,
                base_uri="file:" + settings.SCHEMA_VERSIONS_DIR + "/",
                jsonschema=True,
            )
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
    elif type == "dict":
        d_type = dict()
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
        return str.capitalize(label[label.index('[') + 1 : label.index(']')])
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


def get_users_associated_profile_checkers():
    from src.apps.copo_core.models import AssociatedProfileType

    user = ThreadLocal.get_current_user()
    seq_centres = AssociatedProfileType.objects.filter(users=user)
    return seq_centres


def get_excluded_associated_projects():
    # This function returns a set of projects that should be
    # excluded from the associated project list
    from src.apps.copo_core.models import ProfileType, SequencingCentre

    # Fetch all profile types and sequencing centres
    profile_types = {item.type.upper() for item in ProfileType().get_profile_types()}

    # Add DTOL_ENV to the list of profile types. It is not present in the ProfileType collection
    # with the underscore but present in the AssociatedProfileType collection with the underscore
    profile_types.add('DTOL_ENV')
    sequencing_centres = {
        item.name for item in SequencingCentre().get_sequencing_centres()
    }

    # Combine profile types and sequencing centres into a set of excluded projects
    exclusions = profile_types | sequencing_centres

    return exclusions


@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    # Exponential backoff (2s, 4s, 8s, etc.)
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,  # Raise exception if all attempts fail
)
def check_and_save_bia_image_url(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if response.status_code == 200 and 'image' in response.headers.get(
            'Content-Type', ''
        ):
            return url  # URL is valid and leads to an image
    except requests.RequestException as e:
        l.exception(f'Retrying due to error fetching image: {e}')
        raise  # Raise exception to trigger retry
    return None  # URL does not exist or is not an image


def get_thumbnail_folder(profile_id):
    """
    This function returns the thumbnail folder for a given profile id
    :param profile_id:
    :return:
    """
    thumbnail_folder = os.path.join(settings.UPLOAD_PATH, profile_id)
    if not os.path.exists(thumbnail_folder):
        os.makedirs(thumbnail_folder)
    return thumbnail_folder

"""
def describe_regex(pattern):
    cleaned = re.sub(r'\|\(\^[^$]+\$\)', '', pattern)
    cleaned = re.sub(r'\(\^[^$]+\$\)\|', '', cleaned)
    cleaned = cleaned.strip('|')
    
    # Check for common patterns
    description_parts = []
    
    # Date/time patterns
    if re.search(r'\[12\]\[0-9\]\{3\}', cleaned):
        description_parts.append("year (1000-2999)")
    
    if re.search(r'0\[1-9\]\|1\[0-2\]', cleaned):
        description_parts.append("month (01-12)")
    
    if re.search(r'0\[1-9\]\|\[12\]\[0-9\]\|3\[01\]', cleaned):
        description_parts.append("day (01-31)")
    
    if re.search(r'T\[0-9\]\{2\}:\[0-9\]\{2\}', cleaned):
        description_parts.append("time (HH:MM)")
    
    if re.search(r':\[0-9\]\{2\}\)', cleaned) and 'time' in ' '.join(description_parts):
        # Seconds are optional
        idx = description_parts.index("time (HH:MM)")
        description_parts[idx] = "time (HH:MM or HH:MM:SS)"
    
    if 'Z?' in cleaned:
        description_parts.append("optional UTC indicator (Z)")
    
    if re.search(r'\[\+-\]\[0-9\]', cleaned):
        description_parts.append("optional timezone offset (±N)")
    
    if re.search(r'/', cleaned) and description_parts:
        description_parts.append("optional date range (using /)")
    
    # Numeric patterns
    if re.search(r'\\d\+', cleaned) and not description_parts:
        description_parts.append("one or more digits")
    
    if re.search(r'\[0-9\]\+', cleaned) and not description_parts:
        description_parts.append("numeric values")
    
    # Decimal patterns
    if re.search(r'\\\.\)', cleaned) or re.search(r'\[0-9\]\*\\\.\?', cleaned):
        description_parts.append("decimal numbers")
    
    # Scientific notation
    if re.search(r'\[Ee\]\[\+-\]', cleaned):
        description_parts.append("scientific notation (E±N)")
    
    # Word patterns
    if re.search(r'\\w\+', cleaned) and not description_parts:
        description_parts.append("word characters")
    
    # Alphanumeric patterns
    if re.search(r'\[A-Za-z0-9\]\+', cleaned) and not description_parts:
        description_parts.append("alphanumeric characters")

    # Email patterns
    if '@' in cleaned and '\\.' in cleaned:
        description_parts.append("email format")
    
    # Build sentence
    if not description_parts:
        return ""
    
    if len(description_parts) == 1:
        return description_parts[0]
    else:
        main_parts = description_parts[:-1]
        last_part = description_parts[-1]
        return f"{', '.join(main_parts)}, and {last_part}."
"""

def extract_exact_phrases_from_regex(pattern: str) -> List[str]:
    """
    Extract all exact text phrases from a regex pattern.
    Looks for patterns like (^exact text$) which match complete strings exactly.
    Args:
        pattern: The regex pattern to analyze
    Returns:
        List of exact text phrases found in the pattern
    Example:
        >>> pattern = r"(^hello$)|(^world$)|(\d+)"
        >>> extract_exact_phrases(pattern)
        ['hello', 'world']
    """
    # Pattern to find exact matches: (^text$)
    exact_phrase_pattern = r'\(\^([^$\)\]\}\+\?]+)\$\)'
    matches = re.findall(exact_phrase_pattern, pattern)
    return matches


def get_db_data_sources(source=None):
    from common.dal.copo_da import EnaReadPlatformCollection

    data_sources = {
        'sequencing_instrument': EnaReadPlatformCollection().get_sequencing_instrument_dropdown()
    }
    
    if source not in data_sources or source is None:
        l.error(f'No data source found for: {source}')
        return []
    return data_sources[source]