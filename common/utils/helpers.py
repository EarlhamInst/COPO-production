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


def describe_regex(pattern):
    '''Return a human-readable description for a regex pattern'''

    def normalise_pattern(p):
        if not isinstance(p, str):
            return p
        try:
            return p.encode('utf-8').decode('unicode_escape')
        except Exception:
            return p

    try:
        # Known regex patterns
        known_patterns = {
            r'\\d+': 'a whole number (digits only)',
            r'[+-]?[0-9]+.?[0-9]*': 'a positive or negative integer or decimal number',
            r'(0|((0\\.)|([1-9][0-9]*\\.?))[0-9]*)([Ee][+-]?[0-9]+)?': 'a number in standard or scientific notation',
            r'(^[12][0-9]{3}(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01])(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?(/[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?)?$)|(^not applicable$)|(^not collected$)|(^not provided$)|(^restricted access$)|(^missing: control sample$)|(^missing: sample group$)|(^missing: synthetic construct$)|(^missing: lab stock$)|(^missing: third party data$)|(^missing: data agreement established pre-2023$)|(^missing: endangered species$)|(^missing: human-identifiable$)|(^missing$)': 'A date/time value or one of the allowed missing-data keywords (e.g. not provided, missing, restricted access).<br> If the date includes an unadded "00:00:00" time component, follow <a target="_blank" href="https://copo-docs.readthedocs.io/en/latest/help/faq/faq-errors-and-solutions.html#invalid-date-in-column">solution option 2</a> for resolution',
            r'((0|((0\\.)|([1-9][0-9]*\\.?))[0-9]*)([Ee][+-]?[0-9]+)?)|((^not collected$)|(^not provided$)|(^restricted access$)|(^missing: control sample$)|(^missing: sample group$)|(^missing: synthetic construct$)|(^missing: lab stock$)|(^missing: third party data$)|(^missing: data agreement established pre-2023$)|(^missing: endangered species$)|(^missing: human-identifiable$)|(^missing$))': 'a numeric value or one of several allowed missing-data keywords (e.g. not provided, missing, restricted access)',
            r'^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$': 'a year, year and month, or a full date (YYYY, YYYY-MM or YYYY-MM-DD)',
            r'[+-]?(0|((0\\.)|([1-9][0-9]*\\.?))[0-9]*)([Ee][+-]?[0-9]+)?': 'a positive or negative number with an optional decimal or exponent',
            r'(^(E|D|S)RZ[0-9]{6,}$)|(^GC(A|F)_[0-9]{9}.[0-9]+$|^[A-Z]{1}[0-9]{5}.[0-9]+$|^[A-Z]{2}[0-9]{6}.[0-9]+$|^[A-Z]{2}[0-9]{8}$|^[A-Z]{4}[0-9]{2}S?[0-9]{6,8}$|^[A-Z]{6}[0-9]{2}S?[0-9]{7,9}$)': 'an accession or reference code (e.g. ENA, GenBank, GCF, GCA)',
            r'(^[+-]?[0-9]+.?[0-9]{0,8}$)|(^not applicable$)|(^not collected$)|(^not provided$)|(^restricted access$)|(^missing: control sample$)|(^missing: sample group$)|(^missing: synthetic construct$)|(^missing: lab stock$)|(^missing: third party data$)|(^missing: data agreement established pre-2023$)|(^missing: endangered species$)|(^missing: human-identifiable$)|(^missing$)': 'a decimal number (up to 8 digits) or missing-data keyword (e.g. not provided, missing, restricted access)',
            r'[+-]?[0-9]+': 'a positive or negative integer number',
            r'^(\\d|[1-9]\\d|\\d\\.\\d{1,2}|[1-9]\\d\\.\\d{1,2}|100)$': 'a percentage or numeric value between 0 and 100 (up to two decimals)',
            r'([+-]?(0|((0\\.)|([1-9][0-9]*\\.?))[0-9]*)([Ee][+-]?[0-9]+)?)|((^not collected$)|(^not provided$)|(^restricted access$)|(^missing: control sample$)|(^missing: sample group$)|(^missing: synthetic construct$)|(^missing: lab stock$)|(^missing: third party data$)|(^missing: data agreement established pre-2023$)|(^missing: endangered species$)|(^missing: human-identifiable$)|(^missing$))': 'a numeric or allowed missing-data value (e.g. not provided, missing, restricted access)',
            r'(^[ESD]R[SR]\\d{6,}(,[ESD]R[SR]\\d{6,})*$)|(^SAM[END][AG]?\\d+(,SAM[END][AG]?\\d+)*$)|(^EGA[NR]\\d{11}(,EGA[NR]\\d{11})*$)|(^[ESD]R[SR]\\d{6,}-[ESD]R[SR]\\d{6,}$)|(^SAM[END][AG]?\\d+-SAM[END][AG]?\\d+$)|(^EGA[NR]\\d{11}-EGA[NR]\\d{11}$)': 'a list or range of accession IDs (e.g. ENA, SRA, or EGA)',
            r'((0|((0\\.)|([1-9][0-9]*\\.?))[0-9]*)([Ee][+-]?[0-9]+)?)|((^not collected$)|(^not provided$)|(^restricted access$)|(^missing: control sample$)|(^missing: sample group$)|(^missing: synthetic construct$)|(^missing: lab stock$)|(^missing: third party data$)|(^missing: data agreement established pre-2023$)|(^missing: endangered species$)|(^missing: human-identifiable$))': 'a numeric value or missing-data keyword (e.g. not provided, missing, restricted access)',
            r'(0|((0\\.)|([1-9][0-9]*\\.?))[0-9]*)': 'a non-negative integer or decimal number',
            r'[1-9][0-9]*\\.?[0-9]*([Ee][+-]?[0-9]+)?': 'a positive number with no leading zeros, optional decimal/exponent',
            r'[0-9]+': 'a whole number (digits only)',
            r'^[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?$': 'a date or datetime in ISO 8601 format (YYYY-MM-DD or extended)',
            r'[0-9]+(\\.[0-9]*)?': 'an integer or decimal number',
            r'[0-9]*(\\.[0-9]*)?': 'an optional integer or decimal number',
            r'(0|((0\\.)|([1-9][0-9]*)))': 'a whole number with no leading zeros',
            r'^150|1[0-4][0-9]|[1-9][0-9]|[0-9]|not provided$': "an integer between 0 and 150 or 'not provided'",
            r'[\\d][0-4]?(\\.[\\d]{1,10})?': 'a number with one or two digits before the decimal (second digit can be 0-4) and an optional decimal part of up to 10 digits',
            r'([0-9]*)': 'any number of digits or an empty/blank string',
            r'(^[ESD]RS\\d{6,}$)|(^SAM[END][AG]?\\d+$)|(^EGAN\\d{11}$)': 'a single accession ID (ENA/SRA/EGA format)',
            r'(^[ESD]RS\\d{6,}(,[ESD]RS\\d{6,})*$)|(^SAM[END][AG]?\\d+(,SAM[END][AG]?\\d+)*$)|(^EGAN\\d{11}(,EGAN\\d{11})*$)': 'multiple accession IDs, comma-separated (ENA/SRA/EGA format)',
            r'^[12][0-9]{3}(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01])(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?(/[0-9]{4}(-[0-9]{2}(-[0-9]{2}(T[0-9]{2}:[0-9]{2}(:[0-9]{2})?Z?([+-][0-9]{1,2})?)?)?)?)?$': 'ISO date or date range (e.g. 2023-05-01 or 2024-01-01)',
            r'(^[+-]?[0-9]+.?[0-9]{0,8}$)': 'a decimal number up to 8 digits',
            r'^[a-zA-Z0-9]+$': 'alphanumeric characters only (with no spaces)',
            r'^[A-Za-z]+(?: [A-Za-z]+)*[a-z]+$': 'letters and spaces, ending with lowercase letter',
            r'^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])$': 'a date in YYYY-MM-DD (year-month-day) or YYYY-MM (year-month) format',
            r'^-?(180(\\.0+)?|((1[0-7]\\d)|(\\d{1,2}))(\\.\\d+)?)$': 'a value between -180 and 180',
            r'^[A-Za-z]+(?: [A-Za-z]+)*$': 'alphabetic words with optional spaces',
            r'^[A-Za-z. ]*[a-z]+$': 'an alphabetic text with periods/spaces, ending in lowercase',
            r'^[a-zA-Z0-9]+(?: [a-zA-Z0-9]+)*$': 'alphanumeric words separated by spaces',
            r'^[A-Za-z]*[a-z]+(?:[0-9]+)*$': 'letters followed optionally by digits',
            r'^[A-Za-z\\s]+[a-z]+$': 'letters with spaces, ending in lowercase',
            r'^https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)+(?: \\| https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)*)*$': "One or more valid URLs separated by '|'",
            r'^((\\d{4})(-\\d{2}(-\\d{2}(T\\d{2}:\\d{2}(:\\d{2})?(Z|[+-]\\d{2}:?\\d{2})?)?)?)?(/(\\d{4}|(\\d{2}(-\\d{2}(T\\d{2}:\\d{2}(:\\d{2})?(Z|[+-]\\d{2}:?\\d{2})?)?)?)?))?)$': 'a date or date range in extended ISO 8601 format',
            r'^-?\\d+(\\.\\d+)?$': 'an integer or decimal number (may be negative)',
            r'^\\d{4}-(0[1-9]|1[0-2])(-([0-2]\\d|3[01]))?$': 'a date in YYYY-MM-DD (year-month-day) or YYYY-MM (year-month) format',
            r'^(\\d+\\s*years?)?\\s*(\\d+\\s*weeks?)?\\s*(\\d+\\s*days?)?$': "a duration or age (e.g. '3 years 2 weeks 5 days' or '1 year')",
            r'^(https?|ftp):\\/\\/[^\\s/$.?#].[^\\s]*$': 'a valid HTTP or FTP URL',
            r'^[A-Za-z0-9_]+$': 'an alphanumeric text with underscores only',
        }

        # Normalise regex patterns to escape slashes
        known_patterns = {normalise_pattern(k): v for k, v in known_patterns.items()}

        if pattern in known_patterns:
            return known_patterns[pattern]
        else:
            l.log(f'Missing regex pattern: {pattern}. No regex description found.')

        # Heuristic checks for similar regex patterns
        if '@' in pattern and '\\.' in pattern:
            return 'a valid email address'
        if '\\d{4}' in pattern and '-' in pattern:
            return 'a date in YYYY-MM-DD format'
        if any([protocol in pattern for protocol in ['https?', 'http?', 'ftp']]):
            return 'a valid URL'
        if '[A-Z]' in pattern and '\\d' in pattern:
            return 'an alphanumeric code'
        if '\\d' in pattern:
            return 'a numeric value'

        # Fallback for unknown patterns
        return 'a value in a required format'
    except Exception as e:
        l.exception(f"Error describing regex pattern '{pattern}': {e}")
