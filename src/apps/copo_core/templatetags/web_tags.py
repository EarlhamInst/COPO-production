__author__ = 'fshaw'
from django import template
from django.contrib.auth.models import Group
from django.conf import settings
from datetime import datetime
from src.apps.copo_core.models import SequencingCentre
import re

register = template.Library()


@register.filter("mongo_id")
def mongo_id(value):
    # return the $oid field of _id (which is a dict)
    try:
        return str(value['_id']['$oid'])
    except:
        return str(value['_id'])


@register.filter("datafile_title")
def datafile_title(value):
    from common.dal.copo_da import DataFile
    d = DataFile().get_record(value)
    cu = DataFile().get_relational_record_for_id(d['file_id'])
    return cu.filename


@register.filter("make_repo_name")
def make_repo_name(value):
    str = ''
    if value == "ena":
        str += "ENA"
    else:
        str += value.capitalize()
    return str + ' ' + 'Submission'


@register.filter("make_file_count")
def make_file_count(value):
    num = len(value.get('bundle', 0))
    if num > 1:
        return str(num) + ' ' + 'file'
    else:
        return str(num) + ' ' + 'files'


@register.filter("produce_submission_header")
def produce_submission_header(value):
    str = ''
    str += make_repo_name(value.get('repository', 'Default'))
    str += ' - '
    str += make_file_count(value)

    return str


@register.filter("has_group")
def check_group(user, group_name):
    group = Group.objects.get(name=group_name)
    return group in user.groups.all()


@register.filter(is_safe=True, name="get_blank_manifest_url")
def get_blank_manifest_url(value):
    manifest_version = settings.MANIFEST_VERSION
    version = manifest_version.get(value.upper(), "")
    version = "_v" + version if version else ""
    return settings.MANIFEST_DOWNLOAD_URL.format(value.upper(), version)


@register.filter(is_safe=True, name="get_sop_url")
def get_sop_url(value):
    manifest_version = settings.MANIFEST_VERSION
    version = manifest_version.get(value.upper(), "")
    version = "_v" + version if version else ""
    return settings.SOP_DOWNLOAD_URL.format(value.upper(), version)

@register.filter(is_safe=True, name="get_short_profile_type")
def get_short_profile_type(value):
    result = re.search(r"\((.*?)\)", value)
    return result.group(1) if result else value


@register.filter(is_safe=True, name="get_first_value_from_array")
def get_first_value_from_array(value):
    if value and type(value) is list:
        result = value[0]
        if type(result) is datetime:
            result = result.strftime('%a, %d %b %Y %H:%M')
        return result
    return value


@register.filter(is_safe=True, name="is_list_empty")
def is_list_empty(value):
    # Check if list is an empty list
    if value and isinstance(value, list):
        if len(value) == 1 and '' in value:
            return False
        return True
    else:
        return False
    
@register.filter(is_safe=True, name="get_sequencing_centre_label")
def get_sequencing_centre_label(value):
    # Get the label of the sequencing centre based on the abbreviation
    centre = SequencingCentre.objects.get(name=value)
    return f"{centre.label.title()} ({value})"

