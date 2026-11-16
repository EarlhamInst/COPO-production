'''Unit tests for checking a profile's fields before it is saved.

Only the fields shown in the profile form are checked. Fields left out of the
form (e.g. an EDP profile's sample batch fields, which are entered in a
separate dialog) aren't sent when a profile is edited, so whatever sets them
checks them instead.
'''

from unittest import mock

from common.dal import profile_da
from common.dal.profile_da import Profile

SCHEMA = [
    {'id': 'copo.profile.title', 'label': 'Title', 'required': 'true', 'show_in_form': True},
    {'id': 'copo.profile.sample_type', 'label': 'Sample type', 'required': 'true', 'show_in_form': False},
    {'id': 'copo.profile.related_pip', 'label': 'Jira PIP', 'required': 'true', 'show_in_form': False,
     'regex': '^PIP-[0-9]+$'},
]


def validate(auto_fields):
    with mock.patch.object(Profile, 'get_component_schema', return_value=SCHEMA), \
            mock.patch.object(profile_da, 'PROFILE_TITLE_MIN_LENGTH', 1):
        return Profile().validate_record(auto_fields=auto_fields)


def test_fields_left_out_of_the_form_are_not_required():
    result = validate({'copo.profile.title': 'Profile'})

    assert result['status'] == 'success'


def test_fields_left_out_of_the_form_are_not_checked_against_their_regex():
    result = validate({'copo.profile.title': 'Profile', 'copo.profile.related_pip': 'not a PIP'})

    assert result['status'] == 'success'


def test_fields_in_the_form_are_still_required():
    result = validate({'copo.profile.title': ''})

    assert result == {'status': 'error', 'message': 'Missing required field: Title title'}
