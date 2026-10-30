'''Unit tests for checking whether a blank LIMS manifest can be downloaded.

A blank LIMS manifest under an EDP profile lists only the samples from its Sapio
project that are yet to be submitted.

Users are warned if there are no more samples remaining to be submitted and 
shown an error message if the profile has no Sapio project to take the 
samples from before the blank manifest can be downloaded.
'''

from unittest import mock

import pytest

from src.apps.ei_edp.utils import EDPSchemaHandler as edp_module
from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES

PROFILE_ID = 'profile1'
PROJECT_ID = 'PR0001'
SCHEMAS = {'sample': []}


def check(profile, lims_samples=()):
    with mock.patch.object(edp_utils, 'Profile') as profile_da, \
            mock.patch.object(edp_utils, 'SinglecellSchemas') as schemas_da, \
            mock.patch.object(edp_module, 'get_lims_adapter') as get_lims_adapter:
        profile_da.return_value.get_record.return_value = profile
        schemas_da.return_value.get_collection_handle.return_value.find_one.return_value = {
            'schemas': SCHEMAS
        }
        get_lims_adapter.return_value.get_project_samples.return_value = list(lims_samples)
        return edp_utils.check_lims_blank_manifest(PROFILE_ID), get_lims_adapter.return_value


@pytest.mark.parametrize('profile', [
    None,  # Profile not found
    {},
    {'sapio_project_id': ''},
])
def test_error_without_sapio_project(profile):
    result, lims = check(profile)

    assert result == {'status': 'error', 'message': MESSAGES['missing_sapio_project']}
    lims.get_project_samples.assert_not_called()


@pytest.mark.parametrize('lims_samples', [
    [],  # the project has no samples
    [
        {'sample_id': 'S1', 'submitter_sample_reference': 'cust1'},
        {'sample_id': 'S2', 'submitter_sample_reference': 'cust2'},
    ],
])
def test_warns_when_no_samples_remain(lims_samples):
    result, lims = check({'sapio_project_id': PROJECT_ID}, lims_samples)

    assert result == {'status': 'warning', 'message': MESSAGES['all_samples_submitted']}
    lims.get_project_samples.assert_called_once_with(PROJECT_ID, SCHEMAS)


@pytest.mark.parametrize('pending_reference', ['', None])
def test_downloads_when_samples_remain(pending_reference):
    result, _ = check(
        {'sapio_project_id': PROJECT_ID},
        [
            {'sample_id': 'S1', 'submitter_sample_reference': 'cust1'},
            {'sample_id': 'S2', 'submitter_sample_reference': pending_reference},
        ],
    )

    assert result == {'status': 'success', 'message': ''}
