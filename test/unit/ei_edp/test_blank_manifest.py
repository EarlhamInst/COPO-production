'''Unit tests for checking whether a blank LIMS manifest can be downloaded.

Each batch of samples under an EDP profile has its own LIMS manifest. A blank
LIMS manifest lists only the batch's samples that are yet to be submitted.

Users are warned if there are no more samples remaining to be submitted and 
shown an error message if the profile has no Sapio project to take the 
samples from before the blank manifest can be downloaded.
'''

from unittest import mock

import pytest

from src.apps.ei_edp.utils import EDPSchemaHandler as edp_module
from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES

PROJECT_ID = 'PR0001'
SCHEMAS = {'sample': []}
BATCH = {'batch_id': 'b1', 'no_of_samples': 2, 'lims_sample_record_ids': None}
PROFILE = {'sapio_project_id': PROJECT_ID, 'sample_batches': [BATCH]}


def check(profile, lims_samples=(), batch_id=''):
    with mock.patch.object(edp_utils, 'SinglecellSchemas') as schemas_da, \
            mock.patch.object(edp_module, 'get_lims_adapter') as get_lims_adapter:
        schemas_da.return_value.get_collection_handle.return_value.find_one.return_value = {
            'schemas': SCHEMAS
        }
        get_lims_adapter.return_value.get_batch_samples.return_value = list(lims_samples)
        return edp_utils.check_lims_blank_manifest(profile, batch_id), get_lims_adapter.return_value


@pytest.mark.parametrize('profile', [
    {},  # Profile not found
    {'sapio_project_id': ''},
])
def test_error_without_sapio_project(profile):
    result, lims = check(profile)

    assert result == {'status': 'error', 'message': MESSAGES['missing_sapio_project']}
    lims.get_batch_samples.assert_not_called()


@pytest.mark.parametrize('lims_samples', [
    [],  # the project has no samples
    [
        {'sample_id': 'S1', 'submitter_sample_reference': 'cust1'},
        {'sample_id': 'S2', 'submitter_sample_reference': 'cust2'},
    ],
])
def test_warns_when_no_samples_remain(lims_samples):
    result, lims = check(PROFILE, lims_samples)

    assert result == {'status': 'warning', 'message': MESSAGES['all_samples_submitted']}
    lims.get_batch_samples.assert_called_once_with(PROFILE, 'b1', SCHEMAS)


@pytest.mark.parametrize('pending_reference', ['', None])
def test_downloads_when_samples_remain(pending_reference):
    result, _ = check(
        PROFILE,
        [
            {'sample_id': 'S1', 'submitter_sample_reference': 'cust1'},
            {'sample_id': 'S2', 'submitter_sample_reference': pending_reference},
        ],
    )

    assert result == {'status': 'success', 'message': ''}


def test_a_batch_must_be_chosen_when_there_are_several():
    profile = {**PROFILE, 'sample_batches': [BATCH, {**BATCH, 'batch_id': 'b2'}]}

    result, lims = check(profile)

    assert result == {'status': 'error', 'message': MESSAGES['select_sample_batch']}
    lims.get_batch_samples.assert_not_called()


def test_the_chosen_batchs_samples_are_checked():
    profile = {**PROFILE, 'sample_batches': [BATCH, {**BATCH, 'batch_id': 'b2'}]}

    result, lims = check(profile, [{'sample_id': 'S3', 'submitter_sample_reference': ''}], batch_id='b2')

    assert result == {'status': 'success', 'message': ''}
    lims.get_batch_samples.assert_called_once_with(profile, 'b2', SCHEMAS)
