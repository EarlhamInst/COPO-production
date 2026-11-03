'''Unit tests for the broker checks run before a manifest is downloaded or uploaded.

The broker matches on the schema to decide which checks apply. For the EDP
schema used by LIMS manifests, samples must be submitted before a LIMS
manifest can be downloaded or uploaded and a blank LIMS manifest also needs
samples still remaining to be submitted in its LIMS project.
Other schemas have no checks, so the download or upload goes ahead.
'''

from unittest import mock

import pytest

from src.apps.copo_core import broker_da as broker_module
from src.apps.ei_edp.utils.constants import EDP_SCHEMA_NAME

PROFILE_ID = 'profile1'
PROFILE = {'_id': PROFILE_ID, 'type': 'ei_edp'}
SUCCESS = {'status': 'success', 'message': ''}
NO_SAMPLES = {'status': 'warning', 'message': 'no samples submitted'}
ALL_SUBMITTED = {'status': 'warning', 'message': 'all samples submitted'}


def run(task, schema_name, samples_submitted=SUCCESS, blank_manifest=SUCCESS):
    with mock.patch.object(broker_module, 'DAComponent'), \
            mock.patch.object(broker_module, 'Profile') as profile_da, \
            mock.patch.object(broker_module, 'edp_utils') as edp_utils:
        profile_da.return_value.get_record.return_value = PROFILE
        edp_utils.check_edp_samples_submitted.return_value = samples_submitted
        edp_utils.check_lims_blank_manifest.return_value = blank_manifest
        broker = broker_module.BrokerDA(
            context={},
            component='lims_integration',
            profile_id=PROFILE_ID,
            request_dict={'schema_name': schema_name},
        )
        context = getattr(broker, task)()
        return context['action_feedback'], edp_utils


@pytest.mark.parametrize('task', ['do_check_blank_manifest', 'do_check_manifest_upload'])
@pytest.mark.parametrize('schema_name', ['COPO_SINGLE_CELL', 'COPO_READ', ''])
def test_other_schemas_have_no_checks(task, schema_name):
    # The component is the same as for EDP, so only the schema tells them apart
    feedback, edp_utils = run(task, schema_name)

    assert feedback == SUCCESS
    edp_utils.check_edp_samples_submitted.assert_not_called()
    edp_utils.check_lims_blank_manifest.assert_not_called()


def test_lims_blank_manifest_warns_when_no_samples_submitted():
    feedback, edp_utils = run(
        'do_check_blank_manifest', EDP_SCHEMA_NAME, samples_submitted=NO_SAMPLES
    )

    assert feedback == NO_SAMPLES
    edp_utils.check_edp_samples_submitted.assert_called_once_with(profile=PROFILE)
    edp_utils.check_lims_blank_manifest.assert_not_called()


def test_lims_blank_manifest_warns_when_no_samples_remain():
    feedback, edp_utils = run(
        'do_check_blank_manifest', EDP_SCHEMA_NAME, blank_manifest=ALL_SUBMITTED
    )

    assert feedback == ALL_SUBMITTED
    edp_utils.check_lims_blank_manifest.assert_called_once_with(profile=PROFILE)


def test_lims_blank_manifest_downloads_when_both_checks_pass():
    feedback, _ = run('do_check_blank_manifest', EDP_SCHEMA_NAME)

    assert feedback == SUCCESS


@pytest.mark.parametrize('samples_submitted', [SUCCESS, NO_SAMPLES])
def test_lims_manifest_upload_depends_on_samples_submitted(samples_submitted):
    feedback, edp_utils = run(
        'do_check_manifest_upload', EDP_SCHEMA_NAME, samples_submitted=samples_submitted
    )

    assert feedback == samples_submitted
    edp_utils.check_edp_samples_submitted.assert_called_once_with(profile=PROFILE)
    edp_utils.check_lims_blank_manifest.assert_not_called()
