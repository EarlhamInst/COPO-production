'''Unit tests for locking an EDP profile's sample, container and library types.

The sample type decides which LIMS manifest is offered for a profile and, all
three types are written to every sample in the Sapio project on each save. Once
samples have been submitted (their `C_CustomerSampleName` field is set), changing
any of them would relabel the submitted samples and hide their manifest, so the
change is rejected.
'''

from unittest import mock

import pytest

from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES
from src.apps.ei_edp.utils.lims.sapio import adapter as sapio_adapter
from src.apps.ei_edp.utils.lims.sapio.adapter import SapioAdapter

PROJECT_ID = 'PR0001'
SAVED_PROFILE = {
    'sapio_project_id': PROJECT_ID,
    'no_of_samples': '5',
    'sample_type': 'Tissue',
    'container_type': 'tube',
    'library_type': '',
}


class FakeSample:
    def __init__(self, customer_name):
        self.customer_name = customer_name

    def get_field_value(self, field):
        return self.customer_name if field == 'C_CustomerSampleName' else None


def validate(submitted, changed_fields, total=5):
    # A Sapio project holding `total` samples, the first `submitted` of
    # which have a customer name
    samples = [FakeSample(f'cust{i}' if i < submitted else '') for i in range(total)]
    project = mock.Mock()
    project.get_children_of_type.return_value = samples
    sapio = mock.Mock()
    sapio.return_value.dataRecordManager.query_data_records.return_value.result_list = [object()]
    sapio.return_value.inst_man.add_existing_record.return_value = project

    with mock.patch.object(sapio_adapter, 'Sapio', sapio):
        return SapioAdapter.__new__(SapioAdapter).validate_profile_change(
            SAVED_PROFILE, total, changed_fields
        )


@pytest.mark.parametrize('submitted, changed_fields', [
    (0, ['sample_type']),  # nothing submitted yet, so any type can change
    (0, ['sample_type', 'container_type', 'library_type']),
    (3, []),  # submitted but, no locked field changes
])
def test_allowed_changes(submitted, changed_fields):
    assert validate(submitted, changed_fields) == {'status': 'success'}


@pytest.mark.parametrize('changed_fields, labels', [
    (['sample_type'], 'Sample type'),
    (['container_type'], 'Container type'),
    (['sample_type', 'library_type'], 'Sample type, Library type'),
])
def test_locked_fields_cannot_change_after_submission(changed_fields, labels):
    result = validate(2, changed_fields)

    assert result == {
        'status': 'error',
        'message': MESSAGES['profile_fields_locked'].format(
            submitted_sample_count=2, total_sample_count=5, project_id=PROJECT_ID, fields=labels
        ),
    }


@pytest.fixture
def adapter():
    with mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter, \
            mock.patch.object(edp_utils, 'Profile') as profile:
        profile.return_value.get_record.return_value = SAVED_PROFILE
        lims = get_lims_adapter.return_value
        lims.validate_profile_change.return_value = {'status': 'success'}
        yield lims


def save(**fields):
    auto_fields = {'copo.profile.no_of_samples': '5'}
    auto_fields.update({f'copo.profile.{field}': value for field, value in fields.items()})
    return edp_utils.pre_save_edp_profile(auto_fields, target_id='p1')


@pytest.mark.parametrize('fields, changed_fields', [
    ({}, []),  # locked fields missing from the form are left as they are
    ({'sample_type': 'Tissue', 'container_type': 'tube', 'library_type': ''}, []),
    ({'sample_type': ' Tissue ', 'library_type': None}, []),  # whitespace and blanks match
    ({'sample_type': 'Total RNA'}, ['sample_type']),
    ({'container_type': 'plate', 'library_type': 'pacbio'}, ['container_type', 'library_type']),
])
def test_changed_locked_fields_are_passed_on(adapter, fields, changed_fields):
    assert save(**fields) == {'status': 'success'}
    adapter.validate_profile_change.assert_called_once_with(SAVED_PROFILE, 5, changed_fields)
