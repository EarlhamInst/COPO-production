'''Unit tests for changing the number of samples on an EDP profile.

Saving an EDP profile synchronises its sample count to the linked Sapio project. Once
samples have been submitted (their `Submitter sample reference` field from the 
LIMS manifest is written to Sapio's `C_CustomerSampleName` field), they 
cannot be deleted, so the sample  count may only go down as far as the 
number of submitted samples.
'''

from unittest import mock

import pytest

from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES
from src.apps.ei_edp.utils.lims.sapio import adapter as sapio_adapter
from src.apps.ei_edp.utils.lims.sapio.adapter import SapioAdapter

PROJECT_ID = 'PR0001'


class FakeSample:
    def __init__(self, customer_name):
        self.customer_name = customer_name

    def get_field_value(self, field):
        return self.customer_name if field == 'C_CustomerSampleName' else None


def validate(total, submitted, requested):
    # A Sapio project holding `total` samples, the first `submitted` of
    # which have a customer name
    samples = [FakeSample(f'cust{i}' if i < submitted else '') for i in range(total)]
    project = mock.Mock()
    project.get_children_of_type.return_value = samples
    sapio = mock.Mock()
    sapio.return_value.dataRecordManager.query_data_records.return_value.result_list = [object()]
    sapio.return_value.inst_man.add_existing_record.return_value = project

    profile = {'sapio_project_id': PROJECT_ID, 'no_of_samples': str(total)}
    with mock.patch.object(sapio_adapter, 'Sapio', sapio):
        return SapioAdapter.__new__(SapioAdapter).validate_profile_change(profile, requested)


@pytest.mark.parametrize('total, submitted, requested', [
    (5, 0, 0),  # nothing submitted yet, so any decrease is fine
    (5, 3, 3),  # down to exactly the submitted samples
    (5, 3, 4),
    (5, 5, 5),  # unchanged
    (5, 3, 8),  # increase
])
def test_allowed_sample_counts(total, submitted, requested):
    assert validate(total, submitted, requested) == {'status': 'success'}


@pytest.mark.parametrize('total, submitted, requested', [
    (5, 5, 3),  # all submitted: the case the old check let through
    (5, 3, 2),  # partially submitted, below the submitted samples
    (5, 1, 0),
])
def test_cannot_drop_below_submitted_samples(total, submitted, requested):
    result = validate(total, submitted, requested)

    assert result['status'] == 'error'
    assert result['message'] == MESSAGES['sample_count_below_submitted'].format(
        submitted_sample_count=submitted, total_sample_count=total, project_id=PROJECT_ID
    )


def test_missing_sapio_project_is_an_error():
    sapio = mock.Mock()
    sapio.return_value.dataRecordManager.query_data_records.return_value.result_list = []
    with mock.patch.object(sapio_adapter, 'Sapio', sapio):
        result = SapioAdapter.__new__(SapioAdapter).validate_profile_change(
            {'sapio_project_id': PROJECT_ID}, 1
        )

    assert result == {
        'status': 'error',
        'message': MESSAGES['sapio_project_not_found'].format(project_id=PROJECT_ID),
    }


@pytest.fixture
def adapter():
    with mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter, \
            mock.patch.object(edp_utils, 'Profile') as profile:
        profile.return_value.get_record.return_value = {'sapio_project_id': PROJECT_ID}
        lims = get_lims_adapter.return_value
        lims.validate_profile_change.return_value = {'status': 'success'}
        yield lims


@pytest.mark.parametrize('raw, expected', [('4', 4), (' 4 ', 4), ('', 0), (7, 7)])
def test_form_value_is_passed_on_as_a_number(adapter, raw, expected):
    # The form's field is free text, so the count arrives as a string
    result = edp_utils.pre_save_edp_profile({'copo.profile.no_of_samples': raw}, target_id='p1')

    assert result == {'status': 'success'}
    adapter.validate_profile_change.assert_called_once_with({'sapio_project_id': PROJECT_ID}, expected)


@pytest.mark.parametrize('raw', ['abc', '-1', '2.5'])
def test_invalid_sample_count_is_rejected(adapter, raw):
    result = edp_utils.pre_save_edp_profile({'copo.profile.no_of_samples': raw}, target_id='p1')

    assert result == {
        'status': 'error',
        'message': MESSAGES['invalid_sample_count'].format(sample_count=raw),
    }
    adapter.validate_profile_change.assert_not_called()


def test_new_profile_skips_the_lims_check(adapter):
    # A new profile has no Sapio project yet, so there's nothing to compare with
    result = edp_utils.pre_save_edp_profile({'copo.profile.no_of_samples': '3'})

    assert result == {'status': 'success'}
    adapter.validate_profile_change.assert_not_called()
