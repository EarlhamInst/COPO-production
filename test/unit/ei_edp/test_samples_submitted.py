'''Unit tests for checking whether samples have been submitted under an EDP profile.

A LIMS manifest describes the samples submitted under an EDP profile so, users
are warned if no samples have been submitted yet before a blank LIMS manifest
can be downloaded or a LIMS manifest can be uploaded.
'''

from unittest import mock

import pytest

from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES

PROFILE_ID = 'profile1'


def check(submitted_sample_count):
    with mock.patch.object(edp_utils, 'Sample') as sample_da:
        count_documents = sample_da.return_value.get_collection_handle.return_value.count_documents
        count_documents.return_value = submitted_sample_count
        return edp_utils.check_edp_samples_submitted(PROFILE_ID), count_documents


def test_warns_when_no_samples_submitted():
    result, _ = check(0)

    assert result == {'status': 'warning', 'message': MESSAGES['no_samples_submitted']}


@pytest.mark.parametrize('submitted_sample_count', [1, 3])
def test_proceeds_when_samples_submitted(submitted_sample_count):
    result, _ = check(submitted_sample_count)

    assert result == {'status': 'success', 'message': ''}


def test_counts_only_submitted_samples_in_the_profile():
    _, count_documents = check(1)

    query = count_documents.call_args.args[0]
    assert query['profile_id'] == PROFILE_ID
    assert query['deleted'] == edp_utils.helpers.get_not_deleted_flag()
    assert query['biosampleAccession'] == {'$exists': True, '$nin': ['', None]}
