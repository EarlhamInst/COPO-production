'''Unit tests for discarding the Sapio changes queued by a failed sync.

Sapio's record model manager is shared by every request in a COPO process and
only clears the changes queued in it after a successful commit. If a sync or
manifest submission fails, its changes must be rolled back. Otherwise, they
are sent again with the next commit, which then fails as well (e.g. on a
project that has since been deleted).
'''

from unittest import mock

from src.apps.ei_edp.utils.constants import PROFILE_RELATED_ENQ, PROFILE_SAPIO_PROJECT_ID
from src.apps.ei_edp.utils.lims.sapio import adapter as sapio_adapter
from src.apps.ei_edp.utils.lims.sapio.adapter import SapioAdapter

PROFILE = {
    '_id': 'profile_id',
    PROFILE_SAPIO_PROJECT_ID: 'PR0001',
    PROFILE_RELATED_ENQ: 'ENQ0001',
}


def sync_project(commit_error=None):
    # Syncs an existing project with no samples and returns the result and
    # the mocked Sapio record model manager
    sapio = mock.Mock()
    sapio.return_value.dataRecordManager.query_data_records.return_value.result_list = [
        mock.Mock()
    ]
    sapio.return_value.inst_man.add_existing_record.return_value.get_children_of_type.return_value = []
    rec_man = sapio.return_value.rec_man
    rec_man.store_and_commit.side_effect = commit_error

    adapter = SapioAdapter.__new__(SapioAdapter)
    with mock.patch.object(sapio_adapter, 'Sapio', sapio), \
            mock.patch.object(sapio_adapter, '_group_samples_by_batch', return_value=[]), \
            mock.patch.object(SapioAdapter, '_attach_project_to_department'), \
            mock.patch.object(SapioAdapter, '_sync_plate_well_elements'):
        result = adapter.sync_project(PROFILE)
    return result, rec_man


def test_failed_project_sync_discards_pending_changes():
    result, rec_man = sync_project(commit_error=Exception('Failed to commit changes.'))

    assert result['status'] == 'warning'
    rec_man.rollback.assert_called_once()


def test_successful_project_sync_keeps_committed_changes():
    result, rec_man = sync_project()

    assert result['status'] == 'success'
    rec_man.store_and_commit.assert_called_once()
    rec_man.rollback.assert_not_called()


def test_failed_manifest_submission_discards_pending_changes():
    sapio = mock.Mock()
    adapter = SapioAdapter.__new__(SapioAdapter)
    with mock.patch.object(sapio_adapter, 'Sapio', sapio), \
            mock.patch.object(
                SapioAdapter, '_submit_manifest', side_effect=Exception('Failed to commit changes.')
            ):
        result = adapter.submit_manifest('PR0001', schemas={}, components={})

    assert result['status'] == 'error'
    sapio.return_value.rec_man.rollback.assert_called_once()
