'''Unit tests for submitting Single-cell studies to Zenodo.

If Zenodo returns an error, the study is set back to 'pending' with the error
recorded on it and taken off the queue, the same as a failed ENA submission.

process_pending_submission_zendo() takes one of three paths for each study,
depending on what Zenodo already holds for it:

- no deposition yet: create one and upload every Zenodo file
- a draft deposition: unlock it, update the metadata and upload changed files
- a published deposition ('done'): ask Zenodo for a new version, then edit
  the new draft, not the published record

The last path is the subtle one. Zenodo's newversion action returns the
*published* deposition, not the draft it creates; the draft's id is only in
links.latest_draft. Editing the returned deposition would fail, because a
published record can't be edited.

Every collaborator (Mongo data access, the Zenodo client, notifications) is
patched, so these tests make no network or database calls.
'''

from io import BytesIO
from unittest import mock

import pytest

from src.apps.copo_single_cell_submission.utils import zenodo_submission

PROFILE_ID = 'profile_1'
STUDY_ID = 'study_1'
SUBMISSION_ID = 'submission_1'
PUBLISHED_ID = 100
DRAFT_ID = 202
LOCAL_PATH = f'/copo/media/{PROFILE_ID}/matrix.h5ad'
FILE_HASH = 'md5:5d41402abc4b2a76b9719d911017c592'

SINGLECELL = {
    '_id': 'singlecell_1',
    'schema_name': 'COPO_SINGLE_CELL',
    'checklist_id': 'checklist_1',
    'components': {
        'study': [{'study_id': STUDY_ID, 'title': 'A study title', 'description': 'A study description'}],
        'person': [{'givenName': 'Ada', 'familyName': 'Lovelace', 'institution': 'EI', 'orcid': ''}],
    },
}


def bucket(deposition_id):
    return f'https://sandbox.zenodo.org/api/files/bucket-{deposition_id}'


def deposition(deposition_id, state='unsubmitted', files=()):
    return {
        'id': deposition_id,
        'state': state,
        'doi': '',
        'files': list(files),
        'links': {'bucket': bucket(deposition_id)},
    }


def submission(accessions=None):
    return {
        '_id': SUBMISSION_ID,
        'profile_id': PROFILE_ID,
        'study': [STUDY_ID],
        'accessions': {'study': accessions or []},
    }


@pytest.fixture
def collaborators():
    '''Patch everything process_pending_submission_zendo() talks to.

    Each name is patched where zenodo_submission looks it up. The classes are
    instantiated on every call (e.g. Zenodo_deposition().read(...)), so tests
    configure and inspect `<mock>.return_value`, the shared instance.
    '''
    names = ['Submission', 'Singlecell', 'SinglecellSchemas', 'SingleCellSchemasHandler',
             'EnaFileTransfer', 'DataFile', 'Zenodo_deposition', 'notify_singlecell_status', 'Logger']
    with mock.patch.multiple(zenodo_submission, **{name: mock.DEFAULT for name in names}) as mocks:
        mocks['Singlecell'].return_value.get_collection_handle.return_value.find_one.return_value = SINGLECELL
        mocks['SinglecellSchemas'].return_value.get_all_files.return_value = ['matrix.h5ad']
        mocks['EnaFileTransfer'].return_value.get_all_records_columns.return_value = [{'local_path': LOCAL_PATH}]
        mocks['DataFile'].return_value.get_all_records_columns.return_value = [
            {'file_name': 'matrix.h5ad', 'file_hash': FILE_HASH}]
        yield mocks


def run(collaborators, sub):
    collaborators['Submission'].return_value.get_pending_submission.return_value = [sub]
    zenodo_submission.process_pending_submission_zendo()
    return collaborators['Zenodo_deposition'].return_value


def uploaded(zenodo):
    '''(bucket link, file paths) of the single upload_files() call.'''
    zenodo.upload_files.assert_called_once()
    kwargs = zenodo.upload_files.call_args.kwargs
    assert isinstance(kwargs['bytesstring'], BytesIO), 'the manifest should be uploaded too'
    return kwargs['bucket_link'], kwargs['files']


def test_nothing_happens_when_no_submission_is_pending(collaborators):
    collaborators['Submission'].return_value.get_pending_submission.return_value = []

    zenodo_submission.process_pending_submission_zendo()

    collaborators['Zenodo_deposition'].assert_not_called()


class TestNewStudy:
    @pytest.fixture
    def zenodo(self, collaborators):
        collaborators['Zenodo_deposition'].return_value.create.return_value = deposition(DRAFT_ID)
        return run(collaborators, submission())

    def test_creates_a_deposition_with_the_study_metadata(self, zenodo):
        metadata = zenodo.create.call_args.kwargs['deposition']['metadata']
        assert metadata['title'] == 'A study title'
        assert metadata['description'] == 'A study description'
        assert metadata['upload_type'] == 'dataset'
        assert {'name': 'Ada Lovelace', 'affiliation': 'EI', 'orcid': ''} in metadata['creators']
        assert STUDY_ID in metadata['keywords']

    def test_uploads_every_zenodo_file_to_the_new_bucket(self, zenodo):
        assert uploaded(zenodo) == (bucket(DRAFT_ID), [LOCAL_PATH])

    def test_records_the_new_deposition_id(self, zenodo, collaborators):
        collaborators['Submission'].return_value.add_component_submission_accession.assert_called_once_with(
            sub_id=SUBMISSION_ID, component='study',
            accessions=[{'study_id': STUDY_ID, 'deposition_id': DRAFT_ID}])

    def test_marks_the_study_accepted_and_clears_it_from_the_queue(self, zenodo, collaborators):
        status = collaborators['Singlecell'].return_value.update_component_status.call_args.kwargs
        assert status['repository'] == 'zenodo'
        assert status['status_column_value']['status'] == 'accepted'
        assert status['status_column_value']['accession'] == str(DRAFT_ID)
        collaborators['Submission'].return_value.remove_component_from_submission.assert_called_once_with(
            sub_id=SUBMISSION_ID, component='study', component_ids=[STUDY_ID])

    def test_does_not_touch_any_existing_deposition(self, zenodo):
        zenodo.read.assert_not_called()
        zenodo.unlock.assert_not_called()
        zenodo.new_version.assert_not_called()


class TestPublishedStudy:
    '''Resubmitting a study whose deposition has already been published.'''

    @pytest.fixture
    def zenodo(self, collaborators):
        zenodo = collaborators['Zenodo_deposition'].return_value
        published = deposition(PUBLISHED_ID, state='done')
        draft = deposition(DRAFT_ID, files=[{'id': 'f1', 'filename': 'matrix.h5ad', 'checksum': FILE_HASH}])
        zenodo.read.side_effect = lambda _id: {str(PUBLISHED_ID): published, str(DRAFT_ID): draft}[str(_id)]
        # Like the real API: newversion returns the *published* deposition and
        # only points at the new draft through links.latest_draft.
        zenodo.new_version.return_value = {
            **published,
            'links': {**published['links'],
                      'latest_draft': f'https://sandbox.zenodo.org/api/deposit/depositions/{DRAFT_ID}'},
        }
        zenodo.update.return_value = draft
        return run(collaborators, submission([{'study_id': STUDY_ID, 'deposition_id': PUBLISHED_ID}]))

    def test_asks_zenodo_for_a_new_version_of_the_published_deposition(self, zenodo):
        zenodo.new_version.assert_called_once_with(_id=PUBLISHED_ID)
        zenodo.unlock.assert_not_called()

    def test_edits_the_new_draft_not_the_published_deposition(self, zenodo):
        zenodo.update.assert_called_once()
        assert str(zenodo.update.call_args.kwargs['_id']) == str(DRAFT_ID)

    def test_points_the_study_at_the_new_draft(self, zenodo, collaborators):
        collaborators['Submission'].return_value.update_component_submission_accession.assert_called_once_with(
            profile_id=PROFILE_ID, repository='zenodo', component='study', identifier='study_id',
            accession={'study_id': STUDY_ID, 'deposition_id': DRAFT_ID})
        status = collaborators['Singlecell'].return_value.update_component_status.call_args.kwargs
        assert status['status_column_value']['accession'] == str(DRAFT_ID)

    def test_uploads_only_the_manifest_when_no_file_changed(self, zenodo):
        assert uploaded(zenodo) == (bucket(DRAFT_ID), [])

    @pytest.mark.parametrize('latest_draft', [
        f'https://sandbox.zenodo.org/api/deposit/depositions/{DRAFT_ID}',
        f'https://sandbox.zenodo.org/api/deposit/depositions/{DRAFT_ID}/',
    ])
    def test_draft_id_is_read_with_or_without_a_trailing_slash(self, collaborators, latest_draft):
        zenodo = collaborators['Zenodo_deposition'].return_value
        zenodo.read.side_effect = None
        zenodo.read.return_value = deposition(PUBLISHED_ID, state='done')
        zenodo.new_version.return_value = {'id': PUBLISHED_ID, 'links': {'latest_draft': latest_draft}}
        zenodo.update.return_value = deposition(DRAFT_ID)
        zenodo.update.reset_mock()

        run(collaborators, submission([{'study_id': STUDY_ID, 'deposition_id': PUBLISHED_ID}]))

        assert str(zenodo.update.call_args.kwargs['_id']) == str(DRAFT_ID)


class TestDraftStudy:
    '''Resubmitting a study whose deposition is still an unpublished draft.'''

    def resubmit(self, collaborators, files_on_zenodo):
        zenodo = collaborators['Zenodo_deposition'].return_value
        zenodo.read.return_value = deposition(DRAFT_ID)
        zenodo.update.return_value = deposition(DRAFT_ID, files=files_on_zenodo)
        return run(collaborators, submission([{'study_id': STUDY_ID, 'deposition_id': DRAFT_ID}]))

    def test_unlocks_and_updates_the_same_deposition(self, collaborators):
        zenodo = self.resubmit(collaborators, [])

        zenodo.unlock.assert_called_once_with(_id=DRAFT_ID)
        zenodo.update.assert_called_once()
        assert zenodo.update.call_args.kwargs['_id'] == DRAFT_ID
        zenodo.new_version.assert_not_called()
        collaborators['Submission'].return_value.update_component_submission_accession.assert_not_called()

    def test_uploads_a_file_missing_from_zenodo(self, collaborators):
        zenodo = self.resubmit(collaborators, [])

        assert uploaded(zenodo) == (bucket(DRAFT_ID), [LOCAL_PATH])

    def test_reuploads_a_file_whose_checksum_changed(self, collaborators):
        zenodo = self.resubmit(collaborators, [{'id': 'f1', 'filename': 'matrix.h5ad', 'checksum': 'md5:stale'}])

        assert uploaded(zenodo) == (bucket(DRAFT_ID), [LOCAL_PATH])
        zenodo.delete_file.assert_not_called()

    def test_skips_a_file_already_on_zenodo(self, collaborators):
        zenodo = self.resubmit(collaborators, [{'id': 'f1', 'filename': 'matrix.h5ad', 'checksum': FILE_HASH}])

        assert uploaded(zenodo) == (bucket(DRAFT_ID), [])

    def test_deletes_a_file_no_longer_in_the_study(self, collaborators):
        zenodo = self.resubmit(collaborators, [
            {'id': 'f1', 'filename': 'matrix.h5ad', 'checksum': FILE_HASH},
            {'id': 'f2', 'filename': 'old_barcodes.tsv', 'checksum': 'md5:old'},
        ])

        zenodo.delete_file.assert_called_once_with(_id=DRAFT_ID, file_id='f2')


class TestZenodoError:
    '''A failed submission is handled like a failed ENA one.'''

    @pytest.fixture
    def collaborators(self, collaborators):
        collaborators['Zenodo_deposition'].return_value.create.side_effect = RuntimeError('403 Forbidden')
        run(collaborators, submission())
        return collaborators

    def test_is_shown_to_the_user(self, collaborators):
        notify = collaborators['notify_singlecell_status']
        assert any(c.kwargs['action'] == 'error' and '403 Forbidden' in c.kwargs['msg']
                   for c in notify.call_args_list)

    def test_is_recorded_on_the_study_which_goes_back_to_pending(self, collaborators):
        status = collaborators['Singlecell'].return_value.update_component_status.call_args.kwargs
        assert status['repository'] == 'zenodo'
        assert status['identifier_value'] == STUDY_ID
        assert status['status_column_value']['status'] == 'pending'
        assert '403 Forbidden' in status['status_column_value']['error']

    def test_takes_the_study_off_the_queue_so_it_can_be_resubmitted(self, collaborators):
        collaborators['Submission'].return_value.remove_component_from_submission.assert_called_once_with(
            sub_id=SUBMISSION_ID, component='study', component_ids=[STUDY_ID])

    def test_records_no_deposition(self, collaborators):
        collaborators['Submission'].return_value.add_component_submission_accession.assert_called_once_with(
            sub_id=SUBMISSION_ID, component='study', accessions=[])

    def test_a_failure_to_record_the_error_still_takes_the_study_off_the_queue(self, collaborators):
        collaborators['Singlecell'].return_value.update_component_status.side_effect = RuntimeError('mongo down')
        collaborators['Submission'].return_value.remove_component_from_submission.reset_mock()

        run(collaborators, submission())

        collaborators['Submission'].return_value.remove_component_from_submission.assert_called_once()


def test_a_study_missing_from_the_database_is_dropped_from_the_submission(collaborators):
    collaborators['Singlecell'].return_value.get_collection_handle.return_value.find_one.return_value = None

    zenodo = run(collaborators, submission())

    collaborators['Submission'].return_value.remove_study_from_singlecell_submission.assert_called_once_with(
        sub_id=SUBMISSION_ID, study_id=STUDY_ID)
    zenodo.create.assert_not_called()
