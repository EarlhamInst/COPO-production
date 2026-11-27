'''Unit tests for queuing a data file to be downloaded from S3 to COPO (and uploaded to ENA).

make_transfer_record creates or updates the file's EnaFileTransfer record,
matched by its local path. Setting the record to 'pending' with
transfer_status 1 makes the transfer task download the file from S3 again.

- Files only needed in COPO (no_remote_location, e.g. assemblies, which
  Webin-CLI reads from MEDIA_ROOT) are deleted by housekeeping_local_uploads
  a few days after they are downloaded and flagged as archived. A
  resubmission must download them again, otherwise Webin-CLI fails with
  "Could not read data file".
- Files uploaded to ENA (e.g. reads) are only downloaded again when they
  must go to a different ENA location.
- A failed transfer is retried when the file is resubmitted.

MongoDB is mocked.

To execute these tests, run: $ pytest test/unit/ena_utils/test_make_transfer_record.py
'''

from unittest import mock

import pytest

from common.ena_utils import FileTransferUtils
from common.ena_utils.FileTransferUtils import TransferStatus

LOCAL_PATH = '/copo/media/ena_assembly_files/profile1/genome.fasta.gz'
DATA_FILE = {
    '_id': 'file1',
    'file_name': 'genome.fasta.gz',
    'file_location': LOCAL_PATH,
    'ecs_location': 'profile1/genome.fasta.gz',
    'profile_id': 'profile1',
    'type': 'fasta',
}

# Existing transfer records
DOWNLOADED = {'status': 'complete', 'transfer_status': 4}
ARCHIVED = {'status': 'complete', 'transfer_status': 4, 'is_archived': True}  # Deleted by housekeeping
DOWNLOADING = {'status': 'processing', 'transfer_status': 2}
FAILED = {'status': 'failed', 'transfer_status': 10, 'failure_count': 5, 'failure_reason': 'NoSuchKey'}
UPLOADED_TO_ENA = {'status': 'complete', 'transfer_status': 5, 'remote_path': 'sub1/reads/'}
ARCHIVED_UPLOADED_TO_ENA = dict(UPLOADED_TO_ENA, is_archived=True)


@pytest.fixture
def transfer_record():
    '''Run make_transfer_record for DATA_FILE against an existing transfer record (or None).

    Yields a function returning make_transfer_record's result and the fields
    written to MongoDB (None if nothing was written).
    '''
    with mock.patch.object(FileTransferUtils, 'DataFile') as data_file, \
            mock.patch.object(FileTransferUtils, 'EnaFileTransfer') as ena_file_transfer, \
            mock.patch.object(FileTransferUtils, 'Logger'):
        data_file.return_value.get_record.return_value = DATA_FILE
        collection = ena_file_transfer.return_value.get_collection_handle.return_value

        def run(existing, **kwargs):
            collection.find_one.return_value = dict(existing) if existing else None
            collection.update_one.reset_mock()

            result = FileTransferUtils.make_transfer_record(file_id='file1', submission_id='sub1', **kwargs)

            collection.find_one.assert_called_once_with({'local_path': LOCAL_PATH})
            collection.find_one.reset_mock()
            if not collection.update_one.called:
                return result, None
            query, update = collection.update_one.call_args.args
            assert query == {'local_path': LOCAL_PATH}
            assert collection.update_one.call_args.kwargs == {'upsert': True}
            return result, update['$set']

        yield run


def assert_queued_for_download(written):
    assert written['status'] == 'pending'
    assert written['transfer_status'] == 1


# A new file

def test_new_file_is_queued_for_download(transfer_record):
    result, written = transfer_record(None, no_remote_location=True)

    assert result == (True, 'Transfer record created successfully')
    assert_queued_for_download(written)
    assert written['local_path'] == LOCAL_PATH
    assert written['ecs_location'] == 'profile1/genome.fasta.gz'
    assert written['file_id'] == 'file1'
    assert written['submission_id'] == 'sub1'
    assert 'remote_path' not in written  # Only downloaded to COPO, not uploaded to ENA


def test_new_file_for_ena_gets_a_remote_path(transfer_record):
    _, written = transfer_record(None)

    assert_queued_for_download(written)
    assert written['remote_path'] == 'sub1/reads/'


def test_new_file_not_in_s3_is_rejected(transfer_record):
    result, written = transfer_record(None, no_remote_location=True, etag=None)

    assert result == (False, 'Please upload the file genome.fasta.gz to COPO first')
    assert written is None


# Files only needed in COPO (e.g. assemblies)

def test_archived_file_is_downloaded_again_on_resubmission(transfer_record):
    result, written = transfer_record(ARCHIVED, no_remote_location=True)

    assert result == (True, 'Transfer record created successfully')
    assert_queued_for_download(written)
    assert written['is_archived'] is False


def test_file_still_on_disk_is_not_downloaded_again(transfer_record):
    result, written = transfer_record(DOWNLOADED, no_remote_location=True)

    assert result == (True, 'Transfer record already exists for this file')
    assert written is None


def test_file_being_downloaded_is_left_alone(transfer_record):
    result, written = transfer_record(dict(DOWNLOADING, is_archived=True), no_remote_location=True)

    assert result == (True, 'Transfer record created successfully')
    assert written is None
    FileTransferUtils.Logger.return_value.log.assert_called_once_with(
        f'The file is downloading, will not download it again: {LOCAL_PATH}')


@pytest.mark.parametrize('no_remote_location', [True, False])
def test_failed_transfer_is_retried_on_resubmission(transfer_record, no_remote_location):
    _, written = transfer_record(dict(FAILED, remote_path='sub1/reads/'), no_remote_location=no_remote_location)

    assert_queued_for_download(written)
    assert written['failure_count'] == 0
    assert written['failure_reason'] == ''


# Files uploaded to ENA (e.g. reads)

def test_file_already_in_ena_is_not_downloaded_again(transfer_record):
    # Once in ENA the local copy isn't needed, so archiving it doesn't matter
    result, written = transfer_record(ARCHIVED_UPLOADED_TO_ENA)

    assert result == (True, 'Transfer record already exists for this file')
    assert written is None


def test_file_on_disk_for_a_new_ena_location_is_uploaded_without_downloading(transfer_record):
    _, written = transfer_record(UPLOADED_TO_ENA, remote_location='sub2/reads/')

    assert written['status'] == 'pending'
    assert written['transfer_status'] == 5  # Straight to the ENA upload
    assert written['remote_path'] == 'sub2/reads/'


def test_archived_file_for_a_new_ena_location_is_downloaded_again(transfer_record):
    _, written = transfer_record(ARCHIVED_UPLOADED_TO_ENA, remote_location='sub2/reads/')

    assert_queued_for_download(written)
    assert written['is_archived'] is False
    assert written['remote_path'] == 'sub2/reads/'


# Transfer status mapping used to decide when a file is ready to submit

@pytest.mark.parametrize('record, expected', [
    ({'status': 'pending', 'transfer_status': 1}, TransferStatus.CHECKING_FOR_DOWNLOAD),
    # Being downloaded from S3: not on disk yet, so submissions must keep waiting
    ({'status': 'processing', 'transfer_status': 2}, TransferStatus.DOWNLOADING_TO_LOCAL),
    ({'status': 'pending', 'transfer_status': 3}, TransferStatus.DOWNLOADED_TO_LOCAL),
    (DOWNLOADED, TransferStatus.DOWNLOADED_TO_LOCAL),
    (ARCHIVED, TransferStatus.ARCHIVE),
    ({'status': 'pending', 'transfer_status': 5}, TransferStatus.TRANSFERRING_TO_ENA),
    (UPLOADED_TO_ENA, TransferStatus.TRANSFERRED_TO_ENA),
    (ARCHIVED_UPLOADED_TO_ENA, TransferStatus.ARCHIVE_TRANSFERRED_TO_ENA),
    (FAILED, TransferStatus.FAILED),
])
def test_get_transfer_status(record, expected):
    assert FileTransferUtils.get_transfer_status(record) == expected


@pytest.mark.parametrize('transfer_status', [1, 2])
def test_file_not_on_disk_yet_is_not_ready_to_submit(transfer_status):
    status = FileTransferUtils.get_transfer_status({'status': 'pending', 'transfer_status': transfer_status})

    assert status < TransferStatus.DOWNLOADED_TO_LOCAL
