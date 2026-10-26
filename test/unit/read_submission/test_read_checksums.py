"""Unit tests for checking read data file checksums before submission.

Reads are submitted the same way for biodata and Tree of Life (ASG, DToL,
ERGA) profiles, so these checks apply to both:

- When a read manifest is uploaded, the declared checksums are compared
  with the data files already uploaded. A mismatch is an error. Files
  still being hashed only raise a warning.
- At submission, a mismatch or a file still being hashed blocks the
  submission, because the data files may have been uploaded after the
  manifest.
"""

from unittest import mock

import pandas as pd
import pytest

from common.ena_utils.EnaChecklistHandler import EnaCheckListSpreadsheet
from common.s3 import checksum
from src.apps.copo_read_submission import views
from src.apps.copo_read_submission.utils import ena_read

PROFILE_ID = 'profile1'
MD5_1 = '1' * 32
MD5_2 = '2' * 32
MD5_3 = '3' * 32
OTHER_MD5 = 'f' * 32
MULTIPART_ETAG = 'c' * 32 + '-3'


def manifest_with(rows):
    manifest = EnaCheckListSpreadsheet.__new__(EnaCheckListSpreadsheet)
    manifest.data = pd.DataFrame(rows)
    return manifest


@pytest.fixture(autouse=True)
def no_stored_md5s():
    # No MD5s are known yet for multipart uploads
    with mock.patch.object(checksum, 'DataFileChecksum') as data_file_checksum, \
            mock.patch.object(checksum, 'request_s3_object_md5'):
        data_file_checksum.return_value.get_md5_map.return_value = {}
        yield


# get_file_checksums_from_manifest

def test_single_and_paired_reads_are_mapped_to_their_checksums():
    manifest = manifest_with({
        'File name': ['single.fq.gz', 'r1.fq.gz, r2.fq.gz'],
        'File checksum': [MD5_1, f'{MD5_2}, {MD5_3}'],
    })

    assert manifest.get_file_checksums_from_manifest() == {
        'single.fq.gz': MD5_1, 'r1.fq.gz': MD5_2, 'r2.fq.gz': MD5_3}


def test_file_without_a_checksum_maps_to_empty_string():
    manifest = manifest_with({'File name': ['r1.fq.gz,r2.fq.gz'], 'File checksum': [MD5_1]})

    assert manifest.get_file_checksums_from_manifest() == {'r1.fq.gz': MD5_1, 'r2.fq.gz': ''}


def test_manifest_without_checksum_column_maps_nothing():
    assert manifest_with({'File name': ['r1.fq.gz']}).get_file_checksums_from_manifest() == {}


# Manifest upload: parse_ena_spreadsheet

def undecorated(view):
    # web_page_access_checker does not use functools.wraps,
    # so take the view function from its closure
    view = view.__wrapped__
    return next(cell.cell_contents for cell in view.__closure__
                if getattr(cell.cell_contents, '__name__', '') == 'parse_ena_spreadsheet')


parse_ena_spreadsheet = undecorated(views.parse_ena_spreadsheet)


def upload_manifest(etags):
    request = mock.MagicMock()
    request.session = {'profile_id': PROFILE_ID}
    request.FILES = {'file': mock.MagicMock(name='file')}
    request.FILES['file'].name = 'reads.xlsx'
    request.POST = {'checklist_id': 'read'}

    manifest = mock.MagicMock()
    manifest.loadManifest.return_value = True
    manifest.validate.return_value = True
    manifest.get_filenames_from_manifest.return_value = ['r1.fq.gz,r2.fq.gz']
    manifest.get_file_checksums_from_manifest.return_value = {'r1.fq.gz': MD5_1, 'r2.fq.gz': MD5_2}

    with mock.patch.object(views, 'EnaCheckListSpreadsheet', return_value=manifest), \
            mock.patch.object(views, 's3') as s3, \
            mock.patch.object(views.ghlper, 'notify_read_status') as notify:
        s3.return_value.check_for_s3_bucket.return_value = True
        s3.return_value.check_s3_bucket_for_files.return_value = (etags, '')
        response = parse_ena_spreadsheet(request)

    notifications = [(c.kwargs['action'], c.kwargs['msg']) for c in notify.call_args_list if c.kwargs['msg']]
    return response, manifest, notifications


def test_manifest_with_matching_checksums_is_valid():
    response, manifest, notifications = upload_manifest({'r1.fq.gz': MD5_1, 'r2.fq.gz': MD5_2})

    assert response.status_code == 200
    assert notifications[-1][0] == 'success'
    manifest.collect.assert_called_once()


def test_manifest_with_mismatched_checksum_is_rejected():
    response, manifest, notifications = upload_manifest({'r1.fq.gz': MD5_1, 'r2.fq.gz': OTHER_MD5})

    assert response.status_code == 400
    action, msg = notifications[-1]
    assert action == 'error'
    assert 'Checksum mismatch' in msg and 'r2.fq.gz' in msg and 'r1.fq.gz' not in msg
    manifest.collect.assert_not_called()


def test_manifest_with_checksum_still_being_calculated_warns_and_continues():
    response, manifest, notifications = upload_manifest({'r1.fq.gz': MD5_1, 'r2.fq.gz': MULTIPART_ETAG})

    assert response.status_code == 200
    action, msg = notifications[-1]
    assert action == 'warning'
    assert 'still being calculated' in msg and 'r2.fq.gz' in msg
    assert 'success' not in [action for action, _ in notifications]
    manifest.collect.assert_called_once()


# Submission: submit_read

def submit(file_hashes, etags):
    datafiles = [{'_id': f'{i:024x}', 'file_name': name, 'file_hash': file_hash}
                 for i, (name, file_hash) in enumerate(file_hashes.items())]
    target_id = f"{'a' * 24}_{','.join(d['_id'] for d in datafiles)}"

    with mock.patch.object(ena_read, 'DataFile') as data_file, \
            mock.patch.object(ena_read, 's3') as s3, \
            mock.patch.object(ena_read, 'Submission') as submission, \
            mock.patch.object(ena_read.ThreadLocal, 'get_current_user'):
        data_file.return_value.get_all_records_columns.return_value = datafiles
        s3.return_value.check_s3_bucket_for_files.return_value = (etags, '')
        # Without a submission record, submit_read stops right after
        # the data file checks with "Error 10211"
        submission.return_value.get_collection_handle.return_value.find_one.return_value = None
        return ena_read.submit_read(PROFILE_ID, target_id=target_id)


def test_submission_with_matching_checksums_passes_the_checks():
    result = submit({'r1.fq.gz': MD5_1, 'r2.fq.gz': MD5_2}, {'r1.fq.gz': MD5_1, 'r2.fq.gz': MD5_2})

    assert '10211' in result['message']


def test_submission_with_mismatched_checksum_fails():
    result = submit({'r1.fq.gz': MD5_1, 'r2.fq.gz': MD5_2}, {'r1.fq.gz': MD5_1, 'r2.fq.gz': OTHER_MD5})

    assert result['status'] == 'error'
    assert 'do not match the file checksums' in result['message']
    assert '<strong>r2.fq.gz</strong>' in result['message']
    assert '<strong>r1.fq.gz</strong>' not in result['message']


def test_submission_with_checksum_still_being_calculated_fails():
    result = submit({'big.fq.gz': MD5_1}, {'big.fq.gz': MULTIPART_ETAG})

    assert result['status'] == 'error'
    assert 'still being calculated' in result['message']
    assert '<strong>big.fq.gz</strong>' in result['message']


def test_submission_with_missing_file_fails_before_checksums_are_checked():
    result = submit({'r1.fq.gz': MD5_1}, {})

    assert result['status'] == 'error'
    assert 'missing from storage' in result['message']
