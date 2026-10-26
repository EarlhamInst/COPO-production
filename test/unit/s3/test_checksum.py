"""Unit tests for verify_s3_checksums in common.s3.checksum.

Read and single-cell submissions compare the MD5 checksums declared in a
manifest with the data files stored in S3. A single-part upload's ETag is
its MD5. A multipart upload's ETag is not, so its MD5 is looked up in
DataFileChecksum and, if not known yet, queued for hashing and reported
as pending.
"""

from unittest import mock

import pytest

from common.s3 import checksum

BUCKET = 'profile1'
MD5_A = 'a' * 32
MD5_B = 'b' * 32
MULTIPART_ETAG = 'c' * 32 + '-3'


class StoredMd5s(dict):
    # {key: md5} held in DataFileChecksum for multipart uploads
    pass


@pytest.fixture
def stored():
    stored = StoredMd5s()
    with mock.patch.object(checksum, 'DataFileChecksum') as data_file_checksum, \
            mock.patch.object(checksum, 'request_s3_object_md5') as request_md5:
        data_file_checksum.return_value.get_md5_map.side_effect = (
            lambda bucket, etags: {k: v for k, v in stored.items() if k in etags})
        stored.request_md5 = request_md5
        yield stored


def test_matching_single_part_upload_passes(stored):
    assert checksum.verify_s3_checksums(BUCKET, {'r1.fq.gz': MD5_A}, {'r1.fq.gz': MD5_A}) == ([], [])


def test_mismatched_single_part_upload_is_reported(stored):
    mismatched, pending = checksum.verify_s3_checksums(
        BUCKET, {'r1.fq.gz': MD5_A}, {'r1.fq.gz': MD5_B})

    assert mismatched == ['r1.fq.gz']
    assert pending == []


def test_declared_checksum_is_normalised(stored):
    declared = f'  "{MD5_A.upper()}" '

    assert checksum.verify_s3_checksums(BUCKET, {'r1.fq.gz': declared}, {'r1.fq.gz': MD5_A}) == ([], [])


def test_multipart_upload_uses_stored_md5(stored):
    stored['big.fq.gz'] = MD5_A

    assert checksum.verify_s3_checksums(
        BUCKET, {'big.fq.gz': MD5_B}, {'big.fq.gz': MULTIPART_ETAG}) == (['big.fq.gz'], [])
    stored.request_md5.assert_not_called()


def test_multipart_upload_without_md5_is_pending_and_queued(stored):
    mismatched, pending = checksum.verify_s3_checksums(
        BUCKET, {'big.fq.gz': MD5_A}, {'big.fq.gz': MULTIPART_ETAG})

    assert (mismatched, pending) == ([], ['big.fq.gz'])
    stored.request_md5.assert_called_once_with(BUCKET, 'big.fq.gz', MULTIPART_ETAG)


def test_files_without_declared_checksum_or_upload_are_skipped(stored):
    expected = {'no_checksum.fq.gz': '', 'not_uploaded.fq.gz': MD5_A}
    etags = {'no_checksum.fq.gz': MD5_B}

    assert checksum.verify_s3_checksums(BUCKET, expected, etags) == ([], [])
