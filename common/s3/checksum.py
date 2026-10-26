'''
MD5 checksums of data files stored in S3.

A single-part upload's ETag is the object's MD5 so, it can be used directly.
However, a multipart upload's ETag is an MD5 of the part MD5s with a '-N' 
suffix, so the real MD5 checksum has to be computed from the object.

This is done once per upload while the file is uploaded 
(see `md5_of_uploaded_file` function) or by a background task that reads 
the object back from S3, and stored in `DataFileChecksum` keyed by the 
`ETag`.
'''
import hashlib

from botocore.exceptions import ClientError

from common.dal.copo_da import DataFileChecksum
from common.utils.logger import Logger

CHUNK_SIZE = 8 * 1024 * 1024


def normalise_checksum(checksum):
    return (checksum or '').strip().strip('"').lower()


def is_plain_md5_etag(etag):
    # Multipart ETags end in '-<number of parts>'
    return bool(etag) and '-' not in etag


def md5_of_uploaded_file(uploaded_file):
    # Hash a Django UploadedFile and rewind it so it can still be uploaded
    md5 = hashlib.md5()
    
    for chunk in uploaded_file.chunks(chunk_size=CHUNK_SIZE):
        md5.update(chunk)
    uploaded_file.seek(0)
    return md5.hexdigest()


def compute_s3_object_md5(bucket, key, etag):
    '''
    Stream the object from S3, hash it and store the result. `IfMatch` makes
    sure that the bytes hashed, belong to the upload identified by the etag.
    
    If the file has since been replaced, the hash is dropped and the 
    latest file upload gets its own hash.
    '''
    from common.s3.s3Connection import S3Connection

    try:
        response = S3Connection().s3_client.get_object(Bucket=bucket, Key=key, IfMatch=f'"{etag}"')
        md5 = hashlib.md5()
        
        for chunk in response['Body'].iter_chunks(chunk_size=CHUNK_SIZE):
            md5.update(chunk)
    except ClientError as e:
        code = (e.response or {}).get('Error', {}).get('Code', '')
        if code in ('PreconditionFailed', 'NoSuchKey', 'NoSuchBucket'):
            # Replaced or deleted since the hash was requested
            DataFileChecksum().remove(bucket, [key], etag=etag)
            Logger().log(f'Skipping md5 of {bucket}/{key}: {code}')
            return None
        DataFileChecksum().set_failed(bucket, key, etag)
        raise
    except Exception:
        DataFileChecksum().set_failed(bucket, key, etag)
        raise

    checksum = md5.hexdigest()
    DataFileChecksum().set_md5(bucket, key, etag, checksum)
    return checksum


def request_s3_object_md5(bucket, key, etag):
    # Queue a background hash unless one is already been done or 
    # is currently running
    if DataFileChecksum().claim(bucket, key, etag):
        from src.apps.copo_file.tasks import compute_s3_object_md5_task
        compute_s3_object_md5_task.delay(bucket, key, etag)


def get_s3_object_md5s(bucket, etags, request_missing=True):
    '''
    :param etags: {key: etag} for objects present in the bucket
    :return: {key: md5}, with None for multipart uploads whose MD5 is not
             known yet. Those are queued for hashing if request_missing.
    '''
    etags = {key: normalise_checksum(etag) for key, etag in etags.items()}
    stored = DataFileChecksum().get_md5_map(bucket, etags)
    md5s = dict()
    
    for key, etag in etags.items():
        if is_plain_md5_etag(etag):
            md5s[key] = etag
        else:
            md5s[key] = stored.get(key)
            if md5s[key] is None and request_missing:
                request_s3_object_md5(bucket, key, etag)
    return md5s


def verify_s3_checksums(bucket, expected, etags):
    '''
    Compare the checksums declared in a manifest with the stored objects.

    :param expected: {file name: declared md5}; files without one are skipped
    :param etags: {file name: etag} for the files present in the bucket
    :return: (mismatched, pending) lists of file names. pending files are
             multipart uploads still being hashed.
    '''
    to_check = {f: etags[f] for f, checksum in expected.items() if checksum and f in etags}
    md5s = get_s3_object_md5s(bucket, to_check)
    mismatched, pending = [], []
    
    for file_name, md5 in md5s.items():
        if md5 is None:
            pending.append(file_name)
        elif md5 != normalise_checksum(expected[file_name]):
            mismatched.append(file_name)
    return mismatched, pending
