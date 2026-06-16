import os
import threading
import time
from common.dal.copo_da import EnaFileTransfer, DataFile
from common.dal.profile_da import Profile
from common.s3.s3Connection import S3Connection as s3
from datetime import datetime, timedelta
from bson import ObjectId
from botocore.exceptions import ClientError
from common.utils.logger import Logger
import gzip
from .generic_helper import transfer_to_ena
from common.utils.helpers import get_env, get_datetime, notify_submission_status
from datetime import datetime
from src.apps.copo_core.models import StatusMessage, User
from src.apps.copo_file.utils.CopoFiles import create_image_thumbnail
import hashlib
from pathlib import Path
from PIL import ImageFile, Image
from django.conf import settings
from enum import IntEnum
from common.utils.helpers import get_not_deleted_flag


class PermanentTransferError(Exception):
    """Raised when a transfer failure is non-retryable (e.g. S3 object does not
    exist, access denied). The caller should mark the record as failed rather
    than reset it for retry."""
    pass


# S3 error codes that mean "this will never succeed — don't retry".
_NON_RETRYABLE_S3_CODES = {"NoSuchBucket", "NoSuchKey", "AccessDenied", "404", "403"}

# After this many consecutive transient failures on the same record, give up
# and mark it failed. The user can resubmit to reset the counter.
_MAX_CONSECUTIVE_FAILURES = 5

ImageFile.LOAD_TRUNCATED_IMAGES = True

Image.MAX_IMAGE_PIXELS = None


class TransferStatus(IntEnum):
    ARCHIVE = -2  # the file is archived, not in local_uploads folder
    ARCHIVE_COMPLETED_VALIDATION_IN_ENA = -4
    ARCHIVE_TRANSFERRED_TO_ENA = -3
    CHECKING_FOR_DOWNLOAD = -1
    DOWNLOADING_TO_LOCAL = 0
    DOWNLOADED_TO_LOCAL = 1
    TRANSFERRING_TO_ENA = 2
    TRANSFERRED_TO_ENA = 3
    COMPLETED_VALIDATION_IN_ENA = 4
    FAILED = 10  # terminal: will not retry without user action (resubmission)


TransferStatusNames = {
    TransferStatus.CHECKING_FOR_DOWNLOAD: "Checking for download",
    TransferStatus.DOWNLOADING_TO_LOCAL: "Downloading to local",
    TransferStatus.DOWNLOADED_TO_LOCAL: "Transferred to COPO",
    TransferStatus.TRANSFERRING_TO_ENA: "Transferring to ENA",
    TransferStatus.TRANSFERRED_TO_ENA: "Transferred to ENA",
    TransferStatus.COMPLETED_VALIDATION_IN_ENA: "Completed validation in ENA",
    TransferStatus.ARCHIVE: "The file is archived, need to download from COPO again",
    TransferStatus.ARCHIVE_TRANSFERRED_TO_ENA: "The file is archived, but transferred to ENA",
    TransferStatus.ARCHIVE_COMPLETED_VALIDATION_IN_ENA: "The file is archived, but completed validation in ENA",
    TransferStatus.FAILED: "Transfer failed — please resubmit to retry",
}


def make_transfer_record(
    file_id,
    submission_id,
    remote_location=None,
    no_remote_location=False,
    etag="default",
):
    # etag: with etag, it means the file exists in ECS/MinIO,  if you don't need to check for the file existence, don't pass etag
    # make transfer object
    result = True
    file = DataFile().get_record(file_id)
    tx = dict()
    if not no_remote_location:
        remote_location = (
            remote_location if remote_location else submission_id + "/reads/"
        )
        tx["remote_path"] = remote_location

    tx["local_path"] = file["file_location"]
    tx["ecs_location"] = file["ecs_location"]
    tx["file_id"] = str(file["_id"])
    tx["profile_id"] = file["profile_id"]
    tx["file_type"] = file["type"]
    # tx["status"] = "pending"
    tx["submission_id"] = submission_id
    tx["deleted"] = get_not_deleted_flag()
    # N.B. Transfer Status
    # 0 transfer complete
    # 1 check for presences of file on ecs
    # 2 transfer to COPO
    # 3 check for gzip
    # 4 check for md5ß  -- complete if no remote location
    # 5 transfer to ENA -- complete if transfer to ENA is successful, ena_complete if ENA validation is successful
    # 10 Error
    # tx["transfer_status"] = 1

    need_update = False
    ena_file = (
        EnaFileTransfer()
        .get_collection_handle()
        .find_one({"local_path": file["file_location"]})
    )
    if not ena_file:
        if not etag:
            return False, f"Please upload the file {file['file_name']} to COPO first"
        ena_file = {
            "status": "pending",
            "remote_path": remote_location,
            "transfer_status": 1,
            "created": get_datetime(),
        }
        tx["created"] = get_datetime()
        tx["transfer_status"] = 1
        need_update = True

    if ena_file["status"] != "processing":
        if not no_remote_location and remote_location:
            if ena_file.get("remote_path", "") != remote_location:
                # if remote location is different, update it and transfer it again to ENA
                if get_transfer_status(ena_file) >= TransferStatus.DOWNLOADED_TO_LOCAL:
                    tx["transfer_status"] = 5
                elif get_transfer_status(ena_file) in [TransferStatus.ARCHIVE, TransferStatus.ARCHIVE_TRANSFERRED_TO_ENA, TransferStatus.ARCHIVE_COMPLETED_VALIDATION_IN_ENA]:
                    tx["transfer_status"] = 1
                    tx["is_archive"] = "0"
                tx["remote_path"] = remote_location
                need_update = True

        # If the previous attempt was marked failed (or legacy "error"), a
        # resubmission should reset it to pending so the pipeline retries.
        if ena_file.get("status") in ("failed", "error") or ena_file.get("transfer_status") == int(TransferStatus.FAILED):
            tx["transfer_status"] = 1
            tx["failure_count"] = 0
            tx["failure_reason"] = ""
            need_update = True

        if not need_update:
            return True, "Transfer record already exists for this file"

        tx["last_checked"] = get_datetime()
        tx["status"] = "pending"
        EnaFileTransfer().get_collection_handle().update_one(
            {"local_path": file["file_location"]}, {"$set": tx}, upsert=True
        )
    else:
        Logger().log(
            "The file is downloading, will not download it again: " + tx["local_path"]
        )
    return True, "Transfer record created successfully"


def check_for_stuck_transfers():
    # N.B. called from celery
    processing_tx = EnaFileTransfer().get_processing_transfers()
    if processing_tx:
        for tx in processing_tx:
            '''
            check how long this has been processing. transfers can be allowed up to a day, but s3check, gzip and md5 should be quite quick,
            so should be reset
            to pending after a few minutes as they are probably stuck
            '''
            tx_status = tx["transfer_status"]
            chk = tx["last_checked"]
            delta = datetime.utcnow() - chk
            if tx_status in (1, 3, 4):
                # these are the processes which should be quick
                if delta.total_seconds() > 60 * 10:
                    EnaFileTransfer().set_pending(tx["_id"])
                    Logger().log("resetting to pending transfer: " + tx["local_path"])
            elif tx_status == 2:
                # these are the processes which could take a long time so should have a much longer timeout
                if delta.total_seconds() > 60 * 60 * 12:
                    EnaFileTransfer().set_pending(tx["_id"])
                    Logger().log("resetting to pending transfer: " + tx["local_path"])
            elif tx_status == 5:
                # these are the processes which could take a long time so should have a much longer timeout
                if delta.total_seconds() > 60 * 60 * 12:
                    EnaFileTransfer().set_pending(tx["_id"])
                    Logger().log("resetting to pending transfer: " + tx["local_path"])


def insert_message(message, user):
    sm = StatusMessage(message_owner=user, message=message)
    sm.save()


def _notify_transfer(profile_id, message, action="info"):
    """Push a transfer event to the submission info sidebar."""
    if not profile_id:
        return
    try:
        notify_submission_status(
            action=action,
            msg=message,
            data={"profile_id": str(profile_id)},
            html_id="submission_info",
        )
    except Exception as e:
        Logger().error(f"_notify_transfer: failed to push notification: {e}")


def _notify_copo_download_progress(profile_id, local_path, pct, done=False):
    """Push an in-place progress bar update for an ECS → COPO download."""
    if not profile_id:
        return
    base = os.path.basename(local_path) or local_path
    progress_id = f'copo_dl_{profile_id}_{base}'.replace(' ', '_')
    try:
        notify_submission_status(
            action="progress",
            msg=f'[COPO] {base}: {pct}%',
            data={
                "profile_id": str(profile_id),
                "progress_id": progress_id,
                "pct": pct,
                "file": base,
                "method": "COPO",
                "done": done,
            },
            html_id="submission_info",
        )
    except Exception as e:
        Logger().error(f"_notify_copo_download_progress: failed to push: {e}")


def process_pending_file_transfers():
    log = Logger()
    # get pending transfers
    docs = EnaFileTransfer().get_pending_transfers()
    # N.B. Transfer Status
    # 0 transfer complete
    # 1 check for presences of file on ecs
    # 2 transfer to COPO
    # 3 check for gzip
    # 4 check for md5
    # 5 transfer to ENA
    if docs:
        # cast cursor to list for double iteration
        # docs = list(docs)
        # first iterate all transfer records and set to processing so celery won't pick them again and send for processing as this
        # can lead to circular operations which won't terminate
        tx_ids = [tx["_id"] for tx in docs]
        EnaFileTransfer().set_processing(tx_ids)

        for tx in docs:
            try:
                # set userdetails to active_task for notifications to work
                pid = tx["profile_id"]
                uid = Profile().get_record(ObjectId(pid))["user_id"]
                user = User.objects.get(pk=uid)
                ud = user.userdetails
                ud.active_task = True
                ud.save()
            except Exception as e:
                log.error(
                    f"Skipping transfer record {tx.get('_id')} "
                    f"(profile_id={tx.get('profile_id')!r}, local_path={tx.get('local_path')!r}): "
                    f"{e}"
                )
                # Mark the bad record as pending again so we don't leave it orphaned
                # in 'processing' forever. It will either be fixed or keep erroring
                # each tick, which is visible in the logs.
                try:
                    EnaFileTransfer().set_pending(tx["_id"])
                except Exception as inner:
                    log.error(f"Failed to reset bad transfer record {tx.get('_id')}: {inner}")
                continue

            tx_status = tx["transfer_status"]

            if tx_status == 1:
                # check if is on ECS
                # chk = check_file_in_ecs(tx)
                chk = (
                    True  # for now, as all files should be in ECS before they get here
                )
                if not chk:
                    # not much we can do here...this should not happen, just update last checked
                    log.error(tx["local_path"] + " not in ecs ")
                    reset_status_counter(tx, user=user)
                else:
                    # no need to update last checked
                    increment_status_counter(tx)
                # continue
            elif tx_status == 2:
                # transfer to COPO
                msg = "Transferring file to COPO: " + tx["ecs_location"]
                insert_message(message=msg, user=user)
                _notify_transfer(tx.get("profile_id"), msg)
                try:
                    get_ecs_file(tx)
                    _notify_transfer(
                        tx.get("profile_id"),
                        f"Download to COPO complete: {os.path.basename(tx['local_path'])}",
                    )
                    # create thumbnail for image file
                    increment_status_counter(tx)
                except PermanentTransferError as e:
                    # Non-retryable S3 failure (e.g. NoSuchKey, NoSuchBucket,
                    # AccessDenied). Mark failed so we don't loop forever.
                    log.error(f"Permanent transfer failure, marking failed: {e}")
                    mark_failed(tx, reason=str(e), user=user)
                    continue
                except Exception as e:
                    log.exception(e)
                    log.error("error downloading from ecs: " + str(e))
                    reset_status_counter(tx, user=user)
                    continue

                # Generate a thumbnail for the file if it's an image
                if tx.get("file_type", "") == "image":
                    create_image_thumbnail(
                        file_name=os.path.basename(tx['local_path']),
                        uploaded_file=tx['local_path'], 
                        profile_id=tx.get('profile_id'), 
                        local_path=tx['local_path'] 
                    )
            elif tx_status == 3:
                increment_status_counter(tx)
                continue
                '''
                if check_gzip(tx):
                    increment_status_counter(tx)
                else:
                    record_error("file not gzipped")
                    reset_status_counter(tx)
                '''
            elif tx_status == 4:
                # insert_message(message="Checking MD5: " + tx["ecs_location"], user=user)
                if True:  # check_md5(tx):
                    if not tx.get("remote_path", ""):
                        log.log("no ecs location, skipping transfer to ENA")
                        mark_complete(tx)
                        continue
                    else:
                        increment_status_counter(tx)
                else:
                    # Todo - need to do something cleverer here
                    reset_status_counter(tx, user=user)
            elif tx_status == 5:

                # EnaFileTransfer().set_processing(tx["_id"])
                msg = f'Transferring to ENA: {os.path.basename(tx["local_path"])} -> {tx["remote_path"]}'
                insert_message(message=msg, user=user)
                _notify_transfer(tx.get("profile_id"), msg)
                to_ena(user_details=ud, tx=tx, user=user)
                """
                log.log("transfering to ENA: " + tx["local_path"])

                thread = ToENA(tx=tx, user_details=ud, pid=pid)
                thread.start()
                """
                # transfer_to_ena(tx)


def record_error(error):
    Logger().error(error)


def increment_status_counter(tx):
    tx["transfer_status"] = tx["transfer_status"] + 1
    tx["last_checked"] = get_datetime()
    tx["status"] = "pending"
    # progress made — reset the transient-failure counter
    tx["failure_count"] = 0
    EnaFileTransfer().get_collection_handle().update_one(
        {"_id": tx["_id"]}, {"$set": tx}
    )


def decrement_status_counter(tx):
    tx["transfer_status"] = tx["transfer_status"] - 1
    tx["last_checked"] = get_datetime()
    tx["status"] = "pending"
    EnaFileTransfer().get_collection_handle().update_one(
        {"_id": tx["_id"]}, {"$set": tx}
    )


def mark_failed(tx, reason="", user=None):
    """Terminal state for records that will never succeed on retry.
    Use this (rather than reset_status_counter) when a PermanentTransferError
    has been raised or the consecutive-failure cap has been hit.

    The record sits in status="failed" until the user resubmits, at which
    point make_transfer_record will reset it to pending.
    """
    tx["transfer_status"] = int(TransferStatus.FAILED)
    tx["last_checked"] = get_datetime()
    tx["status"] = "failed"
    if reason:
        tx["failure_reason"] = reason
    EnaFileTransfer().get_collection_handle().update_one(
        {"_id": tx["_id"]}, {"$set": tx}
    )

    # Surface the failure to the user if we have a User object to attach
    # the StatusMessage to. Best-effort — never let a notification failure
    # prevent the DB state change above.
    if user is not None:
        try:
            file_name = os.path.basename(tx.get("local_path", "")) or tx.get("local_path", "")
            msg = f"Transfer permanently failed for {file_name}"
            if reason:
                msg += f": {reason}"
            msg += ". Please resubmit to retry."
            insert_message(message=msg, user=user)
            _notify_transfer(tx.get("profile_id"), msg, action="error")
        except Exception as e:
            Logger().error(f"mark_failed: could not post status message: {e}")


# Backwards-compatible alias. Older code paths called mark_error(); funnel them
# through the same terminal handling so the UI sees a single failure state.
def mark_error(tx):
    mark_failed(tx, reason="error")


def mark_complete(tx):
    # tx["transfer_status"] = 0
    tx["last_checked"] = get_datetime()
    tx["status"] = "complete"
    EnaFileTransfer().get_collection_handle().update_one(
        {"_id": tx["_id"]}, {"$set": tx}
    )


def reset_status_counter(tx, user=None):
    # Count consecutive transient failures. If we exceed the cap, mark the
    # record failed rather than spin on it forever. The user can resubmit.
    failure_count = tx.get("failure_count", 0) + 1
    if failure_count >= _MAX_CONSECUTIVE_FAILURES:
        Logger().error(
            f"Giving up after {failure_count} failures on {tx['local_path']} "
            f"(transfer_status={tx.get('transfer_status')}); marking failed."
        )
        mark_failed(
            tx,
            reason=f"Exceeded {_MAX_CONSECUTIVE_FAILURES} consecutive failures",
            user=user,
        )
        return

    Logger().log(f"resetting ({failure_count}/{_MAX_CONSECUTIVE_FAILURES}): " + tx["local_path"])
    # the file is already downloaded to local, so, just restart the transfer for remote location
    if tx["transfer_status"] > 2:
        # this is a new transfer
        tx["transfer_status"] = 3
    # restart the transfer to local
    elif tx["transfer_status"] == 2:
        # this is a new transfer
        tx["transfer_status"] = 1
    tx["last_checked"] = get_datetime()
    tx["status"] = "pending"
    tx["failure_count"] = failure_count
    EnaFileTransfer().get_collection_handle().update_one(
        {"_id": tx["_id"]}, {"$set": tx}
    )


def update_last_checked(tx):
    tx["last_checked"] = get_datetime()
    EnaFileTransfer().get_collection_handle().update_one(
        {"_id": tx["_id"]}, {"$set": tx}
    )


def get_ecs_file(tx):
    file = DataFile().get_collection_handle().find_one({"_id": ObjectId(tx["file_id"])})
    Path(tx["local_path"]).parent.mkdir(parents=True, exist_ok=True)

    # Download to a temp file first, then rename on success.
    # This prevents partial files from being left on disk if the download fails.
    tmp_path = tx["local_path"] + ".tmp"
    profile_id = tx.get("profile_id")
    s3_client = s3()
    total = s3_client.head_object_size(file["bucket_name"], file["file_name"])
    progress = {"bytes": 0, "last_emit": 0.0, "last_pct": -1, "lock": threading.Lock()}

    def _cb(chunk):
        if not profile_id or not total:
            return
        with progress["lock"]:
            progress["bytes"] += chunk
            pct = int(progress["bytes"] * 100 / total)
            now = time.time()
            if pct != progress["last_pct"] and (now - progress["last_emit"] >= 1.0 or pct in (0, 100)):
                progress["last_emit"] = now
                progress["last_pct"] = pct
                _notify_copo_download_progress(profile_id, tx["local_path"], pct)

    try:
        s3_client.get_object(
            bucket=file["bucket_name"], key=file["file_name"], loc=tmp_path, callback=_cb
        )
        os.rename(tmp_path, tx["local_path"])
        if profile_id:
            _notify_copo_download_progress(profile_id, tx["local_path"], 100, done=True)
    except ClientError as e:
        # Clean up partial temp file on failure
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        code = (e.response or {}).get("Error", {}).get("Code", "")
        if code in _NON_RETRYABLE_S3_CODES:
            # Translate known-permanent S3 failures into our dedicated
            # exception so the caller can stop retrying.
            raise PermanentTransferError(
                f"S3 download permanently failed ({code}) for "
                f"{file.get('bucket_name')}/{file.get('file_name')}: {e}"
            ) from e
        raise
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    if not file["file_hash"]:
        hash_md5 = hashlib.md5()
        with open(tx["local_path"], "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        calc = hash_md5.hexdigest()
        DataFile().update_file_hash(file["_id"], calc)


def check_file_in_ecs(tx):
    Logger().log("checking for file: " + tx["local_path"])
    file = DataFile().get_collection_handle().find_one({"_id": ObjectId(tx["file_id"])})
    return s3().check_s3_bucket_for_files(file["bucket_name"], [file["file_name"]])


def check_gzip(tx):
    Logger().log("checking gzip status: " + tx["local_path"])
    with gzip.open(tx["local_path"], 'r') as fh:
        try:
            fh.read(1)
            return True
        except OSError as e:
            Logger.error(e)
            return False


def check_md5(tx):
    Logger().log("checking md5: " + tx["local_path"])
    file = DataFile().get_collection_handle().find_one({"_id": ObjectId(tx["file_id"])})
    hash_md5 = hashlib.md5()
    with open(tx["local_path"], "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    calc = hash_md5.hexdigest()
    if calc == file["file_hash"]:
        return True
    else:
        Logger().log(
            "md5 mismatch, should be: " + file["file_hash"] + ", but got: " + calc
        )
        return False


def get_transfer_status(tx):
    """
    :param ena_transfer_record: ena transfer record
    :return: transfer status
    """
    if tx:
        transfer_status = tx.get("transfer_status", 0)
        is_archive = tx.get("is_archive", "0")
        status = tx.get("status", str())
        # terminal failure — record will not progress without user resubmission.
        # Covers the new "failed" status and the legacy "error" / transfer_status==10 pair.
        if status in ("failed", "error") or transfer_status == 10:
            return TransferStatus.FAILED
        #if the file has been uploaded to ENA, the status won't change without regarding to the is_archive flag
        if transfer_status == 5 and status == "ena_complete":
            if is_archive == "1":
                return TransferStatus.ARCHIVE_COMPLETED_VALIDATION_IN_ENA
            else: 
                return TransferStatus.COMPLETED_VALIDATION_IN_ENA
        elif transfer_status == 5 and status == "complete":
            if is_archive == "1":
                return TransferStatus.ARCHIVE_TRANSFERRED_TO_ENA
            else:
                return TransferStatus.TRANSFERRED_TO_ENA
        
        #if the file is archived, then return False
        if is_archive == "1":
            return TransferStatus.ARCHIVE
        
        if transfer_status == 5 and status == "pending":
            return TransferStatus.TRANSFERRING_TO_ENA
        elif transfer_status > 2:
            return TransferStatus.DOWNLOADED_TO_LOCAL
        elif transfer_status == 2:
            return TransferStatus.TRANSFERRING_TO_ENA
        elif transfer_status == 1:
            return TransferStatus.CHECKING_FOR_DOWNLOAD
        elif transfer_status == 0:  # for compatibility with old records
            return TransferStatus.TRANSFERRED_TO_ENA
        else:
            # unknown status
            Logger().error(
                f"Unknown transfer status: {transfer_status} for file: {tx['local_path']}"
            )
    else:
        return False

def to_ena(user_details, tx, user=None):
    kwargs = dict(profile_id=tx.get("profile_id", ""))
    result = transfer_to_ena(
            tx["remote_path"],
            [tx["local_path"]],
            **kwargs,
        )
    if not result:
        reset_status_counter(tx, user=user)
        return
    # now check if active tasks can be marked False
    mark_complete(tx)
    transfers = (
        EnaFileTransfer().get_collection_handle().find({"profile_id": tx["profile_id"]})
    )
    complete = True
    # if os.path.exists(self.tx["local_path"]):
    # Logger().log("deleting file after check")
    # os.remove(self.tx["local_path"])  #don't remove file as need resubmission
    for t in transfers:
        if not t["status"] == "complete":
            complete = False
            break
    if complete == True:
        user_details.active_task = False
        user_details.save()


"""
def transfer_to_ena_deleted(tx):
    # transfer_to_ena(webin_user, pass_word, remote_path, file_paths=list(), **kwargs):
    ena_service = get_env('ENA_SERVICE')
    pass_word = get_env('WEBIN_USER_PASSWORD')
    user_token = get_env('WEBIN_USER').split("@")[0]
    webin_user = get_env('WEBIN_USER')
    webin_domain = get_env('WEBIN_USER').split("@")[1]
    # Logger().log("transfering file: " + tx["file_id"])
    kwargs = dict()
    try:
        Logger().log("doing transfer")
        to_ena(webin_user, pass_word, tx["remote_path"], [tx["local_path"]], **kwargs)
        Logger().log("deleting file")
        if os.path.exists(tx["local_path"]):
            Logger().log("deleting file after check")
            os.remove(tx["local_path"])
    except Exception as e:
        Logger().exception(e)
        record_error("error transfering to ENA: " + str(e))
        reset_status_counter(tx)
"""

def housekeeping_local_uploads():
    """
    Housekeeping local uploads
    """
    # delete all files in local_uploads older than 30 days
    time = datetime.now() - timedelta(days=settings.LOCAL_UPLOAD_HOUSEKEEPING_DAYS)
    ena_files = EnaFileTransfer().execute_query(
        {
            #"$or": [
                #{"status": "complete", "remote_path": ""},
                #{"status": "complete", "remote_path": {"$exists": False}},
                #{"status": "ena_complete", "remote_path": {"$exists": True, "$ne": ""}},  #"ena_complele" is an obsolete status
            #],
            'status': {"$in": ["complete", "ena_complete"]},
            "last_checked": {"$lt": time},
            "is_archived": {"$ne": "1"},
        }
    )
    if ena_files:
        for ena_file in ena_files:
            try:
                Logger().debug(f"Deleting file: {ena_file['local_path']}")
                if os.path.exists(ena_file["local_path"]):
                    os.remove(ena_file["local_path"])
            except Exception as e:
                Logger().error(f"Error deleting file {ena_file['local_path']}: {e}")
        # delete ena_file records
        EnaFileTransfer().get_collection_handle().update_many(
            {"_id": {"$in": [ena_file["_id"] for ena_file in ena_files]}},{"$set": {"is_archived": "1", "last_checked": get_datetime()}}
        )


def remove_transfer_record(file_ids=list(), profile_id=None):
    """
    Remove transfer records for given file ids
    """
    if not file_ids:
        return
    EnaFileTransfer().get_collection_handle().delete_many(
        {
            "file_id": {"$in": file_ids},
            "profile_id": profile_id,
            "deleted": get_not_deleted_flag(),
        }
    )
    Logger().debug(f"Removed transfer records for file ids: {file_ids}")
