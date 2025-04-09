__author__ = 'fshaw'
import os
from common.dal.copo_da import EnaFileTransfer, DataFile
from common.dal.profile_da import Profile
from common.s3.s3Connection import S3Connection as s3
from datetime import datetime
from bson import ObjectId
from common.utils.logger import Logger
import gzip
from .generic_helper import transfer_to_ena as to_ena
from common.utils.helpers import get_env, get_thumbnail_folder
from datetime import datetime
from src.apps.copo_core.models import StatusMessage, User
import threading
import hashlib
from pathlib import Path
from PIL import ImageFile, Image
from django.conf import settings

ImageFile.LOAD_TRUNCATED_IMAGES = True

Image.MAX_IMAGE_PIXELS = None

def make_transfer_record(file_id, submission_id, remote_location=None, no_remote_location=False):
    # N.B. called from celery
    # make transfer object
    remote_location = remote_location if remote_location else submission_id + "/reads/"

    file = DataFile().get_record(file_id)
    tx = dict()
    if not no_remote_location:
        tx["remote_path"] = remote_location
        
    tx["local_path"] = file["file_location"]
    tx["ecs_location"] = file["ecs_location"]
    tx["file_id"] = str(file["_id"])
    tx["profile_id"] = file["profile_id"]
    tx["file_type"] = file["file_type"]
    # tx["status"] = "pending"
    tx["submission_id"] = submission_id
    # N.B. Transfer Status
    # 0 transfer complete
    # 1 check for presences of file on ecs
    # 2 transfer to COPO
    # 3 check for gzip
    # 4 check for md5ß
    # 5 transfer to ENA
    # 10 Error
    # tx["transfer_status"] = 1
    print(tx)
    ena_file = EnaFileTransfer().get_collection_handle().find_one({"local_path": file["file_location"]})
    if (not ena_file) or ena_file["status"] != "processing":
        tx["created"] = datetime.utcnow()
        tx["last_checked"] = datetime.utcnow()
        tx["status"] = "pending"
        tx["transfer_status"] = 1
        EnaFileTransfer().get_collection_handle().update_one({"local_path": file["file_location"]}, {"$set": tx},
                                                             upsert=True)
    else:
        Logger().log("The file is downloading, will not download it again: " + tx["local_path"])


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
                if delta.seconds > 60 * 10:
                    EnaFileTransfer().set_pending(tx["_id"])
                    Logger().log("resetting to pending transfer: " + tx["local_path"])
            elif tx_status == 2:
                # these are the processes which could take a long time so should have a much longer timeout
                if delta.seconds > 60 * 60 * 12:
                    EnaFileTransfer().set_pending(tx["_id"])
                    Logger().log("resetting to pending transfer: " + tx["local_path"])
            elif tx_status == 5:
                # these are the processes which could take a long time so should have a much longer timeout
                if delta.seconds > 60 * 60 * 12:
                    EnaFileTransfer().set_pending(tx["_id"])
                    Logger().log("resetting to pending transfer: " + tx["local_path"])


def insert_message(message, user):
    sm = StatusMessage(message_owner=user, message=message)
    sm.save()


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
            # set userdetails to active_task for notifications to work
            pid = tx["profile_id"]
            uid = Profile().get_record(ObjectId(pid))["user_id"]
            user = User.objects.get(pk=uid)
            ud = user.userdetails
            ud.active_task = True
            ud.save()

            tx_status = tx["transfer_status"]

            if tx_status == 1:
                # check if is on ECS
                #chk = check_file_in_ecs(tx)
                chk = True # for now, as all files should be in ECS before they get here
                if not chk:
                    # not much we can do here...this should not happen, just update last checked
                    log.error(tx["local_path"] + " not in ecs ")
                    reset_status_counter(tx)
                else:
                    # no need to update last checked
                    increment_status_counter(tx)
                # continue
            elif tx_status == 2:
                # transfer to COPO
                insert_message(message="Transferring file to COPO: " + tx["ecs_location"], user=user)
                try:
                    get_ecs_file(tx)
                    #create thumbnail for image file
                    increment_status_counter(tx)
                except Exception as e:
                    log.exception(e)
                    log.error("error downloading from ecs: " + str(e))
                    reset_status_counter(tx)
                    continue
                if tx.get("file_type","") == "image":
                    filename = os.path.basename(tx["local_path"])
                    final_dot = filename.rfind(".")
                    file_extension = filename[final_dot:]                    
                    thumbnail_path =  get_thumbnail_folder(tx["profile_id"]) + "/" + filename[:final_dot] +  "_thumb"+ file_extension
                    size = 128, 128
                    im = Image.open(tx["local_path"])
                    im.thumbnail(size)
                    im.save(thumbnail_path)
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
                    increment_status_counter(tx)
                else:
                    # Todo - need to do something cleverer here
                    reset_status_counter(tx)
            elif tx_status == 5:
                if not tx.get("remote_path",""):
                    log.log("no ecs location, skipping transfer to ENA")
                    mark_complete(tx)
                    continue 

                #EnaFileTransfer().set_processing(tx["_id"])
                insert_message(message=f'Transfering to ENA: {tx["local_path"]} to {tx["remote_path"]}', user=user)
                log.log("transfering to ENA: " + tx["local_path"])
                thread = ToENA(tx=tx, user_details=ud, pid=pid)
                thread.start()
                # transfer_to_ena(tx)


def record_error(error):
    Logger().error(error)


def increment_status_counter(tx):
    tx["transfer_status"] = tx["transfer_status"] + 1
    tx["last_checked"] = datetime.utcnow()
    tx["status"] = "pending"
    EnaFileTransfer().get_collection_handle().update_one({"_id": tx["_id"]}, {"$set" : tx})


def decrement_status_counter(tx):
    tx["transfer_status"] = tx["transfer_status"] - 1
    tx["last_checked"] = datetime.utcnow()
    tx["status"] = "pending"
    EnaFileTransfer().get_collection_handle().update_one({"_id": tx["_id"]}, {"$set" : tx})


def mark_error(tx):
    tx["transfer_status"] = 10
    tx["last_checked"] = datetime.utcnow()
    tx["status"] = "error"
    EnaFileTransfer().get_collection_handle().update_one({"_id": tx["_id"]}, {"$set" : tx})


def mark_complete(tx):
    tx["transfer_status"] = 0
    tx["last_checked"] = datetime.utcnow()
    tx["status"] = "complete"
    EnaFileTransfer().get_collection_handle().update_one({"_id": tx["_id"]}, {"$set" : tx})


def reset_status_counter(tx):
    Logger().log("resetting: " + tx["local_path"])
    tx["transfer_status"] = 1
    tx["last_checked"] = datetime.utcnow()
    tx["status"] = "pending"
    EnaFileTransfer().get_collection_handle().update_one({"_id": tx["_id"]}, {"$set" : tx})


def update_last_checked(tx):
    tx["last_checked"] = datetime.utcnow()
    EnaFileTransfer().get_collection_handle().update_one({"_id": tx["_id"]}, {"$set" : tx})


def get_ecs_file(tx):
    file = DataFile().get_collection_handle().find_one({"_id": ObjectId(tx["file_id"])})
    Path(tx["local_path"]).parent.mkdir(parents=True,exist_ok=True)

    s3().get_object(bucket=file["bucket_name"], key=file["file_name"], loc=tx["local_path"])
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
        Logger().log("md5 mismatch, should be: " + file["file_hash"] + ", but got: " + calc)
        return False


class ToENA(threading.Thread):
    def __init__(self, tx, user_details, pid):
        self.tx = tx
        self.ud = user_details
        self.pid = pid
        super(ToENA, self).__init__()

    def run(self):
        # transfer_to_ena(webin_user, pass_word, remote_path, file_paths=list(), **kwargs):
        ena_service = get_env('ENA_SERVICE')
        pass_word = get_env('WEBIN_USER_PASSWORD')
        user_token = get_env('WEBIN_USER').split("@")[0]
        webin_user = get_env('WEBIN_USER')
        webin_domain = get_env('WEBIN_USER').split("@")[1]
        # Logger().log("transfering file: " + tx["file_id"])
        kwargs = dict()
        try:
            to_ena(webin_user, pass_word, self.tx["remote_path"], [self.tx["local_path"]], **kwargs)
        except Exception as e:
            Logger().exception(e)
            record_error("error transfering to ENA: " + str(e))
            reset_status_counter(self.tx)
            return
        # now check if active tasks can be marked False
        mark_complete(self.tx)
        transfers = EnaFileTransfer().get_collection_handle().find({"profile_id": self.pid})
        complete = True
        if os.path.exists(self.tx["local_path"]):
            Logger().log("deleting file after check")
            os.remove(self.tx["local_path"])  #don't remove file as need resubmission
        for t in transfers:
            if not t["transfer_status"] == 0:
                complete = False
        if complete == True:
            self.ud.active_task = False
            self.ud.save()


def transfer_to_ena(tx):
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
