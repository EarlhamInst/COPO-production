import os
import pexpect
import requests
from bson import ObjectId
import common.dal.mongo_util as mutil
from django.conf import settings
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from common.lookup.copo_enums import Loglvl, Logtype
from common.utils import helpers
from common.utils.logger import Logger
import re
import shlex
import subprocess
import time

lg = Logger()
from common.utils.helpers import get_env

BASE_DIR = settings.BASE_DIR
# REPOSITORIES = settings.REPOSITORIES
ASPERA_PATH = get_env("ASPERA_PATH")  #"/root/.aspera/cli"
WEBIN_USER_PASSWORD = get_env('WEBIN_USER_PASSWORD')
WEBIN_USER = get_env('WEBIN_USER')

def get_submission_handle():
    return mutil.get_collection_ref('SubmissionCollection')


def get_submission_queue_handle():
    # Use the shared MONGO_CLIENT (already connected, topology known) rather
    # than spawning a new MongoClient on every call. Creating a fresh client
    # each time leaves topology in Unknown state, causing a 30s
    # serverSelectionTimeout block on every Celery beat tick under gevent.
    return mutil.get_collection_ref('SubmissionQueueCollection')


'''  deprecated
def get_filetransfer_queue_handle():  # this can be safely called by forked process
    mongo_client = mutil.get_mongo_client()
    collection_handle = mongo_client['FileTransferQueueCollection']

    return collection_handle
'''


def get_description_handle():
    return mutil.get_collection_ref('DescriptionCollection')


def get_person_handle():
    return mutil.get_collection_ref('PersonCollection')


def get_datafiles_handle():
    return mutil.get_collection_ref('DataFileCollection')


def get_samples_handle():
    return mutil.get_collection_ref('SampleCollection')


def get_sources_handle():
    return mutil.get_collection_ref('SourceCollection')


def logging_info(message=str(), submission_id=str()):
    """
    function provides a consistent way of logging submission status/information
    :param message:
    :param submission_id:
    :return:
    """

    lg.log('[Submission: ' + submission_id + '] ' + message)

    return True


def logging_debug(message=str(), submission_id=str()):
    """
    function provides a consistent way of logging submission status/information
    :param message:
    :param submission_id:
    :return:
    """

    lg.debug('[Submission: ' + submission_id + '] ' + message)

    return True


def logging_error(message=str(), submission_id=str()):
    """
    function provides a consistent way of logging error during submission
    :param message:
    :param submission_id:
    :return:
    """
    lg.error('[Submission: ' + submission_id + '] ' + message)
 
    return True


def logging_exception(exception):
    lg.exception(exception)


def log_general_info(message):
    """
    logs info not tied to a specific submission record
    :param message:
    :return:
    """

    lg.log('[Submission:] ' + message, level=Loglvl.INFO, type=Logtype.FILE)

    return True


def log_general_error(message):
    """
    logs error not tied to a specific submission record
    :param message:
    :return:
    """
    try:
        lg.log('[Submission: ] ' + message, level=Loglvl.ERROR, type=Logtype.FILE)
    except Exception as e:
        return False

    return True


def update_submission_status(
    status=str(), message=str(), submission_id=str(), notify=True
):
    """
    function updates status of submission
    :param status:
    :param message:
    :param submission_id:
    :param notify:
    :return:
    """

    if not submission_id:
        return False

    collection_handle = get_submission_handle()
    doc = collection_handle.find_one(
        {"_id": ObjectId(submission_id)}, {"transcript": 1, "profile_id": 1}
    )

    if not doc:
        return False

    submission_record = doc
    transcript = submission_record.get("transcript", dict())
    status = dict(type=status, message=message)
    transcript['status'] = status

    if not message:
        transcript.pop('status', '')

    submission_record['transcript'] = transcript
    submission_record['date_modified'] = helpers.get_datetime()

    collection_handle.update_one(
        {"_id": ObjectId(str(submission_record.pop('_id')))},
        {'$set': submission_record},
    )

    # notify client agent on status change
    if notify:
        notify_status_change(
            profile_id=submission_record.get("profile_id", str()),
            submission_id=submission_id,
        )

    return True


def notify_status_change(profile_id=str(), submission_id=str()):
    """
    this function notifies the client agent of potential change to submission status for the target record
    :param profile_id:
    :param submission_id:
    :return:
    """

    if submission_id and profile_id:
        event = dict(type='submission_status')
        event["submission_id"] = submission_id

        group_name = 'submission_status_%s' % profile_id

        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(group_name, event)

    return True


def notify_sample_status(
    profile_id=str(), action="message", msg=str(), data={}, html_id=""
):
    """
    function notifies client changes in Sample creation status
    :param profile_id:
    :param action:
    :param msg:
    :return:
    """
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    group_name = 'sample_status_%s' % profile_id
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_frontend(
    action="message",
    msg=str(),
    data={},
    html_id="",
    profile_id="",
    group_name='dtol_status',
):
    """
    function notifies client changes in Sample creation status
    :param profile_id:
    :param action:
    :param msg:
    :return:
    """
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_assembly_status(
    action="message", msg=str(), data={}, html_id="", profile_id=""
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'assembly_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_read_status(action="message", msg=str(), data={}, html_id="", profile_id=""):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'read_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True


def notify_tagged_seq_status(
    action="message", msg=str(), data={}, html_id="", profile_id=""
):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    group_name = 'tagged_seq_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True

def notify_ena_object_status(action="message", msg=str(), data={}, html_id="", profile_id="", checklist_id=str()):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    if checklist_id.startswith("ERC"):
        group_name = 'read_status_%s' % data["profile_id"]
    else:
        group_name = 'tagged_seq_status_%s' % data["profile_id"]
    event = {
        "type": "msg",
        "action": action,
        "message": msg,
        "data": data,
        "html_id": html_id,
    }
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(group_name, event)
    return True

def notify_singlecell_status(action="message", msg=str(), data={}, html_id="", profile_id="", checklist_id=str()):
    # type points to the object type which will be passed to the socket and is a method defined in consumer.py
    event = {"type": "msg", "action": action, "message": msg, "data": data, "html_id": html_id}
    channel_layer = get_channel_layer()
    group_name = 'singlecell_status_%s' % data["profile_id"]
    async_to_sync(channel_layer.group_send)(
        group_name,
        event
    )
    return True

def notify_transfer_status(profile_id=str(), submission_id=str(), status_message=str()):
    """
    function notifies client of ENA file transfer status
    :param profile_id:
    :param submission_id:
    :param status_message:
    :return:
    """

    if submission_id and profile_id:
        event = dict(type='file_transfer_status')
        event["submission_id"] = submission_id
        event["status_message"] = status_message

        group_name = 'submission_status_%s' % profile_id

        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(group_name, event)

    return True


'''  deprecated
def schedule_file_transfer(submission_id=str(), remote_location=str()):
    """
    function schedules transfer of files associated with a submission to ENA Dropbox's /remote_location
    :param submission_id:
    :param remote_location:
    :return:
    """

    if not submission_id:
        message = 'Error scheduling file transfer. Submission identifier not found!.'
        log_general_error(message)
        return False

    transfer_handle = get_filetransfer_queue_handle()
    doc = transfer_handle.find_one({"submission_id": submission_id})

    submission_record = get_submission_handle().find_one(
        {'_id': ObjectId(submission_id)}, {"profile_id": 1})

    if not doc:  # submission not in queue, add to queue
        fields = dict(
            submission_id=submission_id,
            remote_location=remote_location,
            date_modified=d_utils.get_datetime(),
            processing_status='pending'  # pending or running
        )

        transfer_handle.insert(fields)
        message = 'Submission data files scheduled for upload.'

        status_message = 'Submission data files scheduled for upload. Progress will be reported.'
        notify_transfer_status(profile_id=submission_record['profile_id'], submission_id=submission_id,
                               status_message=status_message)

    else:
        message = 'File transfer already scheduled for this submission.'

    logging_info(message, submission_id)

    return True
'''

'''
def get_ena_remote_files(user_token=str(), pass_word=str()):
    """
    function returns unsubmitted files in ENA's Dropbox
    :param user_token:
    :param pass_word:
    :return:
    """

    headers = {
        'accept': '*/*',
    }

    params = (
        ('format', 'json'),
        ('max-results', '50000'),
    )

    try:
        response = requests.get('https://www.ebi.ac.uk/ena/submit/report/unsubmitted-files', headers=headers,
                                params=params, auth=(user_token, pass_word))
    except Exception as e:
        log_general_error('API call error ' + str(e))
        return list()

    if not response.status_code == 200:
        return list()

    return response.json()
'''


def get_study_status(user_token=str(), pass_word=str(), project_accession=str()):
    """
    function returns the status of a study from ENA
    :param user_token:
    :param pass_word:
    :param project_accession:
    :return:
    """

    headers = {
        'accept': '*/*',
    }

    params = (
        ('format', 'json'),
        ('max-results', '1'),
    )

    try:
        response = requests.get(
            f"{get_env('ENA_ENDPOINT_REPORT')}projects/{project_accession}",
            headers=headers,
            params=params,
            auth=(user_token, pass_word),
        )
    except Exception as e:
        log_general_error('API call error ' + str(e))
        return list()

    if not response.status_code == 200:
        log_general_error(
            f"Error fetching study status for {project_accession}: {response.status_code} {response.text}"
        )   
        return list()

    return response.json()


def touch_files(file_paths=list()):
    """
    function creates empty files as specified in file_paths
    :param file_paths:
    :return:
    """

    for path in file_paths:
        try:
            if not os.path.exists(path):
                with open(path, "w") as f:
                    f.write("TGAC")
        except Exception as e:
            log_general_error('Error touching file ' + path + ' ' + str(e))

    return True

def _notify_transfer_method(method, message, profile_id=""):
    """Send transfer method notification to frontend via channels."""
    lg.log(f'[Transfer method: {method}] {message}')
    if profile_id:
        # Send to submission_status group (used by single cell, read, etc. submission pages)
        notify_frontend(
            action="info",
            msg=f'[{method}] {message}',
            data={"profile_id": profile_id},
            html_id="submission_info",
            group_name=f'submission_status_{profile_id}',
        )


_PCT_RE = re.compile(r'(\d{1,3})%')


def _progress_id_for(file_path, profile_id):
    base = os.path.basename(file_path) or file_path
    return f'tx_{profile_id}_{base}'.replace(' ', '_')


def _notify_transfer_progress(method, file_path, pct, profile_id, extra=None):
    """Emit an in-place progress update for a single file transfer."""
    if not profile_id:
        return
    progress_id = _progress_id_for(file_path, profile_id)
    data = {
        "profile_id": profile_id,
        "progress_id": progress_id,
        "pct": pct,
        "file": os.path.basename(file_path),
        "method": method,
    }
    if extra:
        data.update(extra)
    notify_frontend(
        action="progress",
        msg=f'[{method}] {os.path.basename(file_path)}: {pct}%',
        data=data,
        html_id="submission_info",
        group_name=f'submission_status_{profile_id}',
    )


def _transfer_via_aspera(remote_path, file_paths, submission_id="", profile_id=""):
    """Attempt file transfer to ENA using Aspera CLI. Returns True on success.
    No progress reporting on this path -- ascp's progress output is PTY-only and
    fragile under subprocess. The FTP fallback emits real byte-level progress.
    """
    ascp_bin = f"{ASPERA_PATH}/bin/ascp"
    ascp_key = f"{ASPERA_PATH}/etc/asperaweb_id_dsa.openssh"
    # -P 33001 = ENA's documented Aspera SSH/FASP control port.
    cmd = [
        ascp_bin, "-P", "33001", "-l", "300M", "-i", ascp_key, "-d",
    ] + list(file_paths) + [f"{WEBIN_USER}:{remote_path}"]
    ascp_env = {**os.environ, "ASPERA_SCP_PASS": WEBIN_USER_PASSWORD}

    _notify_transfer_method("Aspera", f'Commencing transfer of {" ".join(file_paths)} to ENA', profile_id)

    try:
        output = subprocess.check_output(
            cmd, env=ascp_env, stderr=subprocess.STDOUT, timeout=12 * 60 * 60
        )
        lg.log(output)
        _notify_transfer_method(
            "Aspera",
            f'Transfer to {remote_path} for {" ".join(file_paths)} completed',
            profile_id,
        )
        return True
    except subprocess.TimeoutExpired:
        lg.error(f'Aspera transfer timed out for {" ".join(file_paths)}')
        return False
    except subprocess.CalledProcessError as e:
        lg.error(
            f'Aspera transfer failed (rc={e.returncode}) for '
            f'{" ".join(file_paths)}: {e.output!r}'
        )
        return False
    except Exception as e:
        lg.error(f'Aspera transfer error for {" ".join(file_paths)}: {e}')
        return False


def _transfer_via_ftp(remote_path, file_paths, submission_id="", profile_id=""):
    """Attempt file transfer to ENA using FTP via curl. Returns True on success."""
    webin_user = WEBIN_USER.split("@")[0]
    ftp_base = f"ftp://webin2.ebi.ac.uk/{remote_path}"

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        ftp_url = f"{ftp_base}{file_name}"
        # -# emits a progress bar to stderr; -N disables output buffering so we see it in real time
        cmd = [
            "curl", "-#", "-N", "-T", file_path,
            ftp_url,
            "--user", f"{webin_user}:{WEBIN_USER_PASSWORD}",
            "--ftp-create-dirs",
            "--retry", "3",
            "--retry-delay", "5",
        ]

        _notify_transfer_method("FTP", f'Uploading {file_path} to ENA', profile_id)

        deadline = time.time() + 12 * 60 * 60
        last_emit = 0.0
        last_pct = -1
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
        except Exception as e:
            lg.error(f'FTP spawn failed for {file_path}: {e}')
            _notify_transfer_method("FTP", f'Transfer failed for {file_path}', profile_id)
            return False

        try:
            assert proc.stderr is not None
            buf = b""
            while True:
                if time.time() > deadline:
                    proc.kill()
                    lg.error(f'FTP transfer timed out for {file_path}')
                    _notify_transfer_method("FTP", f'Transfer timed out for {file_path}', profile_id)
                    return False
                chunk = proc.stderr.read(64)
                if not chunk:
                    if proc.poll() is not None:
                        break
                    continue
                buf += chunk
                # curl -# updates in place with \r; split on \r or \n
                parts = re.split(rb'[\r\n]', buf)
                buf = parts[-1]
                for part in parts[:-1]:
                    text = part.decode(errors="replace").strip()
                    if not text:
                        continue
                    m = _PCT_RE.search(text)
                    if not m:
                        continue
                    pct = int(m.group(1))
                    now = time.time()
                    if pct != last_pct and (now - last_emit >= 1.0 or pct in (0, 100)):
                        _notify_transfer_progress("FTP", file_path, pct, profile_id)
                        last_emit = now
                        last_pct = pct
            rc = proc.wait()
            if rc != 0:
                err_tail = (proc.stdout.read() if proc.stdout else b"")
                lg.error(f'FTP transfer failed (rc={rc}) for {file_path}: {err_tail!r}')
                _notify_transfer_method("FTP", f'Transfer failed for {file_path}', profile_id)
                return False
            _notify_transfer_progress("FTP", file_path, 100, profile_id, {"done": True})
            _notify_transfer_method("FTP", f'Transfer of {file_path} completed', profile_id)
        except Exception as e:
            try:
                proc.kill()
            except Exception:
                pass
            lg.error(f'FTP transfer error for {file_path}: {e}')
            _notify_transfer_method("FTP", f'Transfer failed for {file_path}', profile_id)
            return False

    return True


def transfer_to_ena(remote_path, file_paths=list(), **kwargs):
    if not file_paths:
        return True

    submission_id = kwargs.get("submission_id", str())
    profile_id = kwargs.get("profile_id", str())

    message = f'Commencing transfer of {" ".join(file_paths)} data files to ENA. Progress will be reported.'
    logging_info(message, submission_id)

    # Try Aspera first, fall back to FTP if it fails
    if _transfer_via_aspera(remote_path, file_paths, submission_id, profile_id):
        return True

    _notify_transfer_method("FTP", "Aspera unavailable, falling back to FTP", profile_id)
    lg.log(f'Aspera failed, falling back to FTP for {" ".join(file_paths)}')

    if _transfer_via_ftp(remote_path, file_paths, submission_id, profile_id):
        return True

    lg.error(f'{" ".join(file_paths)} was not uploaded to ENA via Aspera or FTP')
    _notify_transfer_method("FTP", f'All transfer methods failed for {" ".join(file_paths)}', profile_id)
    return False


def transfer_to_ena_old(webin_user, pass_word, remote_path, file_paths=list(), **kwargs):
    """
    function transfers files to ENA using Aspera CLI
    :param webin_user:
    :param pass_word:
    :param remote_path:
    :param file_paths:
    :param kwargs:
    :return:
    """

    submission_id = kwargs.get("submission_id", str())
    # transfer_queue_id = kwargs.get("transfer_queue_id", str())  # set to True to enable status reporting  deprecated
    report_status = kwargs.get(
        "report_status", False
    )  # if status message should be sent via channels layer

    submission_collection_handle = get_submission_handle()
    # transfer_collection_handle = get_filetransfer_queue_handle() deprecated

    if not file_paths:
        return True

    message = f'Commencing transfer of {len(file_paths)} data files to ENA. Progress will be reported.'
    logging_info(message, submission_id)

    # resource_path = os.path.join(BASE_DIR, REPOSITORIES.get('ASPERA', dict()).get('resource_path', str()))
    # os.chdir(resource_path)

    local_paths = ' '.join(file_paths)
    aspera_cmd = f'ascp -d -Q -l50M -L- {local_paths} {webin_user}:{remote_path}'
    lg.log(aspera_cmd, level=Loglvl.DEBUG, type=Logtype.FILE)

    try:
        thread = pexpect.spawn(aspera_cmd, timeout=None)
        thread.expect(["assword:", pexpect.EOF])
        thread.sendline(pass_word)

        cpl = thread.compile_pattern_list([pexpect.EOF, '(.+)'])

        while True:
            i = thread.expect_list(cpl, timeout=None)
            if i == 0:  # signals end of transfer
                message = (
                    '[Submission: '
                    + submission_id
                    + '] '
                    + "Updated remote path "
                    + remote_path
                )
                lg.log(message, level=Loglvl.INFO, type=Logtype.FILE)
                break
            elif i == 1:
                pexp_match = thread.match.group(1)
                tokens_to_match = [
                    "LOG ======= end File Transfer statistics =======",
                    "status=success",
                ]

                if any(tm in pexp_match.decode("utf-8") for tm in tokens_to_match):
                    tokens = pexp_match.decode("utf-8")

                    '''  deprecated
                    # update transfer queue timestamp - useful for rescheduling of stalled transfers
                    if transfer_queue_id:
                        try:
                            transfer_record = transfer_collection_handle.find_one(
                                {'_id': ObjectId(transfer_queue_id)})
                        except Exception as e:
                            pass
                        else:
                            transfer_record['date_modified'] = d_utils.get_datetime()
                            transfer_collection_handle.update(
                                {"_id": ObjectId(str(transfer_record.pop('_id')))},
                                {'$set': transfer_record})
                    
                    if 'status=success' in tokens and submission_id:  # per file success tracking
                        # update submission record
                        submission_record = submission_collection_handle.find_one(
                            {'_id': ObjectId(submission_id)}, {"bundle_meta": 1, "profile_id": 1})

                        profile_id = submission_record.get("profile_id", str())

                        target_files = [tk.split("=")[1].strip('"') for tk in tokens.split(" ") if
                                        tk[:5] == "file=" or tk[:7] == "source="]
                        target_files_meta = [x for x in submission_record['bundle_meta'] if
                                             x.get('file_path', str()) in target_files]

                        for file_meta in target_files_meta:
                            file_meta["upload_status"] = True

                        # push updates to client via to channels layer
                        if report_status:
                            total_files_in_bundle = len(submission_record['bundle_meta'])
                            total_transferred = [x for x in submission_record['bundle_meta'] if
                                                 x.get('upload_status', False) is True]

                            status_message = f"{len(total_transferred)}/{total_files_in_bundle}" \
                                             f" data files transferred..."
                            notify_transfer_status(profile_id=profile_id, submission_id=submission_id,
                                                   status_message=status_message)

                        submission_record = dict(bundle_meta=submission_record['bundle_meta'])
                        submission_collection_handle.update(
                            {"_id": ObjectId(submission_id)},
                            {'$set': submission_record})
                    '''

                    if 'LOG ======= end File Transfer statistics =======' in tokens:
                        message = (
                            '[Submission: '
                            + submission_id
                            + '] '
                            + "Transfer to remote location "
                            + remote_path
                            + " completed."
                        )
                        lg.log(message, level=Loglvl.INFO, type=Logtype.FILE)

        thread.close()
        return True
    except Exception as e:
        message = (
            '[Submission: ' + submission_id + '] ' + 'File transfer error ' + str(e)
        )
        lg.log(message, level=Loglvl.ERROR, type=Logtype.FILE)
        raise e


def delete_submisison_bundle(submission_id):
    # submission = get_submission_handle().find_one({"_id": ObjectId(submission_id)})
    get_submission_handle().update_one(
        {"_id": ObjectId(submission_id)}, {"$set": {"bundle": []}}
    )
