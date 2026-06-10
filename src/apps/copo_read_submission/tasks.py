from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask
import common.ena_utils.FileTransferUtils as tx
from .utils.ena_read_submission import EnaReads


@app.task(bind=True, base=CopoBaseClassForTask)
def process_ena_submission(self):
    Logger().debug("Running process_ena_submission")
    EnaReads().process_queue()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def process_pending_file_transfers(self):
    Logger().debug("Running process_pending_file_transfers")
    tx.process_pending_file_transfers()
    return True


@app.task(bind=True, base=CopoBaseClassForTask)
def check_for_stuck_transfers(self):
    Logger().debug("Running check_for_stuck_transfers")
    tx.check_for_stuck_transfers()
    return True
 
