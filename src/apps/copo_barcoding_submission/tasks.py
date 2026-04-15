from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask
from .utils.EnaTaggedSequence import EnaTaggedSequence

@app.task(bind=True, base=CopoBaseClassForTask)
def processing_pending_tagged_seq_submission(self):
    Logger().debug("Running processing_pending_tagged_seq_submission")
    EnaTaggedSequence().processing_pending_tagged_seq_submission()
    return True
