from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask
from .utils import EnaAnnotation as enaAnnotation


@app.task(bind=True, base=CopoBaseClassForTask)
def poll_asyn_seq_annotation_submission_receipt(self):
    Logger().debug("poll_asyn_annotation_submission_receipt")
    enaAnnotation.poll_asyn_seq_annotation_submission_receipt()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def process_seq_annotation_submission(self):
    Logger().debug("Running process_seq_annotation_submission")
    enaAnnotation.process_seq_annotation_pending_submission()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def update_seq_annotation_submission_pending(self):
    Logger().debug("Running update_seq_annotation_submission_pending")
    enaAnnotation.update_seq_annotation_submission_pending()
    return True
