from .utils.copo_sample import  process_pending_submission
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask


@app.task(bind=True, base=CopoBaseClassForTask)
def process_pending_sample_submission(self):
    Logger().debug("Running process_pending_submission")
    process_pending_submission()
    return True
