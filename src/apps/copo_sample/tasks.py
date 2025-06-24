from .utils.copo_sample import  process_pending_submission 
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask, only_one
from src.celery import app

 
 
@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_pending_sample_submission", timeout=5)
def process_pending_sample_submission(self):
    Logger().debug("Running process_pending_submission")
    process_pending_submission()
    return True
