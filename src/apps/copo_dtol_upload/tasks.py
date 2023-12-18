from .utils.validation_celery_handler import ProcessValidationQueue
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask

@app.task(bind=True,  base=CopoBaseClassForTask)
def process_tol_validations(self):
    Logger().debug("Running process_tol_validations")
    ProcessValidationQueue().process_validation_queue()
    return True

