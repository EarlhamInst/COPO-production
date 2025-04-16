from .utils.SingleCellSchemasHandler import SingleCellSchemasHandler
import utils.zenodo_submission as zenodo_submission
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask, only_one
from src.celery import app

@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="update_singlecell_schema", timeout=5)
def update_singlecell_schema(self):
    Logger().debug("Running update_singlecell_schema")
    SingleCellSchemasHandler().updateSchemas()
    return True

 
@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_assembly_submission", timeout=5)
def process_zenodo_submission(self):
    Logger().debug("Running process_zenodo_submission")
    zenodo_submission.process_pending_submission()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_update_assembly_submission_pending", timeout=5)
def update_zenodo_submission_pending(self):
    Logger().debug("Running update_zenodo_submission_pending")
    zenodo_submission.update_submission_pending()
    return True
