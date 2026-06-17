from .utils.SingleCellSchemasHandler import SingleCellSchemasHandler
from .utils.zenodo_submission import  process_pending_submission_zendo
from .utils.ena_submission import process_pending_submission_ena, poll_asyn_analysis_submission_receipt
from .utils.copo_single_cell import update_submission_pending
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask

@app.task(bind=True, base=CopoBaseClassForTask)
def update_singlecell_schema(self):
    Logger().debug("Running update_singlecell_schema")
    SingleCellSchemasHandler().updateSchemas()
    return True


@app.task(bind=True, base=CopoBaseClassForTask)
def process_zenodo_submission(self):
    Logger().debug("Running process_submission_zenodo")
    process_pending_submission_zendo()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def process_ena_submission(self):
    Logger().debug("Running process_submission_ena")
    process_pending_submission_ena()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def update_zenodo_submission_pending(self):
    Logger().debug("Running update_zenodo_submission_pending")
    update_submission_pending()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def process_asyn_analysis_submission_receipt(self):
    Logger().debug("Running poll_asyn_analysis_submission_receipt")
    poll_asyn_analysis_submission_receipt()
    return True