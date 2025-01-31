from .utils.SingleCellSchemasHandler import SingleCellSchemasHandler
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
