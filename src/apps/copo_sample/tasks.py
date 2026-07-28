from .utils.copo_sample import process_pending_submission, fetch_system_records
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask


@app.task(bind=True, base=CopoBaseClassForTask)
def process_pending_sample_submission(self):
    Logger().debug("Running process_pending_submission")
    process_pending_submission()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
def sync_system_records_with_ena(self):
    # Sync ASG and DToL source and sample changes from ENA
    # to their corresponding records in COPO
    Logger().debug('Running sync_system_records_with_ena')
    fetch_system_records(sample_types=['dtol', 'asg'])
    return True
