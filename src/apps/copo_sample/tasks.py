from .utils.copo_sample import process_pending_submission, fetch_ena_updates
from src.celery import app
from common.utils.logger import Logger
from src.apps.copo_core.tasks import CopoBaseClassForTask


@app.task(bind=True, base=CopoBaseClassForTask)
def process_pending_sample_submission(self):
    Logger().debug("Running process_pending_submission")
    process_pending_submission()
    return True

# Handle 429 error response 'Too Many Requests' from API
# by retrying the task with exponential backoff
@app.task(
    bind=True,
    base=CopoBaseClassForTask,
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=None,
)
def sync_ena_updates(self):
    # Sync updates from ENA for ASG and DToL source and sample records in the system
    Logger().debug('Running sync_ena_updates')
    fetch_ena_updates(sample_types=['dtol', 'asg'])
    return True
