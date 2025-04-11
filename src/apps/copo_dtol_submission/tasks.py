from .utils import Dtol_Submission as dtol
from .utils import Dtol_Bioimage_Submission as dtol_bioimage
from .utils.copo_email import Email

from common.dal.sample_da import Sample
from src.celery import app
from src.apps.copo_core.tasks import only_one, CopoBaseClassForTask
from common.utils.logger import Logger
                 
@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_dtol_sample_submission", timeout=5)
def process_dtol_sample_submission(self):
    Logger().debug("Running process_dtol_sample_submission")
    dtol.process_pending_dtol_samples()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_stale_dtol_sample_submission", timeout=5)
def process_stale_dtol_sample_submission(self):
    Logger().debug("Running process_stale_dtol_sample_submission")
    dtol.process_stale_dtol_samples_submission()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_bioimage_submission", timeout=5)
def process_bioimage_submission(self):
    Logger().debug("Running process_bioimage_submission")
    dtol_bioimage.process_bioimage_pending_submission()
    return True

"""
@app.task(bind=True,   base=CopoBaseClassForTask)
def find_incorrectly_rejected_samples(self):
    Logger().debug("Running find_incorrectly_rejected_samples")
    Sample().find_incorrectly_rejected_samples()
    return True
"""

@app.task(bind=True,   base=CopoBaseClassForTask)
def poll_missing_tolids(self):
    Logger().debug("Running poll_missing_tolids")
    dtol.query_awaiting_tolids()
    return True


@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="process_poll_asyn_ena_submission", timeout=5)
def poll_asyn_ena_submission(self):
    Logger().debug("Running poll_asyn_ena_submission")
    dtol.poll_asyn_ena_submission()
    return True
  
"""
@app.task(bind=True, base=CopoBaseClassForTask)
def process_bioimage_housekeeping(self):
    Logger().debug("Running process_bioimage_housekeeping")
    dtol_bioimage.housekeeping_bioimage_archive()
    return True
"""

@app.task(bind=True, base=CopoBaseClassForTask)
def send_fortnightly_pending_manifest_notification(self):
    Logger().debug("Running send_fortnightly_pending_manifest_notification")
    Email().remind_manifest_pending_for_associated_project_type_checker()
    return True
