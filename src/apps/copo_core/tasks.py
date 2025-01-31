import celery
from common.dal.copo_da import Stats
from common.dal.sample_da import Sample
from .models import ViewLock
from src.celery import app
from common.utils.logger import Logger
import celery
import redis
from functools import wraps
from django.conf import settings
from common.ena_utils.EnaChecklistHandler import ChecklistHandler, ReadChecklistHandler

REDIS_CLIENT = redis.Redis(host=settings.SESSION_REDIS_HOST, port=settings.SESSION_REDIS_PORT)


def only_one(fun=None, key="", timeout=None):   
    def actual_only_one(fun): 
        """Enforce only one celery task at a time."""
        @wraps(fun)
        def inner_func(self, *args, **kwargs):
            ret_value = None
            have_lock = False
            lock = REDIS_CLIENT.lock(key, timeout=timeout)
            try:
                have_lock = lock.acquire(blocking=False)
                if have_lock:
                    return fun(self, *args, **kwargs)
                else:
                    Logger().log("quit the task " + fun.__name__ + " as the previous one is still running" ) 
            finally:
                if have_lock:
                    try:
                        lock.release()
                    except Exception as e:
                        Logger().log(f"{fun.__name__} : {e}")
        return inner_func
    return actual_only_one    

class CopoBaseClassForTask(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        Logger().error('{0!r} failed: {1!r}'.format(task_id, exc))
        Logger().error(einfo)
           

@app.task(bind=True,   base=CopoBaseClassForTask)
def update_stats(self):
    Logger().debug("Running update_stats")
    Stats().update_stats()
    return True



@app.task(bind=True,   base=CopoBaseClassForTask)
def poll_expired_viewlocks(self):
    Logger().debug("Running poll_expired_viewlocks")
    ViewLock().remove_expired_locks()
    return True


@app.task(bind=True, base=CopoBaseClassForTask)
def process_housekeeping(self):
    Logger().debug("Running process_housekeeping")
    Logger().housekeeping_logfile()
    #dtol_bioimage.housekeeping_bioimage_archive()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="update_ena_checklist", timeout=5)
def update_ena_checklist(self):
    Logger().debug("Running update_ena_checklist")
    ChecklistHandler().updateCheckList()
    return True

@app.task(bind=True, base=CopoBaseClassForTask)
@only_one(key="update_ena_read_checklist", timeout=5)
def update_ena_read_checklist(self):
    Logger().debug("Running update_ena_read_checklist")
    ReadChecklistHandler().updateCheckList()
    return True