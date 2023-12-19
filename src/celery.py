from __future__ import absolute_import, unicode_literals

import os
from datetime import timedelta
from celery import Celery
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'src.main_config.settings.all')
# crontab(minute="*/1")
app = Celery('src')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# define periodic tasks here

app.conf.beat_schedule = {
    'update_stats': {
        'task': 'src.apps.copo_core.tasks.update_stats',
        'schedule': timedelta(hours=24)
    },
    'poll_expired_viewlocks': {
        'task': 'src.apps.copo_core.tasks.poll_expired_viewlocks',
        'schedule': timedelta(seconds=60)
    },
    'process_housekeeping': {
        'task': 'src.apps.copo_core.tasks.process_housekeeping',
        'schedule': timedelta(days=1)
    },
    'update_ena_checklist': {
        'task': 'src.apps.copo_core.tasks.update_ena_checklist',
        'schedule': timedelta(seconds=10)
    },     
    'update_ena_read_checklist': {
        'task': 'src.apps.copo_core.tasks.update_ena_read_checklist',
        'schedule': timedelta(seconds=10)
    },
    'process_tol_validations': {
        'task': 'src.apps.copo_dtol_upload.tasks.process_tol_validations',
        'schedule': timedelta(seconds=3)
    },

    'process_dtol_sample_submission': {
        'task': 'src.apps.copo_dtol_submission.tasks.process_dtol_sample_submission',
        'schedule': timedelta(seconds=10)
    },
    'process_bioimage_submission': {
        'task': 'src.apps.copo_dtol_submission.tasks.process_bioimage_submission',
        'schedule': timedelta(seconds=30)
    },    
    'process_bioimage_housekeeping': {
        'task': 'src.apps.copo_dtol_submission.tasks.process_bioimage_housekeeping',
        'schedule': timedelta(days=1)
    },    
    'find_incorrectly_rejected_samples': {
        'task': 'src.apps.copo_dtol_submission.tasks.find_incorrectly_rejected_samples',
        'schedule': timedelta(seconds=60)
    },
    'poll_missing_tolids': {
        'task': 'src.apps.copo_dtol_submission.tasks.poll_missing_tolids',
        'schedule': timedelta(hours=2)  # shortened cause sometimes it doesn't work?
    },
    'poll_asyn_ena_submission': {
        'task': 'src.apps.copo_dtol_submission.tasks.poll_asyn_ena_submission',
        'schedule': timedelta(seconds=10)
    },        
    'process_ena_submission': {
        'task': 'src.apps.copo_read_submission.tasks.process_ena_submission',
        'schedule': timedelta(seconds=20)  # execute every n minutes minute="*/n"
    },
    'process_ena_transfers': {
        'task': 'src.apps.copo_read_submission.tasks.process_pending_file_transfers',
        'schedule': timedelta(seconds=5)
    },
    'check_for_stuck_transfers': {
        'task': 'src.apps.copo_read_submission.tasks.check_for_stuck_transfers',
        'schedule': timedelta(seconds=20)
    },
    'update_assembly_submission_pending': {
        'task': 'src.apps.copo_assembly_submission.tasks.update_assembly_submission_pending',
        'schedule': timedelta(seconds=10)
    },     
    'process_assembly_submission': {
        'task': 'src.apps.copo_assembly_submission.tasks.process_assembly_submission',
        'schedule': timedelta(seconds=10)
    },    

    'process_seq_annotation_submission': {
        'task': 'src.apps.copo_seq_annotation_submission.tasks.process_seq_annotation_submission',
        'schedule': timedelta(seconds=10)
    },
    'poll_asyn_seq_annotation_submission_receipt': {
        'task': 'src.apps.copo_seq_annotation_submission.tasks.poll_asyn_seq_annotation_submission_receipt',
        'schedule': timedelta(seconds=10)
    },
    'update_seq_annotation_submission_pending': {
        'task': 'src.apps.copo_seq_annotation_submission.tasks.update_seq_annotation_submission_pending',
        'schedule': timedelta(seconds=10)
    },
    'processing_pending_tagged_seq_submission': {
        'task': 'src.apps.copo_barcoding_submission.tasks.processing_pending_tagged_seq_submission',
        'schedule': timedelta(seconds=10)
    },  

}

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
