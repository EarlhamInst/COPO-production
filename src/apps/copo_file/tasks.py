from src.celery import app
from common.utils.logger import Logger
from common.s3.checksum import compute_s3_object_md5
from src.apps.copo_core.tasks import CopoBaseClassForTask


@app.task(bind=True, base=CopoBaseClassForTask)
def compute_s3_object_md5_task(self, bucket, key, etag):
    Logger().debug(f'Running compute_s3_object_md5 for {bucket}/{key}')
    return compute_s3_object_md5(bucket, key, etag)
