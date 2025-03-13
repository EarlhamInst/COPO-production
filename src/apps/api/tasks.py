import shutil

from common.utils.logger import Logger
from django.conf import settings
from rocrate_validator import services
from src.apps.copo_core.tasks import CopoBaseClassForTask
from src.celery import app

lg = Logger()


# Helpers for generating RO-Crate objects
def validate_rocrate_object(dir_path, manifest_id):
    '''
    Method to validate a Ro-Crate object
    :param dir_path: Path to the temporary RO-Crate directory.
    :param manifest_id: the manifest id of the object
    :return: a boolean indicating if the manifest is valid
    '''
    # Access Ro-Crate settings
    rocrate_settings = settings.ROCRATE_SETTINGS
    # Set the URI of the RO-Crate object tothe file path
    rocrate_settings.rocrate_uri = dir_path
    result = services.validate(rocrate_settings)

    # Check if the validation was successful
    if result.has_issues():
        lg.error(f'Invalid RO-Crate object for sample with manifest_id: {manifest_id}')
        for issue in result.get_issues():
            # Every issue object has a reference to the check that failed, the severity of the issue, and a message describing the issue.
            message = f"Detected issue of severity {issue.severity.name} with check '{issue.check.identifier}': {issue.message}"
            lg.error(message)
    else:
        lg.log(f'Valid RO-Crate object for sample with manifest_id: {manifest_id}')
    return result.has_issues()


@app.task(bind=True, base=CopoBaseClassForTask)
def validate_rocrate_task(self, temp_dir, manifest_id):
    '''Runs RO-Crate validation as a background celery task'''
    try:
        lg.debug('Running validate_rocrate_task')
        validate_rocrate_object(temp_dir, manifest_id)
    except Exception as e:
        lg.error(f'Error in validate_rocrate_task: {str(e)}')
    finally:
        # Clean up temporary directory after validation is done
        shutil.rmtree(temp_dir, ignore_errors=True)
