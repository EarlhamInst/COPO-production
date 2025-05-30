from common.dal.submission_da import Submission
from .da import Singlecell, SinglecellSchemas, ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
from common.utils.logger import Logger
from common.dal.copo_da import EnaFileTransfer, DataFile 
from bson import regex
import os
from common.utils.helpers import  notify_singlecell_status, get_not_deleted_flag
from io import BytesIO
from .SingleCellSchemasHandler import SingleCellSchemasHandler
from .zenodo.deposition import Zenodo_deposition
from common.ena_utils.ena_helper import EnaSubmissionHelper
import tempfile

def process_pending_submission_ena():
    submissions = Submission().get_pending_submission(repository="ena", component="study")
    if not submissions:
        return
    
    for sub in submissions:

        ena_submission_helper = EnaSubmissionHelper(profile_id=sub["profile_id"], submission_id=str(sub["_id"]))

        for study_id in sub["study"]:
            ena_submission_helper.logging_info(f"Processing submission for study: {study_id}")
         
            singlecell = Singlecell().get_collection_handle().find_one(
                 {"profile_id":sub["profile_id"], "study_id":study_id,"deleted": get_not_deleted_flag()},
                 {"schema_name":1,"checklist_id":1, "study_id":1, "components":1})
            #generate the manifest for submission
            if not singlecell:
                msg = f"Missing singlecell for study: {study_id}"
                ena_submission_helper.logging_error(msg)
                Submission().remove_study_from_singlecell_submission(sub_id=str(sub["_id"]), study_id=study_id)
                continue

            singlecell_components = singlecell.get("components",{})

            studies = singlecell_components.get("study",[])
            if not studies:
                msg = f"Missing study for singlecell: {study_id}"
                ena_submission_helper.logging_error(msg)
                Submission().remove_study_from_singlecell_submission(sub_id=str(sub["_id"]), study_id=study_id)
                continue
            
            output_location = tempfile.mkdtemp()

            context = ena_submission_helper.get_submission_xml(output_location)

            if context['status'] is False:
                ena_submission_helper.logging_error( context.get("message", str()))
                return context

            submission_xml_path = context['value']

            context = ena_submission_helper.get_edit_submission_xml(output_location, submission_xml_path)
            if context['status'] is False:
                ena_submission_helper.logging_error(context.get("message", str()))
                return context
            
            modify_submission_xml_path = context['value']

             #submit study to ena 
            return ena_submission_helper.register_project(submission_xml_path=submission_xml_path, modify_submission_xml_path=modify_submission_xml_path, singlecell=singlecell)   
