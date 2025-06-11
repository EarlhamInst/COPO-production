from common.dal.submission_da import Submission
from .da import Singlecell, SinglecellSchemas
from common.utils.logger import Logger
from common.utils.helpers import  get_not_deleted_flag
from common.ena_utils.ena_helper import EnaSubmissionHelper
import tempfile
from .da import SinglecellSchemas
import pandas as pd


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
            context = ena_submission_helper.register_project(submission_xml_path=submission_xml_path, modify_submission_xml_path=modify_submission_xml_path, singlecell=singlecell)   
            Submission().remove_component_from_submission(sub_id=str(ena_submission_helper.submission_id), component="study", component_ids=[singlecell["study_id"]])

            if context['status']:
                file_component_df, identifier_map = _prepare_file_submission(singlecell=singlecell)
                context = ena_submission_helper.register_files(submission_xml_path=submission_xml_path, modify_submission_xml_path=modify_submission_xml_path, component_df=file_component_df, identifier_map=identifier_map, singlecell=singlecell)   
                #Submission().remove_component_from_submission(sub_id=str(ena_submission_helper.submission_id), component="study", component_ids=[singlecell["study_id"]])


def _prepare_file_submission(singlecell):
    checklist_id = singlecell.get("checklist_id", "")
    schema_name = singlecell.get("schema_name", "")
    components = singlecell.get("components", {})

    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, target_id=checklist_id)
    term_mapping = SinglecellSchemas().get_term_mapping(schema_name=schema_name)

    identifier_map, foreign_key_map = SinglecellSchemas().get_key_map(schemas=schemas)
    parent_map = SinglecellSchemas().get_parent_map(foreign_key_map)
    file_component_data = components.get("file", [])
    if not file_component_data:
        return
    file_component_df = pd.DataFrame.from_records(file_component_data)
    file_component_df.fillna(value="", inplace=True)
    file_component_schema = schemas.get("file", {})
    if not file_component_schema:
        Logger().error(f"Missing schema for file component in singlecell submission: {singlecell.get('study_id', 'Unknown')}")
        return
    file_component_schema_df = pd.DataFrame.from_records(file_component_schema)
    file_component_schema_df.fillna(value="", inplace=True)
    schema_columns = file_component_df.columns[file_component_df.columns.isin(file_component_schema_df["term_name"].tolist() + ["accession_ena"])]
    rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in file_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}

    file_component_df = file_component_df[schema_columns]

    component_df = _merge_paranent_data(component_df=file_component_df, identifier_map=identifier_map, component_name="file", parent_map=parent_map, singlecell=singlecell, schemas=schemas, term_mapping=term_mapping)
    component_df.rename(columns=rename_columns, inplace=True)

    #component_df.drop(columns=[identifier_map["file"]], inplace=True, errors='ignore')
    return component_df, identifier_map



    
def _merge_paranent_data(component_df, identifier_map, component_name, parent_map, singlecell, schemas=None, term_mapping=None):
    for referenced_component, foreign_key in parent_map[component_name].items():
        if referenced_component in ["study", "sample"]:
            continue
        referenced_component_data = singlecell["components"].get(referenced_component, [])
        if not referenced_component_data:
            continue 
        referenced_component_df = pd.DataFrame.from_records(referenced_component_data)
        referenced_component_df.fillna(value="", inplace=True)
        referenced_component_df.drop(columns=["study_id"], inplace=True, errors='ignore')
        
        referenced_component_schema = schemas.get(referenced_component, {})
        if not referenced_component_schema:
            continue
        referenced_component_schema_df = pd.DataFrame.from_records(referenced_component_schema)
        referenced_component_schema_df.fillna(value="", inplace=True)

        schema_columns = referenced_component_df.columns[referenced_component_df.columns.isin(referenced_component_schema_df["term_name"].tolist())]
        rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in referenced_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}

        referenced_component_df = referenced_component_df[schema_columns]
        referenced_component_df.rename(columns=rename_columns, inplace=True)

        component_df = component_df.merge(
            referenced_component_df,
            left_on=foreign_key,
            right_on=identifier_map[referenced_component])
        component_df.rename(columns=rename_columns, inplace=True)
        component_df = _merge_paranent_data(component_df, identifier_map, referenced_component, parent_map, singlecell, schemas, term_mapping=term_mapping)
        component_df.drop(columns=[identifier_map[referenced_component]], inplace=True, errors='ignore')
    return component_df


def release_study(profile_id, singlecell):
    submissions = Submission().get_all_records_columns(filter_by={"profile_id": profile_id, "repository": "ena", "deleted": get_not_deleted_flag()}, projection={"_id": 1, "accessions": 1})
    if submissions:
        ena_submission_helper = EnaSubmissionHelper(profile_id=profile_id, submission_id=str(submissions[0]["_id"]))
        return ena_submission_helper.release_study(singlecell=singlecell)
    else:
        Logger().error(f"No submission found for profile_id: {profile_id} in ENA repository.")
        return {"status": False, "message": "No submission found for the given profile."}