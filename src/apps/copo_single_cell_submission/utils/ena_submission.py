from common.dal.submission_da import Submission
from .da import Singlecell, SinglecellSchemas
from common.utils.logger import Logger
from common.utils.helpers import  get_not_deleted_flag, get_env
from common.ena_utils.ena_helper import EnaSubmissionHelper
import tempfile
from .da import SinglecellSchemas
import pandas as pd
import requests
import xml.etree.ElementTree as ET

l = Logger()

ena_service = get_env('ENA_SERVICE')
pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]
webin_user = get_env('WEBIN_USER')
webin_domain = get_env('WEBIN_USER').split("@")[1]
ena_v2_service_async = get_env("ENA_V2_SERVICE_ASYNC")


def process_pending_submission_ena():
    repository = "ena"
    submissions = Submission().get_pending_submission(repository=repository, component="study")
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
            schema_name = singlecell["schema_name"]
            schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=singlecell["checklist_id"])
            term_mapping = SinglecellSchemas().get_term_mapping(schema_name=schema_name)
            
            submission_repository_df = SinglecellSchemas().get_submission_repositiory(schema_name=schema_name)
            submisison_repository_component_map = submission_repository_df.to_dict('index')
            submission_components = {}
            for component, respositories in submisison_repository_component_map.items():
                    if repository in respositories and respositories[repository]:
                        submission_components[respositories[repository]] = component

            study_component_schema = schemas.get("study", {})
            rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in study_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}    
            study_component_df = pd.DataFrame.from_records(studies)
            study_component_df.rename(columns=rename_columns, inplace=True)
            study = study_component_df.to_dict('records')[0]

            #submit study to ena 
            context = ena_submission_helper.register_project(submission_xml_path=submission_xml_path, modify_submission_xml_path=modify_submission_xml_path, study=study, singlecell_id=singlecell["_id"])   
            Submission().remove_component_from_submission(sub_id=str(ena_submission_helper.submission_id), component="study", component_ids=[study["study_id"]])

            #submit run to ena
            if context['status']:
                if "read" in submission_components:
                    read = singlecell_components.get(submission_components["read"],[])
                    if read:
                        singlecell = Singlecell().get_collection_handle().find_one(
                            {"profile_id":sub["profile_id"], "study_id":study_id,"deleted": get_not_deleted_flag()},
                            {"schema_name":1,"checklist_id":1, "study_id":1, "components":1})
                        file_component_df, identifier_map = _prepare_file_submission(singlecell=singlecell, file_component_name=submission_components["read"])
                        context = ena_submission_helper.register_files(submission_xml_path=submission_xml_path, modify_submission_xml_path=modify_submission_xml_path, component_df=file_component_df, identifier_map=identifier_map, singlecell=singlecell, file_component_name=submission_components["read"])   

                        #submit assembly to ena
                        if context['status']:
                            singlecell = None
                            if "assembly" in submission_components:
                                assembly = singlecell_components.get(submission_components["assembly"],[])
                                if assembly:
                                    if singlecell == None:
                                        singlecell = Singlecell().get_collection_handle().find_one(
                                            {"profile_id":sub["profile_id"], "study_id":study_id,"deleted": get_not_deleted_flag()},
                                            {"schema_name":1,"checklist_id":1, "study_id":1, "components":1})
                                    assembly_component_data_df, assembly_run_ref_data_df,assembly_file_data_df, parent_map = _prepare_analysis_submission(singlecell=singlecell, component_name=submission_components["assembly"], is_assembly=True)
                                    context = ena_submission_helper.register_assembly(identifier=identifier_map[submission_components["assembly"]],  assembly_component_data_df=assembly_component_data_df, assembly_run_ref_data_df=assembly_run_ref_data_df, assembly_file_data_df=assembly_file_data_df, parent_map=parent_map, singlecell_id=singlecell["_id"])   

                            if "sequencing_annotation" in submission_components:    
                                annotation = singlecell_components.get(submission_components["sequencing_annotation"],[])
                                if annotation:
                                    if singlecell == None:
                                        singlecell = Singlecell().get_collection_handle().find_one(
                                            {"profile_id":sub["profile_id"], "study_id":study_id,"deleted": get_not_deleted_flag()},
                                            {"schema_name":1,"checklist_id":1, "study_id":1, "components":1})
                                    sequencing_annotation_component_data_df, sequencing_annotation_run_ref_data_df,sequencing_annotation_file_data_df, parent_map = _prepare_analysis_submission(singlecell=singlecell, component_name=submission_components["sequencing_annotation"])
                                    context = ena_submission_helper.register_sequencing_annotation(analysis_run_ref_data_df=sequencing_annotation_run_ref_data_df, 
                                                                                                   analysis_file_data_df = sequencing_annotation_file_data_df,
                                                                                                   analysis_component_data_df=sequencing_annotation_component_data_df, 
                                                                                                   identifier=identifier_map[submission_components["sequencing_annotation"]],
                                                                                                   parent_map=parent_map,
                                                                                                   singlecell_id=str(singlecell["_id"]),
                                                                                                   analysis_component_name=submission_components["sequencing_annotation"]
                                                                                                   )    

def _prepare_analysis_submission(singlecell, component_name="sequencing_annotation", is_assembly=False):
    #get info from the components: sequencing_annotation, sequencing_annotation_run_ref[run accession / experiment accession], sequencing_annotation_file
    checklist_id = singlecell.get("checklist_id", "")
    schema_name = singlecell.get("schema_name", "")
    components = singlecell.get("components", {})

    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=checklist_id)
    term_mapping = SinglecellSchemas().get_term_mapping(schema_name=schema_name)

    identifier_map, foreign_key_map = SinglecellSchemas().get_key_map(schemas=schemas)
    parent_map = SinglecellSchemas().get_parent_map(foreign_key_map)
    child_map = SinglecellSchemas().get_child_map(foreign_key_map)
    analysis_component_data = components.get(component_name, [])
    if not analysis_component_data:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), dict()
    analysis_component_data_df = pd.DataFrame.from_records(analysis_component_data)

    #drop rows where accession_ena is not empty (only for assembly)
    if is_assembly:
        analysis_component_data_df = analysis_component_data_df.drop(analysis_component_data_df[analysis_component_data_df["accession_ena"]!=""].index)

    analysis_component_schema = schemas[component_name]
    analysis_component_schema_df = pd.DataFrame.from_records(analysis_component_schema)
    analysis_component_schema_df.fillna(value="", inplace=True)

    schema_columns = analysis_component_data_df.columns[analysis_component_data_df.columns.isin(analysis_component_schema_df["term_name"].tolist() + ["accession_ena"])]
    rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in analysis_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}
    analysis_component_data_df.fillna(value="", inplace=True)
    analysis_component_data_df = analysis_component_data_df[schema_columns]
    analysis_component_data_df.rename(columns=rename_columns, inplace=True)
    child_component_data_df_map = dict()

    #get study accession_ena and biosampleAccession
    study_component_data = components.get("study", [])
    study_accession = study_component_data[0].get("accession_ena", "")
    if not study_accession:
        Logger().error(f"Missing study accession for singlecell submission: {singlecell.get('study_id', 'Unknown')}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), dict()
    
    sample_component_data = components.get("sample", [])
    sample_component_data_df = pd.DataFrame.from_records(sample_component_data)
    sample_component_data_df.fillna(value="", inplace=True)
    sample_component_data_df = sample_component_data_df[[identifier_map["sample"], "biosampleAccession"]]
    analysis_component_data_df = analysis_component_data_df.merge(sample_component_data_df, left_on=parent_map[component_name]["sample"], right_on=identifier_map["sample"], how="left") 
    analysis_component_data_df["study_accession"] = study_accession


    for component, child_foreign_key in child_map[component_name].items():
        child_component_data = components.get(component, [])
        if not child_component_data:
            continue
        child_component_schema = schemas.get(component, {})
        if not child_component_schema:
            Logger().error(f"Missing schema for component {component} in singlecell submission: {singlecell.get('study_id', 'Unknown')}")
            continue
        child_component_schema_df = pd.DataFrame.from_records(child_component_schema)
        child_component_schema_df.fillna(value="", inplace=True)
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        schema_columns = child_component_data_df.columns[child_component_data_df.columns.isin(child_component_schema_df["term_name"].tolist())]
        rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in analysis_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}
        child_component_data_df.fillna(value="", inplace=True)
        child_component_data_df = child_component_data_df[schema_columns]
        child_component_data_df.rename(columns=rename_columns, inplace=True)
        if component not in [f"{component_name}_run_ref", f"{component_name}_file"]:
            analysis_component_data_df = analysis_component_data_df.merge(
                child_component_data_df,
                left_on=["study_id", identifier_map[component_name]],
                right_on=["study_id", child_foreign_key], how="left")
        else:
            child_component_data_df_map[component] = child_component_data_df

    analysis_run_ref_data_df = child_component_data_df_map.get(f"{component_name}_run_ref", pd.DataFrame())
    analysis_file_data_df = child_component_data_df_map.get(f"{component_name}_file", pd.DataFrame())

    """
    if is_assembly:
        assembly_genome_data_df = child_component_data_df_map.get("assembly_genome", pd.DataFrame())
        if not assembly_genome_data_df.empty:
            assembly_genome_data_df["submission_type"] = "genome"
            analysis_component_data_df = analysis_component_data_df.merge (            
                    assembly_genome_data_df,
                    right_on=["study_id", child_map[component_name]["assembly_genome"]],
                    left_on=["study_id", identifier_map[component_name]], how="left")
    """

    for parent_component_name, referenced_key in parent_map[f"{component_name}_run_ref"].items():
        if parent_component_name in ["study", component_name]:
            continue
        parent_component_data = components.get(parent_component_name, [])
        parent_component_data_df = pd.DataFrame.from_records(parent_component_data)

        analysis_run_ref_data_df = analysis_run_ref_data_df.merge(
            parent_component_data_df,
            left_on=["study_id", referenced_key],
            right_on=["study_id", identifier_map[parent_component_name]], how="left")
 
    return analysis_component_data_df, analysis_run_ref_data_df, analysis_file_data_df, parent_map



def _prepare_assembly_submission(singlecell, component_name="assembly"):
    #get info from the components: assembly, assembly_run_ref[run accession / experiment accession], assembly_genome, assembly_file
    checklist_id = singlecell.get("checklist_id", "")
    schema_name = singlecell.get("schema_name", "")
    components = singlecell.get("components", {})

    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=checklist_id)
    term_mapping = SinglecellSchemas().get_term_mapping(schema_name=schema_name)

    identifier_map, foreign_key_map = SinglecellSchemas().get_key_map(schemas=schemas)
    parent_map = SinglecellSchemas().get_parent_map(foreign_key_map)
    child_map = SinglecellSchemas().get_child_map(foreign_key_map)
    assembly_component_data = components.get(component_name, [])
    if not assembly_component_data:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), dict()
    assembly_component_data_df = pd.DataFrame.from_records(assembly_component_data)
    #drop rows where accession_ena is not empty
    assembly_component_data_df = assembly_component_data_df.drop(assembly_component_data_df[assembly_component_data_df["accession_ena"]!=""].index)
    assembly_component_schema = schemas[component_name]
    assembly_component_schema_df = pd.DataFrame.from_records(assembly_component_schema)
    assembly_component_schema_df.fillna(value="", inplace=True)

    schema_columns = assembly_component_data_df.columns[assembly_component_data_df.columns.isin(assembly_component_schema_df["term_name"].tolist() + ["accession_ena"])]
    rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in assembly_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}
    assembly_component_data_df.fillna(value="", inplace=True)
    assembly_component_data_df = assembly_component_data_df[schema_columns]
    assembly_component_data_df.rename(columns=rename_columns, inplace=True)
    child_component_data_df_map = dict()

    #get study accession_ena and biosampleAccession
    study_component_data = components.get("study", [])
    study_accession = study_component_data[0].get("accession_ena", "")
    if not study_accession:
        Logger().error(f"Missing study accession for singlecell submission: {singlecell.get('study_id', 'Unknown')}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), dict()
    
    sample_component_data = components.get("sample", [])
    sample_component_data_df = pd.DataFrame.from_records(sample_component_data)
    sample_component_data_df.fillna(value="", inplace=True)
    sample_component_data_df = sample_component_data_df[[identifier_map["sample"], "biosampleAccession"]]
    assembly_component_data_df = assembly_component_data_df.merge(sample_component_data_df, left_on=parent_map["assembly"]["sample"], right_on=identifier_map["sample"], how="left") 
    assembly_component_data_df["study_accession"] = study_accession


    for component, foreign_key in child_map[component_name].items():
        child_component_data = components.get(component, [])
        if not child_component_data:
            continue
        child_component_schema = schemas.get(component, {})
        if not child_component_schema:
            Logger().error(f"Missing schema for component {component} in singlecell submission: {singlecell.get('study_id', 'Unknown')}")
            continue
        child_component_schema_df = pd.DataFrame.from_records(child_component_schema)
        child_component_schema_df.fillna(value="", inplace=True)
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        schema_columns = child_component_data_df.columns[child_component_data_df.columns.isin(child_component_schema_df["term_name"].tolist())]
        rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in assembly_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}
        child_component_data_df.fillna(value="", inplace=True)
        child_component_data_df = child_component_data_df[schema_columns]
        child_component_data_df.rename(columns=rename_columns, inplace=True)
        child_component_data_df_map[component] = child_component_data_df

    assembly_run_ref_data_df = child_component_data_df_map.get("assembly_run_ref", pd.DataFrame())
    assembly_genome_data_df = child_component_data_df_map.get("assembly_genome", pd.DataFrame())
    assembly_file_data_df = child_component_data_df_map.get("assembly_file", pd.DataFrame())

    if not assembly_genome_data_df.empty:
        assembly_genome_data_df["submission_type"] = "genome"
        assembly_component_data_df = assembly_component_data_df.merge (            
                assembly_genome_data_df,
                right_on=["study_id", child_map["assembly"]["assembly_genome"]],
                left_on=["study_id", identifier_map["assembly"]], how="left")
    

    for parent_component_name, referenced_key in parent_map["assembly_run_ref"].items():
        if parent_component_name in ["study", "assembly"]:
            continue
        parent_component_data = components.get(parent_component_name, [])
        parent_component_data_df = pd.DataFrame.from_records(parent_component_data)

        assembly_run_ref_data_df = assembly_run_ref_data_df.merge(
            parent_component_data_df,
            left_on=["study_id", referenced_key],
            right_on=["study_id", identifier_map[parent_component_name]], how="left")

    """
    assembly_run_ref_data_df = assembly_run_ref_data_df.merge(
            file_component_data_df,
            left_on=["study_id", parent_map["assembly_run_ref"]["file"]],
            right_on=["study_id", identifier_map["file"]] )
    """
    return assembly_component_data_df, assembly_run_ref_data_df, assembly_file_data_df, parent_map


def merge_parent_component(singlecell,schemas, component_name, component_df):
    checklist_id = singlecell.get("checklist_id", "")
    schema_name = singlecell.get("schema_name", "")

    #schemas = SinglecellSchemas().get_schema(schema_name=schema_name, target_id=checklist_id)
    term_mapping = SinglecellSchemas().get_term_mapping(schema_name=schema_name)

    identifier_map, foreign_key_map = SinglecellSchemas().get_key_map(schemas=schemas)
    parent_map = SinglecellSchemas().get_parent_map(foreign_key_map)

    component_df = _merge_paranent_data(component_df=component_df, identifier_map=identifier_map, component_name=component_name, parent_map=parent_map, singlecell=singlecell, schemas=schemas, term_mapping=term_mapping)
    return component_df


def _prepare_file_submission(singlecell, file_component_name="file"):
    checklist_id = singlecell.get("checklist_id", "")
    schema_name = singlecell.get("schema_name", "")
    components = singlecell.get("components", {})

    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=checklist_id)
    term_mapping = SinglecellSchemas().get_term_mapping(schema_name=schema_name)

    identifier_map, foreign_key_map = SinglecellSchemas().get_key_map(schemas=schemas)
    parent_map = SinglecellSchemas().get_parent_map(foreign_key_map)
    file_component_data = components.get(file_component_name, [])
    if not file_component_data:
        return
    file_component_df = pd.DataFrame.from_records(file_component_data)
    file_component_df.fillna(value="", inplace=True)
    file_component_schema = schemas.get(file_component_name, {})
    if not file_component_schema:
        Logger().error(f"Missing schema for {file_component_name} component in singlecell submission: {singlecell.get('study_id', 'Unknown')}")
        return
    file_component_schema_df = pd.DataFrame.from_records(file_component_schema)
    file_component_schema_df.fillna(value="", inplace=True)
    schema_columns = file_component_df.columns[file_component_df.columns.isin(file_component_schema_df["term_name"].tolist() + ["accession_ena"])]
    rename_columns = {field["term_name"]: term_mapping[field["copo_name"]].get("ENA",field["term_name"]) for field in file_component_schema if field["copo_name"] and field["copo_name"] in term_mapping}

    file_component_df = file_component_df[schema_columns]

    component_df = _merge_paranent_data(component_df=file_component_df, identifier_map=identifier_map, component_name=file_component_name, parent_map=parent_map, singlecell=singlecell, schemas=schemas, term_mapping=term_mapping)
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
        return {"status": "error", "message": "No submission found for the given profile."}
    

def poll_asyn_analysis_submission_receipt():
    submissions = Submission().get_async_analysis_submission()

    with requests.Session() as session:
        session.auth = (user_token, pass_word)    
        for submission in submissions:
            ena_submission_helper = EnaSubmissionHelper(profile_id=submission["profile_id"], submission_id=str(submission["_id"]))
            for analysis_sub in submission["analysis_submission"]:
                accessions = ""
                response = session.get(analysis_sub["href"])
                if response.status_code == requests.codes.accepted:
                    continue
                elif response.status_code == requests.codes.ok:
                    l.log("ENA RECEIPT " + response.text)
                    try:
                        tree = ET.fromstring(response.text)
                        accessions = _handle_submit_receipt(submission=submission, tree=tree, analysis_sub=analysis_sub)
                    except ET.ParseError as e:
                        l.exception(e)
                        message = " Unrecognised response from ENA - " + str(
                            response.content) + " Please try again later, if it persists contact admins"
                        ena_submission_helper.logging_error(message)
                        continue
                    except Exception as e:
                        l.exception(e)
                        message = 'API call error ' + "Submitting project xml to ENA via CURL. href is: " + analysis_sub["href"]
                        ena_submission_helper.logging_error(message)
                        continue

                    if not accessions:
                        ena_submission_helper.logging_error(f"No accessions found in the receipt for profile: {submission['profile_id']}")
                        continue
                    elif accessions["status"] == "success":
                        msg = "Annotation Submitted"
                        ena_submission_helper.logging_info(msg)
                    
                    else:
                        msg = "Analysis Submission Rejected: <p>" + accessions["msg"] + "</p>"
                        ena_submission_helper.logging_error(msg)
                    
                    Submission().get_collection_handle().update_one({"_id": submission["_id"]},
                                  {"$pull": {"analysis_submission": {"id": analysis_sub["id"]}}})


def _handle_submit_receipt(submission, tree, analysis_sub):
    ena_submission_helper = EnaSubmissionHelper(profile_id=submission["profile_id"], submission_id=str(submission["_id"]))

    success_status = tree.get('success')
    analysis_ids = []
    if success_status == 'false':
        msg = ""
        for child in tree.iter():
            if child.tag == 'ANALYSIS':
                analysis_ids.append(child.get('alias'))
            
        error_blocks = tree.find('MESSAGES').findall('ERROR')
        for error in error_blocks:
            msg += error.text + "<br>"
        if not msg:
            msg = "Undefined error"
        status = {"status": "error", "msg": msg}
        # print(status)
        ena_submission_helper.logging_error(msg)
        singlecells = Singlecell().get_all_records_columns(
            filter_by={"profile_id":submission["profile_id"], "study_id":analysis_sub["study_id"], "deleted": get_not_deleted_flag()},
            projection={ "_id":1, "schema_name":1,"checklist_id":1, "study_id":1})
        
        if not singlecells:
            l.error(f"Cannot find singlecell for study: {analysis_sub['study_id']}")
            return status
        
        schemas = SinglecellSchemas().get_schema(schema_name=singlecells[0]["schema_name"], schemas=dict(), target_id=singlecells[0]["checklist_id"])   
        identifier_map = SinglecellSchemas().get_identifier_map(schemas=schemas)

        for id in analysis_ids:
            analysis_component_id = id.split(":")[1]
            Singlecell().update_component_status(id=str(singlecells[0]["_id"]), component=analysis_sub["component"], identifier=identifier_map[analysis_sub["component"]], identifier_value=analysis_component_id, repository="ena", status_column_value={"status": "rejected",  "error": msg})  

        l.error(msg)
        return status
    else:
        # retrieve id and update record
        # return get_biosampleId(receipt, sample_id, collection_id)
        return _get_accession(tree, submission,analysis_sub )
    
def _get_accession(tree, submission, analysis_sub):
    '''parsing ENA sample bundle accessions from receipt and
    storing in sample and submission collection object'''
    #tree = ET.fromstring(receipt)
   
    status = {"status": "error", "msg": "No accessions found"}
    singlecells = Singlecell().get_all_records_columns(
        filter_by={"profile_id":submission["profile_id"], "study_id":analysis_sub["study_id"], "deleted": get_not_deleted_flag()},
        projection={ "_id":1, "schema_name":1,"checklist_id":1, "study_id":1, "components":1})
    
    if not singlecells:
        l.error(f"Cannot find singlecell for study: {analysis_sub['study_id']}")
        return status
    schemas = SinglecellSchemas().get_schema(schema_name=singlecells[0]["schema_name"], schemas=dict(), target_id=singlecells[0]["checklist_id"])   
    identifier_map = SinglecellSchemas().get_identifier_map(schemas=schemas)

    for child in tree.iter():
        if child.tag == 'ANALYSIS':
            analysis_id = child.get('alias')
            accession = child.get('accession')
            analysis_component_id = analysis_id.split(":")[1]

            status_column_value={"status": "accepted",  "error": "", "accession": accession}
            Singlecell().update_component_status(id=str(singlecells[0]["_id"]), 
                                                 component=analysis_sub["component"], 
                                                 identifier=identifier_map[analysis_sub["component"]], 
                                                 identifier_value=analysis_component_id, 
                                                 repository="ena", 
                                                 status_column_value=status_column_value, 
                                                 with_child_components=True)  

            analysis_accession = dict(
                accession = accession,
                alias = analysis_id
            )
            Submission().get_collection_handle().update_one({"_id": submission["_id"]},
                                                            {"$addToSet": {f"accessions.{analysis_sub['component']}.accession": analysis_accession, }})

    status = {"status": "success" }
    return status
