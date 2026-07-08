from common.utils.logger import Logger
from .da import SinglecellSchemas, Singlecell, ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
import pandas as pd
from common.utils.helpers import get_datetime, get_thumbnail_folder
from common.dal.profile_da import Profile
from common.dal.copo_da import  DataFile, EnaFileTransfer
from common.utils.helpers import get_not_deleted_flag
import requests
from common.dal.submission_da import Submission
from common.dal.mongo_util import cursor_to_list
from django.conf import settings
from bson import regex
import os
import common.ena_utils.FileTransferUtils as tx
from common.utils.helpers import get_env, get_current_user, notify_submission_status
from . import zenodo_submission
from . import ena_submission
from common.s3.s3Connection import S3Connection as s3
from src.apps.copo_file.utils.CopoFiles import delete_image_thumbnail


l = Logger()

# ENA Webin authentication credentials, used for querying file processing status
pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]  # Extract username portion before '@'
session = requests.Session()
session.auth = (user_token, pass_word)

def _query_ena_file_processing_status(accession_no):
    """Query the ENA reporting API for the archive/release status of files
    associated with a given run accession number.

    Returns an HTML-formatted string of "fileName : archiveStatus : releaseStatus"
    for each file, separated by <br/> tags. Returns empty string if no data found.
    """
    result = ""
    url = f"{get_env('ENA_ENDPOINT_REPORT')}run-files/{accession_no}?format=json"
    with requests.Session() as session:
        session.auth = (user_token, pass_word)
        headers = {'Accept': '/'}

        try:
            response = session.get(url, headers=headers)
            if response.status_code == requests.codes.ok:
                response_body = response.json()
                # Build a pipe-delimited string of file statuses, then convert pipes to <br/>
                for r in response_body:
                    report = r.get("report",{})
                    if report:
                        result += "|"+ report.get("fileName") + " : " + report.get("archiveStatus") + " : " + report.get("releaseStatus")
                if result:
                    # Strip leading pipe and replace remaining pipes with line breaks for HTML display
                    result = result[1:].replace("|", "<br/>")

            else:
                result = "Cannot get file processing result from ENA"
                l.error(str(response.status_code) + ":" + response.text)
        except Exception as e:
            l.exception(e)
        return result
    
def generate_singlecell_record(profile_id, checklist_id=str(), study_id=str(), schema_name=str()):
    """Build the complete data structure for the single-cell UI DataTable.

    Assembles column definitions, row data, file transfer statuses, and
    per-repository submission statuses for every component (study, sample,
    experiment, run, etc.) belonging to a given profile/checklist/study.

    Returns a dict with keys: dataSet, columns, submission_repository,
    components, study_id — consumed directly by the frontend DataTable renderer.
    """

    data_set = {}           # {component_name: [row_dicts]} — the actual table row data
    columns = {}            # {component_name: [column_defs]} — DataTable column config
    column_keys = {}        # {component_name: [term_names]} — tracks which schema columns exist per component
    studies = []
    identifier_map = {}     # {component_name: identifier_term_name} — the primary key field for each component
    submission_repository = {}  # {component_name: [repository_names]} — which repos each component submits to

    """
    profile = Profile().get_record(profile_id)
    if not profile:
        return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))
    """
    studies = []
    # Resolve schema_name from the study record if not explicitly provided
    if not schema_name:
        if study_id:
            studies = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"study_id": study_id, "checklist_id": checklist_id}, projection={"study_id": 1, "schema_name": 1})
            if studies:
                schema_name = studies[0].get("schema_name", "")
    # Re-query with schema_name to get all studies under this schema (may span multiple study_ids)
    if schema_name:
        studies = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"schema_name": schema_name, "checklist_id": checklist_id}, projection={"study_id": 1, "components.study":1})
    
    if not studies:
        return dict(dataSet=data_set, columns=columns, submission_repository=submission_repository, components=list(columns.keys()))

    # Default to the first study if no specific study_id was requested
    if not study_id:
        study_id = studies[0]["study_id"]

    if checklist_id:
        # Load the full schema definition for all components under this checklist
        schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=checklist_id)

        repositories = set()
        # Get the mapping of which repositories each component submits to (e.g. ena, zenodo)
        submission_repository_df = SinglecellSchemas().get_submission_repository(schema_name=schema_name)
        submisison_repository_component_map = submission_repository_df.to_dict('index')
        # Prefixes like "accession", "status", "error" that get suffixed with repository name
        additional_columns_prefix_default_value = ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE
        additional_fields_map = {}  # {component: [extra_column_names]} e.g. "accession_ena", "status_zenodo"
        file_df_map = {}            # {component: [file_term_names]} — tracks which schema fields are file uploads
        for component, respositories in submisison_repository_component_map.items():
            # Build list of active repositories for this component
            submission_repository[component] = [repository for repository, value in respositories.items() if value]
            # Build the additional column names by combining each prefix with each active repository
            additional_fields_map[component] = [f"{prefix}_{repository}" for repository, value in respositories.items() if value for prefix in list(additional_columns_prefix_default_value.keys())]
            repositories.update(submission_repository[component])

        # Build DataTable column definitions for each component from its schema
        for component_name, component_schema in schemas.items():
            columns[component_name] = []
            component_schema_df = pd.DataFrame.from_records(component_schema)
            # Find the identifier (primary key) field for this component
            identifier_df = component_schema_df.loc[component_schema_df['identifier'], 'term_name']
            # Find any file-type fields (used later to show file transfer status)
            file_df = component_schema_df.loc[component_schema_df['term_type'] == 'file', 'term_name']

            if not identifier_df.empty:
                identifier_map[component_name]= identifier_df.iloc[0]

            # There will be not be a details button for
            # components that do not have submission buttons
            if submission_repository.get(component_name, []):
                detail_dict = dict(
                    className='summary-details-control detail-hover-message',
                    orderable=False,
                    data=None,
                    title='',
                    defaultContent='',
                    width="5%",
                )

            # Hidden columns for internal row identification
            columns[component_name].append(dict(data="record_id", visible=False))
            columns[component_name].append(dict(data="DT_RowId", visible=False))
            # Visible columns from the schema — file fields use a thumbnail image renderer
            columns[component_name].extend([dict(data=item["term_name"],  title=item["term_label"], defaultContent='',
                                                    render = "render_thumbnail_image_column_function" if item["term_type"] == "file" else None
                                                  ) for item in component_schema])


            column_keys[component_name] = ([item["term_name"] for item in component_schema])

            # Append repository-specific columns (accession, status, error) with appropriate renderers
            for name in additional_fields_map.get(component_name, []):
                    prefix = name.split("_")[0]
                    columns[component_name].append(dict(data=name, title=name.replace("_", " for ").title(), render="render_ena_accession_function" if name.lower().endswith("accession_ena") else "render_zenodo_accession_function" if name.lower().endswith("accession_zenodo") else "", defaultContent= additional_columns_prefix_default_value[prefix])) # render = "render_accession_column_function"
                    column_keys[component_name].append(name)

            # If this component has file fields, add file status and ENA processing status columns
            if not file_df.empty:
                columns[component_name].append(dict(data="file_status", title="File Status", defaultContent=''))

                if "ena" in submission_repository.get(component_name,[]):
                    columns[component_name].append(dict(data="ena_file_processing_status", title="ENA File Processing Status", defaultContent='', className="ena_file_processing_status"))
                file_df_map[component_name] = file_df.values.tolist()

        # Retrieve full component data for the primary study
        singlecell = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"checklist_id": checklist_id, "study_id": study_id})
        if not singlecell or not singlecell[0]["components"].get("study",[]):
            return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))

        if len(studies) > 1:
            # Merge study component rows from all other studies into the first one
            # so the UI shows a unified view across studies sharing the same schema
            for study in studies:
                if study["study_id"] != study_id:
                    singlecell[0]["components"]["study"].extend(study["components"].get("study",[]))

        # Collect all filenames referenced in any component of this single-cell record
        files = SinglecellSchemas().get_all_files(singlecell=singlecell[0], schemas=schemas)

        # Query EnaFileTransfer collection for the transfer status of each file
        # Uses regex matching on local_path endings to find matching transfer records
        local_path = [regex.Regex(f'{x}$') for x in files]
        projection = {'_id':0, 'local_path':1, 'status':1, "transfer_status":1}
        filter = dict()
        filter['local_path'] = {'$in': local_path}
        filter['profile_id'] = profile_id

        enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)

        # Map filename → human-readable transfer status name (e.g. "Uploading", "Complete")
        enaFile_map = {os.path.basename(enafile["local_path"]) : tx.TransferStatusNames[tx.get_transfer_status(enafile)] for enafile in enaFiles}

        submission_status_map = {}
        submission_error_map = {}
        submission_status={}    # {repository: aggregated_status} — worst-case status across child components
        submission_error=[]     # Collected error messages from all child components
        for component_name, component_data in singlecell[0]["components"].items():
            if not component_data:
                continue

            component_data_df = pd.DataFrame.from_records(component_data)

            # Strip columns not in the schema (e.g. internal MongoDB fields)
            for column in component_data_df.columns:
                if column not in column_keys.get(component_name, []):
                    component_data_df.drop(column, axis=1, inplace=True)

            if component_name != "study":
                # Propagate the worst-case submission status from child components up to study level.
                # Priority: rejected > pending > accepted — if any child is rejected, study shows rejected
                for repository in submission_repository.get(component_name, []):
                    status_column = f"status_{repository}"
                    error_column = f"error_{repository}"
                    if status_column in component_data_df.columns:
                        status = "rejected" if any(component_data_df[status_column] == "rejected") else "pending" if any(component_data_df[status_column] != "accepted") else "accepted"
                        if submission_status.get(repository, "accepted") != status and status != "accepted":
                            submission_status[repository] = status
                    if error_column in component_data_df.columns:
                        error = component_data_df[error_column].dropna().tolist()
                        if error:
                            submission_error.extend(error)

            # Generate unique DataTable row IDs: "componentName_studyId_identifier"
            component_data_df["DT_RowId"] = component_name + "_"+ study_id + "_" + component_data_df.get(identifier_map.get(component_name,""), "")
            component_data_df["record_id"] = component_data_df["DT_RowId"]
            component_data_df.fillna("", inplace=True)

            # Build the file_status column by concatenating "filename : transferStatus" for each file field
            file_terms = file_df_map.get(component_name, [])
            if file_terms:
                component_data_df["file_status"] = ""
                for term in file_terms:
                    component_data_df["file_status"] = component_data_df["file_status"] + component_data_df[term].apply(lambda x: (x+ " : " + enaFile_map.get(x, "unknown") + "  ") if x else "") if term in component_data_df.columns else ""

            data_set[component_name] = component_data_df.to_dict(orient="records")


        # Apply the aggregated child-component statuses to the study row.
        # The study's status per repository becomes the worst of its own status and its children's.
        study_component = data_set["study"][0]
        for repository, repository_status in submission_status.items():
            status_for_study = [study_component[ f"status_{repository}"],repository_status]
            study_component[ f"status_{repository}"] = "rejected" if (any(status == "rejected" for status in status_for_study)) else "pending" if (any(status == "pending" for status in status_for_study)) else study_component[ f"status_{repository}"]

            # Append any child-component errors to the study's error field
            study_component[f"error_{repository}"] = study_component[f"error_{repository}"] + " " + "<br/>".join(submission_error) if submission_error else ""


    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       submission_repository=submission_repository,
                       components=list(columns.keys()),
                       study_id = study_id,
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict


def generate_accessions_singlecell(profile_id, study_id ):
    """Build the DataTable data structure for displaying accession numbers.

    Fetches all accession data (from all repositories) for a given study
    and formats it with column definitions suitable for the frontend DataTable.
    ENA and Zenodo accessions get specialised link renderers.
    """
    profile = Profile().get_record(profile_id)
    if not profile:
        return dict(status='error', message="Profile not found!")

    data_set = []
    columns = []
    # Hidden columns for internal row tracking
    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))

    # Fetch accession data across all repositories for this study
    accessions = get_accession(profile_id, study_id,repository="", schema_name="", is_published=False)
    if accessions:
        # Dynamically build column definitions from the keys in the first accession record
        for column in accessions[0].keys():
            columns.append(dict(data=column, title=column.replace("_", " ").title(), defaultContent='', render="render_ena_accession_function" if column.lower().endswith("accession_ena")or column.lower()=="biosampleaccession" else "render_zenodo_accession_function" if column.lower().endswith("accession_zenodo") else ""))
        # Assign unique row IDs using the biosample accession (or study_id as fallback)
        for accession in accessions:
            accession["DT_RowId"] = "accession_" + accession.get("biosampleAccession") if "biosampleAccession" in accession else accession.get("study_id")
            accession["record_id"] = accession["DT_RowId"]
        data_set = accessions

    return dict(dataSet=data_set, columns=columns)


def _check_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map):
    """Recursively check whether any child component records have accession numbers.

    Used as a pre-deletion guard — records with accessions cannot be deleted because
    they've already been submitted to a repository (ENA/Zenodo). Walks the component
    hierarchy via child_map and returns (False, error_message) if any child has accessions,
    or (True, "") if deletion is safe.
    """
    for child_component_name, foreign_key in child_map.get(component_name, {}).items():
        child_component_data = singlecell_data["components"].get(child_component_name, [])
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        child_identifier_key = identifier_map.get(child_component_name, "")

        if not child_component_data_df.empty:
            # Filter to only child rows that reference the parent identifiers being deleted
            children_df = child_component_data_df.loc[child_component_data_df[foreign_key].isin(identifiers)]
            # Find all accession columns (e.g. accession_ena, accession_zenodo)
            accession_columns = child_component_data_df.columns[child_component_data_df.columns.str.startswith("accession_")].tolist()
            if accession_columns:
                # Block deletion if any child has ALL accession fields filled (submitted to all repos)
                children_has_accession_df = children_df.loc[children_df.apply(lambda x:  all(x[accession_columns] != ""), axis=1), child_identifier_key]
                if not children_has_accession_df.empty:
                    return False,  f'{child_component_name}:{children_has_accession_df.tolist()} : record with accession number'

            # Recurse deeper into grandchild components
            if child_identifier_key:
                return _check_child_component_data(singlecell_data, child_component_name, children_df[child_identifier_key].tolist(),  identifier_map, child_map)
    return True, ""


def _delete_datafile(profile_id, to_be_delete_component_data_df, schema):
    """Delete data files, their transfer records, and any generated thumbnails.

    Given a DataFrame of component rows to be deleted and their schema definition,
    this function:
    1. Identifies all file-type fields from the schema
    2. Collects filenames from those fields in the rows being deleted
    3. Removes corresponding DataFile records from MongoDB
    4. Removes corresponding EnaFileTransfer records (upload tracking)
    5. Removes thumbnail images from disk for any image files
    """
    schema_df = pd.DataFrame.from_records(schema)
    # Find which schema fields are file-type (uploads)
    schema_file_df = schema_df.loc[schema_df["term_type"] == "file", "term_name"]
    if not schema_file_df.empty:
        # Extract just the file columns from the rows being deleted
        file_df = to_be_delete_component_data_df[schema_file_df.tolist()]
        file_df = file_df.dropna()
        fileslist = file_df.values.tolist()
        # Flatten the 2D list of filenames into a single list, removing empty strings
        filelist = []
        for files in fileslist:
            filelist.extend(list(filter(None, files)))

        if filelist:
            # Look up MongoDB _id values for these files
            fileIdList = cursor_to_list(DataFile().get_collection_handle().find({"profile_id": profile_id, "file_name": {"$in": filelist}}, {"_id": 1}))

            # Delete file metadata records from DataFile collection
            DataFile().get_collection_handle().delete_many({"profile_id": profile_id, "_id": {"$in": [file["_id"] for file in fileIdList]}})
            # Delete associated ENA file transfer tracking records
            EnaFileTransfer().get_collection_handle().delete_many({"profile_id": profile_id, "file_id": {"$in": [str(file["_id"]) for file in fileIdList]}})

            # Clean up thumbnail images from disk for image file types
            for filename in filelist:
                delete_image_thumbnail(filename, profile_id)

def _delete_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map, schemas):
    """Recursively delete child component data linked to the given parent identifiers.

    Walks down the component hierarchy (e.g. study → sample → experiment → run),
    deleting associated data files and removing rows from the singlecell document's
    components dict. Mutates singlecell_data in place.
    """
    for child_component_name, foreign_key in child_map.get(component_name, {}).items():
        child_schema = schemas.get(child_component_name, [])
        child_component_data = singlecell_data["components"].get(child_component_name, [])
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        child_component_identifier_key = identifier_map.get(child_component_name, "")

        if not child_component_data_df.empty:
            # Find child rows that reference the parent identifiers being deleted
            to_be_delete_child_component_data_df = child_component_data_df.loc[child_component_data_df[foreign_key].isin(identifiers)]
            if not to_be_delete_child_component_data_df.empty:
                # Recurse into grandchildren before deleting this level (depth-first)
                if child_component_identifier_key:
                    _delete_child_component_data(singlecell_data, child_component_name, to_be_delete_child_component_data_df[child_component_identifier_key].tolist(), identifier_map, child_map, schemas)
                # Delete any uploaded files associated with these child rows
                _delete_datafile(singlecell_data["profile_id"], to_be_delete_child_component_data_df, child_schema)
                '''
                child_schema_df = pd.DataFrame.from_records(child_schema)
                child_schema_file_df = child_schema_df.loc[child_schema_df["term_type"] == "file", "term_name"]
                if not child_schema_file_df.empty:
                    filelist = []
                    fileslist = to_be_delete_child_component_data_df[child_schema_file_df.values.tolist()]
                    for files in fileslist:
                        filelist.extend(list(filter(None, files)))
                    if filelist:
                        #delete the files
                        DataFile().get_collection_handle().delete_many({"profile_id":singlecell_data["profile_id"], "file_name": {"$in": filelist}})        
                '''
                # Remove the deleted rows from the DataFrame and update singlecell_data in place
                child_component_data_df = child_component_data_df.drop(child_component_data_df[child_component_data_df[foreign_key].isin(identifiers)].index)
                if not child_component_data_df.empty:
                    singlecell_data["components"][child_component_name] = child_component_data_df.to_dict(orient="records")
                else:
                    # Remove the component entirely if no rows remain
                    singlecell_data["components"].pop(child_component_name, None)

def delete_singlecell_records(profile_id, checklist_id, target_ids=[],target_id="", study_id="", schema_name=""):
    """Delete selected single-cell records and all their descendant data.

    Handles the full deletion workflow:
    1. Parse target_ids to determine which component and identifiers to delete
    2. Validate that none of the targeted records (or their children) have accession numbers
    3. Recursively delete child component data and associated files
    4. Update the singlecell MongoDB document, or delete it entirely if empty
    5. Reset study-level submission statuses to "pending" after deletion

    target_id format: "componentName_studyId_identifier" (e.g. "sample_STUDY123_SAMP001")
    Special case for study: "study_studyId"
    """
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to delete!")

    dt = get_datetime()
    study_ids = []
    if study_id:
        is_single_study = True
        study_ids.append(study_id)
    else:
        is_single_study = False

    profile = Profile().get_record(profile_id)
    if not profile:
        return dict(status='error', message="Profile not found!")

    # Load schema definitions and build relationship maps for the component hierarchy
    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=checklist_id)
    identifier_map, foreignkey_map = SinglecellSchemas().get_key_map(schemas)  # Maps component → primary key, component → foreign keys
    child_map = SinglecellSchemas().get_child_map(foreignkey_map)  # Maps parent component → {child_component: foreign_key}

    identifiers = []
    component_name = ""

    # Parse each target_id to extract the component name, study ID, and record identifier.
    # All target_ids must belong to the same component (batch deletion within one component only).
    # target_id format: "component_studyId_identifier" or "study_studyId" for study records
    for target_id in target_ids:

        tmp = target_id.split("_")
        if len(tmp) >= 3:
            # Standard format: last segment is identifier, second-to-last is study_id, rest is component name
            identifier = tmp[len(tmp)-1]
            tmp_study_id = tmp[len(tmp)-2]
            tmp_component_name = "_".join(tmp[:len(tmp)-2])
        elif len(tmp) == 2 and tmp[0] == "study":
                # Special case: study component uses "study_studyId" format
                tmp_component_name = "study"
                tmp_study_id = tmp[1]
                identifier = tmp[1]
        else:
            return dict(status='error', message="Target ID is incorrect: " + target_id)

        # Enforce all target_ids belong to the same component
        if not component_name:
            component_name = tmp_component_name
        elif component_name != tmp_component_name:
            return dict(status='error', message="Please select records from the same component!")

        # Enforce all target_ids belong to the same study (if a specific study was requested)
        if study_id != tmp_study_id and is_single_study:
            return dict(status='error', message="Please select records from the same study!")
        elif not is_single_study:
            study_ids.append(tmp[1])

        identifiers.append(identifier)

    identifier_key = identifier_map.get(component_name, "")
    if not identifier_key:
        return dict(status='error', message="Identifier not found for component: " + component_name)
    
    # --- Validation pass: check that no targeted records have accession numbers ---
    # Records with accessions have been submitted to repositories and cannot be deleted.
    study_message_map = {}
    for study_id in study_ids:
        singlecell_data =  Singlecell(profile_id=profile_id).get_collection_handle().find_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id }, {"components": 1, "profile_id":1, "checklist_id":1})

        if not singlecell_data:
            message=f"Record not found"
            study_message_map[study_id] = message
            continue

        # Check if any of the targeted rows themselves have accession numbers
        component_data_df = pd.DataFrame.from_records(singlecell_data["components"][component_name])
        component_data_has_accession_df = component_data_df.loc[component_data_df.apply(lambda x: x[identifier_key] in identifiers and any(x[accession_column] != "" for accession_column in list(component_data_df.columns.values) if accession_column.startswith("accession_")), axis=1), identifier_key]

        if not component_data_has_accession_df.empty:
            if component_name == "study":
                message= ' record with accession number'
            else:
                message= f'{component_name}:{component_data_has_accession_df.tolist()}: record with accession number'
            study_message_map[study_id] = message
            continue

        # Recursively check child components for accession numbers
        result, message =  _check_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map)
        if not result:
            study_message_map[study_id] = message

    # If any study had records blocking deletion, return all errors at once
    if study_message_map:
        message = "Record deleted failed!"
        for key, msg in study_message_map.items():
            message += f"<br/>study:'{key}'| {msg}"
        return dict(status='error', message=message)

    # --- Deletion pass: actually remove the records and their descendants ---
    component_schema = schemas.get(component_name, [])

    for study_id in study_ids:

        component_data_df = pd.DataFrame.from_records(singlecell_data["components"][component_name])

        # First delete all child/grandchild component data (depth-first recursive)
        _delete_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map, schemas)

        # Delete files associated with the targeted rows themselves
        to_be_delete_component_data_df = component_data_df.loc[component_data_df[identifier_key].isin(identifiers)]
        _delete_datafile(singlecell_data["profile_id"], to_be_delete_component_data_df, component_schema)

        # Remove the targeted rows from the component DataFrame
        component_data_df = component_data_df.drop(component_data_df[component_data_df[identifier_key].isin(identifiers)].index)

        if not component_data_df.empty:
            singlecell_data["components"][component_name] = component_data_df.to_dict(orient="records")
        else:
            # Remove the component entirely if no rows remain
            singlecell_data["components"].pop(component_name, None)

        if singlecell_data["components"]:
            # Reset study-level submission status to "pending" for all repositories,
            # since the data has changed and needs re-submission
            submission_repository_df = SinglecellSchemas().get_submission_repository(
                schema_name=schema_name
            )
            submission_repository_component_map = submission_repository_df.to_dict(
                'index'
            )
            repositories = submission_repository_component_map.get("study", {})
            for repository, value in repositories.items():
                if value:
                    status_column = f"status_{repository}"
                    singlecell_data["components"]["study"][0][status_column] = "pending"

            # Persist the modified components back to MongoDB
            Singlecell(profile_id=profile_id).get_collection_handle().update_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id}, {"$set": {"components": singlecell_data["components"], "last_modified": dt, "last_update_by": dt}})
        else:
            # If all components are empty, delete the entire singlecell document
            Singlecell(profile_id=profile_id).get_collection_handle().delete_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id})

    return {"status": "success", "message": "Record deleted successfully!"}


"""
def submit_singlecell_ena(profile_id, target_ids, target_id,checklist_id, study_id):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to submit!")

    user = ThreadLocal.get_current_user()
    dt = get_datetime()

    sub = Submission().get_collection_handle().find_one(
        {"profile_id": profile_id, "deleted": get_not_deleted_flag()})

    if not sub:
        return dict(status='error', message="Please contact System Support Error 10211!")
    
    return dict(status='error', message="Not Implement.")        
"""

def query_submit_result(profile_id, study_id,schema_name, repository="ena"):
    """Check the status of a submission and return accession data if complete.

    Looks up the Submission document for this profile/repository. If study_status
    is "complete", fetches and returns all accession numbers. Otherwise returns
    an "in progress" message. Used by the frontend to poll for submission results.
    """
    submission = Submission().get_collection_handle().find_one({"profile_id": profile_id, "repository": repository, "deleted": get_not_deleted_flag()})
    if not submission:
        return dict(status='error', message="No submission record found.")

    submission_status = submission.get("study_status", "pending")

    match submission_status:
        case "complete":
            # Submission finished — fetch and return the accession numbers
            result = get_accession(profile_id=profile_id, study_id=study_id, schema_name=schema_name, repository=repository, is_published=False)
            if not result:
                return dict(status='error', message="No record found.")
            return dict(status='success', message="Submission is completed.", data=result)
        case _:
            # Any other status (pending, downloading, uploading, sending) means still in progress
            return dict(status='error', message="Submission is in progress, please try again later!")  
        
        
def submit_singlecell(profile_id, study_id, schema_name="", repository="ena", credential_source="user"):
    """Initiate a single-cell submission to a repository (ENA or Zenodo).

    This is the main submission entry point called from the frontend. It:
    1. Validates the study exists and schema matches
    2. Checks aggregated component statuses to ensure submission is needed/allowed
    3. Creates a Submission document if one doesn't exist yet
    4. Creates EnaFileTransfer records for all files (with S3 etag verification)
    5. Sets the submission status to "downloading" (triggers the Celery pipeline)
    6. Updates the singlecell study component status to "processing"

    The Celery beat task will pick up the "downloading" submission and handle
    the actual file transfer and repository submission asynchronously.
    """
    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "deleted": get_not_deleted_flag(), "study_id" : study_id})
    if not singlecell:
        return dict(status='error', message="Study not found.")
    if schema_name and schema_name != singlecell.get("schema_name",""):
        return dict(status='error', message=f"schema {schema_name} does not match with the study .")

    studies = singlecell.get("components",{}).get("study",[])

    # Get the repository configuration for each component to determine valid submission targets
    submission_repository_df = SinglecellSchemas().get_submission_repository(
        schema_name=singlecell["schema_name"]
    )
    submission_repository_component_map = submission_repository_df.to_dict('index')

    # Compute the aggregate status across all components for this repository.
    # If any child component is rejected/pending, that propagates up to the final status.
    status_column = f"status_{repository}"
    final_status = studies[0].get(status_column, "pending")
    for component, repositories in submission_repository_component_map.items():
        if component == "study" and repository not in repositories:
            return dict(status='error', message=f"Repository {repository} is not supported for study submission!")

        component_data_df = pd.DataFrame.from_records(singlecell.get("components", {}).get(component, []))
        if status_column in component_data_df.columns:
            status = "rejected" if any(component_data_df[status_column] == "rejected") else "pending" if any(component_data_df[status_column] != "accepted") else "accepted"
            if final_status != status and status != "accepted":
                final_status = status

    # Attach the submitter + credential choice to any existing submission for
    # this study *before* the in-progress guard below. Otherwise a resubmit
    # while a prior submission is still running returns early and the document
    # keeps whatever (or no) submitter it had — so the async pipeline falls
    # back to COPO's default credentials instead of the user's own.
    existing = Submission().execute_query({"profile_id": profile_id, "repository": repository, "deleted": get_not_deleted_flag()})
    if existing:
        Submission().get_collection_handle().update_one(
            {"_id": existing[0]["_id"]},
            {"$set": {"submitter": get_current_user().id, "credential_source": credential_source}},
        )

    # Guard against submitting when there's nothing to submit or submission is already running
    match final_status:
        case "accepted" | "published":
            return dict(status='error', message="There is no pending change for submission!")
        case "processing":
            return dict(status='error', message="Submission is in progress, please wait until it is completed!")

    # Find or create the Submission document for this profile/repository
    submissions = Submission().execute_query({"profile_id": profile_id, "repository": repository, "deleted": get_not_deleted_flag()})
    if not submissions:
        now = get_datetime()
        user_id = get_current_user().id
        insert_sub = dict()
        insert_sub["date_created"] = now
        insert_sub["created_by"] = user_id
        insert_sub["repository"] = repository
        insert_sub["accessions"] = dict()
        insert_sub["profile_id"] = profile_id
        insert_sub["date_modified"] = now
        insert_sub["deleted"] =  get_not_deleted_flag()

        submission = Submission().get_collection_handle().insert_one(insert_sub)
        submissions = [{"_id":submission.inserted_id}]

    # Record who is submitting and which credentials to use, so the async ENA
    # pipeline can resolve the submitter's own Webin credentials (or fall back
    # to COPO defaults). credential_source == "copo_default" is the popup's
    # one-off "use COPO default" choice. Written on every submit so a repeated
    # submission always reflects the latest choice.
    Submission().get_collection_handle().update_one(
        {"_id": submissions[0]["_id"]},
        {"$set": {"submitter": get_current_user().id, "credential_source": credential_source}},
    )

    # Prepare file transfer records for all files referenced in the singlecell record
    schemas = SinglecellSchemas().get_schema(schema_name=singlecell.get("schema_name", singlecell["schema_name"]), schemas=dict(), target_id=singlecell["checklist_id"])
    files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas)
    if files:
        # Pre-flight: verify every file referenced in the manifest is actually
        # present in S3 *before* creating transfer records or flipping the
        # submission to "downloading". Without this guard a user can click
        # Submit on a manifest whose data files were never uploaded, leading
        # to silently broken EnaFileTransfer rows and a confusing failure later.
        s3obj = s3()
        etags, _ = s3obj.check_s3_bucket_for_files(bucket_name=profile_id, file_list=files, just_return_etags=True)
        # Defensive: helper may return non-dict on bucket-missing/auth errors.
        if not isinstance(etags, dict):
            etags = {}
        missing_files = [f for f in files if f not in etags]
        if missing_files:
            return dict(
                status='error',
                message=(
                    "Cannot submit the following data files because they are missing from storage: "
                    + ", ".join(f"<strong>{f}</strong>" for f in missing_files) + ".<br><br>"
                    + "Upload them via <strong>Data files</strong> on the "
                    "<strong>Work profiles</strong> page and try again."
                ),
            )

        datafiles = DataFile().get_all_records_columns(filter_by={"profile_id": profile_id, "file_name": {"$in": files}}, projection={"_id": 1, "file_name": 1})
        errors = []
        # Create an EnaFileTransfer record for each file (tracks download/upload progress)
        for file in datafiles:
            result, message = tx.make_transfer_record(file_id=file["_id"], submission_id=str(submissions[0]["_id"]), no_remote_location=True if repository == "zenodo" else False, etag=etags.get(file["file_name"], ""))
            if not result:
                errors.append(message)
        if errors:
            return dict(status='error', message="Failed to create transfer record for files: " + ", ".join(errors))

    # Set submission status to "downloading" — this is the trigger for the Celery beat task
    # (update_submission_pending) to pick up and process this submission
    result =  Submission().make_submission_downloading(profile_id=profile_id, component="study", component_id=study_id, repository=repository)
    if result.get("status","") == "error":
        return result
    else:
        # Mark the study component as "processing" so the UI shows the correct state
        Singlecell().update_component_status(singlecell["_id"], component="study", identifier="study_id", identifier_value=study_id, repository=repository, status_column_value={"status":"processing"})

        # Immediately notify the frontend that the submission is queued, so
        # the user sees feedback without waiting for the next Celery beat tick.
        notify_submission_status(
            data={"profile_id": profile_id},
            msg="Queued for submission...",
            action="info",
            html_id="submission_info",
        )

        # Dispatch the submission task directly instead of waiting up to 10s
        # for celery beat. The periodic schedule still acts as a safety net.
        if repository == "ena":
            from src.apps.copo_single_cell_submission.tasks import process_ena_submission
            process_ena_submission.delay()
        elif repository == "zenodo":
            from src.apps.copo_single_cell_submission.tasks import process_zenodo_submission
            process_zenodo_submission.delay()

        submission_id = str(submissions[0]["_id"])
        return dict(status='success', message=f"Submission has been scheduled. Submission ID: {submission_id}", submission_id=submission_id)


def get_accession(profile_id, study_id, schema_name="", repository="", is_published=False):
    """Collect accession numbers across all components for a given study.

    Merges accession columns from each component (study, sample, experiment, run, etc.)
    into a single flat table, joined via foreign keys. Optionally filters by repository
    and/or published status.

    Args:
        is_published: If True, only include accessions from repositories where the
                      study has actually been published (state=PUBLIC for ENA, etc.)

    Returns a list of dicts suitable for DataTable display, or empty list if not found.
    """
    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "deleted": get_not_deleted_flag(), "study_id" : study_id})
    if not singlecell:
        return []
    if schema_name and schema_name != singlecell.get("schema_name",""):
        return []

    schema_name = singlecell["schema_name"]

    schemas = SinglecellSchemas().get_schema(schema_name=schema_name, schemas=dict(), target_id=singlecell["checklist_id"])

    repositories = set()
    submission_repository_df = SinglecellSchemas().get_submission_repository(
        schema_name=schema_name
    )
    submission_repository_component_map = submission_repository_df.to_dict('index')
    identifier_map, foreignkey_map = SinglecellSchemas().get_key_map(schemas=schemas)
    submission_repository = {}

    for component, repositories in submission_repository_component_map.items():
        submission_repository[component] = [
            repository for repository, value in repositories.items() if value
        ]

    # When filtering for published-only, determine which repositories are actually published
    must_in_repository_if_published = []
    if is_published:
        study = singlecell.get("components", {}).get("study", [])
        repositories = submission_repository.get("study", [])
        for repository  in repositories:
            if repository == "ena":
                # ENA uses state_ena="PUBLIC" to indicate published
                if study[0].get(f"state_{repository}", "") == "PUBLIC":
                    must_in_repository_if_published.append(repository)
            elif repository == "zenodo":
                # Zenodo uses status_zenodo="published" and state_zenodo="done"
                if study[0].get(f"status_{repository}","") == "published" and study[0].get(f"state_{repository}", "done"):
                    must_in_repository_if_published.append(repository)
                    
 
       
    # Pre-load sample data for joining with other components later
    sample_df = pd.DataFrame.from_records(singlecell.get("components", {}).get("sample", []))

    result = pd.DataFrame()
    # Iterate through each component and collect its accession-related columns
    for component, repositories in submission_repository.items():
        if repository and repository not in repositories:
            continue
        # Load the component's data from the singlecell document
        component_df = pd.DataFrame.from_records(singlecell.get("components", {}).get(component, []))
        if component_df.empty:
            continue

        # Determine key columns (identifier + foreign keys) needed for joining
        identifier = identifier_map[component]
        foreign_keys = [ value["foreign_key"] for value in  foreignkey_map.get(component, {})]
        keys = [identifier] + foreign_keys
        new_repository = []
        if repository:
            new_repository.append(repository)
        else:
            new_repository = repositories

        # If filtering for published, skip repositories that haven't been published yet
        if is_published:
            new_repository = [repository for repository in new_repository if repository in must_in_repository_if_published]
            if not new_repository:
                continue

        # Select only key columns and columns ending with a repository name (accession, status, etc.)
        component_df_new = component_df.loc[:, component_df.columns.str.endswith(tuple(new_repository)) | component_df.columns.isin(keys)]

        # Prefix repository-specific columns with the component name to avoid collisions when merging
        # e.g. "accession_ena" in the "experiment" component becomes "experiment_accession_ena"
        prefix = ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE.keys()

        new_column_mapper = { col: f"{component}_{col}"
                                for col in component_df_new.columns
                                    if col not in keys
                                        and any(col.lower().startswith(f"{p}_") for p in prefix)}

        component_df_new.rename(columns=new_column_mapper, inplace=True)

        # If this component doesn't have sample_id directly, try to get it via parent hierarchy
        # so we can join with the sample component for a complete view
        if "sample_id" not in component_df_new.columns:
            component_with_parent_df = ena_submission.merge_parent_component(singlecell=singlecell,schemas=schemas, component_name=component, component_df=component_df)
            if "sample_id" in component_with_parent_df.columns:
                component_df_new["sample_id"] = component_with_parent_df["sample_id"]
                component_df_new = component_df_new.merge(sample_df, how="left", on=["study_id","sample_id"])

        # Progressively merge each component's data into a single result DataFrame
        if result.empty:
            result = component_df_new
        else:
            # Merge on study_id + sample_id if available, otherwise just study_id
            if "sample_id" in component_df_new.columns and "sample_id" in result.columns:
                merge_on = ["study_id", "sample_id"]
            else:
                merge_on = ["study_id"]
            result = result.merge(component_df_new, how="left" , on=merge_on)

    return result.to_dict(orient="records")


def publish_singlecell(profile_id, study_id, schema_name, repository="ena"):
    """Make a previously submitted (and accepted) study publicly available.

    This is a separate step from submission — submission sends data to the repository
    privately, and publishing makes it publicly accessible. Only "accepted" submissions
    can be published. Delegates to repository-specific publish functions.
    """
    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "deleted": get_not_deleted_flag(), "study_id" : study_id})
    if not singlecell:
        return dict(status='error', message="No record found.")
    if schema_name and schema_name != singlecell.get("schema_name",""):
        return dict(status='error', message="Schema name does not match the record.")

    # Guard: only "accepted" status allows publishing; all others are blocked
    studies = singlecell.get("components",{}).get("study",[])
    match  studies[0].get(f"status_{repository}", ""):
        case "processing":
            return dict(status='error', message="Submission is in progress, please wait until it is completed!")
        case "published":
            return dict(status='error', message="Submission is already published!")
        case "rejected":
            return dict(status='error', message="Submission is rejected, please fix the errors and resubmit!")
        case "pending":
            return dict(status='error', message="Please do the submission first!")

    # Dispatch to the appropriate repository's publish function
    if repository == "ena":
        result = ena_submission.release_study(profile_id=profile_id, singlecell=singlecell)
    elif repository == "zenodo":
        accession = studies[0].get(f"accession_{repository}","")
        if not accession:
            return dict(status='error', message="Please do the submission first!")
        result = zenodo_submission.publish_zendo(profile_id=profile_id, deposition_id=accession, singlecell=singlecell)

    return result

def update_submission_pending():
    """Celery beat task: transition submissions from "downloading" to "pending" once all files are transferred.

    This runs periodically (via Celery beat) and checks all submissions currently in
    "downloading" status. For each submission, it verifies that every file referenced
    by the singlecell record has a corresponding EnaFileTransfer record with status="complete".

    Once ALL files for a submission are confirmed downloaded, the submission status is
    advanced to "pending", which signals the next stage of the pipeline (actual submission
    to the repository) to begin.

    If any files are missing transfer records or haven't completed, the submission stays
    in "downloading" and will be re-checked on the next beat cycle.
    """
    component = "study"
    # Find all submissions currently waiting for file downloads to complete
    subs = Submission().get_submission_downloading(component=component)
    all_downloaded_sub_ids = []
    for sub in subs:
        all_file_downloaded = True
        for study_id in sub[component]:
            singlecell = Singlecell().get_collection_handle().find_one(
                 {"profile_id":sub["profile_id"], "study_id":study_id,"deleted":get_not_deleted_flag()},
                 {"schema_name":1,"checklist_id":1, "components":1})
            if not singlecell:
                # Study was deleted while submission was in progress — remove it from the submission
                Submission().remove_study_from_singlecell_submission(sub_id=str(sub["_id"]), study_id= study_id)
                continue

            # Get all files referenced by this singlecell record's schema
            schemas = SinglecellSchemas().get_schema(schema_name=singlecell["schema_name"], schemas=dict(), target_id=singlecell["checklist_id"])
            files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas)

            # Look up the transfer status for each file
            local_path = [regex.Regex(f'{x}$') for x in files]
            projection = {'_id':0, 'local_path':1, 'status':1}
            filter = dict()
            filter['local_path'] = {'$in': local_path}
            filter['profile_id'] = sub["profile_id"]

            enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)

            if not files and not enaFiles:
                # No files to download for this study, skip to next
                continue

            elif len(files) > len(enaFiles):
                # Some files don't have transfer records yet — still waiting
                all_file_downloaded = False
                missing_files = set(files) - {os.path.basename(enaFile["local_path"]) for enaFile in enaFiles}
                Logger().error(f"Files not uploaded for submission {sub['_id']} : study {study_id} : {missing_files} ")
                break

            elif not all( enaFile["status" ] == "complete" for enaFile in enaFiles):
                # All transfer records exist but not all are complete yet
                all_file_downloaded = False
                break

        if all_file_downloaded:
            all_downloaded_sub_ids.append(sub["_id"])

    # Batch-update all fully-downloaded submissions to "pending" status
    if all_downloaded_sub_ids:
        Submission().update_submission_pending(all_downloaded_sub_ids, component="study")


def make_snapshot(profile_id, target_ids, target_id, checklist_id, study_id):
    """Export a singlecell record to a manifest snapshot file. NOT YET IMPLEMENTED."""
    """
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to make snapshot!")

    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "deleted": get_not_deleted_flag(), "study_id" : study_id})
    if not singlecell:
        return dict(status='error', message="No record found.")
        
    #export singlecell record to manifest

    #update snapshot version of the singlecell record
    bytesstring = BytesIO()
    schemas = SinglecellSchemas().get_collection_handle().find_one({"name": singlecell["schema_name"]})
    SingleCellSchemasHandler().write_manifest(singlecell_schema=schemas, checklist_id=singlecell["checklist_id"], singlecell=singlecell, file_path=bytesstring)

    with open("my_file.txt", "wb") as binary_file:
        binary_file.write(bytesstring.getvalue())
    """
    return {"status":"error", "message": "Not Implemented yet."}
 


def get_snapshot_file(profile_id, study_id, snapshot_version):
    """
    Get the snapshot filename for a single cell study.
    The filename is in the format: study_id_snapshot_version_snapshot.xlsx
    """
    return f"{study_id}_snapshot_{snapshot_version}.xlsx"

"""
class _GET_ENA_FILE_PROCESSING_STATUS(threading.Thread):
    def __init__(self, profile_id, run_accession_number_map, data_map=dict(), columns=dict(), ena_file_transfer_map=dict()):
        self.profile_id = profile_id
        self.run_accession_number_map = run_accession_number_map
        self.data_map = data_map
        self.ena_file_transfer_map = ena_file_transfer_map

        super(_GET_ENA_FILE_PROCESSING_STATUS, self).__init__() 

    def run(self):
        sent_2_frontend_every = 4000
        #data = []
        i = 0
        #data = self.return_dict["dataSet"]
        ecs_file_complete = []

        for run_accession in self.run_accession_number_map.keys():
            i += 1
            file_processing_status = _query_ena_file_processing_status(run_accession)
            if file_processing_status:
               #data.append({"run_accession":run_accession, "msg":file_processing_status})

               row = self.data_map.get(self.run_accession_number_map.get(run_accession), dict())
               row["ena_file_processing_status"] = file_processing_status
               complete_cnt = file_processing_status.count("File archived")
               if complete_cnt > 0:
                   file_ids = row["DT_RowId"][4:].split("_")    #row_data["DT_RowId"] = "row_fileid1_fileid2"
                   if complete_cnt == len(file_ids):
                        for file_id in file_ids:
                            ecs_location = self.ena_file_transfer_map.get(file_id, {}).get("ecs_location","")
                            if ecs_location:
                                ecs_file_complete.append(ecs_location)


            if i == sent_2_frontend_every:                 
               #notify_read_status(data={"profile_id": self.profile_id, "file_processing_status":data},  msg="", action="file_processing_status" )
               notify_read_status(data={"profile_id": self.profile_id, "table_data" : list(self.data_map.values())},
                        msg="Refreshing table for file processing status", action="file_processing_status", html_id="sample_info")
               i = 0
               #data=[]
        if i>0:
            #notify_read_status(data={"profile_id": self.profile_id, "file_processing_status":data},  msg="", action="file_processing_status" )
            notify_read_status(data={"profile_id": self.profile_id, "table_data" : list(self.data_map.values())},
                        msg="Refreshing table for file processing status", action="file_processing_status", html_id="sample_info")
            
        if ecs_file_complete:
            EnaFileTransfer().complete_remote_transfer_status_by_ecs_path( ecs_locations=ecs_file_complete)
"""
