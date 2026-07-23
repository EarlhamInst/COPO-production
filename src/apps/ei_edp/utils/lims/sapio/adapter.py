"""Sapio implementation of the LIMS adapter.

All `sapiopylib` usage for the EDP flow lives here. The logic in each method was
lifted verbatim (behaviour-preserving) from the previous inline Sapio code in
`edp_utils.py` and `EDPSchemasHandler.py`; only the surrounding COPO-native
plumbing (profile fetches, group/email sharing, worksheet building) stayed
behind in those modules.

Sapio-specific concepts that must not leak into the generic `LIMSAdapter`
interface are contained here: the `Project`/`Sample`/`Plate` data model, the
96-well plate packing, and the `sapio_name` ("Object:Field") schema-column
convention used to map COPO terms to Sapio fields.
"""

import math
from typing import List, Dict, Any

import pandas as pd

from common.utils.logger import Logger
from sapiopylib.rest.utils.recordmodel.PyRecordModel import PyRecordModel

from .datamanager import Sapio
from ..base import LIMSAdapter, LIMSUnavailable

l = Logger()


def _describe_error(error: Exception) -> str:
    """One-line summary of a Sapio request failure: status + URL, never the body.

    Deliberately avoids ``str(error)``: a Sapio ``SapioServerException`` stringifies
    to the full response body — a large JSON blob containing a Java stack trace.
    We dig out the underlying ``requests`` Response (directly, or via the
    ``client_error`` that ``SapioServerException`` wraps) and report only the
    HTTP status, reason and URL. Falls back to the attempted request URL for
    connection errors, then to the bare exception class name.
    """
    response = getattr(error, "response", None)
    if response is None:
        # sapiopylib SapioServerException wraps the real requests error here
        wrapped = getattr(error, "client_error", None)
        response = getattr(wrapped, "response", None)
    if response is not None and getattr(response, "status_code", None):
        reason = (getattr(response, "reason", "") or "").strip()
        return f"HTTP {response.status_code} {reason}".rstrip() + f" from {response.url}"
    request = getattr(error, "request", None)
    if request is not None and getattr(request, "url", None):
        return f"could not reach {request.url}"
    return type(error).__name__


class SapioAdapter(LIMSAdapter):

    # ------------------------------------------------------------------ #
    # Form support
    # ------------------------------------------------------------------ #
    def get_sample_type_options(self) -> List[Dict[str, str]]:
        # Fetch the "Exemplar Sample Types" picklist from Sapio and return as
        # {value, label} pairs for use in COPO form dropdowns. The dropdown is
        # required for a valid submission, so a LIMS failure must abort the form
        # build (see LIMSUnavailable) rather than yield a partial form.
        try:
            config = Sapio().picklistManager.get_picklist("Exemplar Sample Types")
        except Exception as error:
            l.error(f"Sapio get_sample_type_options failed: {error}")
            raise LIMSUnavailable(
                "Sapio", "loading the Sample Types picklist", _describe_error(error)
            ) from error
        return [{"value": s, "label": s} for s in config.entry_list]

    # ------------------------------------------------------------------ #
    # Project lifecycle
    # ------------------------------------------------------------------ #
    def validate_profile_change(self, profile: Dict[str, Any],
                                requested_sample_count) -> Dict[str, str]:
        sapio_project_id = profile.get("sapio_project_id", "")
        if not sapio_project_id:
            return {"status": "success"}

        no_of_samples = requested_sample_count
        project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project",
                                                data_field_name="C_ProjectIdentifier",
                                                value_list=[sapio_project_id]).result_list
        if not project_records or len(project_records) == 0:
            return {"status": "error", "message": f"Sapio Project {profile['sapio_project_id']} not found."}
        project_record = project_records[0]
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], 'Sample')
        samples_under_project: List[PyRecordModel] = project.get_children_of_type('Sample')
        if samples_under_project:
            if len(samples_under_project) > int(no_of_samples):
                # Count how many samples already have a customer name — those can't be deleted
                diff = len(samples_under_project) - int(profile["no_of_samples"])
                for sample in samples_under_project:
                    if not sample.get_field_value("C_CustomerSampleName"):
                        diff -= 1

                    if diff <= 0:
                        break
                if diff > 0:
                    return {"status": "error", "message": f"Sapio Project {profile['sapio_project_id']} has customer samples associated. Cannot decrease the no. of samples."}

        return {"status": "success"}

    def sync_project(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        # ProjectName is Required in Sapio; use jira_ticket_number with fallback to profile title.
        project_name = profile.get("jira_ticket_number") or profile.get("title") or ""
        if not project_name:
            return {"status": "success", "project_id": profile.get("sapio_project_id", "")}

        # The project id we are working with; may be minted below on first save.
        # Returned in all paths (including partial failure) so the caller can
        # persist it and avoid creating a duplicate project on retry.
        project_id = profile.get("sapio_project_id", "")
        project_record = None
        try:
            if not profile.get("sapio_project_id", ""):
                # New profile — create a Sapio Project record.
                # Field names here must match the Project data type in the Sapio instance.
                # C_ProjectIdentifier is auto-generated by Sapio and used as our foreign key.
                project_fields = {
                    "ProjectName": project_name,
                    "ProjectDesc": profile.get("description") or "",
                    "C_SampleCount": int(profile.get("no_of_samples") or 0),
                }
                if profile.get("budget_user"):
                    project_fields["C_BudgetHolder"] = profile["budget_user"]
                project_records = Sapio().dataRecordManager.add_data_records_with_data(
                    data_type_name="Project", field_map_list=[project_fields])

                project_id = project_records[0].get_field_value('C_ProjectIdentifier')
                project_record = project_records[0]

                # Attach the new project to Directory 1 (the root directory in Sapio)
                directories = Sapio().dataRecordManager.query_data_records(data_type_name="Directory",
                                                            data_field_name="RecordId",
                                                            value_list=[1]).result_list
                directory_record = directories[0]
                directory: PyRecordModel = Sapio().inst_man.add_existing_record(directory_record)
                Sapio().relationship_man.load_children([directory], 'Project')
                project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
                directory.add_child(project)

            else:
                # Existing profile — look up the project by its Sapio identifier and update fields
                project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project",
                                                            data_field_name="C_ProjectIdentifier",
                                                            value_list=[profile["sapio_project_id"]]).result_list
                if not project_records or len(project_records) == 0:
                    raise Exception(f"Failed to Find Sapio Project {profile['sapio_project_id']}")
                project_record = project_records[0]
                project_record.set_field_value("ProjectName", project_name)
                project_record.set_field_value("ProjectDesc", profile.get("description") or "")
                project_record.set_field_value("C_SampleCount", int(profile.get("no_of_samples") or 0))
                if profile.get("budget_user"):
                    project_record.set_field_value("C_BudgetHolder", profile["budget_user"])
                Sapio().dataRecordManager.commit_data_records([project_record])

            # Load all samples and plates currently linked to this project
            project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
            Sapio().relationship_man.load_children([project], 'Sample')
            samples_under_project: List[PyRecordModel] = project.get_children_of_type('Sample')
            samples_under_project = sorted(samples_under_project, key=lambda x: x.get_field_value("PlateId"))
            Sapio().relationship_man.load_children([project], 'Plate')
            plates_under_project: List[PyRecordModel] = project.get_children_of_type('Plate')

            # Track which plates need samples removed after deletion
            assigned_plates_map_for_samples_to_delete = {}
            samples_to_remove = []

            existing_plate_ids_under_project = set()
            for plate in plates_under_project:
                existing_plate_ids_under_project.add(plate.get_field_value("PlateId"))

            no_of_samples = int(profile.get("no_of_samples") or 0)
            sample_type = profile.get("sample_type") or ""
            container_type = profile.get("container_type") or ""
            library_type = profile.get("library_type") or ""

            # Create samples up to the requested count if we don't have enough yet
            if not samples_under_project or len(samples_under_project) < no_of_samples:
                existing_no_of_samples = len(samples_under_project) if samples_under_project else 0
                sample_records = Sapio().dataRecordManager.add_data_records_with_data(data_type_name="Sample",
                                                                                      field_map_list=[{"ExemplarSampleType": sample_type,
                                                                                                      "ContainerType": container_type,
                                                                                                      "C_LibraryType": library_type}
                                                                                                      for _ in range(existing_no_of_samples, no_of_samples)])
                samples: List[PyRecordModel] = Sapio().inst_man.add_existing_records(sample_records)
                project.add_children(samples)
                samples_under_project.extend(samples)

            # Remove excess samples, but only those without a customer name (named samples are committed)
            diff = len(samples_under_project) - no_of_samples
            if diff > 0:
                for sample in samples_under_project:
                    if not sample.get_field_value("C_CustomerSampleName"):
                        samples_to_remove.append(sample)
                        diff -= 1
                        assigned_plate_id = sample.get_field_value("PlateId")
                        if assigned_plate_id:
                            if assigned_plate_id not in assigned_plates_map_for_samples_to_delete:
                                assigned_plates_map_for_samples_to_delete[assigned_plate_id] = []
                            assigned_plates_map_for_samples_to_delete[assigned_plate_id].append(sample)
                    if diff <= 0:
                        break
                if diff > 0:
                    raise Exception(f"Sapio Project {profile['sapio_project_id']} has customer samples associated. Cannot decrease the no. of samples.")

                project.remove_children(samples_to_remove)
                samples_under_project = [s for s in samples_under_project if s not in samples_to_remove]

            # Sync sample type fields on all remaining samples
            for sample in samples_under_project:
                sample.set_field_value("ExemplarSampleType", sample_type)
                sample.set_field_value("ContainerType", container_type)
                sample.set_field_value("C_LibraryType", library_type)

            plates_under_project_map = {plate.get_field_value("PlateId"): plate for plate in plates_under_project}

            # Detach removed samples from their plates before deletion
            for plate_id, samples in assigned_plates_map_for_samples_to_delete.items():
                plate = plates_under_project_map.get(plate_id, None)
                if plate:
                    plate.remove_children(samples)

            # Find samples not yet assigned to any plate (PlateId and StorageUnitPath both unset)
            samples_without_plate = set()
            if samples_under_project:
                for sample in samples_under_project:
                    assigned_plate = sample.get_field_value("PlateId")
                    if not assigned_plate:
                        assigned_plate = sample.get_field_value("StorageUnitPath")
                        if not assigned_plate:
                            samples_without_plate.add(sample)

            # Fill empty slots in existing plates first before creating new ones
            Sapio().relationship_man.load_children(plates_under_project, 'Sample')
            if samples_without_plate:
                for plate in plates_under_project:
                    sample_for_plate: List[PyRecordModel] = []
                    # Build a position map for every well in a standard 96-well plate (8 rows × 12 columns)
                    plate_assignments = {(str(column), row): False for column in range(1, 13) for row in ["A", "B", "C", "D", "E", "F", "G", "H"]}
                    samples_under_plate: List[PyRecordModel] = plate.get_children_of_type('Sample')
                    for sample in samples_under_plate:
                        plate_assignments_key = (sample.get_field_value("ColPosition"), sample.get_field_value("RowPosition"))
                        plate_assignments[plate_assignments_key] = True

                    for _ in range(len(samples_under_plate), 96):
                        if not samples_without_plate:
                            break
                        key = next((k for k, v in plate_assignments.items() if not v), None)
                        if not key:
                            l.error("No more positions available in plate when assigning samples!")
                            break
                        sample = samples_without_plate.pop()
                        sample_for_plate.append(sample)
                        sample.set_field_value("PlateId", plate.get_field_value("PlateId"))
                        plate_assignments[key] = True
                        sample.set_field_value("ColPosition", key[0])
                        sample.set_field_value("RowPosition", key[1])
                    if sample_for_plate:
                        plate.add_children(sample_for_plate)

            # If samples still remain unplated, create as many new 96-well plates as needed
            if samples_without_plate:
                no_of_plates_needed = math.ceil(len(samples_without_plate) / 96)
                new_plate_records = Sapio().dataRecordManager.add_data_records_with_data(data_type_name="Plate",
                                                                                      field_map_list=[{"PlateSampleType": sample_type,
                                                                                                      "PlateColumns": 12, "PlateRows": 8}
                                                                                                      for _ in range(no_of_plates_needed)])
                new_plates: List[PyRecordModel] = Sapio().inst_man.add_existing_records(new_plate_records)
                project.add_children(new_plates)
                Sapio().relationship_man.load_children(new_plates, 'Sample')

                for plate in new_plates:
                    sample_for_plate: List[PyRecordModel] = []
                    plate_assignments = {(str(column), row): False for column in range(1, 13) for row in ["A", "B", "C", "D", "E", "F", "G", "H"]}
                    for _ in range(96):
                        if not samples_without_plate:
                            break
                        key = next((k for k, v in plate_assignments.items() if not v), None)
                        if not key:
                            l.error("No more positions available in plate when assigning samples!")
                            break
                        sample = samples_without_plate.pop()
                        sample_for_plate.append(sample)
                        sample.set_field_value("PlateId", plate.get_field_value("PlateId"))
                        plate_assignments[key] = True
                        sample.set_field_value("ColPosition", key[0])
                        sample.set_field_value("RowPosition", key[1])
                    if sample_for_plate:
                        plate.add_children(sample_for_plate)

            Sapio().rec_man.store_and_commit()
            # Hard-delete removed sample records from Sapio (recursive to clean up children)
            Sapio().dataRecordManager.delete_data_record_list([sample.get_data_record() for sample in samples_to_remove], recursive_delete=True)

            if samples_without_plate:
                l.error("Not all samples have been assigned to plates!")
                return {"status": "warning", "project_id": project_id, "message": "Profile has been saved. However, it is failed to update to Sapio! "}

        except Exception as e:
            l.exception(e)
            l.error("Failed to create or update sapio project for profile id: " + str(profile["_id"]) + " Error: " + str(e))
            return {"status": "warning", "project_id": project_id, "message": "Profile has been saved. However, it is failed to update to Sapio! "}

        return {"status": "success", "project_id": project_id}

    def delete_project(self, project_id: str) -> Dict[str, str]:
        try:
            record = Sapio().dataRecordManager.query_data_records(data_type_name="Project",
                                                    data_field_name="C_ProjectIdentifier",
                                                    value_list=[project_id]).result_list[0]
            Sapio().dataRecordManager.delete_data_record(record=record, recursive_delete=True)
        except Exception as e:
            l.exception(e)
            l.error("Failed to delete sapio project " + str(project_id) + " Error: " + str(e))
            return {"status": "warning", "message": "Profile has been deleted. However, it is failed to delete from Sapio! "}
        return {"status": "success"}

    # ------------------------------------------------------------------ #
    # Manifest
    # ------------------------------------------------------------------ #
    def _sample_field_map(self, schemas: Dict[str, Any]) -> Dict[str, str]:
        """Map COPO term_name → Sapio field for the sample component.

        Built from the `sapio_name` schema column ("Object:Field"), keeping only
        fields whose object is `Sample`.
        """
        sapio_column_map = {}
        sample_schema = schemas.get("sample", [])
        for field in sample_schema:
            sapio_name = field.get("sapio_name")
            # sapio_name can be NaN (a float) when the schema cell is blank — guard on
            # str, not truthiness, since NaN is truthy and ":" in <float> raises.
            if isinstance(sapio_name, str) and ":" in sapio_name:
                sapio_object = sapio_name.split(":")[0]
                sapio_field = sapio_name.split(":")[1]
                if sapio_object.lower() == "sample":
                    sapio_column_map[field["term_name"]] = sapio_field
        return sapio_column_map

    def _load_project_samples(self, project_id: str) -> List[PyRecordModel]:
        project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project",
                                                data_field_name="C_ProjectIdentifier",
                                                value_list=[project_id]).result_list
        if not project_records or len(project_records) == 0:
            l.error(f"Sapio Project {project_id} not found.")
            return []
        project_record = project_records[0]
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], 'Sample')
        return project.get_children_of_type('Sample')

    def get_project_samples(self, project_id: str,
                            schemas: Dict[str, Any]) -> List[Dict[str, Any]]:
        samples_under_project = self._load_project_samples(project_id)
        if not samples_under_project:
            return []
        sapio_column_map = self._sample_field_map(schemas)
        records_list = []
        for sample in samples_under_project:
            record_dict = {}
            for term_name, sapio_field in sapio_column_map.items():
                record_dict[term_name] = sample.get_field_value(sapio_field)
            records_list.append(record_dict)
        return records_list

    def get_project_metadata(self, project_id: str) -> Dict[str, Any]:
        project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project",
                                                data_field_name="C_ProjectIdentifier",
                                                value_list=[project_id]).result_list
        if not project_records or len(project_records) == 0:
            l.error(f"Sapio Project {project_id} not found.")
            return {}
        project_record = project_records[0]
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], 'Sample')
        samples_under_project: List[PyRecordModel] = project.get_children_of_type('Sample')

        metadata = {"health_and_safety": project.get_field_value("C_HandS")}
        if samples_under_project:
            metadata["sample_return"] = samples_under_project[0].get_field_value("C_SampleReturn")
        return metadata

    def submit_manifest(self, project_id: str, schemas: Dict[str, Any],
                        components: Dict[str, Any]) -> Dict[str, str]:
        project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project",
                                            data_field_name="C_ProjectIdentifier",
                                            value_list=[project_id]).result_list
        if not project_records or len(project_records) == 0:
            return {"status": "error", "message": f"Sapio Project {project_id} not found."}

        project_record = project_records[0]

        # Load samples under the project and index by SampleId for fast lookup
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], 'Sample')
        samples_under_project: List[PyRecordModel] = project.get_children_of_type('Sample')
        samples_under_project_map = {s.get_field_value("SampleId"): s for s in samples_under_project}

        # Build a mapping from COPO term_name → Sapio field name, grouped by Sapio data type.
        # Only fields with a sapio_name in "DataType:FieldName" format and not marked
        # "protected" are included.
        sapio_mapping_df = pd.DataFrame(columns=["term_name", "sapio_name"])
        for component_name, component_schema in schemas.items():
            component_schema_df = pd.DataFrame.from_records(component_schema)
            sapio_component_mapping_df = component_schema_df[(~pd.isna(component_schema_df["sapio_name"])
                                                               & (component_schema_df["sapio_name"].str.contains(":"))
                                                               & (component_schema_df["term_manifest_behavior"] != "protected"))
                                                               ][["term_name", "sapio_name"]]
            sapio_mapping_df = pd.concat([sapio_mapping_df, sapio_component_mapping_df], ignore_index=True)

        sapio_mapping_df["sapio_object"] = sapio_mapping_df["sapio_name"].apply(lambda x: x.split(":")[0])
        sapio_mapping_df["sapio_field"] = sapio_mapping_df["sapio_name"].apply(lambda x: x.split(":")[1])
        sapio_mapping_df.drop(columns=["sapio_name"], inplace=True)

        # Convert to {DataType: {term_name: sapio_field}} dict for O(1) lookups below
        sapio_object_dict = {}
        for c, sapio_object_df in sapio_mapping_df.groupby("sapio_object", sort=False):
            sapio_object_df.set_index("term_name", inplace=True)
            sapio_object_dict[c] = sapio_object_df["sapio_field"].to_dict()

        project_sapio_mapping = sapio_object_dict.get("Project", {})
        sample_sapio_mapping = sapio_object_dict.get("Sample", {})

        for component_name, component_schema in schemas.items():
            component_schema_df = pd.DataFrame.from_records(component_schema)
            component_data_df = pd.DataFrame.from_records(components.get(component_name, []))
            if component_data_df.empty:
                continue

            # Drop columns not in the schema (e.g. internal COPO metadata fields)
            columns = component_data_df.columns
            component_data_df.drop(columns=[column for column in columns if column not in component_schema_df["term_name"].values]
                                   , axis=1, inplace=True)

            if component_name == "study":
                # Study fields map to the Sapio Project record, and some also propagate to all samples
                for index, row in component_data_df.iterrows():
                    for column in component_data_df.columns:
                        sapio_field = project_sapio_mapping.get(column, "")
                        if sapio_field:
                            project.set_field_value(sapio_field, row[column])
                        sapio_field = sample_sapio_mapping.get(column, "")
                        if sapio_field:
                            for sapio_sample in samples_under_project:
                                sapio_sample.set_field_value(sapio_field, row[column])

            if component_name == "sample":
                # Sample fields are matched by SampleId to the specific Sapio Sample record
                for index, row in component_data_df.iterrows():
                    sapio_sample_id = row.get("sample_id", "")
                    sapio_sample = samples_under_project_map.get(sapio_sample_id, None)
                    if not sapio_sample:
                        l.error(f"Sample with Sample ID {sapio_sample_id} not found in Sapio Project {project_id}. Skipping...")
                        continue
                    for column in component_data_df.columns:
                        sapio_field = sample_sapio_mapping.get(column, "")
                        if sapio_field:
                            sapio_sample.set_field_value(sapio_field, row[column])

        Sapio().rec_man.store_and_commit()

        return {"status": "success", "message": f"EDP data submitted to Sapio Project {project_id} successfully."}
