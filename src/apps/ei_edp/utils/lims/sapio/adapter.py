'''Sapio implementation of the LIMS adapter.

All `sapiopylib` usage for the EDP flow lives here. The logic in each method was
lifted verbatim (behaviour-preserving) from the previous inline Sapio code in
`edp_utils.py` and `EDPSchemaHandler.py`; only the surrounding COPO-native
plumbing (profile fetches, group/email sharing, worksheet building) stayed
behind in those modules.

Sapio-specific concepts that must not leak into the generic `LIMSAdapter`
interface are contained here: the `Project`/`Sample`/`Plate` data model, the
96-well plate packing, and the `sapio_name` ("Object:Field") schema-column
convention used to map COPO terms to Sapio fields.
'''

import math
from typing import Any, Dict, List

import pandas as pd
from django.conf import settings
from sapiopylib.rest.User import SapioServerException
from sapiopylib.rest.utils.recordmodel.PyRecordModel import PyRecordModel

from common.utils.logger import Logger

from ...constants import (
    BATCH_ID,
    BATCH_LIMS_SAMPLE_RECORD_IDS,
    COPO_TO_SAPIO_VALUES,
    LIMS_HEALTH_AND_SAFETY,
    LIMS_SAMPLE_RETURN,
    PROFILE_BUDGET_HOLDER,
    PROFILE_CONTAINER_TYPE,
    PROFILE_DESCRIPTION,
    PROFILE_LIBRARY_TYPE,
    PROFILE_NO_OF_SAMPLES,
    PROFILE_RELATED_ENQ,
    PROFILE_SAMPLE_TYPE,
    PROFILE_SAPIO_PROJECT_ID,
    PROFILE_TITLE,
    SAMPLE_ID,
    SAMPLE_TO_QC_DATUM_FIELDS,
    SAPIO_ALIQUOT_SAMPLE_RECORD_ID,
    SAPIO_BUDGET_HOLDER,
    SAPIO_COL_POSITION,
    SAPIO_CONTAINER_TYPE,
    SAPIO_CUSTOMER_SAMPLE_NAME,
    SAPIO_DATA_RECORD_NAME,
    SAPIO_DATUM_TYPE,
    SAPIO_DEPARTMENT_DATA_TYPE,
    SAPIO_DIRECTORY_DATA_TYPE,
    SAPIO_EXEMPLAR_SAMPLE_STATUS,
    SAPIO_EXEMPLAR_SAMPLE_TYPE,
    SAPIO_HEALTH_AND_SAFETY,
    SAPIO_INDEX_BARCODE_DATA_TYPE,
    SAPIO_INDEX_ID,
    SAPIO_IS_CONTROL,
    SAPIO_LAYER,
    SAPIO_LIBRARY_TYPE,
    SAPIO_NAME,
    SAPIO_OTHER_SAMPLE_ID,
    SAPIO_PLATE_COLUMNS,
    SAPIO_PLATE_DATA_TYPE,
    SAPIO_PLATE_ID,
    SAPIO_PLATE_RECORD_ID,
    SAPIO_PLATE_ROWS,
    SAPIO_PLATE_SAMPLE_TYPE,
    SAPIO_PLATE_WELL_ELEMENT_DATA_TYPE,
    SAPIO_PROJECT_DATA_TYPE,
    SAPIO_PROJECT_DESC,
    SAPIO_PROJECT_IDENTIFIER,
    SAPIO_PROJECT_NAME,
    SAPIO_QC_DATUM_DATA_TYPE,
    SAPIO_RECORD_ID,
    SAPIO_ROW_POSITION,
    SAPIO_SAMPLE_DATA_TYPE,
    SAPIO_SAMPLE_ID,
    SAPIO_SAMPLE_RETURN,
    SAPIO_SOURCE_DATA_TYPE_NAME,
    SAPIO_SOURCE_RECORD_ID,
    SAPIO_STORAGE_UNIT_PATH,
    TERM_ACCESS,
    TERM_NAME,
)
from ...edp_validation_messages import MESSAGES
from ...sample_batches import get_sample_batches
from ..base import LIMSAdapter, LIMSUnavailable
from .datamanager import Sapio

l = Logger()

# `Customer Supplied` refers to when customers take
# measurements and report them in a manifest
CUSTOMER_SUPPLIED_DATUM_TYPE = 'Customer Supplied'

# Samples are arranged onto standard 96-well plates (8 rows × 12 columns)
PLATE_ROWS = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
PLATE_COLUMNS = 12
PLATE_WELLS = len(PLATE_ROWS) * PLATE_COLUMNS


def _describe_error(error: Exception) -> str:
    """One-line summary of a Sapio request failure: status + URL, never the body.

    Deliberately avoids ``str(error)``: a Sapio ``SapioServerException`` stringifies
    to the full response body - a large JSON blob containing a Java stack trace.
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
        return (
            f"HTTP {response.status_code} {reason}".rstrip() + f" from {response.url}"
        )
    request = getattr(error, "request", None)
    if request is not None and getattr(request, "url", None):
        return f"could not reach {request.url}"
    return type(error).__name__


def _is_blank(value):
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return isinstance(value, str) and not value.strip()


def _to_sapio_value(term_name, value):
    # Swap a LIMS manifest value for the value that Sapio expects
    # if the field has one listed in `COPO_TO_SAPIO_VALUES`.
    # Values typed into a `suggested_enum`` dropdown menu can list more
    # than one answer separated by semicolons (e.g. 'UK-GDPR; CITES')
    # so, each answer is swapped separately and any duplicates
    # are removed (e.g. 'UK-GDPR; EU-GDPR' becomes 'GDPR').
    sapio_values = {
        copo_value.upper(): sapio_value
        for copo_value, sapio_value in COPO_TO_SAPIO_VALUES.get(term_name, {}).items()
    }
    if not isinstance(value, str) or not sapio_values:
        return value

    answers = [answer.strip() for answer in value.split(';') if answer.strip()]
    swapped_answers = [sapio_values.get(answer.upper(), answer) for answer in answers]
    if swapped_answers == answers:
        return value
    return '; '.join(dict.fromkeys(swapped_answers))


def _to_number(value):
    # QCDatum's QC fields (A260280, RIN, ...) are Decimal fields in Sapio, but
    # manifest cells can arrive as strings
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return value
    return value

class _SapioSyncWarning(Exception):
    # A sync failure whose message is shown to the user as it is
    pass


def _discard_pending_changes():
    # The record model manager is shared by every request in this process
    # and it only clears the changes queued in it after a successful commit.
    # Otherwise, the changes from a failed sync are sent again with the next
    # commit, which then fails as well (e.g. on a project that has since been deleted)
    Sapio().rec_man.rollback()


def _group_samples_by_batch(batches, samples_under_project):
    '''Pair each batch with its samples in the Sapio project.

    A batch whose record IDs are `None` was moved from the fields an EDP
    profile used to have so, it claims every sample in the project that
    no other batch has.
    '''
    samples_by_record_id = {sample.record_id: sample for sample in samples_under_project}
    claimed_record_ids = {
        record_id
        for batch in batches
        for record_id in (batch.get(BATCH_LIMS_SAMPLE_RECORD_IDS) or [])
    }
    for batch in batches:
        record_ids = batch.get(BATCH_LIMS_SAMPLE_RECORD_IDS)
        if record_ids is None:
            batch_samples = [
                sample
                for sample in samples_under_project
                if sample.record_id not in claimed_record_ids
            ]
            claimed_record_ids.update(sample.record_id for sample in batch_samples)
        else:
            batch_samples = [
                samples_by_record_id[record_id]
                for record_id in record_ids
                if record_id in samples_by_record_id
            ]
        yield batch, batch_samples


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
    def _load_project(self, project_id: str):
        project_records = (
            Sapio()
            .dataRecordManager.query_data_records(
                data_type_name=SAPIO_PROJECT_DATA_TYPE,
                data_field_name=SAPIO_PROJECT_IDENTIFIER,
                value_list=[project_id],
            )
            .result_list
        )
        if not project_records:
            return None
        return Sapio().inst_man.add_existing_record(project_records[0])

    def validate_profile_change(self, profile: Dict[str, Any]) -> Dict[str, str]:
        sapio_project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, "")
        if not sapio_project_id:
            return {"status": "success"}

        if not self._load_project(sapio_project_id):
            return {
                "status": "error",
                "message": MESSAGES['sapio_project_not_found'].format(
                    project_id=sapio_project_id
                ),
            }
        return {"status": "success"}

    def get_submitted_sample_counts(self, profile: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        # Samples that have a customer's name associated with them have been
        # submitted, so the batch they belong to can no longer be changed
        sapio_project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, "")
        batches = get_sample_batches(profile)
        project = self._load_project(sapio_project_id) if sapio_project_id else None
        samples_under_project: List[PyRecordModel] = []
        if project:
            Sapio().relationship_man.load_children([project], SAPIO_SAMPLE_DATA_TYPE)
            samples_under_project = project.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)

        return {
            batch[BATCH_ID]: {
                'submitted': sum(
                    1
                    for sample in batch_samples
                    if sample.get_field_value(SAPIO_CUSTOMER_SAMPLE_NAME)
                ),
                'total': len(batch_samples),
            }
            for batch, batch_samples in _group_samples_by_batch(batches, samples_under_project)
        }

    def delete_samples(self, record_ids: List[int]) -> Dict[str, str]:
        if not record_ids:
            return {"status": "success"}
        try:
            records = []
            page_criteria = None
            while True:
                page = Sapio().dataRecordManager.query_data_records_by_id(
                    SAPIO_SAMPLE_DATA_TYPE, list(record_ids), paging_criteria=page_criteria
                )
                records.extend(page.result_list)
                if not page.is_next_page_available:
                    break
                page_criteria = page.next_page_criteria
            Sapio().dataRecordManager.delete_data_record_list(
                records, recursive_delete=True
            )
        except Exception as e:
            l.exception(e)
            l.error(f"Failed to delete sapio samples {record_ids}. Error: {e}")
            return {"status": "warning", "message": MESSAGES['sapio_project_update_failed']}
        return {"status": "success"}

    def _resize_batch(
        self,
        project: PyRecordModel,
        plates_under_project: List[PyRecordModel],
        batch: Dict[str, Any],
        batch_samples: List[PyRecordModel],
        created_sample_record_ids: List[int],
    ):
        # Create or remove samples so that the batch has as many as it requests.
        # Returns the batch's samples and the samples to delete from Sapio.
        no_of_samples = int(batch.get(PROFILE_NO_OF_SAMPLES) or 0)
        batch_samples = sorted(
            batch_samples, key=lambda sample: sample.get_field_value(SAPIO_PLATE_ID) or ''
        )

        if len(batch_samples) < no_of_samples:
            try:
                sample_records = Sapio().dataRecordManager.add_data_records_with_data(
                    data_type_name=SAPIO_SAMPLE_DATA_TYPE,
                    field_map_list=[
                        {
                            SAPIO_EXEMPLAR_SAMPLE_TYPE: batch.get(PROFILE_SAMPLE_TYPE) or '',
                            SAPIO_CONTAINER_TYPE: (batch.get(PROFILE_CONTAINER_TYPE) or '').capitalize(),
                            SAPIO_LIBRARY_TYPE: batch.get(PROFILE_LIBRARY_TYPE) or '',
                            SAPIO_EXEMPLAR_SAMPLE_STATUS: 'Logged',
                        }
                        for _ in range(len(batch_samples), no_of_samples)
                    ],
                )
            except SapioServerException as e:
                body = getattr(getattr(e.client_error, "response", None), "text", "") or ""
                if "Duplicate values exist on fields that must be unique" in body:
                    l.error(f"Sapio sample ID duplication when creating samples: {body}")
                    raise _SapioSyncWarning(MESSAGES['sapio_duplicate_sample_ids']) from e
                raise
            new_samples: List[PyRecordModel] = Sapio().inst_man.add_existing_records(
                sample_records
            )
            created_sample_record_ids.extend(sample.record_id for sample in new_samples)
            project.add_children(new_samples)
            batch_samples.extend(new_samples)

        # Remove excess samples, but only those without a customer name (named samples are committed).
        # Given that samples are sorted by plate, excess samples will be removed from the last
        # plate first so as to avoid leaving gaps on earlier plates
        samples_to_remove = []
        excess = len(batch_samples) - no_of_samples
        for sample in reversed(batch_samples):
            if excess <= 0:
                break
            if not sample.get_field_value(SAPIO_CUSTOMER_SAMPLE_NAME):
                samples_to_remove.append(sample)
                excess -= 1
        if excess > 0:
            raise Exception(
                f"The number of samples in batch '{batch.get(BATCH_ID)}' cannot be decreased "
                "because customer samples are already associated with it."
            )

        if samples_to_remove:
            removed_record_ids = {sample.record_id for sample in samples_to_remove}
            # Detach removed samples from their plates before deletion
            for plate in plates_under_project:
                samples_on_plate = [
                    sample
                    for sample in plate.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)
                    if sample.record_id in removed_record_ids
                ]
                if samples_on_plate:
                    plate.remove_children(samples_on_plate)
            project.remove_children(samples_to_remove)
            batch_samples = [
                sample for sample in batch_samples if sample.record_id not in removed_record_ids
            ]

        return batch_samples, samples_to_remove

    def _fill_plate(self, plate: PyRecordModel, unplated_samples: List[PyRecordModel]):
        # Place samples in the plate's empty wells, column by column
        taken_wells = {
            (sample.get_field_value(SAPIO_COL_POSITION), sample.get_field_value(SAPIO_ROW_POSITION))
            for sample in plate.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)
        }
        samples_for_plate: List[PyRecordModel] = []
        for column in range(1, PLATE_COLUMNS + 1):
            for row in PLATE_ROWS:
                if not unplated_samples:
                    break
                if (str(column), row) in taken_wells:
                    continue
                sample = unplated_samples.pop(0)
                sample.set_field_value(SAPIO_PLATE_ID, plate.get_field_value(SAPIO_PLATE_ID))
                sample.set_field_value(SAPIO_COL_POSITION, str(column))
                sample.set_field_value(SAPIO_ROW_POSITION, row)
                samples_for_plate.append(sample)
        if samples_for_plate:
            plate.add_children(samples_for_plate)

    def _place_batch_on_plates(
        self,
        project: PyRecordModel,
        plates_under_project: List[PyRecordModel],
        batch_samples: List[PyRecordModel],
        sample_type: str,
    ) -> bool:
        # Plates are kept to one batch so that a plate never mixes sample types.
        # Samples not yet assigned to any plate (PlateId and StorageUnitPath both unset)
        # fill the batch's own plates first before new plates are created.
        # Returns True if any of the batch's samples could not be placed.
        unplated_samples = [
            sample
            for sample in batch_samples
            if not sample.get_field_value(SAPIO_PLATE_ID)
            and not sample.get_field_value(SAPIO_STORAGE_UNIT_PATH)
        ]
        if not unplated_samples:
            return False

        batch_record_ids = {sample.record_id for sample in batch_samples}
        for plate in plates_under_project:
            if any(
                sample.record_id in batch_record_ids
                for sample in plate.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)
            ):
                self._fill_plate(plate, unplated_samples)

        # If samples still remain unplated, create as many new 96-well plates as needed
        if unplated_samples:
            new_plate_records = Sapio().dataRecordManager.add_data_records_with_data(
                data_type_name=SAPIO_PLATE_DATA_TYPE,
                field_map_list=[
                    {
                        SAPIO_PLATE_SAMPLE_TYPE: sample_type,
                        SAPIO_PLATE_COLUMNS: PLATE_COLUMNS,
                        SAPIO_PLATE_ROWS: len(PLATE_ROWS),
                    }
                    for _ in range(math.ceil(len(unplated_samples) / PLATE_WELLS))
                ],
            )
            new_plates: List[PyRecordModel] = Sapio().inst_man.add_existing_records(
                new_plate_records
            )
            project.add_children(new_plates)
            Sapio().relationship_man.load_children(new_plates, SAPIO_SAMPLE_DATA_TYPE)
            plates_under_project.extend(new_plates)
            for plate in new_plates:
                self._fill_plate(plate, unplated_samples)

        return bool(unplated_samples)

    def _take_batch_off_plates(
        self, plates_under_project: List[PyRecordModel], batch_samples: List[PyRecordModel]
    ):
        # Take a batch's samples off any plates they were placed on while
        # its container type was 'plate'. Their well elements are removed
        # when the plates' well elements are synced.
        batch_record_ids = {sample.record_id for sample in batch_samples}
        for plate in plates_under_project:
            samples_on_plate = [
                sample
                for sample in plate.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)
                if sample.record_id in batch_record_ids
            ]
            if not samples_on_plate:
                continue
            for sample in samples_on_plate:
                sample.set_field_value(SAPIO_PLATE_ID, None)
                sample.set_field_value(SAPIO_COL_POSITION, None)
                sample.set_field_value(SAPIO_ROW_POSITION, None)
            plate.remove_children(samples_on_plate)

    def _attach_project_to_department(self, project: PyRecordModel, dry_run=False):
        # 'VeloxDepartment' is Sapio's built-in department data type.
        # The 'Projects' folder in Sapio's UI navigator is how it groups
        # a department's children of type 'Project'.
        # Returns 'linked', 'already_linked' or 'department_not_found'.
        # A dry run only says whether the project would be linked.
        department_records = (
            Sapio()
            .dataRecordManager.query_data_records(
                data_type_name=SAPIO_DEPARTMENT_DATA_TYPE,
                data_field_name=SAPIO_DATA_RECORD_NAME,
                value_list=[settings.SAPIO_PROJECT_DEPARTMENT],
            )
            .result_list
        )

        if not department_records:
            msg = MESSAGES['sapio_department_not_found'].format(
                department=settings.SAPIO_PROJECT_DEPARTMENT
            )
            l.error(msg)
            return 'department_not_found'

        department: PyRecordModel = Sapio().inst_man.add_existing_record(
            department_records[0]
        )
        Sapio().relationship_man.load_children([department], SAPIO_PROJECT_DATA_TYPE)

        # Skip if the project is already linked to the department in Sapio
        if any(
            child.record_id == project.record_id
            for child in department.get_children_of_type(SAPIO_PROJECT_DATA_TYPE)
        ):
            return 'already_linked'

        if not dry_run:
            department.add_child(project)
        return 'linked'

    def _sync_plate_well_elements(self, plates: List[PyRecordModel], dry_run=False):
        # Sync 'PlateDesignerWellElement' records with the samples on each plate.
        # Create missing ones and delete those whose sample is no longer on the plate.
        # Sapio's Plate View needs one per well to show the sample; without it the
        # well is shown as a black colour with no sample details on hover.
        # Returns (number added, number removed). A dry run changes nothing.
        if not plates:
            return 0, 0

        existing_well_elements = []
        page_criteria = None
        while True:
            page = Sapio().dataRecordManager.query_data_records(
                data_type_name=SAPIO_PLATE_WELL_ELEMENT_DATA_TYPE,
                data_field_name=SAPIO_PLATE_RECORD_ID,
                value_list=[plate.record_id for plate in plates],
                paging_criteria=page_criteria,
            )
            existing_well_elements.extend(page.result_list)
            if not page.is_next_page_available:
                break
            page_criteria = page.next_page_criteria

        samples_on_plates = {
            (plate.record_id, sample.record_id): sample
            for plate in plates
            for sample in plate.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)
        }
        well_elements_to_keep = set()
        well_elements_to_delete = []
        for well_element in existing_well_elements:
            key = (
                well_element.get_field_value(SAPIO_PLATE_RECORD_ID),
                well_element.get_field_value(SAPIO_ALIQUOT_SAMPLE_RECORD_ID),
            )
            if key in samples_on_plates and key not in well_elements_to_keep:
                well_elements_to_keep.add(key)
            else:
                well_elements_to_delete.append(well_element)

        # Samples are treated as both the original source and the aliquot 
        # if they are placed directly onto a plate without being split 
        # or transferred from another container.
        new_well_elements = [
            {
                SAPIO_PLATE_RECORD_ID: plate_record_id,
                SAPIO_ROW_POSITION: sample.get_field_value(SAPIO_ROW_POSITION),
                SAPIO_COL_POSITION: sample.get_field_value(SAPIO_COL_POSITION),
                SAPIO_LAYER: 1,
                SAPIO_SOURCE_DATA_TYPE_NAME: SAPIO_SAMPLE_DATA_TYPE,
                SAPIO_SOURCE_RECORD_ID: sample.record_id,
                SAPIO_ALIQUOT_SAMPLE_RECORD_ID: sample.record_id,
                SAPIO_IS_CONTROL: False,
            }
            for (plate_record_id, _), sample in samples_on_plates.items()
            if (plate_record_id, sample.record_id) not in well_elements_to_keep
        ]

        if dry_run:
            return len(new_well_elements), len(well_elements_to_delete)

        if well_elements_to_delete:
            Sapio().dataRecordManager.delete_data_record_list(well_elements_to_delete)
        if new_well_elements:
            Sapio().dataRecordManager.add_data_records_with_data(
                data_type_name=SAPIO_PLATE_WELL_ELEMENT_DATA_TYPE,
                field_map_list=new_well_elements,
            )
        return len(new_well_elements), len(well_elements_to_delete)

    def sync_project(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        # The project ID might be minted below on first save.
        # Returned in all paths (including partial failure) so the caller can
        # persist it and avoid creating a duplicate project on retry.
        project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, "")
        # Returned so that the caller can link each batch to its samples and,
        # if the sync fails, remove the samples it created
        batch_sample_record_ids: Dict[str, List[int]] = {}
        created_sample_record_ids: List[int] = []

        # ProjectName is Required in Sapio; use related_enq with fallback to profile title.
        project_name = profile.get(PROFILE_RELATED_ENQ) or profile.get(PROFILE_TITLE) or ""
        if not project_name:
            return {"status": "success", "project_id": project_id}
        project_record = None
        try:
            if not project_id:
                # New profile - create a Sapio Project record.
                # Field names here must match the Project data type in the Sapio instance.
                # C_ProjectIdentifier is auto-generated by Sapio and used as our foreign key.
                project_fields = {
                    SAPIO_PROJECT_NAME: project_name,
                    SAPIO_PROJECT_DESC: profile.get(PROFILE_DESCRIPTION) or "",
                }
                if profile.get(PROFILE_BUDGET_HOLDER):
                    project_fields[SAPIO_BUDGET_HOLDER] = profile[PROFILE_BUDGET_HOLDER]
                project_records = Sapio().dataRecordManager.add_data_records_with_data(
                    data_type_name=SAPIO_PROJECT_DATA_TYPE, field_map_list=[project_fields]
                )

                project_id = project_records[0].get_field_value(SAPIO_PROJECT_IDENTIFIER)
                project_record = project_records[0]

                # Attach the new project to Directory 1 (the root directory in Sapio)
                directories = (
                    Sapio()
                    .dataRecordManager.query_data_records(
                        data_type_name=SAPIO_DIRECTORY_DATA_TYPE,
                        data_field_name=SAPIO_RECORD_ID,
                        value_list=[1],
                    )
                    .result_list
                )
                directory_record = directories[0]
                directory: PyRecordModel = Sapio().inst_man.add_existing_record(
                    directory_record
                )
                Sapio().relationship_man.load_children([directory], SAPIO_PROJECT_DATA_TYPE)
                project: PyRecordModel = Sapio().inst_man.add_existing_record(
                    project_record
                )
                directory.add_child(project)

                # Attach profile created in COPO to its project department in Sapio
                self._attach_project_to_department(project)
            else:
                # Existing profile - look up the project by its Sapio identifier and update fields
                project_records = (
                    Sapio()
                    .dataRecordManager.query_data_records(
                        data_type_name=SAPIO_PROJECT_DATA_TYPE,
                        data_field_name=SAPIO_PROJECT_IDENTIFIER,
                        value_list=[project_id],
                    )
                    .result_list
                )
                if not project_records or len(project_records) == 0:
                    raise Exception(f"Failed to find project '{project_id}' in Sapio")
                project_record = project_records[0]
                project_record.set_field_value(SAPIO_PROJECT_NAME, project_name)
                project_record.set_field_value(
                    SAPIO_PROJECT_DESC, profile.get(PROFILE_DESCRIPTION) or ""
                )
                if profile.get(PROFILE_BUDGET_HOLDER):
                    project_record.set_field_value(
                        SAPIO_BUDGET_HOLDER, profile[PROFILE_BUDGET_HOLDER]
                    )
                Sapio().dataRecordManager.commit_data_records([project_record])

                # Attach existing profile that was created in COPO to its project
                # department in Sapio if the link is missing
                project: PyRecordModel = Sapio().inst_man.add_existing_record(
                    project_record
                )
                self._attach_project_to_department(project)

            # Load all samples and plates currently linked to this project
            project: PyRecordModel = Sapio().inst_man.add_existing_record(
                project_record
            )
            Sapio().relationship_man.load_children([project], SAPIO_SAMPLE_DATA_TYPE)
            samples_under_project: List[PyRecordModel] = project.get_children_of_type(
                SAPIO_SAMPLE_DATA_TYPE
            )
            Sapio().relationship_man.load_children([project], SAPIO_PLATE_DATA_TYPE)
            plates_under_project: List[PyRecordModel] = project.get_children_of_type(
                SAPIO_PLATE_DATA_TYPE
            )
            Sapio().relationship_man.load_children(plates_under_project, SAPIO_SAMPLE_DATA_TYPE)

            # Each batch's samples are synced on their own so that the
            # types of one batch are never written to another batch's samples
            samples_to_remove: List[PyRecordModel] = []
            samples_left_unplated = False
            for batch, batch_samples in _group_samples_by_batch(
                get_sample_batches(profile), samples_under_project
            ):
                batch_samples, removed_samples = self._resize_batch(
                    project, plates_under_project, batch, batch_samples, created_sample_record_ids
                )
                samples_to_remove.extend(removed_samples)

                sample_type = batch.get(PROFILE_SAMPLE_TYPE) or ''
                container_type = (batch.get(PROFILE_CONTAINER_TYPE) or '').capitalize()
                library_type = batch.get(PROFILE_LIBRARY_TYPE) or ''
                for sample in batch_samples:
                    sample.set_field_value(SAPIO_EXEMPLAR_SAMPLE_TYPE, sample_type)
                    sample.set_field_value(SAPIO_CONTAINER_TYPE, container_type)
                    sample.set_field_value(SAPIO_LIBRARY_TYPE, library_type)

                # Only batches identified as 'plate' batches should have samples placed on plates
                if container_type == 'Plate':
                    if self._place_batch_on_plates(
                        project, plates_under_project, batch_samples, sample_type
                    ):
                        samples_left_unplated = True
                else:
                    self._take_batch_off_plates(plates_under_project, batch_samples)

                batch_sample_record_ids[batch[BATCH_ID]] = [
                    sample.record_id for sample in batch_samples
                ]

            Sapio().rec_man.store_and_commit()
            # Hard-delete removed sample records from Sapio (recursive to clean up children)
            if samples_to_remove:
                Sapio().dataRecordManager.delete_data_record_list(
                    [sample.get_data_record() for sample in samples_to_remove],
                    recursive_delete=True,
                )

            # Give each plated sample a well element so that it shows
            # as a coloured well in Sapio's Plate View
            self._sync_plate_well_elements(project.get_children_of_type(SAPIO_PLATE_DATA_TYPE))

            if samples_left_unplated:
                l.error("Not all samples have been assigned to plates!")
                return {
                    "status": "warning",
                    "project_id": project_id,
                    "message": MESSAGES['sapio_project_update_failed'],
                    "created_sample_record_ids": created_sample_record_ids,
                }

        except _SapioSyncWarning as warning:
            _discard_pending_changes()
            l.error(
                f"Failed to sync sapio project {project_id} for profile id: {profile['_id']}. {warning}"
            )
            return {
                "status": "warning",
                "project_id": project_id,
                "message": str(warning),
                "created_sample_record_ids": created_sample_record_ids,
            }
        except Exception as e:
            _discard_pending_changes()
            l.exception(e)
            l.error(
                "Failed to create or update sapio project for profile id: "
                + str(profile["_id"])
                + " Error: "
                + str(e)
            )
            return {
                "status": "warning",
                "project_id": project_id,
                "message": MESSAGES['sapio_project_update_failed'],
                "created_sample_record_ids": created_sample_record_ids,
            }

        return {
            "status": "success",
            "project_id": project_id,
            "batch_sample_record_ids": batch_sample_record_ids,
        }

    def delete_project(self, project_id: str) -> Dict[str, str]:
        try:
            record = (
                Sapio()
                .dataRecordManager.query_data_records(
                    data_type_name=SAPIO_PROJECT_DATA_TYPE,
                    data_field_name=SAPIO_PROJECT_IDENTIFIER,
                    value_list=[project_id],
                )
                .result_list[0]
            )
            Sapio().dataRecordManager.delete_data_record(
                record=record, recursive_delete=True
            )
        except Exception as e:
            l.exception(e)
            l.error(
                "Failed to delete sapio project "
                + str(project_id)
                + " Error: "
                + str(e)
            )
            return {
                "status": "warning",
                "message": MESSAGES['sapio_project_delete_failed'],
            }
        return {"status": "success"}

    def upgrade_project(self, profile: Dict[str, Any], dry_run=True) -> Dict[str, Any]:
        '''Bring an older EDP profile's Sapio project up to date.

        Only fixes what is out of date; samples are never created or removed.
        A dry run reads Sapio and reports what would change without writing.

        1. Department: projects created before COPO linked them are attached
           to the department in `settings.SAPIO_PROJECT_DEPARTMENT`.
        2. Batches: each sample batch is paired with its samples (a batch moved
           from the fields the profile used to have claims every sample no other
           batch has). The record IDs are returned for the caller to save.
        3. Container types: samples whose container type isn't the title case
           value that Sapio accepts (e.g. 'plate' rather than 'Plate') are fixed.
        4. Plate colours: plated samples without a 'PlateDesignerWellElement'
           get one so that they show as coloured wells in Sapio's Plate View.
        '''
        project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, "")
        try:
            project = self._load_project(project_id) if project_id else None
            if not project:
                return {
                    'status': 'error',
                    'message': MESSAGES['sapio_project_not_found'].format(project_id=project_id),
                }
            summary = {'status': 'success', 'message': ''}

            # 1. Department
            summary['department'] = self._attach_project_to_department(project, dry_run=dry_run)

            # 2. Batches and 3. container types
            Sapio().relationship_man.load_children([project], SAPIO_SAMPLE_DATA_TYPE)
            samples_under_project = project.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)
            summary['batch_sample_record_ids'] = {}
            summary['container_types_fixed'] = 0
            for batch, batch_samples in _group_samples_by_batch(
                get_sample_batches(profile), samples_under_project
            ):
                summary['batch_sample_record_ids'][batch[BATCH_ID]] = [
                    sample.record_id for sample in batch_samples
                ]
                container_type = (batch.get(PROFILE_CONTAINER_TYPE) or '').capitalize()
                for sample in batch_samples:
                    if container_type and sample.get_field_value(SAPIO_CONTAINER_TYPE) != container_type:
                        summary['container_types_fixed'] += 1
                        if not dry_run:
                            sample.set_field_value(SAPIO_CONTAINER_TYPE, container_type)

            if not dry_run:
                Sapio().rec_man.store_and_commit()

            # 4. Plate colours
            Sapio().relationship_man.load_children([project], SAPIO_PLATE_DATA_TYPE)
            plates = project.get_children_of_type(SAPIO_PLATE_DATA_TYPE)
            Sapio().relationship_man.load_children(plates, SAPIO_SAMPLE_DATA_TYPE)
            summary['well_elements_added'], summary['well_elements_removed'] = (
                self._sync_plate_well_elements(plates, dry_run=dry_run)
            )
            return summary
        except Exception as error:
            _discard_pending_changes()
            l.exception(error)
            return {
                'status': 'error',
                'message': f'Sapio project {project_id} could not be upgraded: {_describe_error(error)}',
            }

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
            sapio_name = field.get(SAPIO_NAME)
            # sapio_name can be NaN (a float) when the schema cell is blank - guard on
            # str, not truthiness, since NaN is truthy and ":" in <float> raises.
            if isinstance(sapio_name, str) and ":" in sapio_name:
                sapio_object = sapio_name.split(":")[0]
                sapio_field = sapio_name.split(":")[1]
                if sapio_object.lower() == SAPIO_SAMPLE_DATA_TYPE.lower():
                    sapio_column_map[field[TERM_NAME]] = sapio_field
        return sapio_column_map

    def _load_project_samples(self, project_id: str) -> List[PyRecordModel]:
        project_records = (
            Sapio()
            .dataRecordManager.query_data_records(
                data_type_name=SAPIO_PROJECT_DATA_TYPE,
                data_field_name=SAPIO_PROJECT_IDENTIFIER,
                value_list=[project_id],
            )
            .result_list
        )
        if not project_records or len(project_records) == 0:
            l.error(f"Sapio Project {project_id} not found.")
            return []
        project_record = project_records[0]
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], SAPIO_SAMPLE_DATA_TYPE)
        return project.get_children_of_type(SAPIO_SAMPLE_DATA_TYPE)

    def _to_term_records(
        self, samples: List[PyRecordModel], schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        sapio_column_map = self._sample_field_map(schemas)
        return [
            {
                term_name: sample.get_field_value(sapio_field)
                for term_name, sapio_field in sapio_column_map.items()
            }
            for sample in samples
        ]

    def get_project_samples(
        self, project_id: str, schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        samples_under_project = self._load_project_samples(project_id)
        if not samples_under_project:
            return []
        return self._to_term_records(samples_under_project, schemas)

    def get_batch_samples(
        self, profile: Dict[str, Any], batch_id: str, schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, "")
        samples_under_project = self._load_project_samples(project_id) if project_id else []
        for batch, batch_samples in _group_samples_by_batch(
            get_sample_batches(profile), samples_under_project
        ):
            if batch[BATCH_ID] == batch_id:
                return self._to_term_records(batch_samples, schemas)
        return []

    def get_project_metadata(self, project_id: str) -> Dict[str, Any]:
        project_records = (
            Sapio()
            .dataRecordManager.query_data_records(
                data_type_name=SAPIO_PROJECT_DATA_TYPE,
                data_field_name=SAPIO_PROJECT_IDENTIFIER,
                value_list=[project_id],
            )
            .result_list
        )
        if not project_records or len(project_records) == 0:
            l.error(f"Sapio Project {project_id} not found.")
            return {}
        project_record = project_records[0]
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], SAPIO_SAMPLE_DATA_TYPE)
        samples_under_project: List[PyRecordModel] = project.get_children_of_type(
            SAPIO_SAMPLE_DATA_TYPE
        )

        metadata = {LIMS_HEALTH_AND_SAFETY: project.get_field_value(SAPIO_HEALTH_AND_SAFETY)}
        if samples_under_project:
            metadata[LIMS_SAMPLE_RETURN] = samples_under_project[0].get_field_value(
                SAPIO_SAMPLE_RETURN
            )
        return metadata

    def _set_customer_supplied_qc_datum(
        self, sample: PyRecordModel, qc_values: Dict[str, Any]
    ):
        # One 'QCDatum' child is mapped per Sample with the 'Customer Supplied' Instrument Type (i.e. DatumType).
        # Sapio will later fill in the units, the 'Linked Sample' parent link and the default
        # 'Passed' value for the 'QC Status' field or 'Pending Approval' status for the 'Instrument Data Status' field.
        qc_datum = next(
            (
                child
                for child in sample.get_children_of_type(SAPIO_QC_DATUM_DATA_TYPE)
                if child.get_field_value(SAPIO_DATUM_TYPE) == CUSTOMER_SUPPLIED_DATUM_TYPE
            ),
            None,
        )
        if not qc_datum:
            qc_datum = Sapio().inst_man.add_new_record(SAPIO_QC_DATUM_DATA_TYPE)
            sample.add_child(qc_datum)

        sample_id = sample.get_field_value(SAPIO_SAMPLE_ID)
        qc_datum.set_field_value(SAPIO_DATUM_TYPE, CUSTOMER_SUPPLIED_DATUM_TYPE)
        qc_datum.set_field_value(SAPIO_SAMPLE_ID, sample_id)
        qc_datum.set_field_value(
            SAPIO_DATA_RECORD_NAME, f'{CUSTOMER_SUPPLIED_DATUM_TYPE} {sample_id}'
        )

        # A field from the schema can only point to one record type in Sapio
        # so, relevant sample fields (like 'Concentration' and 'Volume') are 
        # copied onto the `QCDatum` record in Sapio since more than one 
        # record type applies to them.
        for sample_field, qc_datum_field in SAMPLE_TO_QC_DATUM_FIELDS.items():
            value = sample.get_field_value(sample_field)
            if not _is_blank(value):
                qc_datum.set_field_value(qc_datum_field, value)

        for sapio_field, value in qc_values.items():
            qc_datum.set_field_value(sapio_field, _to_number(value))

    def _set_customer_supplied_index_barcode(
        self, sample: PyRecordModel, index_values: Dict[str, Any]
    ):
        # One 'IndexBarcode' child is mapped per 'Sample'
        # with no link to an index kit.
        # In Sapio, an 'IndexBarcode' record is displayed as 'Assigned Index'
        index_barcodes = sample.get_children_of_type(SAPIO_INDEX_BARCODE_DATA_TYPE)
        if index_barcodes:
            index_barcode = index_barcodes[0]
        else:
            index_barcode = Sapio().inst_man.add_new_record(SAPIO_INDEX_BARCODE_DATA_TYPE)
            sample.add_child(index_barcode)

        for sapio_field, value in index_values.items():
            index_barcode.set_field_value(sapio_field, value)

        # 'Sample Name' on an index is the `submitter_library_name` which
        # is mapped from the LIMS manifest via the 'OtherSampleId' Sapio field
        sample_id = sample.get_field_value(SAPIO_SAMPLE_ID)
        index_barcode.set_field_value(SAPIO_SAMPLE_ID, sample_id)
        index_barcode.set_field_value(
            SAPIO_DATA_RECORD_NAME,
            ' '.join(
                str(value)
                for value in (
                    index_barcode.get_field_value(SAPIO_INDEX_ID),
                    sample_id,
                    index_barcode.get_field_value(SAPIO_OTHER_SAMPLE_ID),
                )
                if not _is_blank(value)
            ),
        )

    def submit_manifest(
        self, project_id: str, schemas: Dict[str, Any], components: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            return self._submit_manifest(project_id, schemas, components)
        except Exception as error:
            _discard_pending_changes()
            l.exception(error)
            return {
                'status': 'error',
                'message': MESSAGES['sapio_manifest_submission_failed'].format(
                    project_id=project_id, error=_describe_error(error)
                ),
            }

    def _submit_manifest(
        self, project_id: str, schemas: Dict[str, Any], components: Dict[str, Any]
    ) -> Dict[str, Any]:
        project_records = (
            Sapio()
            .dataRecordManager.query_data_records(
                data_type_name=SAPIO_PROJECT_DATA_TYPE,
                data_field_name=SAPIO_PROJECT_IDENTIFIER,
                value_list=[project_id],
            )
            .result_list
        )
        if not project_records or len(project_records) == 0:
            return {
                "status": "error",
                "message": MESSAGES['sapio_project_not_found'].format(project_id=project_id),
            }

        project_record = project_records[0]

        # Load samples under the project and index by SampleId for fast lookup
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
        Sapio().relationship_man.load_children([project], SAPIO_SAMPLE_DATA_TYPE)
        samples_under_project: List[PyRecordModel] = project.get_children_of_type(
            SAPIO_SAMPLE_DATA_TYPE
        )
        samples_under_project_map = {
            s.get_field_value(SAPIO_SAMPLE_ID): s for s in samples_under_project
        }

        # Build a mapping from COPO term_name → Sapio field name, grouped by Sapio data type.
        # Only fields with a sapio_name in "DataType:FieldName" format and not marked
        # "protected" are included.
        sapio_mapping_df = pd.DataFrame(columns=[TERM_NAME, SAPIO_NAME])
        for component_name, component_schema in schemas.items():
            component_schema_df = pd.DataFrame.from_records(component_schema)
            sapio_component_mapping_df = component_schema_df[
                (
                    ~pd.isna(component_schema_df[SAPIO_NAME])
                    & (component_schema_df[SAPIO_NAME].str.contains(":"))
                    & (component_schema_df[TERM_ACCESS] != "protected")
                )
            ][[TERM_NAME, SAPIO_NAME]]
            sapio_mapping_df = pd.concat(
                [sapio_mapping_df, sapio_component_mapping_df], ignore_index=True
            )

        sapio_mapping_df["sapio_object"] = sapio_mapping_df[SAPIO_NAME].apply(
            lambda x: x.split(":")[0]
        )
        sapio_mapping_df["sapio_field"] = sapio_mapping_df[SAPIO_NAME].apply(
            lambda x: x.split(":")[1]
        )
        sapio_mapping_df = sapio_mapping_df.drop(columns=[SAPIO_NAME])

        # Convert to {DataType: {term_name: sapio_field}} dict for O(1) lookups below
        sapio_object_dict = {}
        for c, sapio_object_df in sapio_mapping_df.groupby("sapio_object", sort=False):
            sapio_object_df = sapio_object_df.set_index(TERM_NAME)
            sapio_object_dict[c] = sapio_object_df["sapio_field"].to_dict()

        project_sapio_mapping = sapio_object_dict.get(SAPIO_PROJECT_DATA_TYPE, {})
        sample_sapio_mapping = sapio_object_dict.get(SAPIO_SAMPLE_DATA_TYPE, {})
        
        # 'QC' means Quality control. It refers to the lab measurements 
        # that check whether samples meet required sequence standards.
        qc_datum_sapio_mapping = sapio_object_dict.get(SAPIO_QC_DATUM_DATA_TYPE, {})
        
        index_barcode_sapio_mapping = {
            term_name: sapio_field
            for term_name, sapio_field in sapio_object_dict.get(SAPIO_INDEX_BARCODE_DATA_TYPE, {}).items()
            if sapio_field != SAPIO_SAMPLE_ID
        }

        # QC values that customers supply reside on a 'QCDatum' in Sapio and are displayed 
        # as 'Instrument Data' while the indices reside on a 'IndexBarcode' child that 
        # is displayed as 'Assigned Index' in Sapio.
        # Existing 'QCDatum' and 'IndexBarcode' children are loaded for 
        # existing project samples to avoid creating duplicates.
        if samples_under_project:
            if qc_datum_sapio_mapping:
                Sapio().relationship_man.load_children(samples_under_project, SAPIO_QC_DATUM_DATA_TYPE)
            if index_barcode_sapio_mapping:
                Sapio().relationship_man.load_children(
                    samples_under_project, SAPIO_INDEX_BARCODE_DATA_TYPE
                )

        # Flag manifest sample IDs in COPO that have no matching samples
        # in Sapio so that they can be reported to the user
        skipped_samples = []

        for component_name, component_schema in schemas.items():
            component_schema_df = pd.DataFrame.from_records(component_schema)
            component_data_df = pd.DataFrame.from_records(
                components.get(component_name, [])
            )
            if component_data_df.empty:
                continue

            # Drop columns not in the schema (e.g. internal COPO metadata fields)
            columns = component_data_df.columns
            component_data_df = component_data_df.drop(
                columns=[
                    column
                    for column in columns
                    if column not in component_schema_df[TERM_NAME].values
                ],
            )

            # Swap manifest values for the values Sapio expects
            for column in component_data_df.columns:
                if column in COPO_TO_SAPIO_VALUES:
                    component_data_df[column] = component_data_df[column].apply(
                        lambda value, column=column: _to_sapio_value(column, value)
                    )

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
                    sapio_sample_id = row.get(SAMPLE_ID, "")
                    sapio_sample = samples_under_project_map.get(sapio_sample_id, None)
                    if not sapio_sample:
                        l.error(
                            f"Sample with Sample ID {sapio_sample_id} not found in Sapio Project {project_id}. Skipping..."
                        )
                        skipped_samples.append(str(sapio_sample_id))

                        continue
                    for column in component_data_df.columns:
                        sapio_field = sample_sapio_mapping.get(column, "")
                        if sapio_field:
                            sapio_sample.set_field_value(sapio_field, row[column])

                    qc_values = {
                        qc_datum_sapio_mapping[column]: row[column]
                        for column in component_data_df.columns
                        if column in qc_datum_sapio_mapping
                        and not _is_blank(row[column])
                    }
                    
                    if qc_values:
                        self._set_customer_supplied_qc_datum(sapio_sample, qc_values)

                    index_values = {
                        index_barcode_sapio_mapping[column]: row[column]
                        for column in component_data_df.columns
                        if column in index_barcode_sapio_mapping
                        and not _is_blank(row[column])
                    }
                    
                    # The `submitter_library_name` field is mandatory so an 'IndexBarcode'
                    # record type in Sapio is only created when an index value is also provided
                    if any(field != SAPIO_OTHER_SAMPLE_ID for field in index_values):
                        self._set_customer_supplied_index_barcode(
                            sapio_sample, index_values
                        )

        Sapio().rec_man.store_and_commit()

        if skipped_samples:
            return {
                'status': 'warning',
                'message': MESSAGES['sapio_manifest_samples_skipped'].format(
                    project_id=project_id, sample_ids=', '.join(skipped_samples)
                ),
                'record_id': project.record_id,
                'skipped_samples': skipped_samples,
            }

        return {
            "status": "success",
            "message": MESSAGES['sapio_manifest_submitted'].format(project_id=project_id),
            "record_id": project.record_id,
            "skipped_samples": [],
        }
