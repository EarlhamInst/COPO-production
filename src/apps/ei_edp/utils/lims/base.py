'''LIMS adapter interface.

COPO's EDP flow needs to talk to a Laboratory Information Management System
(LIMS) at a handful of well-defined points: when a profile is saved or deleted,
when a manifest is generated or ingested, and when the sample-type dropdown is
built. Historically all of these called Sapio (via `sapiopylib`) directly from
`edp_utils.py` and `EDPSchemaHandler.py`.

`LIMSAdapter` is the seam that decouples that. Each supported LIMS ships a
concrete subclass; the active one is chosen at runtime from the
`LIMS_ADAPTER` setting (see `get_lims_adapter`). The rest of COPO only ever
sees this interface.

Design rules that keep the interface LIMS-neutral:

- Inputs and outputs speak COPO's language, never the LIMS's. Methods take the
  COPO `profile` dict and single-cell `schemas`, and return records keyed by
  COPO `term_name`. All translation to LIMS field names (e.g. Sapio's
  `sapio_name` schema column, "Object:Field") lives *inside* the adapter.
- Status-returning methods use the same `{"status": ..., "message": ...}`
  convention as the rest of the codebase, so they drop straight into the
  existing profile-hook call sites.
- Physical-lab concepts that not every LIMS shares (plates, wells, containers)
  are never exposed here. `sync_project` is intent-level ("make the LIMS
  reflect this profile") so a Sapio adapter can do 96-well packing internally
  while another LIMS ignores the concept entirely.
'''

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..constants import BATCH_ID, PROFILE_SAPIO_PROJECT_ID
from ..edp_validation_messages import MESSAGES
from ..sample_batches import get_sample_batches


class LIMSUnavailable(Exception):
    '''Raised when a LIMS operation cannot complete because of a backend error.

    Carries a user-facing summary - which LIMS, what it was doing, and the
    underlying error - so callers (e.g. the EDP form build) can fail cleanly
    with an informative message instead of a blank 503 or a partially-built
    form missing its LIMS-sourced fields.
    '''

    def __init__(self, lims_name: str, operation: str, detail: str):
        self.lims_name = lims_name
        self.operation = operation
        self.detail = detail
        super().__init__(f"{lims_name} LIMS error while {operation}: {detail}")


class LIMSAdapter(ABC):
    '''Operations EDP flow performs against a LIMS.

    A concrete adapter must be safe to instantiate cheaply (COPO resolves one
    per call via `get_lims_adapter`); hold connection setup in the constructor
    or lazily, as suits the backend.
    '''

    # ------------------------------------------------------------------ #
    # Form support
    # ------------------------------------------------------------------ #
    @abstractmethod
    def get_sample_type_options(self) -> List[Dict[str, str]]:
        '''Return sample-type choices for the profile form's dropdown.

        Replaces `get_sapio_sample_type_options`. Each item is
        `{"value": ..., "label": ...}`. Return an empty list if the LIMS has no
        such concept.
        '''
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Project lifecycle - called from the EDP profile save/delete hooks
    # ------------------------------------------------------------------ #
    @abstractmethod
    def validate_profile_change(self, profile: Dict[str, Any]) -> Dict[str, str]:
        # Guard changes to existing profiles before they are saved. Reject 
        # changes if the profiles' LIMS project can no longer be found.
        raise NotImplementedError

    @abstractmethod
    def get_submitted_sample_counts(self, profile: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        '''Count each sample batch's samples

        Returns `{batch_id: {"submitted": ..., "total": ...}}` for every
        batch on the profile. A batch with submitted samples is locked.
        '''
        raise NotImplementedError

    @abstractmethod
    def delete_samples(self, record_ids: List[int]) -> Dict[str, str]:
        '''Delete samples in Sapio's LIMS by record ID, e.g. those created by a failed sync.

        Return `{"status": "success"}` or `{"status": "warning", "message": ...}`.
        '''
        raise NotImplementedError

    @abstractmethod
    def sync_project(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        '''Create-or-update the LIMS project so it reflects `profile`.

        Replaces the "Sapio project sync" block of `post_save_edp_profile`
        (the COPO group/email sharing stays in `edp_utils`, it is not a LIMS
        concern). Responsible end to end for: creating the project on first
        save, updating its fields thereafter, reconciling each sample batch's
        samples against its `no_of_samples` and types (without touching other
        batches' samples), and any LIMS-internal placement such as plate/well
        assignment.

        On first creation the adapter mints the LIMS project identifier and
        returns it as `project_id`; the caller persists it on the profile
        (today: `sapio_project_id`).

        Return `{"status": "success", "project_id": ..., "batch_sample_record_ids":
        {batch_id: [record_id, ...]}}` so the caller can link each batch to its
        samples, or `{"status": "warning"|"error", "project_id": ..., "message": ...,
        "created_sample_record_ids": [...]}` so the caller can undo the sync.
        '''
        raise NotImplementedError

    @abstractmethod
    def delete_project(self, project_id: str) -> Dict[str, str]:
        '''Delete the LIMS project and its dependent records.

        Replaces `post_delete_edp_profile`. Return
        `{"status": "success"}` or `{"status": "warning", "message": ...}`;
        a failure here should not block the COPO-side delete.
        '''
        raise NotImplementedError

    def upgrade_project(self, profile: Dict[str, Any], dry_run: bool = True) -> Dict[str, Any]:
        '''Bring an older EDP profile's LIMS project up to date.

        Used by the `migrate_edp_profiles` command. Returns
        `{"status": ..., "message": ..., "batch_sample_record_ids": {...}}` plus
        any counts of what was (or, for a dry run, would be) changed. The
        default does nothing, for LIMSs that have nothing to upgrade.
        '''
        return {"status": "success", "message": "", "batch_sample_record_ids": {}}

    # ------------------------------------------------------------------ #
    # Manifest - generation (read) and ingestion (write)
    # ------------------------------------------------------------------ #
    @abstractmethod
    def get_project_samples(
        self, project_id: str, schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        '''Return existing LIMS samples to pre-fill a manifest.

        Replaces the sample read at the top of `write_manifest`. Each record is
        keyed by COPO `term_name` (the adapter maps LIMS fields → terms using
        `schemas`), so the caller can drop values straight into manifest
        columns. Return an empty list if the project has no samples yet.
        '''
        raise NotImplementedError

    @abstractmethod
    def get_batch_samples(
        self, profile: Dict[str, Any], batch_id: str, schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        '''Return the LIMS samples of one of the profile's sample batches.

        Records are keyed by COPO `term_name` like `get_project_samples`.
        '''
        raise NotImplementedError

    @abstractmethod
    def get_project_metadata(self, project_id: str) -> Dict[str, Any]:
        '''Return study-level values needed to pre-fill the manifest header.

        Replaces the L8/L9 reads in `write_manifest`. Known keys:
        `health_and_safety`, `sample_return`. Missing values may be omitted.
        '''
        raise NotImplementedError

    @abstractmethod
    def submit_manifest(
        self, project_id: str, schemas: Dict[str, Any], components: Dict[str, Any]
    ) -> Dict[str, str]:
        '''Write an ingested manifest's values back into the LIMS.

        Replaces `submit_lims_manifest_to_sapio`. `components` is the COPO single-cell
        submission payload (study- and sample-level rows); the adapter maps each
        COPO term to its LIMS field and applies study fields to the project and
        sample fields to the matching samples.

        Return `{"status": "success" | "warning" | "error", "message": ...}`.
        The `record_id` (which is the LIMS record's identifier
        of the project is stored as the submission accession) and
        the `skipped_samples` (which is a list of manifest sample IDs
        with no matching LIMS sample; a non-empty list means "warning")
        are included for successful and warning messages.
        '''
        raise NotImplementedError


class NullLIMSAdapter(LIMSAdapter):
    '''No-op adapter for facilities with no LIMS, and for tests.

    Every operation succeeds and reads return empty, so the EDP flow runs end
    to end (profiles save, manifests generate/ingest) without any external
    system. This is the default when `LIMS_ADAPTER` is unset.
    '''

    def get_sample_type_options(self) -> List[Dict[str, str]]:
        return []

    def validate_profile_change(self, profile):
        return {"status": "success"}

    def get_submitted_sample_counts(self, profile):
        return {
            batch[BATCH_ID]: {"submitted": 0, "total": 0}
            for batch in get_sample_batches(profile)
        }

    def delete_samples(self, record_ids):
        return {"status": "success"}

    def sync_project(self, profile):
        return {
            "status": "success",
            "project_id": profile.get(PROFILE_SAPIO_PROJECT_ID, ""),
            "batch_sample_record_ids": {},
        }

    def delete_project(self, project_id):
        return {"status": "success"}

    def get_project_samples(self, project_id, schemas):
        return []

    def get_batch_samples(self, profile, batch_id, schemas):
        return []

    def get_project_metadata(self, project_id):
        return {}

    def submit_manifest(self, project_id, schemas, components):
        return {
            "status": "success",
            "message": MESSAGES['no_lims_configured'],
        }
