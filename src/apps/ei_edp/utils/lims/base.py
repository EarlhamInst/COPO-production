"""LIMS adapter interface.

COPO's EDP flow needs to talk to a Laboratory Information Management System
(LIMS) at a handful of well-defined points: when a profile is saved or deleted,
when a manifest is generated or ingested, and when the sample-type dropdown is
built. Historically all of these called Sapio (via `sapiopylib`) directly from
`edp_utils.py` and `EDPSchemasHandler.py`.

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
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class LIMSAdapter(ABC):
    """Operations COPO's EDP flow performs against a LIMS.

    A concrete adapter must be safe to instantiate cheaply (COPO resolves one
    per call via `get_lims_adapter`); hold connection setup in the constructor
    or lazily, as suits the backend.
    """

    # ------------------------------------------------------------------ #
    # Form support
    # ------------------------------------------------------------------ #
    @abstractmethod
    def get_sample_type_options(self) -> List[Dict[str, str]]:
        """Return sample-type choices for the profile form's dropdown.

        Replaces `get_sapio_sample_type_options`. Each item is
        `{"value": ..., "label": ...}`. Return an empty list if the LIMS has no
        such concept.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Project lifecycle — called from the EDP profile save/delete hooks
    # ------------------------------------------------------------------ #
    @abstractmethod
    def validate_profile_change(self, profile: Dict[str, Any],
                                requested_sample_count: int) -> Dict[str, str]:
        """Guard a pending profile change before it is written to COPO.

        Replaces the Sapio-specific checks in `pre_save_edp_profile`: chiefly,
        refuse a sample-count reduction that would delete samples the lab has
        already committed to (e.g. named/registered samples).

        Return `{"status": "success"}` to allow the save, or
        `{"status": "error", "message": ...}` to abort it.
        """
        raise NotImplementedError

    @abstractmethod
    def sync_project(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Create-or-update the LIMS project so it reflects `profile`.

        Replaces the "Sapio project sync" block of `post_save_edp_profile`
        (the COPO group/email sharing stays in `edp_utils`, it is not a LIMS
        concern). Responsible end to end for: creating the project on first
        save, updating its fields thereafter, reconciling sample count against
        `no_of_samples`, and any LIMS-internal placement such as plate/well
        assignment.

        On first creation the adapter mints the LIMS project identifier and
        returns it as `project_id`; the caller persists it on the profile
        (today: `sapio_project_id`).

        Return `{"status": "success", "project_id": ...}` or
        `{"status": "warning"|"error", "message": ...}`.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_project(self, project_id: str) -> Dict[str, str]:
        """Delete the LIMS project and its dependent records.

        Replaces `post_delete_edp_profile`. Return
        `{"status": "success"}` or `{"status": "warning", "message": ...}`;
        a failure here should not block the COPO-side delete.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Manifest — generation (read) and ingestion (write)
    # ------------------------------------------------------------------ #
    @abstractmethod
    def get_project_samples(self, project_id: str,
                            schemas: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return existing LIMS samples to pre-fill a manifest.

        Replaces the sample read at the top of `write_manifest`. Each record is
        keyed by COPO `term_name` (the adapter maps LIMS fields → terms using
        `schemas`), so the caller can drop values straight into manifest
        columns. Return an empty list if the project has no samples yet.
        """
        raise NotImplementedError

    @abstractmethod
    def get_project_metadata(self, project_id: str) -> Dict[str, Any]:
        """Return study-level values needed to pre-fill the manifest header.

        Replaces the L8/L9 reads in `write_manifest`. Known keys:
        `health_and_safety`, `sample_return`. Missing values may be omitted.
        """
        raise NotImplementedError

    @abstractmethod
    def submit_manifest(self, project_id: str, schemas: Dict[str, Any],
                        components: Dict[str, Any]) -> Dict[str, str]:
        """Write an ingested manifest's values back into the LIMS.

        Replaces `submit_edp_to_sapio`. `components` is the COPO single-cell
        submission payload (study- and sample-level rows); the adapter maps each
        COPO term to its LIMS field and applies study fields to the project and
        sample fields to the matching samples.

        Return `{"status": "success", "message": ...}` or
        `{"status": "error", "message": ...}`.
        """
        raise NotImplementedError


class NullLIMSAdapter(LIMSAdapter):
    """No-op adapter for facilities with no LIMS, and for tests.

    Every operation succeeds and reads return empty, so the EDP flow runs end
    to end (profiles save, manifests generate/ingest) without any external
    system. This is the default when `LIMS_ADAPTER` is unset.
    """

    def get_sample_type_options(self) -> List[Dict[str, str]]:
        return []

    def validate_profile_change(self, profile, requested_sample_count):
        return {"status": "success"}

    def sync_project(self, profile):
        return {"status": "success", "project_id": profile.get("sapio_project_id", "")}

    def delete_project(self, project_id):
        return {"status": "success"}

    def get_project_samples(self, project_id, schemas):
        return []

    def get_project_metadata(self, project_id):
        return {}

    def submit_manifest(self, project_id, schemas, components):
        return {"status": "success", "message": "No LIMS configured; nothing submitted."}
