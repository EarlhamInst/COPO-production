# LIMS Adapter — Decoupling the EDP Flow from Sapio

## Context

COPO's EDP (Earlham Data Portal) flow integrates with a Laboratory Information
Management System (LIMS) to create projects, reconcile samples, pre-fill
manifests, and push submitted data back. Historically this was hardwired to
**Sapio** (via `sapiopylib`): the Sapio calls lived inline in
`src/apps/ei_edp/utils/edp_utils.py` and `EDPSchemasHandler.py`.

This document describes the `LIMSAdapter` interface that decouples that
integration so any facility can plug in their own LIMS (Benchling, Illumina
Clarity, an in-house system, or none) without touching COPO. It is a landed
**reference implementation** of the pluggable-backend pattern that
[`DECOUPLING_ANALYSIS.md`](DECOUPLING_ANALYSIS.md) recommends for the ENA
submission engines (SP1/SP2), applied here to the LIMS integration.

**Status:** implemented and behaviour-preserving. `LIMS_ADAPTER` defaults to the
Sapio adapter, so production behaviour is unchanged; the decoupling is invisible
until another adapter is selected.

---

## 1. Why

- **Sapio was hardwired.** ~330 lines of `sapiopylib` code sat inline in the EDP
  utils, so the flow could not run — or be tested — without a live Sapio stack.
- **The concept note promises "system-agnostic via pluggable adapters."** This
  is the seam that delivers it for the LIMS side.
- **The seams were already roughly right.** All Sapio usage was contained within
  `ei_edp/utils/`, with no leakage into the rest of COPO — an "extract an
  adapter" situation, not an "untangle a mess" one.

---

## 2. The interface

`LIMSAdapter` (in `src/apps/ei_edp/utils/lims/base.py`) is an ABC with the finite
set of operations the EDP flow actually performs against a LIMS. Each maps
directly to a place the old inline Sapio code lived:

| Method | Replaces (old inline Sapio in…) | Purpose |
|--------|--------------------------------|---------|
| `get_sample_type_options()` | `get_sapio_sample_type_options` | Choices for the profile form's sample-type dropdown |
| `validate_profile_change(profile, requested_sample_count)` | `pre_save_edp_profile` | Guard a sample-count change that would delete committed samples |
| `sync_project(profile)` | `post_save_edp_profile` (Sapio block) | Create-or-update the project + reconcile samples (& plates) |
| `delete_project(project_id)` | `post_delete_edp_profile` | Delete the project and dependents |
| `get_project_samples(project_id, schemas)` | `write_manifest` (sample read) | Existing samples, keyed by COPO `term_name`, to pre-fill a manifest |
| `get_project_metadata(project_id)` | `write_manifest` (L8/L9 reads) | Study-level header values (`health_and_safety`, `sample_return`) |
| `submit_manifest(project_id, schemas, components)` | `submit_edp_to_sapio` | Write an ingested manifest's values back into the LIMS |

### Design rules that keep the interface LIMS-neutral

1. **Inputs and outputs speak COPO's language, never the LIMS's.** Methods take
   the COPO `profile` dict and single-cell `schemas`, and return records keyed
   by COPO `term_name`. All translation to LIMS field names — e.g. Sapio's
   `sapio_name` schema column (`"Object:Field"`) — lives *inside* the adapter.
2. **Status-returning methods use the house `{"status": ..., "message": ...}`
   convention**, so they drop straight into the existing profile-hook call
   sites.
3. **Physical-lab concepts that not every LIMS shares (plates, wells,
   containers) are never exposed.** `sync_project` is intent-level ("make the
   LIMS reflect this profile"), so the Sapio adapter can do 96-well packing
   internally while another LIMS ignores the concept entirely.

---

## 3. Package layout

```
src/apps/ei_edp/utils/lims/
├── __init__.py        # get_lims_adapter() resolver + exports   (vendor-neutral)
├── base.py            # LIMSAdapter ABC, NullLIMSAdapter          (vendor-neutral)
└── sapio/             # the Sapio plugin — self-contained
    ├── __init__.py    # exposes SapioAdapter
    ├── adapter.py     # SapioAdapter: all sapiopylib business logic
    └── datamanager.py # Sapio client (auth/transport): the `Sapio` connection class
```

The directory encodes the architecture: the **top level is the contract every
LIMS shares**; each **subfolder is one vendor's implementation**. A second LIMS
is a sibling folder (`lims/benchling/…`) plus a one-line settings change — no
edits to neutral code.

Within `sapio/`, `datamanager.py` (the connection/transport layer — reads
`SAPIO_*` env vars, builds a `SapioUser`, exposes the `sapiopylib` managers) is
deliberately kept separate from `adapter.py` (the business operations). The
adapter depends on the client; swapping auth or mocking for tests touches only
the client.

---

## 4. How the adapter is selected

`get_lims_adapter()` (in `lims/__init__.py`) returns the adapter named by the
`LIMS_ADAPTER` Django setting — a dotted path to a `LIMSAdapter` subclass —
defaulting to the no-op `NullLIMSAdapter` when unset. The instance is cached for
the process lifetime.

```python
# src/main_config/settings/base.py
LIMS_ADAPTER = "src.apps.ei_edp.utils.lims.sapio.adapter.SapioAdapter"
```

- **Sapio (default):** `...lims.sapio.adapter.SapioAdapter`
- **No LIMS / tests:** `...lims.base.NullLIMSAdapter` — every operation succeeds
  and reads return empty, so the EDP flow runs end to end without any external
  system.
- **Custom:** any importable `LIMSAdapter` subclass.

---

## 5. Where it's wired in

The five external call sites were **not** changed — they still call the same
`edp_utils` functions, which are now thin delegators to `get_lims_adapter()`:

| Caller | Function | Delegates to |
|--------|----------|--------------|
| `copo_core/management/commands/setup_profile_types.py` (profile hooks) | `pre_save_edp_profile` | `validate_profile_change` |
| " | `post_save_edp_profile` | `sync_project` (+ COPO group/email sharing stays local) |
| " | `post_delete_edp_profile` | `delete_project` |
| `copo_core/broker_da.py` | `submit_edp_to_sapio` | `submit_manifest` |
| `ei_edp/utils/EDPSchemasHandler.py` | `write_manifest` | `get_project_samples`, `get_project_metadata` |

Note that `post_save_edp_profile` keeps its COPO-native work (customer-email
group sharing, invite tokens, persisting the returned `project_id` to the
profile) — only the LIMS project/sample sync moved into the adapter.

---

## 6. Writing a new LIMS plugin

1. Create a package `src/apps/ei_edp/utils/lims/<yourlims>/`.
2. Add `adapter.py` with a `class YourAdapter(LIMSAdapter)` implementing all
   seven methods. Keep any client/transport code in a sibling module (mirror
   `sapio/datamanager.py`).
3. Do all COPO-term ↔ LIMS-field translation inside the adapter. Return records
   keyed by COPO `term_name`; return `{"status": ...}` dicts from the
   status-returning methods.
4. Point `LIMS_ADAPTER` at `...lims.<yourlims>.adapter.YourAdapter`.

`NullLIMSAdapter` in `base.py` is the minimal reference: it satisfies the ABC
with no-ops and empty reads.

---

## 7. Behaviour notes & caveats

- **One intentional behaviour change.** Previously, if a profile had a
  `sapio_project_id` but the LIMS could not find that project, `write_manifest`
  returned an error and **aborted** manifest generation. Now the adapter logs it
  and returns empty prefill, so the manifest still generates (blank sample
  data). This was chosen so `Null`/other adapters behave naturally; if the strict
  abort is wanted for Sapio, it must be threaded back through the interface.
- **`sync_project` returns `project_id` on every path, including partial
  failure.** This preserves the original behaviour where a newly minted project
  id was persisted immediately, so a mid-sync crash does not cause a *duplicate*
  project on retry.
- **Manifest generation now issues two LIMS reads** (`get_project_samples` +
  `get_project_metadata`) where the old code queried the project once. Minor;
  acceptable for the cleaner seam.
- **The `sapio_name` schema column still exists** in the single-cell schemas.
  It is read only inside `SapioAdapter`. Moving that mapping out of the schema
  and into each adapter (so the schema is fully LIMS-neutral) is a deliberate
  *future* step, not done here — don't gold-plate before a second LIMS exists.
- **Profile fields** (`sapio_project_id`, `sample_type`, `container_type`,
  `library_type`, `budget_user`) remain Sapio-flavoured on the COPO profile.
  Generalising these to neutral names + an adapter-declared blob is a separate
  future step.

---

## 8. Verification / how to use

- **Interface consistency (no stack needed):** `NullLIMSAdapter` instantiates
  (proves the ABC is fully implemented); `SapioAdapter` defines all seven
  abstract methods.
- **Resolver wiring (needs Django):**
  ```
  python manage.py shell -c "from src.apps.ei_edp.utils.lims import get_lims_adapter; print(get_lims_adapter())"
  ```
- **End-to-end (needs Django + Mongo + live Sapio):** with `LIMS_ADAPTER`
  defaulting to Sapio, exercise the four paths and confirm they match
  pre-refactor behaviour:
  1. Save a new EDP profile → creates project + samples/plates.
  2. Save again with a changed sample count → reconciles.
  3. Generate a DNA manifest → sample prefill + L8/L9 header populate.
  4. Submit a manifest → values land on the Sapio project/samples.

**No automated tests exist for this flow yet** (consistent with the platform-wide
gap noted in `DECOUPLING_ANALYSIS.md` §7). A characterization test running the
profile hooks against `NullLIMSAdapter` — no external stack required — is the
cheapest first net and is now possible *because* of this decoupling.
