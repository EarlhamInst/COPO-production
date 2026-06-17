# COPO Codebase — Functional-Group Analysis & Decoupling Assessment

## Context

COPO is a Django genomics-metadata submission platform (~50k+ LOC in the analysed subsystems) that brokers sample/metadata submissions to external repositories (ENA, Zenodo; stubs for Dataverse/DSpace/figshare). This document maps the codebase into functional groups, identifies where coupling has accumulated, and gives a ranked, sequenced recommendation of what can and should be decoupled — and what should be left alone.

This is an **analysis deliverable**, not a change to execute immediately. Findings were gathered by parallel exploration of the working tree and verified against real files.

The single most important finding up front: **there is essentially no test coverage** — one real test file (`test/testcases/test_copologinlogout.py`) for the whole platform. This is the dominant constraint and reshapes every recommendation below: safe mechanical extractions first, characterization tests before any structural surgery.

---

## 1. Functional groups

| # | Group | Location (LOC) | Role |
|---|-------|----------------|------|
| 1 | **Platform core** | `src/apps/copo_core` (~10.7k) | God-app: Postgres ORM models (UserDetails, ProfileType, Component), `broker_da.py` central CRUD dispatcher, 9 websocket consumers, celery tasks, views. 42 files in other apps import from it. |
| 2 | **Data access layer (DAL)** | `common/dal` (~5.7k) | MongoDB access via a `DAComponent` inheritance base; per-entity DAOs (profile/sample/submission/copo_da). The Mongo half of a dual-DB design. |
| 3 | **Submission engines** | `copo_read/single_cell/assembly/barcoding/seq_annotation_submission` (~15k combined) | Five apps, each submitting one data type to ENA (single_cell also Zenodo). Heavy copy-paste between them. |
| 4 | **ENA integration** | `common/ena_utils` (~4.5k) | `ena_helper.py` (1757-LOC `EnaSubmissionHelper` monolith), `FileTransferUtils.py` (Aspera→ENA), `generic_helper.py`, checklist/platform handlers. WEBIN creds read at module scope in 23 files. |
| 5 | **Sample / manifest / DToL domain** | `copo_dtol_upload`, `copo_dtol_submission`, `copo_sample` (~8k) | DToL/ToL manifest upload, validation, and submission. ToL-specific logic bleeds into generic sample handling. |
| 6 | **Schema / validation / lookup** | `common/schemas`, `common/validators`, `common/schema_versions`, `common/lookup` | `dtol_lookups.py` (1490 lines) is the "coupling epicenter" (field→ENA maps, enums, rules, even hardcoded institution lat/longs). `lookup.py` (~40k) is a path/config junk-drawer. Validators are ENA/ToL-centric with blocking network calls. |
| 7 | **Cross-cutting / platform glue** | `common/utils/helpers.py`, `src/celery.py`, `src/main_config` | `helpers.py` god-module (8 near-identical `notify_*` functions, env access, threadlocal context). Centralized celery beat + websocket routing. |

Supporting/peripheral apps (low coupling, not focus areas): `api`, `copo_profile`, `copo_login`, `copo_landing_page`, `copo_news`, `copo_file`, `copo_accession(s)`, `copo_tol_dashboard`, `copo_manifest_wizard`.

---

## 2. The core structural problems

1. **`copo_core` is a god-app.** `broker_da.py` (lines 5–17) statically imports util classes from 6 submission apps and routes *all* CRUD through `do_save_edit()`; meanwhile 36+ files import back from `copo_core` (`models.ProfileType/UserDetails`, `views.web_page_access_checker`). This is a genuine **circular dependency**, currently survivable only because of import timing.

2. **The 5 submission engines are ~80% duplication.** The webin-cli invocation string (`java -jar {ENA_CLI} … -context {type} -manifest …`) is copy-pasted across ≥4 files (`EnaAssembly.py`, `ena_helper.py`, `EnaTaggedSequence.py`, the read path). Each app re-implements the same Celery-task → queue-poll → transfer → notify pipeline. No shared base class.

3. **Config/credentials are scattered.** WEBIN creds + ENA endpoints read from env at module scope in 23 files; rotating a secret or overriding in a test means editing many files and fighting import order.

4. **Notification is a god-function family.** `helpers.py` has 8 near-identical `notify_*` functions differing only by a hardcoded channel-group string, mirrored by 9 near-identical websocket consumers in `copo_core/consumers.py`.

5. **DToL specifics permeate generic code.** Validators branch on profile-type strings (`"DTOL"/"ASG"/"ERGA"`); validator discovery is reflection-based over hardcoded modules (`validation_celery_handler.py` lines 57–70); `dtol_lookups.py` mixes mappings, rules, and reference data (and has a latent bug: `TOL_PROFILE_TYPES_FULL` defined at line 1486 then overwritten at 1490).

6. **Dual Postgres/Mongo split** with no cross-DB transactional consistency — structural, but deep and not worth unwinding (see §6).

7. **No safety net.** One real test file. Refactoring hazard is the gating concern.

---

## 3. Decoupling assessment — ranked targets

### Tier 1 — Quick wins (behavior-preserving, low risk, do first)

| ID | Target | Seam | Effort | Risk | Payoff |
|----|--------|------|--------|------|--------|
| **QW1** | Centralize WEBIN/ENA **credentials + endpoints** | new `common/config/` accessor; replace 23 module-scope `get_env(...)` reads with lazy call-time accessors | S | low | **high** — unblocks testability of everything ENA-touching |
| **QW2** | Collapse 8 `notify_*` into **one parameterized notifier** + `{type→channel_group}` registry | `common/utils/helpers.py` lines 86–236; keep named shims for back-compat | S | low | med-high — shrinks god-module, creates registry SP3 needs |
| **QW3** | Move hardcoded **institution coordinates** out of `dtol_lookups.py`; fix the `TOL_PROFILE_TYPES_FULL` double-definition bug | lines 1093–1252 → data file; fix 1486/1490 | S | low | low-med (hygiene + one real bug) |
| **QW4** | Extract a single **`run_webin_cli()`** helper | replace copy-pasted subprocess command in ≥4 files | S-M | med (live path, no tests) | **high** — kills the most-duplicated operational concern; first brick of the submission core |

### Tier 2 — Structural plays (gated on Tier 1 + characterization tests)

| ID | Target | Seam | Effort | Risk | Payoff |
|----|--------|------|--------|------|--------|
| **SP1** | **Shared submission engine** (strategy interface) to kill 5-app duplication | new `common/submission/`: `SubmissionEngine` ABC + pluggable **repository backends** (ENA-webin, ENA-REST, Zenodo…) + **transfer backends** (Aspera…). Each app becomes a thin subclass. | L | high | **very high** — biggest maintenance win |
| **SP2** | **Break the `broker_da` ↔ submission-apps cycle** | replace static imports with a **registry**: apps `register_component_handler(type, Engine)`; broker_da dispatches by string. (The SP1 engine registry *is* this table.) | M-L | high | high — dissolves the god-app via dependency inversion |
| **SP3** | **Consolidate 9 websocket consumers** into one generic consumer | `copo_core/consumers.py`, driven by QW2 registry | M | med | medium |
| **SP4** | **De-DToL-ify validation** (sample-type plugin model) | per-profile-type plugin contributing mapping/enums/rules/validators; replace reflection discovery with explicit registration; split `dtol_lookups.py` into per-profile bundles | L | high | med-high (enables new sample types; lower urgency — DToL is the dominant workload) |

### Tier 3 — Opportunistic
- **LP1** — split `ena_helper.py` monolith into XML-templating / REST-client / status-push (do as part of SP1's ENA backend, not standalone). M / med / med.
- **LP2** — carve `common/lookup/lookup.py` into typed config sections (alongside QW1). M / low-med / low-med.

---

## 4. Sequencing

```
QW1 (config) ─┬───────────────► SP1 (submission engine) ──► SP2 (break cycle, via registry)
QW4 (webin helper) ────────────┘            │
QW2 (notifier registry) ─┬──────────────────┘
                         └──► SP3 (consumer consolidation)
QW3 (coords/bug) — independent, anytime
dtol_lookups split ──► SP4 (validation plugins)

[CHARACTERIZATION TESTS on ≥1 submission flow] MUST precede SP1 / SP2 / SP4
```

Hard rules:
1. **All Tier 1 before any Tier 2** — Tier 1 produces the primitives (config, webin helper, notifier registry) that Tier 2 assembles.
2. **Characterization tests before SP1/SP2/SP4** — pin one flow's current behavior end-to-end first (read is smallest: 26-line task). QW1 + QW2 are what make those tests writable.
3. **SP1 and SP2 are one project** — the engine registry that powers SP1 is the dispatch table that breaks the cycle. Don't split them.
4. **Migrate one submission app at a time** behind the new base; keep legacy paths live until each is proven in production.

---

## 5. Target architecture sketch

```
common/
  config/            # QW1: single lazy, injectable secrets/endpoints accessor
  submission/        # SP1+SP2: the new core
    engine.py        #   SubmissionEngine ABC: validate/build/transfer/submit/poll/notify
    registry.py      #   {component → engine}; broker_da dispatches via this (kills cycle)
    repositories/    #   ENA-webin, ENA-REST, Zenodo, (Dataverse/DSpace/figshare)
    transfer/        #   Aspera backend (FileTransferUtils behind an interface)
    webin_cli.py     #   QW4: the one webin invocation helper
  notify/            # QW2: one notifier + {type → channel_group} registry
  validation/        # SP4: framework + per-profile-type plugins (explicit registration)
  reference_data/    # QW3: institution coords, profile-type bundles (was dtol_lookups)

src/apps/<submission app>/   # thin SubmissionEngine subclass: manifest/context only
src/apps/copo_core/
  consumers.py       # SP3: one generic consumer driven by notify registry
  broker_da.py       # depends on submission.registry interface — imports NO submission apps
```

Directional rule: **concrete engines and apps depend on `common/submission` interfaces; `copo_core` depends on those interfaces too — nothing in `common` or `copo_core` imports a concrete submission app.** That inversion dissolves the god-app.

---

## 6. What should NOT be decoupled

- **The Postgres/Mongo dual-DB split.** Unwinding it is a multi-quarter data migration touching all of `common/dal` and every `DAComponent` consumer; the boundary is stable and the inconsistency window is tolerable for metadata. Leave it — just don't deepen the coupling.
- **The `DAComponent` base itself.** It's a reasonable seam already centralizing Mongo access; repository-pattern abstraction here is low payoff.
- **Dataverse/DSpace/figshare stubs** in `submission_da.py.` Don't clean incomplete features; let them become cheap backends *if* SP1 lands.
- **Fully purging DToL from core.** Build the plugin seam (SP4) so new sample types are addable, but don't burn effort removing every DToL reference — DToL is the dominant real workload.
- **Centralized celery beat schedule** (`src/celery.py`). Centralized scheduling aids operational visibility; per-app schedules would scatter it.

---

## 7. Risks & guardrails

- **No test net (dominant risk).** Before SP1/SP2/SP4, write **characterization tests** pinning one submission flow end-to-end (capture the webin command string, notify payloads, Mongo writes). QW1/QW2 are prerequisites that make these writable.
- **The `broker_da` circular import** survives only on import timing. Don't reshuffle its imports — convert to registration (SP2) instead, or startup may break.
- **Reflection-based validator discovery** silently drops a validator if a class moves. When doing SP4, replace with explicit registration *and* add a test asserting the expected validator set loads.
- **Live subprocess paths (webin-cli, Aspera) untested.** Extract behind an interface with the command string kept byte-identical (characterization test on the string) first; change behavior in a separate step.
- **Module-scope env reads → lazy accessors (QW1)** can convert an import-time failure into a call-time one; verify nothing relied on the import-time crash as a config check.
- **Big-bang temptation.** Five near-identical apps invite a single rewrite; resist — migrate incrementally behind the new base.

---

## Verification / how to use this

This is an analysis, so "verification" means sanity-checking the claims against the tree before acting on any item:
- God-app / cycle: inspect `src/apps/copo_core/broker_da.py` imports vs. submission-app imports of `copo_core`.
- Duplication: diff the webin-cli command strings across `common/ena_utils/ena_helper.py`, `copo_assembly_submission/utils/EnaAssembly.py`, `copo_barcoding_submission/utils/EnaTaggedSequence.py`.
- Config scatter: `grep -rl "WEBIN_USER\|ENA_CLI" common/ src/`.
- Coupling epicenter + bug: `common/schema_versions/lookup/dtol_lookups.py` lines 1486/1490 and 1093–1252.
- Test gap: `find test -name 'test_*.py'`.

Recommended first iteration if you choose to act: **QW1 → QW2 → QW3/QW4** (all low-risk), then write characterization tests on the read-submission flow, then scope **SP1+SP2** as a single project migrating one app at a time.
