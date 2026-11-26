import json
import os
import re
import time
import warnings

import openpyxl
import pytest
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, expect

from test.playwright.e2e.utils.helpers import (
    create_biodata_profile,
    delete_profile_by_title as _delete_profile_by_title,
    dismiss_tour_if_present,
    poll_sample_accession as _poll_sample_accession,
    run_django_shell as _run_django_shell,
    upload_manifest,
)

TITLE = "Submission Journey Manifest Validation"
DESCRIPTION = "A description that is at least twenty characters long."

TEST_FILES_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "test_files")
)
SAMPLE_MANIFEST = os.path.join(TEST_FILES_DIR, "COPO_FAANG_manifest_success1.xlsx")
SINGLECELL_MANIFEST = os.path.join(
    TEST_FILES_DIR, "copo_single_cell_manifest_version_faang_sc_rnaseq_success1.xlsx"
)
# Every file name the manifest's data row puts in a term_type=="file" column
# across the `file`, `expression_data_file` and `analysis_derived_data` worksheets.
# submit_singlecell() hard-errors ("missing from storage") if any one of them
# isn't already an object in the profile's bucket, so this list must track
# the manifest exactly.
SINGLECELL_DATA_FILES = [
    os.path.join(TEST_FILES_DIR, "single_cell_read1_4.fastq.gz"),
    os.path.join(TEST_FILES_DIR, "single_cell_read2_4.fastq.gz"),
    os.path.join(TEST_FILES_DIR, "single_cell_index1_4.fastq.gz"),
    os.path.join(TEST_FILES_DIR, "single_cell_index2_4.fastq.gz"),
    os.path.join(TEST_FILES_DIR, "single_cell_expression_data_file4.h5ad"),
    os.path.join(TEST_FILES_DIR, "single_cell_analysis_derived_from_file4.json"),
    os.path.join(TEST_FILES_DIR, "single_cell_white_list_barcode_file4.tsv"),
]

# Checklist dropdown <option> values — confirmed live, both pages have their
# own separate checklist lists. Selected explicitly rather than assumed to
# be the default, since checklist ordering isn't guaranteed stable (these
# lists are populated from a Mongo collection kept current by a Celery task,
# not a fixed order in code).
FAANG_SAMPLE_CHECKLIST = "COPO_FAANG"
# The single-cell checklist's visible label is the full name ("Functional
# Annotation of Animal Genomes metadata"), FAANG being the acronym for it.
FAANG_SINGLECELL_CHECKLIST = "version_faang_sc_rnaseq"

# What COPO_FAANG_manifest_success1.xlsx actually submits (its "Organism"
# field). The single-cell manifest's own sample sheet describes a different
# species by default (Arenicola marina / 6344) -- the two fixtures were
# authored independently and were never meant to describe the same animal.
# When _prepare_singlecell_study patches in the real
# accession from that submission, it must patch these to match too, or the
# single-cell manifest ends up claiming a real Homo sapiens accession is
# Arenicola marina.
SAMPLE_MANIFEST_SCIENTIFIC_NAME = "Homo sapiens"
SAMPLE_MANIFEST_TAXON_ID = 9606

# The single-cell manifest's "study" sheet Study ID -- confirmed by
# inspecting the fixture directly, matches sample!A5 too (both driven by
# the same array formula reading from the study sheet).
SINGLECELL_STUDY_ID = "STUDY004"

# The schema routes each data file to ENA or Zenodo by its term_repository
# column: read FASTQs go to ENA, everything else into the Zenodo deposition,
# alongside the generated manifest.
SINGLECELL_ENA_FILE_NAMES = {os.path.basename(f) for f in SINGLECELL_DATA_FILES[:2]}
SINGLECELL_ZENODO_FILE_NAMES = {
    os.path.basename(f) for f in SINGLECELL_DATA_FILES[2:]
} | {"manifest.xlsx"}

# Where the app under test submits to (ZENODOURL in the copo_web compose
# service). This test's own process may have a different ZENODOURL, so the
# sandbox checks below set it explicitly rather than inherit it: they must
# never read from or delete on production Zenodo.
ZENODO_SANDBOX_URL = os.environ.get("ZENODO_SANDBOX_URL", "https://sandbox.zenodo.org/api")


def _poll_singlecell_study_accession(
    profile_id, study_id, repository="ena", timeout=1200, interval=10
):
    # process_ena_submission / process_submission_zenodo (Celery beat,
    # src/celery.py) are the single-cell equivalents of
    # process_pending_sample_submission -- same "poll the DB, no direct
    # event to wait on" reasoning as _poll_sample_accession.
    #
    # Status lands on a *repository-suffixed* field inside the study
    # element of a *list*, not a bare top-level field like Sample uses:
    # Singlecell.update_component_status() does
    #   components.study.$.<key>_<repository>
    # via positional-array update matched on components.study[].study_id
    # (common/dal .../utils/da.py -- update_component_status()), so the
    # relevant fields are e.g. status_ena / accession_ena / error_ena on
    # whichever components.study[] entry has study_id == study_id.
    #
    # Both repositories record a failure the same way: the study goes back to
    # 'pending' with error_<repository> set, so a failure fails this poll
    # straight away rather than waiting for the timeout.
    code = (
        "from src.apps.copo_single_cell_submission.utils.da import Singlecell\n"
        f"records = Singlecell(profile_id={profile_id!r}).execute_query({{'profile_id': {profile_id!r}}})\n"
        "study = {}\n"
        "for r in records:\n"
        "    for s in r.get('components', {}).get('study', []):\n"
        f"        if s.get('study_id') == {study_id!r}:\n"
        "            study = s\n"
        "print('POLL_RESULT:' + '|'.join(str(study.get(f'{key}_' + "
        f"{repository!r}, '')) for key in ('status', 'accession', 'error')))\n"
    )
    deadline = time.time() + timeout
    status = ""
    while time.time() < deadline:
        marker_line = next(
            (
                line
                for line in _run_django_shell(code).splitlines()
                if line.startswith("POLL_RESULT:")
            ),
            "POLL_RESULT:||",
        )
        status, accession, error = marker_line[len("POLL_RESULT:") :].split("|")
        if status == "accepted" and accession:
            return accession
        if status == "rejected" or error:
            raise AssertionError(
                f"Single-cell study submission to {repository} failed: status={status!r} error={error!r}"
            )
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for a real single-cell study {repository} accession (last status={status!r}). "
        "Check that the Celery beat and worker debug configurations are running."
    )


def _zenodo_sandbox_shell(code):
    # Runs against the Zenodo *sandbox*, whatever this process's own
    # ZENODOURL says (see ZENODO_SANDBOX_URL). ZENODOTOKEN is inherited, so
    # it must be a sandbox token -- the same one the app under test uses.
    return _run_django_shell(
        "from src.apps.copo_single_cell_submission.utils.zenodo.deposition import Zenodo_deposition\n"
        + code,
        env={"ZENODOURL": ZENODO_SANDBOX_URL},
    )


def _read_zenodo_deposition(deposition_id):
    # ZenodoClient.do_get() prints the URL it calls, so pick out the marker line.
    out = _zenodo_sandbox_shell(
        "import json\n"
        f"d = Zenodo_deposition().read(_id={deposition_id!r})\n"
        "print('POLL_RESULT:' + json.dumps({'state': d.get('state'), "
        "'files': [f['filename'] for f in d.get('files', [])]}))\n"
    )
    marker_line = next(line for line in out.splitlines() if line.startswith("POLL_RESULT:"))
    return json.loads(marker_line[len("POLL_RESULT:") :])


def _delete_zenodo_draft(deposition_id):
    # Only unpublished drafts can be deleted; the test never publishes, so
    # this keeps repeated runs from piling up drafts in the sandbox account.
    _zenodo_sandbox_shell(f"Zenodo_deposition().delete(_id={deposition_id!r})\n")


def _click_singlecell_study_action(page, action):
    # See the long comments in _prepare_singlecell_study
    # for why the button must be scoped to the study tab's own table, and why
    # the test waits for the study row's auto-selection instead of clicking it.
    #
    # A page tour step can be shown again while the test waits
    # on a long step and its overlay can block the submission
    # button click so it is dismissed beforehand.
    dismiss_tour_if_present(page)
    page.wait_for_function(
        "() => $.fn.dataTable.isDataTable('#singlecell_table_study') && "
        "$('#singlecell_table_study').DataTable().rows({selected: true}).count() > 0",
        timeout=30000,
    )
    page.click(f'.copo-dt[data-action="{action}"][data-table="singlecell_table_study"]')


def test_sample_manifest_validation_and_singlecell_rejects_stale_accession(
    page: Page, ena_dev_sandbox
):
    # Entirely local/synchronous — no real ENA or Zenodo submissions. See
    # test_singlecell_ena_submission_journey / test_singlecell_zenodo_submission_journey
    # (marked "external") for the
    # real submission flow this manifest data feeds into.
    #
    # create_biodata_profile()'s own success check only confirms *a*
    # matching-titled card is visible -- if a same-titled profile from an
    # interrupted previous run is still around, that check passes without a
    # new profile ever being created, silently reusing stale state. Belt and
    # suspenders: clear out any leftover with this title first.
    _delete_profile_by_title(TITLE)
    create_biodata_profile(page, TITLE, DESCRIPTION)

    # --- Sample manifest: upload, validate, save ---
    page.locator(".pcomponent-button", has_text="Samples").first.click()
    page.wait_for_load_state("networkidle")
    dismiss_tour_if_present(page)

    page.select_option("#checklist_id", FAANG_SAMPLE_CHECKLIST)
    page.click(".new-general-sample-spreadsheet-template")
    # The dialog's own content (including its real #file input) loads via an
    # async request kicked off by the click above — wait for it before
    # touching #file, or upload_manifest() could target stale/wrong content.
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1000)
    upload_manifest(page, SAMPLE_MANIFEST)
    # Validation is genuinely slow server-side (confirmed live: single-digit
    # seconds is not enough, 30s default action timeout isn't either) —
    # give it a generous explicit budget rather than assume the default.
    expect(page.locator("#save_sample_button")).to_be_enabled(timeout=60000)
    page.click("#save_sample_button")

    # --- Single-cell manifest: select FAANG checklist, upload, expect rejection ---
    # This fixture's Biosample Accession column has a hardcoded example value
    # (SAMEA132143677) baked in when it was authored. The app validates that
    # value against real ENA data — and per what Felix flagged, ENA wipes its
    # dev sandbox database every 24 hours, so *any* hardcoded accession goes
    # stale within a day regardless of how recently the fixture was created.
    # A real, currently-valid accession can only come from an actual ENA
    # submission — that's the external journeys' job, not
    # this local/fast test's. So what's actually correct to assert here isn't
    # "validation succeeds" (it structurally can't, deterministically, with a
    # static fixture) but "validation correctly rejects a stale accession" —
    # itself a real, useful check that the app is actually verifying
    # accessions against ENA rather than accepting anything well-formed.
    page.goto("/copo")
    dismiss_tour_if_present(page)
    page.locator(".pcomponent-button", has_text="Single-cell").first.click()
    page.wait_for_load_state("networkidle")
    dismiss_tour_if_present(page)

    page.select_option("#checklist_id", FAANG_SINGLECELL_CHECKLIST)
    page.click(".new-singlecell-spreadsheet-template")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1000)

    with page.expect_response(
        lambda r: "parse_singlecell_spreadsheet" in r.url, timeout=60000
    ) as response_info:
        upload_manifest(page, SINGLECELL_MANIFEST)
    response = response_info.value

    assert response.status == 400
    assert "Invalid ENA sample" in response.text() or "Expected a valid Biosample Accession" in response.text()
    expect(page.locator("#save_singlecell_button")).to_be_disabled()

    # Not the UI delete flow here (already covered by
    # test_case_profile.py) — a profile carrying real sample/single-cell
    # data reliably fails to delete through the UI in ways an empty profile
    # doesn't (confirmed live: the confirm click reports success, but
    # neither the DB record nor any server-side error/log entry ever
    # appears). Clean up directly at the DB level instead, the same way
    # create_test_user is invoked elsewhere in this suite.
    _delete_profile_by_title(TITLE)


# The real submissions are split into one test per repository so that the 
# submissions to ENA and Zenodo are tested independently since either of them 
# can fail, be slow or be rerun without the other.
# 
# Both share `_prepare_singlecell_study()` which gets a study ready to
# submit. That setup registers the sample with ENA because the single-cell
# manifest must reference to a real BioSample accession.
#
# The submit buttons on the study tab are added by place_task_buttons()
# (static/copo/js/generic_handlers_ext2.js), one per repository the schema lists
# for the study component -- not by setup_profile_types.py.


def _prepare_singlecell_study(page, tmp_path, title):
    '''
    Submission process:
    1) Create a profile -> Upload sample manifest 
       -> Submit sample using "Submit to ENA" button against EBI's dev instance 
       -> Wait for a BioSample accession to be assigned 
    2) Patch that BioSample accession into the Single-cell manifest 
       (using openpyxl since ENA wipes its dev instance DB every
       24h so no static accession would stay valid) 
       -> Single-cell manifest validates successfully 
    3) Upload the data files referenced in the Single-cell manifest.
    4) Leave the browser on the profile's Single-cell page and return the
       profile ID.
    '''
    description = "A description that is at least twenty characters long."
    # create_biodata_profile()'s own success check only confirms *a*
    # matching-titled card is visible -- if a same-titled profile from an
    # interrupted previous run is still sitting on the page (e.g. this test
    # failed before reaching its own cleanup), that check passes without a
    # new profile ever being created, silently reusing stale state. Belt and
    # suspenders: clear out any leftover with this title first.
    _delete_profile_by_title(title)
    create_biodata_profile(page, title, description)

    page.locator(".pcomponent-button", has_text="Samples").first.click()
    page.wait_for_load_state("networkidle")
    dismiss_tour_if_present(page)
    # URL is /copo/copo_sample/view/general/<profile_id>/general_sample --
    # the profile id is always second-to-last regardless of host/scheme.
    profile_id = page.url.rstrip("/").split("/")[-2]

    page.select_option("#checklist_id", FAANG_SAMPLE_CHECKLIST)
    page.click(".new-general-sample-spreadsheet-template")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1000)
    upload_manifest(page, SAMPLE_MANIFEST)
    expect(page.locator("#save_sample_button")).to_be_enabled(timeout=60000)
    page.click("#save_sample_button")

    # A new Shepherd tour step (highlighting the submit button itself,
    # data-tour-id="submit_record_button") appears once records exist --
    # dismiss it the same way as every other tour variant on this page.
    page.wait_for_timeout(1000)
    dismiss_tour_if_present(page)

    # Select the sample row -- DataTables Select extension toggles
    # `.selected` on click anywhere in the row -- then submit it.
    page.locator("#sample_table tbody tr").first.click()
    page.click('.copo-dt[data-action="submit_sample"]')

    # ensureRepositoryCredentialsThenSubmit() checks this user's own ENA
    # credentials first; a fresh test user has none, so the popup appears
    # offering COPO's default. If it doesn't appear (credentials already
    # valid), the submission just proceeds -- handle both paths.
    try:
        page.locator("#repo_cred_modal .cred-modal-default").wait_for(
            state="visible", timeout=5000
        )
    except PlaywrightTimeoutError:
        pass
    else:
        page.click("#repo_cred_modal .cred-modal-default")

    accession = _poll_sample_accession(profile_id)

    # Row 5 is the manifest's actual data row (row 3 is a filled-in
    # documentation example, row 4 is a "FILL OUT BELOW" divider) --
    # confirmed by inspecting the fixture directly.
    #
    # data_only=True matters here: most sheets' linking-ID columns (A5/B5,
    # e.g. Study ID) are array formulas, not plain values. openpyxl only
    # keeps a formula's cached result when loaded with data_only=True --
    # loading normally and saving back silently drops those cached values
    # to blank, corrupting every formula cell in the workbook regardless of
    # which cell was actually edited (confirmed live: this exact round-trip
    # blanked sample!A5 "STUDY004"). Loading with data_only=True instead
    # bakes in the last-computed values as plain values, so there's nothing
    # left to lose on save.
    patched_manifest = tmp_path / "singlecell_manifest_patched.xlsx"
    # The template's dropdown lists use an Excel data-validation extension
    # openpyxl can't read, so it warns and drops them from this temporary
    # copy. Harmless: COPO validates the values, not the dropdowns.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Data Validation extension is not supported", category=UserWarning)
        wb = openpyxl.load_workbook(SINGLECELL_MANIFEST, data_only=True)
    sample_sheet = wb["sample"]
    sample_sheet["C5"] = SAMPLE_MANIFEST_SCIENTIFIC_NAME
    sample_sheet["D5"] = SAMPLE_MANIFEST_TAXON_ID
    sample_sheet["E5"] = accession
    wb.save(patched_manifest)

    page.goto("/copo")
    dismiss_tour_if_present(page)
    page.locator(".pcomponent-button", has_text="Single-cell").first.click()
    page.wait_for_load_state("networkidle")
    dismiss_tour_if_present(page)

    page.select_option("#checklist_id", FAANG_SINGLECELL_CHECKLIST)
    page.click(".new-singlecell-spreadsheet-template")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1000)
    upload_manifest(page, str(patched_manifest))

    # Contrast with test_sample_manifest_validation_and_singlecell_rejects_stale_accession:
    # this accession is real and current, so validation should succeed.
    expect(page.locator("#save_singlecell_button")).to_be_enabled(timeout=60000)
    page.click("#save_singlecell_button")
    page.wait_for_load_state("networkidle")

    # Submission gates on the referenced data files actually existing in
    # the profile's S3/MinIO bucket (copo_single_cell.py's
    # check_s3_bucket_for_files(), matched by literal filename against what
    # the manifest's file/expression_data_file sheets reference) -- saving
    # the manifest only registers a DataFile record, it doesn't upload
    # bytes. Upload them via the profile's own Data files component first.
    # #file exists statically in this page's DOM (not injected async like
    # the sample/singlecell manifest dialogs), so a plain set_input_files
    # reliably fires its change handler -- no upload_manifest()-style
    # bypass needed here.
    page.goto(f"/copo/copo_files/view/{profile_id}/files")
    page.wait_for_load_state("networkidle")
    dismiss_tour_if_present(page)
    # .new-local-file opens #uploadModal (copo_files.js do_record_task());
    # #upload_local_files_button itself just programmatically clicks the
    # hidden #file input to open a native OS picker for a real user --
    # set_input_files() sets the files and fires #file's own
    # onchange="upload_files(this.files)" directly, so there's no need to
    # click that button at all.
    page.click(".new-local-file")
    page.set_input_files("#uploadModal #file", SINGLECELL_DATA_FILES)
    # The two ~5MB fastq.gz fixtures alone take 35-45s each against the
    # remote dev MinIO (measured directly server-side, not a hang -- see
    # project_demo_minio_distributed_fragility memory), so the default 60s
    # was not enough to cover all the data files uploading sequentially in one request.
    expect(page.locator("#page_alert_panel")).to_contain_text(
        "Files have been uploaded", timeout=300000
    )

    page.goto(f"/copo/copo_single_cell/view/{profile_id}/COPO_SINGLE_CELL/singlecell")
    page.wait_for_load_state("networkidle")
    dismiss_tour_if_present(page)

    # `#singlecell_table` is never a real element -- it's only a *prefix*.
    # do_render_component_table_tabs() (generic_handlers_ext2.js) builds one
    # real table per component tab, id = `<tableID>_<component>` (e.g.
    # `singlecell_table_study`, `singlecell_table_sample`, ...), and clones
    # the recordAction button toolbar into each tab with its own
    # `data-table="singlecell_table_<component>"` -- hence ~14 DOM matches
    # for the same data-action, one per tab, most pointing at the wrong
    # table. Must scope the click to the study tab's own button.
    #
    # No manual row click needed: on every `posttablerefresh`, the study
    # tab's own JS (copo_single_cell.js) auto-selects the first row via a
    # synthetic click *if nothing is already selected*
    # (`current_study_id == ''`) -- so with only one study, DataTables'
    # Select state should already show it selected once the table settles.
    # A manual click here is actively harmful: the study tab's custom click
    # handler calls load_records() on every click, which redraws (and
    # `rows().deselect()`s) the table, racing the very selection it's
    # trying to set. Just wait for the auto-select to land instead of
    # fighting it. _click_singlecell_study_action() does that wait.
    page.wait_for_timeout(1000)
    return profile_id


def _assert_submit_accepted(resp, action):
    assert resp.status == 200, f"{action} request failed: {resp.status} {resp.text()[:500]}"
    body = resp.json()
    assert (
        body.get("action_feedback", {}).get("status") == "success"
    ), f"{action} rejected: {body.get('action_feedback')}"


@pytest.mark.external
def test_singlecell_ena_submission_journey(page: Page, tmp_path, ena_dev_sandbox):
    # Submit the study to ENA's dev instance and wait for it to be accepted.
    title = "External ENA Submission Journey Profile"
    profile_id = _prepare_singlecell_study(page, tmp_path, title)

    with page.expect_response(lambda r: "copo_forms" in r.url, timeout=30000) as ri:
        _click_singlecell_study_action(page, "submit_singlecell_ena")

        # Same credentials-popup gate as the sample submission -- the real
        # copo_forms POST only fires once credentials are resolved, so this
        # has to happen inside the expect_response block.
        try:
            page.locator("#repo_cred_modal .cred-modal-default").wait_for(
                state="visible", timeout=5000
            )
        except PlaywrightTimeoutError:
            pass
        else:
            # A Shepherd tour step ("component_table_with_accessions") can
            # open on top of the credentials modal here and intercept the
            # click -- same overlay-blocking pattern as every other tour
            # variant on this page, dismiss it the same way.
            dismiss_tour_if_present(page)
            page.click("#repo_cred_modal .cred-modal-default")
    _assert_submit_accepted(ri.value, "submit_singlecell_ena")

    # The completion notification (not just the earlier "Submission has been
    # scheduled" queuing one) only exists because of a matching app fix in
    # common/ena_utils/ena_helper.py -- Singlecell.update_component_status()'s
    # own notify_frontend() call sends an empty message that never reaches
    # #submission-activity-log, it only silently repaints the status column.
    # Wait for the real logged completion line, mentioning both acceptance
    # and a real PRJ* project accession -- this is the actual "the test
    # isn't done until the UI says so" signal, not just a DB poll.
    expect(
        page.locator(
            '#submission-activity-log div[data-alert-type="success"]',
            has_text="accepted by ENA",
        )
    ).to_contain_text(re.compile(r"PRJ\w+"), timeout=1200000)

    accession = _poll_singlecell_study_accession(
        profile_id, SINGLECELL_STUDY_ID, timeout=30
    )
    assert accession and accession.startswith("PRJ")

    _delete_profile_by_title(title)


@pytest.mark.external
def test_singlecell_zenodo_submission_journey(page: Page, tmp_path, ena_dev_sandbox):
    # Submit the study to the Zenodo sandbox then, check that the draft deposition 
    # holds the non-read files and the manifest then, delete the draft.
    title = "External Zenodo Submission Journey Profile"
    profile_id = _prepare_singlecell_study(page, tmp_path, title)

    # No credentials popup modal will appear here because `copo_single_cell.js` 
    # only shows it for ENA
    with page.expect_response(lambda r: "copo_forms" in r.url, timeout=30000) as ri:
        _click_singlecell_study_action(page, "submit_singlecell_zenodo")
    _assert_submit_accepted(ri.value, "submit_singlecell_zenodo")

    # Celery copies the Zenodo files out of S3 first, then
    # process_submission_zenodo creates the deposition and uploads them.
    deposition_id = _poll_singlecell_study_accession(
        profile_id, SINGLECELL_STUDY_ID, repository="zenodo"
    )
    assert deposition_id.isdigit(), f"Unexpected Zenodo deposition id: {deposition_id!r}"

    try:
        deposition = _read_zenodo_deposition(deposition_id)
        assert deposition["state"] == "unsubmitted", f"Deposition should still be a draft: {deposition}"
        uploaded = set(deposition["files"])
        assert SINGLECELL_ZENODO_FILE_NAMES <= uploaded, (
            f"Missing from the Zenodo deposition: {sorted(SINGLECELL_ZENODO_FILE_NAMES - uploaded)}"
        )
        assert not SINGLECELL_ENA_FILE_NAMES & uploaded, (
            f"Read FASTQs belong in ENA, not Zenodo: {sorted(SINGLECELL_ENA_FILE_NAMES & uploaded)}"
        )
    finally:
        # Delete drafts from Zenodo to clean up, even if the assertions above fail
        _delete_zenodo_draft(deposition_id)

    _delete_profile_by_title(title)
