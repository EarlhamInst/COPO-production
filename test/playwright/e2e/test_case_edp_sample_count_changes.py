import re

import pytest
from playwright.sync_api import Page, expect

from test.playwright.e2e.utils.edp_helpers import (
    build_edp_manifest,
    copo_sample_ids,
    download_edp_manifest,
    edit_edp_profile_sample_count,
    manifest_sample_ids,
    poll_sapio_study_status,
    save_edp_manifest,
    submit_edp_to_sapio,
    submit_sample_to_ena,
    upload_edp_manifest,
)
from test.playwright.e2e.utils.edp_helpers import (
    EDP_PROFILE_FIELDS,
    create_edp_profile,
    edp_related_enq,
    read_sapio_project,
)
from test.playwright.e2e.utils.helpers import delete_profile_by_title

PROFILE_TITLE = 'Playwright EDP Sample Count Changes'
PROFILE_DESCRIPTION = 'A description that is at least twenty characters long.'
# The second batch's sample reuses the first one's ENA sample under its own
# customer name (`Submitter sample reference`: letters and digits, max 15)
SECOND_SUBMITTER_REFERENCE = 'sample1batch2'


def _sapio_samples(project_id):
    return read_sapio_project(project_id, sample_fields=['SampleId', 'C_CustomerSampleName'])['samples']


@pytest.mark.external
def test_edp_samples_submitted_in_batches(page: Page, tmp_path, sapio_test_instance, ena_dev_sandbox):
    # An EDP customer submits their samples, then needs more:
    # 1 sample submitted to Sapio --> lowering the count below it is refused
    # --> raising it to 2 adds a Sapio sample --> the blank manifest lists only
    # the new sample --> re-uploading the first manifest is refused --> the new
    # manifest is accepted and saved alongside the first sample --> 'Submit to
    # Sapio' --> both samples are in Sapio under their own customer names.
    #
    # Clearing leftovers by title also deletes their Sapio projects (the EDP
    # post-delete hook), so an interrupted run doesn't leak projects.
    delete_profile_by_title(PROFILE_TITLE)

    _, profile_id, project_id = create_edp_profile(
        page,
        PROFILE_TITLE,
        PROFILE_DESCRIPTION,
        related_enq=edp_related_enq(),
        **{**EDP_PROFILE_FIELDS, 'no_of_samples': 1},
    )
    assert re.fullmatch(r'PR\d+', project_id), f'Unexpected Sapio project id {project_id!r}'

    accession = submit_sample_to_ena(page, profile_id)

    # --- First batch: the profile's one sample ---
    first_downloaded = download_edp_manifest(page, profile_id, tmp_path / 'first_downloaded.xlsx')
    first_manifest = tmp_path / 'first_patched.xlsx'
    first_sample_id, first_expected = build_edp_manifest(first_downloaded, first_manifest, accession)

    response = upload_edp_manifest(page, profile_id, first_manifest)
    assert response.status == 200, f'First LIMS manifest rejected: {response.text()[:2000]}'
    save_edp_manifest(page)
    feedback = submit_edp_to_sapio(page)
    assert feedback.get('status') == 'success', f'Submit to Sapio failed: {feedback}'
    poll_sapio_study_status(profile_id, project_id)

    # --- Lowering the count below the submitted samples is refused ---
    feedback = edit_edp_profile_sample_count(page, PROFILE_TITLE, 0)
    message = (
        f'1 of 1 samples in Sapio project {project_id} have already been submitted '
        "so, the number of samples cannot be set lower than 1."
    )
    assert feedback == {'status': 'error', 'message': message}
    expect(page.locator('.modal.in .formMessageDiv')).to_contain_text(message)
    assert [s['SampleId'] for s in _sapio_samples(project_id)] == [first_sample_id]

    # --- Raising it adds a sample to the Sapio project ---
    feedback = edit_edp_profile_sample_count(page, PROFILE_TITLE, 2)
    assert feedback.get('status') == 'success', f'Increasing the sample count failed: {feedback}'
    sapio_samples = {s['SampleId']: s['C_CustomerSampleName'] for s in _sapio_samples(project_id)}
    assert len(sapio_samples) == 2, f'Expected 2 Sapio samples, found {sapio_samples}'
    assert sapio_samples[first_sample_id] == first_expected['Submitter sample reference']
    (second_sample_id,) = set(sapio_samples) - {first_sample_id}

    # --- The blank manifest lists only the new sample ---
    second_downloaded = download_edp_manifest(page, profile_id, tmp_path / 'second_downloaded.xlsx')
    assert manifest_sample_ids(second_downloaded) == [second_sample_id]

    # --- Uploading the submitted sample again is refused ---
    response = upload_edp_manifest(page, profile_id, first_manifest)
    assert response.status == 400
    expect(page.locator('#singlecell_info')).to_contain_text(
        "The 'Submitter sample reference' field of a submitted sample cannot be changed. "
        f"Please restore the original 'Submitter sample reference' value of these samples: {first_sample_id}.",
        timeout=30000,
    )

    # --- The new sample is accepted and saved alongside the first ---
    second_manifest = tmp_path / 'second_patched.xlsx'
    sample_id, _ = build_edp_manifest(
        second_downloaded, second_manifest, accession, submitter_reference=SECOND_SUBMITTER_REFERENCE
    )
    assert sample_id == second_sample_id

    response = upload_edp_manifest(page, profile_id, second_manifest)
    assert response.status == 200, f'Second LIMS manifest rejected: {response.text()[:2000]}'
    save_edp_manifest(page)
    assert sorted(copo_sample_ids(profile_id)) == sorted([first_sample_id, second_sample_id])

    feedback = submit_edp_to_sapio(page)
    assert feedback.get('status') == 'success', f'Submit to Sapio failed: {feedback}'
    poll_sapio_study_status(profile_id, project_id)

    sapio_samples = {s['SampleId']: s['C_CustomerSampleName'] for s in _sapio_samples(project_id)}
    assert sapio_samples == {
        first_sample_id: first_expected['Submitter sample reference'],
        second_sample_id: SECOND_SUBMITTER_REFERENCE,
    }

    delete_profile_by_title(PROFILE_TITLE)
    assert not read_sapio_project(project_id)['exists'], f'Sapio project {project_id} was not deleted'
