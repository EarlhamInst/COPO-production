import json
import re

import openpyxl
import pytest
from playwright.sync_api import Page, expect

from test.playwright.e2e.utils.edp_helpers import (
    EDP_CHECKLIST,
    add_edp_sample_batch,
    build_lims_manifest,
    copo_sample_ids,
    download_lims_manifest,
    manifest_sample_ids,
    open_edp_sample_batches,
    poll_sapio_study_status,
    save_lims_manifest,
    submit_lims_manifest_to_sapio,
    submit_sample_to_ena,
    upload_lims_manifest,
)
from test.playwright.e2e.utils.edp_helpers import (
    EDP_PROFILE_FIELDS,
    create_edp_profile,
    edp_related_enq,
    read_sapio_project,
)
from test.playwright.e2e.utils.helpers import (
    close_profile_form_if_open,
    delete_profile_by_title,
    run_django_shell,
)

PROFILE_TITLE_FOR_LIMS_SUBMISSION = 'Playwright EDP LIMS Submission Journey'
PROFILE_DESCRIPTION_FOR_LIMS_SUBMISSION = 'A description that is at least twenty characters long.'

PROFILE_TITLE_FOR_SAMPLE_BATCH_SUBMISSION = 'Playwright EDP Sample Count Changes'
PROFILE_DESCRIPTION_FOR_SAMPLE_BATCH_SUBMISSION = 'A description that is at least twenty characters long.'

# The second batch's sample reuses the first one's ENA sample under its own
# customer name (`Submitter Sample Reference`: letters and digits, max 15)
SECOND_SUBMITTER_REFERENCE = 'sample1batch2'

def _sapio_field_mapping(expected):
    # Which Sapio field each manifest label is written to, straight from the
    # EI_EDP schema's sapio_name column ('Object:Field') -- the same mapping
    # SapioAdapter._submit_manifest() uses. Only Project and Sample fields
    # that aren't 'protected' are written by the adapter.
    #
    # Each expected manifest value is also swapped for the value Sapio stores
    # (e.g. 'UK-GDPR' -> 'GDPR') with the adapter's own _to_sapio_value().
    code = (
        'import json\n'
        'from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas\n'
        'from src.apps.ei_edp.utils.lims.sapio.adapter import _to_sapio_value\n'
        f"expected = json.loads({json.dumps(expected, default=str)!r})\n"
        f"schema = SinglecellSchemas().get_schema(schema_name='EI_EDP', target_id={EDP_CHECKLIST!r})\n"
        'fields = []\n'
        "for component in ('study', 'sample'):\n"
        '    for f in schema.get(component, []):\n'
        "        label = str(f.get('term_label', '')).strip()\n"
        "        fields.append({'label': label, 'sapio_name': f.get('sapio_name'),\n"
        "                       'behaviour': f.get('term_access'),\n"
        "                       'sapio_value': _to_sapio_value(f.get('term_name'), expected.get(label))})\n"
        "print('MAPPING_RESULT:' + json.dumps(fields, default=str))\n"
    )
    marker_line = next(line for line in run_django_shell(code).splitlines() if line.startswith('MAPPING_RESULT:'))
    mapping = {}
    for field in json.loads(marker_line[len('MAPPING_RESULT:'):]):
        sapio_name = field['sapio_name']
        if not isinstance(sapio_name, str) or ':' not in sapio_name or field['behaviour'] == 'protected':
            continue
        sapio_object, sapio_field = sapio_name.split(':', 1)
        if sapio_object in ('Project', 'Sample') and sapio_field:
            mapping[field['label']] = (sapio_object, sapio_field, field['sapio_value'])
    return mapping

def _sapio_samples(project_id):
    return read_sapio_project(project_id, sample_fields=['SampleId', 'C_CustomerSampleName'])['samples']


def _same_value(expected, actual):
    # Sapio returns numbers as numbers and may differ in case for picklist
    # values; the manifest holds what the customer typed.
    try:
        return float(expected) == float(actual)
    except (TypeError, ValueError):
        return str(expected).strip().casefold() == str(actual).strip().casefold()

@pytest.mark.external
def test_edp_samples_submitted_in_batches(page: Page, tmp_path, sapio_test_instance, ena_dev_sandbox):
    # An EDP customer submits their samples, then needs more:
    # 1 sample submitted to Sapio --> its batch can no longer be changed
    # --> adding a batch of 1 adds a Sapio sample --> that batch's blank manifest lists only
    # the new sample --> re-uploading the first manifest is refused --> the new
    # manifest is accepted and saved alongside the first sample --> 'Submit to
    # Sapio' --> both samples are in Sapio under their own customer names.
    #
    # Clearing leftovers by title also deletes their Sapio projects (the EDP
    # post-delete hook), so an interrupted run doesn't leak projects.
    delete_profile_by_title(PROFILE_TITLE_FOR_SAMPLE_BATCH_SUBMISSION)

    _, profile_id, project_id = create_edp_profile(
        page,
        PROFILE_TITLE_FOR_SAMPLE_BATCH_SUBMISSION,
        PROFILE_DESCRIPTION_FOR_SAMPLE_BATCH_SUBMISSION,
        related_enq=edp_related_enq(),
        **{**EDP_PROFILE_FIELDS, 'no_of_samples': 1},
    )
    assert re.fullmatch(r'PR\d+', project_id), f'Unexpected Sapio project id {project_id!r}'

    accession = submit_sample_to_ena(page, profile_id)

    # --- First batch: the profile's one sample ---
    first_downloaded = download_lims_manifest(page, profile_id, tmp_path / 'first_downloaded.xlsx')
    first_manifest = tmp_path / 'first_patched.xlsx'
    first_sample_id, first_expected = build_lims_manifest(first_downloaded, first_manifest, accession)

    response = upload_lims_manifest(page, profile_id, first_manifest)
    assert response.status == 200, f'First LIMS manifest rejected: {response.text()[:2000]}'
    save_lims_manifest(page)
    feedback = submit_lims_manifest_to_sapio(page)
    assert feedback.get('status') == 'success', f'Submit to Sapio failed: {feedback}'
    poll_sapio_study_status(profile_id, project_id)

    # --- The first batch is locked now that its sample has been submitted ---
    open_edp_sample_batches(page, PROFILE_TITLE_FOR_SAMPLE_BATCH_SUBMISSION)
    first_batch_row = page.locator('.sample-batches-table tbody tr').first
    expect(first_batch_row).to_contain_text('Submitted')
    expect(first_batch_row.locator('.edit-sample-batch')).to_have_count(0)
    close_profile_form_if_open(page)
    assert [s['SampleId'] for s in _sapio_samples(project_id)] == [first_sample_id]

    # --- Adding a batch adds a sample to the Sapio project ---
    result = add_edp_sample_batch(
        page,
        PROFILE_TITLE_FOR_SAMPLE_BATCH_SUBMISSION,
        no_of_samples=1,
        sample_type=EDP_PROFILE_FIELDS['sample_type'],
        container_type=EDP_PROFILE_FIELDS['container_type'],
    )
    assert result.get('status') == 'success', f'Adding a batch of samples failed: {result}'
    assert len(result['batches']) == 2
    second_batch_id = result['batches'][1]['batch_id']
    # Each batch has its own LIMS manifest. The first batch's study ID is the
    # Sapio project ID and later batches add their number.
    second_study_id = f'{project_id}-B2'
    close_profile_form_if_open(page)
    sapio_samples = {s['SampleId']: s['C_CustomerSampleName'] for s in _sapio_samples(project_id)}
    assert len(sapio_samples) == 2, f'Expected 2 Sapio samples, found {sapio_samples}'
    assert sapio_samples[first_sample_id] == first_expected['Submitter Sample Reference']
    (second_sample_id,) = set(sapio_samples) - {first_sample_id}

    # --- The second batch's blank manifest lists only its new sample ---
    second_downloaded = download_lims_manifest(
        page, profile_id, tmp_path / 'second_downloaded.xlsx', batch_id=second_batch_id
    )
    assert manifest_sample_ids(second_downloaded) == [second_sample_id]
    assert openpyxl.load_workbook(second_downloaded)['sample']['C5'].value == second_study_id

    # --- Changing the `Submitter Sample Reference` field value of 
    # submitted samples is not allowed because it is used as an identifier in Sapio ---
    changed_manifest = tmp_path / 'first_changed_reference.xlsx'
    build_lims_manifest(
        first_downloaded, changed_manifest, accession, submitter_reference='changed-reference'
    )
    response = upload_lims_manifest(page, profile_id, changed_manifest)
    assert response.status == 400
    expect(page.locator('#singlecell_info')).to_contain_text(
        "The 'Submitter Sample Reference' field of a submitted sample cannot be changed. "
        f"Please restore the original 'Submitter Sample Reference' value of these samples: {first_sample_id}.",
        timeout=30000,
    )

    # --- The new sample is accepted and saved alongside the first ---
    second_manifest = tmp_path / 'second_patched.xlsx'
    sample_id, _ = build_lims_manifest(
        second_downloaded, second_manifest, accession, submitter_reference=SECOND_SUBMITTER_REFERENCE
    )
    assert sample_id == second_sample_id

    response = upload_lims_manifest(page, profile_id, second_manifest)
    assert response.status == 200, f'Second LIMS manifest rejected: {response.text()[:2000]}'
    save_lims_manifest(page)
    assert sorted(copo_sample_ids(profile_id)) == sorted([first_sample_id, second_sample_id])

    feedback = submit_lims_manifest_to_sapio(page, study_id=second_study_id)
    assert feedback.get('status') == 'success', f'Submit to Sapio failed: {feedback}'
    poll_sapio_study_status(profile_id, second_study_id)

    sapio_samples = {s['SampleId']: s['C_CustomerSampleName'] for s in _sapio_samples(project_id)}
    assert sapio_samples == {
        first_sample_id: first_expected['Submitter Sample Reference'],
        second_sample_id: SECOND_SUBMITTER_REFERENCE,
    }

    delete_profile_by_title(PROFILE_TITLE_FOR_SAMPLE_BATCH_SUBMISSION)
    assert not read_sapio_project(project_id)['exists'], f'Sapio project {project_id} was not deleted'


@pytest.mark.external
def test_lims_manifest_submission_to_sapio(page: Page, tmp_path, sapio_test_instance, ena_dev_sandbox):
    # The EDP journey: EDP profile (creates its Sapio project and samples)
    # -> FAANG sample manifest -> real 'Submit to ENA' against EBI's dev
    # sandbox -> wait for a real BioSample accession -> download the LIMS
    # manifest COPO pre-fills for this project (which now lists that sample
    # and its accession) -> fill in the customer's answers -> validate and
    # save it -> 'Submit to Sapio' -> read the project back from Sapio and
    # check every mapped manifest field landed.
    #
    # Clearing leftovers by title also deletes their Sapio projects (the EDP
    # post-delete hook), so an interrupted run doesn't leak projects.
    delete_profile_by_title(PROFILE_TITLE_FOR_LIMS_SUBMISSION)

    related_enq = edp_related_enq()
    _, profile_id, project_id = create_edp_profile(
        page, PROFILE_TITLE_FOR_LIMS_SUBMISSION, PROFILE_DESCRIPTION_FOR_LIMS_SUBMISSION, related_enq=related_enq, **EDP_PROFILE_FIELDS
    )
    assert re.fullmatch(r'PR\d+', project_id), f'Unexpected Sapio project id {project_id!r}'

    # --- Samples: upload, save and submit to ENA ---
    accession = submit_sample_to_ena(page, profile_id)

    # --- LIMS manifest: download the pre-filled one, fill it in ---
    downloaded_manifest = download_lims_manifest(page, profile_id, tmp_path / 'lims_manifest_downloaded.xlsx')

    # The project-level cells come from the profile and can't be edited in
    # the manifest, so they prove the profile -> manifest pre-fill works.
    project_sheet = openpyxl.load_workbook(downloaded_manifest)['sample']
    assert project_sheet['C5'].value == project_id
    assert project_sheet['C4'].value == EDP_PROFILE_FIELDS['budget_holder']
    # The `JIRA PIP` field will be filled in later by the PM team
    # so it should not be pre-filled from the profile
    assert project_sheet['F5'].value is None

    patched_manifest = tmp_path / 'lims_manifest_patched.xlsx'
    sapio_sample_id, expected = build_lims_manifest(downloaded_manifest, patched_manifest, accession)

    # --- LIMS manifest: validate and save ---
    response = upload_lims_manifest(page, profile_id, patched_manifest)
    assert response.status == 200, f'LIMS manifest rejected: {response.text()[:2000]}'
    save_lims_manifest(page)

    # --- Submit to Sapio ---
    # 'warning' means some manifest samples weren't found in the Sapio
    # project and were skipped -- a failure here.
    feedback = submit_lims_manifest_to_sapio(page)
    assert feedback.get('status') == 'success', f'Submit to Sapio failed: {feedback}'

    record_id = poll_sapio_study_status(profile_id, project_id)

    # --- Read the project back from Sapio ---
    mapping = _sapio_field_mapping(expected)
    checks = [
        (label, sapio_object, sapio_field, sapio_value)
        for label, (sapio_object, sapio_field, sapio_value) in mapping.items()
        if expected.get(label) not in (None, '')
    ]
    assert checks, "No manifest fields map to Sapio -- has the EI_EDP schema's sapio_name column changed?"

    sapio = read_sapio_project(
        project_id,
        project_fields=['RecordId', 'ProjectName'] + [f for _, o, f, _ in checks if o == 'Project'],
        sample_fields=['SampleId'] + [f for _, o, f, _ in checks if o == 'Sample'],
    )
    assert sapio['exists'], f'Sapio project {project_id} not found'
    assert str(sapio['project']['RecordId']) == record_id, "COPO's Sapio accession isn't the project's record id"
    assert sapio['project']['ProjectName'] == related_enq

    sapio_sample = next((s for s in sapio['samples'] if s['SampleId'] == sapio_sample_id), None)
    assert sapio_sample, f'Sample {sapio_sample_id} not found under Sapio project {project_id}'

    mismatches = []
    for label, sapio_object, sapio_field, expected_value in checks:
        actual = (sapio['project'] if sapio_object == 'Project' else sapio_sample)[sapio_field]
        if not _same_value(expected_value, actual):
            mismatches.append(f'{label} -> {sapio_object}:{sapio_field}: manifest {expected_value!r}, Sapio {actual!r}')
    assert not mismatches, "Manifest fields that didn't land in Sapio:\n" + '\n'.join(mismatches)

    delete_profile_by_title(PROFILE_TITLE_FOR_LIMS_SUBMISSION)
    assert not read_sapio_project(project_id)['exists'], f'Sapio project {project_id} was not deleted'
