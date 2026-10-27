import json
import re

import openpyxl
import pytest
from playwright.sync_api import Page

from test.playwright.e2e.utils.edp_helpers import (
    EDP_CHECKLIST,
    build_edp_manifest,
    download_edp_manifest,
    poll_sapio_study_status,
    save_edp_manifest,
    submit_edp_to_sapio,
    submit_sample_to_ena,
    upload_edp_manifest,
)
from test.playwright.e2e.utils.edp_helpers import (
    EDP_PROFILE_FIELDS,
    create_edp_profile,
    edp_jira_ticket_number,
    read_sapio_project,
)
from test.playwright.e2e.utils.helpers import (
    delete_profile_by_title,
    run_django_shell,
)

PROFILE_TITLE = 'Playwright EDP LIMS Submission Journey'
PROFILE_DESCRIPTION = 'A description that is at least twenty characters long.'


def _sapio_field_mapping():
    # Which Sapio field each manifest label is written to, straight from the
    # EI_EDP schema's sapio_name column ('Object:Field') -- the same mapping
    # SapioAdapter._submit_manifest() uses. Only Project and Sample fields
    # that aren't 'protected' are written by the adapter.
    code = (
        'import json\n'
        'from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas\n'
        f"schema = SinglecellSchemas().get_schema(schema_name='EI_EDP', target_id={EDP_CHECKLIST!r})\n"
        'fields = [\n'
        "    {'label': str(f.get('term_label', '')).strip(), 'sapio_name': f.get('sapio_name'),\n"
        "     'behavior': f.get('term_manifest_behavior')}\n"
        "    for component in ('study', 'sample') for f in schema.get(component, [])\n"
        ']\n'
        "print('MAPPING_RESULT:' + json.dumps(fields, default=str))\n"
    )
    marker_line = next(line for line in run_django_shell(code).splitlines() if line.startswith('MAPPING_RESULT:'))
    mapping = {}
    for field in json.loads(marker_line[len('MAPPING_RESULT:'):]):
        sapio_name = field['sapio_name']
        if not isinstance(sapio_name, str) or ':' not in sapio_name or field['behavior'] == 'protected':
            continue
        sapio_object, sapio_field = sapio_name.split(':', 1)
        if sapio_object in ('Project', 'Sample') and sapio_field:
            mapping[field['label']] = (sapio_object, sapio_field)
    return mapping


def _same_value(expected, actual):
    # Sapio returns numbers as numbers and may differ in case for picklist
    # values; the manifest holds what the customer typed.
    try:
        return float(expected) == float(actual)
    except (TypeError, ValueError):
        return str(expected).strip().casefold() == str(actual).strip().casefold()


@pytest.mark.external
def test_edp_manifest_submission_to_sapio(page: Page, tmp_path, sapio_test_instance, ena_dev_sandbox):
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
    delete_profile_by_title(PROFILE_TITLE)

    jira_ticket_number = edp_jira_ticket_number()
    _, profile_id, project_id = create_edp_profile(
        page, PROFILE_TITLE, PROFILE_DESCRIPTION, jira_ticket_number=jira_ticket_number, **EDP_PROFILE_FIELDS
    )
    assert re.fullmatch(r'PR\d+', project_id), f'Unexpected Sapio project id {project_id!r}'

    # --- Samples: upload, save and submit to ENA ---
    accession = submit_sample_to_ena(page, profile_id)

    # --- LIMS manifest: download the pre-filled one, fill it in ---
    downloaded_manifest = download_edp_manifest(page, profile_id, tmp_path / 'edp_manifest_downloaded.xlsx')

    # The project-level cells come from the profile and can't be edited in
    # the manifest, so they prove the profile -> manifest pre-fill works.
    project_sheet = openpyxl.load_workbook(downloaded_manifest)['sample']
    assert project_sheet['C5'].value == project_id
    assert project_sheet['C4'].value == EDP_PROFILE_FIELDS['budget_user']
    assert project_sheet['F5'].value == jira_ticket_number

    patched_manifest = tmp_path / 'edp_manifest_patched.xlsx'
    sapio_sample_id, expected = build_edp_manifest(downloaded_manifest, patched_manifest, accession)

    # --- LIMS manifest: validate and save ---
    response = upload_edp_manifest(page, profile_id, patched_manifest)
    assert response.status == 200, f'LIMS manifest rejected: {response.text()[:2000]}'
    save_edp_manifest(page)

    # --- Submit to Sapio ---
    # 'warning' means some manifest samples weren't found in the Sapio
    # project and were skipped -- a failure here.
    feedback = submit_edp_to_sapio(page)
    assert feedback.get('status') == 'success', f'Submit to Sapio failed: {feedback}'

    record_id = poll_sapio_study_status(profile_id, project_id)

    # --- Read the project back from Sapio ---
    mapping = _sapio_field_mapping()
    checks = [
        (label, sapio_object, sapio_field, expected[label])
        for label, (sapio_object, sapio_field) in mapping.items()
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
    assert sapio['project']['ProjectName'] == jira_ticket_number

    sapio_sample = next((s for s in sapio['samples'] if s['SampleId'] == sapio_sample_id), None)
    assert sapio_sample, f'Sample {sapio_sample_id} not found under Sapio project {project_id}'

    mismatches = []
    for label, sapio_object, sapio_field, expected_value in checks:
        actual = (sapio['project'] if sapio_object == 'Project' else sapio_sample)[sapio_field]
        if not _same_value(expected_value, actual):
            mismatches.append(f'{label} -> {sapio_object}:{sapio_field}: manifest {expected_value!r}, Sapio {actual!r}')
    assert not mismatches, "Manifest fields that didn't land in Sapio:\n" + '\n'.join(mismatches)

    delete_profile_by_title(PROFILE_TITLE)
    assert not read_sapio_project(project_id)['exists'], f'Sapio project {project_id} was not deleted'
