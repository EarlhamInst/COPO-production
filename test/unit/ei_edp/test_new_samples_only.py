'''Unit tests for submitting EDP samples in batches.

When an EDP profile's sample count is increased after some samples have been
submitted to the LIMS, the customer submits only the new samples:

- the blank manifest lists only the samples not yet submitted
- a manifest upload that includes samples that have already 
  been submitted will perform an update on those samples it as long as
  their 'Submitter sample reference' field values are unchanged
- saving the new manifest keeps the earlier, submitted samples' rows, since
  an upload otherwise replaces every row of the COPO record

A sample counts as submitted once the LIMS holds its Submitter Sample
Reference (`C_CustomerSampleName` in Sapio).
'''

from unittest import mock

import openpyxl
import pandas as pd
import pytest

from src.apps.ei_edp.utils import EDPSchemaHandler as edp_module
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES
from src.apps.ei_edp.utils.EDPSchemaHandler import (
    EDPSchemaHandler,
    EDPSchemaSpreadsheet,
    get_submitted_sample_ids,
)

PROFILE_ID = 'profile1'
PROJECT_ID = 'PR0001'
# The LIMS project's samples, keyed by COPO term as the adapter returns them
LIMS_SAMPLES = [
    {'sample_id': 'S1', 'submitter_sample_reference': 'cust1'},
    {'sample_id': 'S2', 'submitter_sample_reference': 'cust2'},
    {'sample_id': 'S3', 'submitter_sample_reference': ''},
    {'sample_id': 'S4', 'submitter_sample_reference': None},
]
SUBMITTED = {'S1', 'S2'}


@pytest.fixture
def lims():
    with mock.patch.object(edp_module, 'get_lims_adapter') as get_lims_adapter:
        get_lims_adapter.return_value.get_project_samples.return_value = LIMS_SAMPLES
        yield get_lims_adapter.return_value


def test_submitted_sample_ids(lims):
    assert get_submitted_sample_ids(PROJECT_ID, {'sample': []}) == SUBMITTED
    lims.get_project_samples.assert_called_once_with(PROJECT_ID, {'sample': []})


def test_no_project_means_nothing_submitted(lims):
    assert get_submitted_sample_ids('', {}) == set()
    lims.get_project_samples.assert_not_called()


# --- Saving: rows to keep ---

def carry_over(existing, uploaded):
    # existing/uploaded: {component: {column: [values]}}. Returns the uploaded
    # record's components after the existing rows have been carried over.
    existing_record = {
        'components': {c: pd.DataFrame(d).to_dict('records') for c, d in existing.items()}
    }
    singlecell_record = {'components': {c: pd.DataFrame(d) for c, d in uploaded.items()}}
    with mock.patch.object(edp_module, 'Profile') as profile:
        profile.return_value.get_record.return_value = {'sapio_project_id': PROJECT_ID}
        EDPSchemaHandler().carry_over_existing_rows(
            PROFILE_ID, {}, existing_record, singlecell_record
        )
    return singlecell_record['components']


def test_keeps_submitted_samples_the_upload_leaves_out(lims):
    existing = {'sample': {'sample_id': ['S1', 'S2', 'S3'], 'volumn': ['1', '2', '3']}}
    uploaded = {'sample': {'sample_id': ['S5'], 'volumn': ['5']}}

    saved = carry_over(existing, uploaded)['sample']

    # S3 was never submitted, so the upload is free to replace or drop it
    assert saved['sample_id'].tolist() == ['S1', 'S2', 'S5']
    assert saved['volumn'].tolist() == ['1', '2', '5']


def test_keeps_index_rows_of_submitted_samples(lims):
    existing = {'index': {'sample_id': ['S1', 'S3'], 'submitter_library_name': ['lib1', 'lib3']}}
    uploaded = {'index': {'sample_id': ['S5'], 'submitter_library_name': ['lib5']}}

    assert carry_over(existing, uploaded)['index']['sample_id'].tolist() == ['S1', 'S5']


def test_does_not_duplicate_rows_that_are_in_the_upload(lims):
    existing = {'sample': {'sample_id': ['S1', 'S2']}}
    uploaded = {'sample': {'sample_id': ['S2', 'S5']}}

    assert carry_over(existing, uploaded)['sample']['sample_id'].tolist() == ['S1', 'S2', 'S5']


def test_keeps_only_the_manifest_columns_of_carried_over_rows(lims):
    # Repository status columns are merged back in from the existing record
    # later by save_singlecell_records, so they aren't carried over here
    existing = {'sample': {'sample_id': ['S1'], 'volumn': ['1'], 'status_ena': ['accepted']}}
    uploaded = {'sample': {'sample_id': ['S5'], 'volumn': ['5']}}

    saved = carry_over(existing, uploaded)['sample']

    assert saved.columns.tolist() == ['sample_id', 'volumn']
    assert saved['sample_id'].tolist() == ['S1', 'S5']


def test_keeps_nothing_for_components_without_samples(lims):
    existing = {'study': {'study_id': [PROJECT_ID]}}
    uploaded = {'study': {'study_id': [PROJECT_ID]}}

    assert carry_over(existing, uploaded)['study']['study_id'].tolist() == [PROJECT_ID]
    lims.get_project_samples.assert_not_called()


def test_asks_the_lims_once_per_save(lims):
    existing = {'sample': {'sample_id': ['S1']}, 'index': {'sample_id': ['S1']}}
    uploaded = {'sample': {'sample_id': ['S5']}, 'index': {'sample_id': ['S5']}}

    carry_over(existing, uploaded)

    lims.get_project_samples.assert_called_once()


# --- Uploading: reject already-submitted samples ---

def manifest(tmp_path, sample_ids):
    # The parts of the EDP manifest that loadManifest reads: the project
    # header on the sample sheet, its table from row 28, and the index sheet
    workbook = openpyxl.Workbook()
    sample_sheet = workbook.active
    sample_sheet.title = 'sample'
    sample_sheet['C5'] = PROJECT_ID
    sample_sheet['L8'] = 'No'
    sample_sheet['L9'] = 'No'
    sample_sheet.cell(28, 1, 'Sample ID')
    sample_sheet.cell(28, 2, 'Submitter sample reference')
    index_sheet = workbook.create_sheet('index')
    index_sheet.append(['Sample ID', 'Submitter Library Name'])
    for row, sample_id in enumerate(sample_ids, start=29):
        sample_sheet.cell(row, 1, sample_id)
        sample_sheet.cell(row, 2, f'ref{sample_id}')
        index_sheet.append([sample_id, f'lib{sample_id}'])
    path = tmp_path / 'manifest.xlsx'
    workbook.save(path)
    return path


def load(path):
    spreadsheet = EDPSchemaSpreadsheet.__new__(EDPSchemaSpreadsheet)
    spreadsheet.file = path
    spreadsheet.profile_id = PROFILE_ID
    spreadsheet.schema_name = 'EI_EDP'
    spreadsheet.checklist_id = 'version_tissue'
    spreadsheet.component_info = 'singlecell_info'
    spreadsheet.data = {}
    spreadsheet.new_data = {}
    spreadsheet.schemas = {
        'sample': {
            'sample_id': {'term_label': 'Sample ID'},
            'submitter_sample_reference': {'term_label': 'Submitter sample reference'},
        },
        'index': {
            'sample_id': {'term_label': 'Sample ID'},
            'submitter_library_name': {'term_label': 'Submitter Library Name'},
        },
    }
    with mock.patch.object(edp_module, 'Profile') as profile, \
            mock.patch.object(edp_module, 'SinglecellSchemas'), \
            mock.patch.object(edp_module, 'notify_singlecell_status') as notify:
        profile.return_value.get_record.return_value = {'sapio_project_id': PROJECT_ID}
        return spreadsheet.loadManifest('xls'), notify


def test_accepts_samples_not_yet_submitted(lims, tmp_path):
    loaded, notify = load(manifest(tmp_path, ['S3', 'S4']))

    assert loaded is True
    assert not any(c.kwargs.get('action') == 'error' for c in notify.call_args_list)


def test_rejects_samples_already_submitted(lims, tmp_path):
    loaded, notify = load(manifest(tmp_path, ['S1', 'S3', 'S2']))

    assert loaded is False
    notify.assert_called_with(
        data={'profile_id': PROFILE_ID},
        msg=MESSAGES['submitter_sample_reference_changed'].format(sample_ids='S2'),
        action='error',
        html_id='singlecell_info',
        checklist_id='version_tissue',
    )
