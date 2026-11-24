'''Unit tests for the `sequencing_annotation_file` worksheet in a reads manifest.

Sequencing annotations under Biodata profiles are listed in the reads manifest
and submitted with the analysis XML (ANALYSIS_TYPE SEQUENCE_ANNOTATION), like
the 'Sequence annotation' type of the annotation form. Their files must use the
same extensions as the form: uncompressed or gzipped GFF, TAB, FASTA or BED.

- When the manifest is uploaded, SequencingAnnotationFileValidator checks the
  extensions and reports each problem with its row.
- When the annotations are submitted (register_sequencing_annotation), the
  extensions are checked again, and a .gz file must really be gzip compressed.
  The compression is only checked while COPO still has a local copy, because
  these files are uploaded to ENA by FTP and the local copy may be gone.

MongoDB and ENA are mocked.

To execute these tests, run: $ pytest test/unit/seq_annotation_submission/test_sequencing_annotation_worksheet.py
'''

import gzip
from unittest import mock

import pandas as pd
import pytest

from common.ena_utils import ena_helper
from common.validators.ena_validators.validation_messages import MESSAGES
from src.apps.copo_seq_annotation_submission.utils.seq_annotation_utils import get_sequence_annotation_file_error
from src.apps.copo_single_cell_submission.utils.validator.SingleCellSchemaValidators import (
    SequencingAnnotationFileValidator)

PROFILE_ID = 'profile1'
TAB_CONTENT = b'chr1\t100\t900\tgene1\n'


# ── File extensions ─────────────────────────────────────────────────────

@pytest.mark.parametrize('file_name', [
    'features.gff', 'features.gff.gz', 'features.tab', 'features.tab.gz',
    'features.fasta', 'features.fasta.gz', 'features.bed', 'features.bed.gz',
])
def test_valid_sequence_annotation_file_has_no_error(file_name):
    assert get_sequence_annotation_file_error(file_name) == ''


@pytest.mark.parametrize('file_name', [
    'features.gff3',  # GFF3 names are not offered for this route
    'features.tab.bz2',  # bzip2 is not accepted
    'features.txt',
    'features.tab.zip',
])
def test_invalid_sequence_annotation_file_is_rejected(file_name):
    error = get_sequence_annotation_file_error(file_name)

    assert error.startswith(f'Invalid file <strong>{file_name}</strong>')
    assert '<li>.tab.gz</li>' in error  # the valid extensions are listed


# ── File matches its type ───────────────────────────────────────────────

@pytest.mark.parametrize('file_type, file_name', [
    ('gff', 'features.gff'), ('gff', 'features.gff.gz'),
    ('tab', 'features.tab'), ('tab', 'features.tab.gz'),
    ('fasta', 'features.fasta'), ('fasta', 'features.fasta.gz'),
    ('bed', 'features.bed'), ('BED', 'features.bed.gz'),  # the type is not case-sensitive
])
def test_file_matching_its_type_has_no_error(file_type, file_name):
    assert get_sequence_annotation_file_error(file_name, file_type) == ''


@pytest.mark.parametrize('file_type, file_name', [
    ('gff', 'features.bed.gz'),
    ('tab', 'features.gff'),
    ('fasta', 'features.tab.gz'),
    ('bed', 'features.fasta'),
    ('tab', 'features.tab.bz2'),
])
def test_file_not_matching_its_type_is_rejected(file_type, file_name):
    error = get_sequence_annotation_file_error(file_name, file_type)

    assert error.startswith(f'File <strong>{file_name}</strong> does not match its file type <strong>{file_type}</strong>')
    assert f'<li>.{file_type}.gz</li>' in error  # the type's extensions are listed


def test_file_type_of_another_route_is_rejected():
    # 'gff3' is only for decoupled annotations, which ENA's analysis XML does not take
    error = get_sequence_annotation_file_error('genes.gff3.gz', 'gff3')

    assert error.startswith('Sequence annotation file type <strong>gff3</strong> of file <strong>genes.gff3.gz</strong>')


# ── Checked when the manifest is uploaded ───────────────────────────────

def run_validator(file_names, component='sequencing_annotation_file', file_types=None):
    data = pd.DataFrame({'sequencing_annotation_file': file_names})
    if file_types is not None:
        data['sequencing_annotation_file_type'] = file_types
    schema = {'sequencing_annotation_file_type': {'choice': ['gff', 'tab', 'fasta', 'bed']}}
    errors, _, flag, _ = SequencingAnnotationFileValidator(
        profile_id=PROFILE_ID, fields=None, data=data, errors=[], warnings=[], flag=True,
        schema=schema, component=component, isupdate=False).validate()
    return errors, flag


def test_worksheet_validator_accepts_files_matching_their_type():
    assert run_validator(['features.gff.gz', 'features.tab'], file_types=['gff', 'tab']) == ([], True)


def test_worksheet_validator_reports_file_not_matching_its_type_with_its_row():
    errors, flag = run_validator(['features.gff.gz', 'features.bed.gz'], file_types=['gff', 'gff'])

    assert flag is False
    assert errors == [
        'Sheet <strong>sequencing_annotation_file</strong>, row <strong>3</strong>: '
        + get_sequence_annotation_file_error('features.bed.gz', 'gff')]


def test_worksheet_validator_leaves_types_outside_the_dropdown_to_another_validator():
    # Reported by IncorrectValueValidator, so not reported twice
    assert run_validator(['features.vcf'], file_types=['vcf']) == ([], True)


def test_worksheet_validator_accepts_valid_files():
    assert run_validator(['features.gff.gz', 'features.tab']) == ([], True)


def test_worksheet_validator_reports_invalid_file_with_its_row():
    errors, flag = run_validator(['features.gff.gz', 'features.tab.bz2'])

    assert flag is False
    assert len(errors) == 1
    # Row 1 is the header, so the second data row is row 3
    assert errors[0].startswith(
        'Sheet <strong>sequencing_annotation_file</strong>, row <strong>3</strong>: '
        'Invalid file <strong>features.tab.bz2</strong>')


def test_worksheet_validator_skips_empty_file_names():
    # Empty values are reported by MandatoryValuesValidator
    assert run_validator(['', 'features.tab']) == ([], True)


def test_worksheet_validator_ignores_other_worksheets():
    assert run_validator(['features.tab.bz2'], component='assembly_file') == ([], True)


def test_worksheet_validator_ignores_missing_column():
    data = pd.DataFrame({'other_column': ['features.tab.bz2']})
    errors, _, flag, _ = SequencingAnnotationFileValidator(
        profile_id=PROFILE_ID, fields=None, data=data, errors=[], warnings=[], flag=True,
        component='sequencing_annotation_file', isupdate=False).validate()

    assert (errors, flag) == ([], True)


# ── Checked when the annotations are submitted ──────────────────────────

@pytest.fixture
def register_annotation(tmp_path):
    '''Run EnaSubmissionHelper.register_sequencing_annotation for one annotation, with ENA mocked.

    Yields a function taking the annotation's files ({file name: content}) and the names
    of the files COPO still has a local copy of. It returns the mocks so tests can check
    what was submitted and what was rejected.
    '''
    download_dir = tmp_path / 'downloads'
    download_dir.mkdir()

    with mock.patch.object(ena_helper, 'EnaFileTransfer') as ena_file_transfer, \
            mock.patch.object(ena_helper, 'DataFile') as data_file, \
            mock.patch.object(ena_helper, 'Singlecell') as singlecell:
        helper = ena_helper.EnaSubmissionHelper.__new__(ena_helper.EnaSubmissionHelper)
        helper.profile_id = PROFILE_ID
        helper.submission_id = 'sub1'
        helper.logging_info = mock.Mock()
        helper.logging_error = mock.Mock()
        helper._submit_ena_v2 = mock.Mock()

        def run(files, local_files=None):
            local_files = files.keys() if local_files is None else local_files
            transfers = []
            for file_name, content in files.items():
                local_path = download_dir / file_name
                if file_name in local_files:
                    local_path.write_bytes(content)
                transfers.append({'local_path': str(local_path), 'remote_path': 'sub1/reads/'})
            ena_file_transfer.return_value.get_all_records_columns.return_value = transfers
            data_file.return_value.get_all_records_columns.return_value = [
                {'file_name': f, 'file_hash': '0' * 32} for f in files]

            annotation_df = pd.DataFrame([{
                'study_id': 'study1', 'sequencing_annotation_id': 'annotation1',
                'sequencing_annotation_title': 'Features', 'sequencing_annotation_description': 'Gene features',
                'study_accession': 'PRJEB1', 'biosampleAccession': 'SAMEA12345',
            }])
            file_df = pd.DataFrame(
                [('annotation1', f, 'tab') for f in files],
                columns=['sequencing_annotation_id', 'sequencing_annotation_file', 'sequencing_annotation_file_type'])
            parent_map = {
                'sequencing_annotation_run_ref': {'sequencing_annotation': 'sequencing_annotation_id'},
                'sequencing_annotation_file': {'sequencing_annotation': 'sequencing_annotation_id'},
            }

            result = helper.register_sequencing_annotation(
                identifier='sequencing_annotation_id', analysis_component_data_df=annotation_df,
                analysis_file_data_df=file_df, parent_map=parent_map, singlecell_id='singlecell1')

            submitted_ids = [i for c in helper._submit_ena_v2.call_args_list for i in c.kwargs['analysis_ids']]
            return mock.Mock(result=result, submitted_ids=submitted_ids,
                             update_component_status=singlecell.return_value.update_component_status)

        yield run


def assert_annotation_rejected(run, expected_error):
    assert run.submitted_ids == []
    assert run.result['status'] is False
    assert expected_error in run.result['message']
    run.update_component_status.assert_called_once_with(
        id='singlecell1', component='sequencing_annotation', identifier='sequencing_annotation_id',
        identifier_value='annotation1', repository='ena',
        status_column_value={'status': 'rejected', 'error': expected_error})


@pytest.mark.parametrize('files', [
    {'features.tab.gz': gzip.compress(TAB_CONTENT)},
    {'features.tab': TAB_CONTENT},  # uncompressed is fine for this route
])
def test_valid_annotation_files_are_submitted(register_annotation, files):
    run = register_annotation(files)

    assert run.submitted_ids == ['annotation1']
    assert run.result['status'] is True
    run.update_component_status.assert_not_called()


def test_annotation_file_with_invalid_extension_is_rejected(register_annotation):
    # e.g. A manifest uploaded before this check was added on upload
    run = register_annotation({'features.tab.bz2': b'BZh91AY'})

    assert_annotation_rejected(run, get_sequence_annotation_file_error('features.tab.bz2', 'tab'))


def test_annotation_file_not_matching_its_type_is_rejected(register_annotation):
    # Every file row in the fixture has file type 'tab'
    run = register_annotation({'features.bed.gz': gzip.compress(TAB_CONTENT)})

    assert_annotation_rejected(run, get_sequence_annotation_file_error('features.bed.gz', 'tab'))


@pytest.mark.parametrize('content', [
    TAB_CONTENT,  # plain text renamed to .gz
    b'',  # empty file
])
def test_annotation_file_not_really_gzipped_is_rejected(register_annotation, content):
    run = register_annotation({'features.tab.gz': content})

    assert_annotation_rejected(run, MESSAGES['invalid_compressed_file'].format(
        file_name='features.tab.gz', file_extension='.gz'))


def test_annotation_file_without_a_local_copy_is_not_rejected(register_annotation):
    # Already uploaded to ENA by FTP, so the local copy may have been removed
    run = register_annotation({'features.tab.gz': TAB_CONTENT}, local_files=[])

    assert run.submitted_ids == ['annotation1']
    run.update_component_status.assert_not_called()
