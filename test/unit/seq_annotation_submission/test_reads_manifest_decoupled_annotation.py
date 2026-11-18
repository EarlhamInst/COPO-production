'''Unit tests for GFF3 annotations (decoupled annotations) in a reads manifest under Biodata profiles.

A reads manifest's `sequencing_annotation_gff3_file` worksheet lists GFF3 annotations
of assemblies, each referring to its assembly by GCA accession (`primary_id`). Its
`repository_ena` in the reads schema is `decoupled_annotation`, which routes the rows
here when the study is submitted to ENA.

They are submitted like a decoupled annotation from the annotation form:

- the same checks of the downloaded GFF3 file as the form makes when it is saved
  (found, really gzipped, valid GFF3 header),
- the same Webin-CLI manifest and validate-then-submit steps
  (submit_decoupled_annotation_with_webin_cli, shared with the form),
- the ERZ accession, or Webin-CLI's errors, saved on the row.

They do not wait for the reads, because they only need the study's accession.

When the manifest is uploaded, SequencingAnnotationGff3FileValidator checks that each
file name ends in .gff3.gz and, for files already in COPO, that they are gzipped and
start with a valid GFF3 header. Files uploaded later are checked at submission.

MongoDB, S3 and Webin-CLI are mocked.

To execute these tests, run: $ pytest test/unit/seq_annotation_submission/test_reads_manifest_decoupled_annotation.py
'''

import gzip
import io
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from common.ena_utils import ena_helper
from common.validators.ena_validators.validation_messages import MESSAGES
from src.apps.copo_single_cell_submission.utils import ena_submission

PROFILE_ID = 'profile1'
STUDY_ID = 'study1'
STUDY_ACCESSION = 'PRJEB101635'
GCA_ACCESSION = 'GCA_977927465.2'
COMPONENT = 'sequencing_annotation_gff3_file'
IDENTIFIER = 'sequencing_annotation_gff3_file_id'
GFF3_FILE = 'genes.gff3.gz'
VALID_GFF3 = (
    b'##gff-version 3\n'
    b'##sequence-region OZ415857.1 1 122213100\n'
    b'##species https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/68724\n'
    b'OZ415857.1\t.\tgene\t100\t900\t.\t+\t.\tID=gene1\n'
)
VALIDATED = (0, 'The submission has been validated successfully.')
SUBMITTED = (0, 'The submission has been completed successfully. '
                'The following analysis accession was assigned to the submission: ERZ12345678')


@pytest.fixture(autouse=True)
def silent_loggers():
    # Logger writes logs/<date>.log into the repo
    with mock.patch.object(ena_submission, 'l', mock.MagicMock()):
        yield


def make_row(row_id='gff3file1', file_name=GFF3_FILE, **fields):
    return {
        'study_id': STUDY_ID,
        IDENTIFIER: row_id,
        'sequencing_annotation_gff3_title': 'Golden jackal genes',
        'sequencing_annotation_gff3_description': 'Gene models for mCanAur2_v2',
        'primary_id': GCA_ACCESSION,
        'sequencing_annotation_gff3_file': file_name,
        'sequencing_annotation_gff3_file_checksum': '0' * 32,
        'accession_ena': '',
        'status_ena': 'pending',
        'study_accession': STUDY_ACCESSION,
        **fields,
    }


# ── Submitting each row (EnaSubmissionHelper.register_decoupled_annotation) ──

@pytest.fixture
def register(tmp_path, monkeypatch):
    '''Run register_decoupled_annotation with MongoDB and Webin-CLI mocked.

    Yields a function taking the rows, the downloaded files ({file name: content}) and
    Webin-CLI's results, in order. A result is (return code, output), or a function
    called like Webin-CLI, e.g. to write a report. Webin-CLI work folders go in tmp_path.
    '''
    download_dir = tmp_path / 'downloads'
    download_dir.mkdir()
    monkeypatch.setattr(ena_helper.tempfile, 'gettempdir', lambda: str(tmp_path / 'work'))

    with mock.patch.object(ena_helper, 'EnaFileTransfer') as ena_file_transfer, \
            mock.patch.object(ena_helper, 'Singlecell') as singlecell, \
            mock.patch.object(ena_helper, 'Submission') as submission:
        helper = ena_helper.EnaSubmissionHelper.__new__(ena_helper.EnaSubmissionHelper)
        helper.profile_id = PROFILE_ID
        helper.submission_id = 'sub1'
        helper.logging_info = mock.Mock()
        helper.logging_error = mock.Mock()
        helper.run_webin_cli = mock.Mock()

        def run(rows, files=None, webin_cli_results=()):
            files = {GFF3_FILE: gzip.compress(VALID_GFF3)} if files is None else files
            transfers = []
            for file_name, content in files.items():
                (download_dir / file_name).write_bytes(content)
                transfers.append({'local_path': str(download_dir / file_name)})
            ena_file_transfer.return_value.get_all_records_columns.return_value = transfers

            results = iter(webin_cli_results)

            def run_webin_cli(*args, **kwargs):
                result = next(results)
                return result(*args, **kwargs) if callable(result) else result

            helper.run_webin_cli.side_effect = run_webin_cli

            result = helper.register_decoupled_annotation(
                identifier=IDENTIFIER, component_data_df=pd.DataFrame(rows),
                singlecell_id='singlecell1', component_name=COMPONENT)

            update_status = singlecell.return_value.update_component_status
            for c in update_status.call_args_list:
                assert c.kwargs['component'] == COMPONENT and c.kwargs['repository'] == 'ena'
            return mock.Mock(
                result=result,
                statuses={c.kwargs['identifier_value']: c.kwargs['status_column_value']
                          for c in update_status.call_args_list},
                webin_cli=helper.run_webin_cli,
                submission=submission.return_value,
                download_dir=download_dir,
                work_dir=tmp_path / 'work' / PROFILE_ID / STUDY_ID / COMPONENT,
            )

        yield run


def test_manifest_is_the_same_as_the_annotation_forms(register):
    run = register([make_row()], webin_cli_results=[(1, 'ERROR return code 1')])

    # The form's manifest has the same lines, in the same order (test_ena_annotation.py)
    assert (run.work_dir / 'gff3file1' / 'manifest.txt').read_text() == (
        f'NAME\tGolden jackal genes_{STUDY_ID}_gff3file1\n'
        f'STUDY\t{STUDY_ACCESSION}\n'
        'ANALYSIS_TYPE\tDECOUPLED_ANNOTATION\n'
        f'PRIMARY_ID\t{GCA_ACCESSION}\n'
        f'GFF3\t{run.download_dir / GFF3_FILE}\n'
        'DESCRIPTION\tGene models for mCanAur2_v2\n'
    )


def test_successful_submission_saves_the_erz_accession_on_the_row(register):
    run = register([make_row()], webin_cli_results=[VALIDATED, SUBMITTED])

    assert [c.args[1:3] for c in run.webin_cli.call_args_list] == [
        ('annotation', 'validate'), ('annotation', 'submit')]
    assert run.statuses == {'gff3file1': {'status': 'accepted', 'accession': 'ERZ12345678', 'error': ''}}
    run.submission.add_annotation_accessions.assert_called_once_with(
        'sub1', [{'alias': f'Golden jackal genes_{STUDY_ID}_gff3file1', 'accession': 'ERZ12345678'}])
    assert run.result['status'] is True


def test_failed_validation_saves_webin_cli_errors_and_does_not_submit(register):
    def fail_validation(manifest_path, context, action, output_dir=None):
        (Path(output_dir) / 'manifest.txt.report').write_text(
            'ERROR: Invalid PRIMARY_ID\n\tat uk.ac.ebi.ena.webin.cli.WebinCli.main(WebinCli.java:1)\n')
        return 2, 'ERROR return code 2'

    run = register([make_row()], webin_cli_results=[fail_validation])

    assert [c.args[2] for c in run.webin_cli.call_args_list] == ['validate']
    assert run.statuses == {'gff3file1': {
        'status': 'rejected', 'error': 'ERROR return code 2<br>ERROR: Invalid PRIMARY_ID'}}
    run.submission.add_annotation_accessions.assert_not_called()
    assert run.result['status'] is False


def test_submit_without_an_accession_in_the_output_is_an_error(register):
    run = register([make_row()], webin_cli_results=[VALIDATED, (0, 'no accession here')])

    assert run.statuses == {'gff3file1': {'status': 'rejected', 'error': 'no accession here'}}
    run.submission.add_annotation_accessions.assert_not_called()


@pytest.mark.parametrize('files, expected_error', [
    pytest.param({}, MESSAGES['missing_annotation_file'].format(file_name=GFF3_FILE), id='not_downloaded'),
    pytest.param({GFF3_FILE: VALID_GFF3},
                 MESSAGES['invalid_compressed_file'].format(file_name=GFF3_FILE, file_extension='.gz'),
                 id='not_really_gzipped'),
    pytest.param({GFF3_FILE: gzip.compress(b'chr1\t.\tgene\t100\t900\t.\t+\t.\tID=gene1\n')},
                 f'<strong>{GFF3_FILE}</strong> is not a valid GFF3 file:<br>'
                 "The first line must be '##gff-version 3'", id='invalid_gff3'),
])
def test_invalid_gff3_file_is_rejected_without_running_webin_cli(register, files, expected_error):
    run = register([make_row()], files=files)

    run.webin_cli.assert_not_called()
    assert run.statuses['gff3file1']['status'] == 'rejected'
    assert run.statuses['gff3file1']['error'].startswith(expected_error)
    assert run.result['status'] is False


def test_rows_already_submitted_are_skipped(register):
    run = register([make_row(accession_ena='ERZ00000001')])

    run.webin_cli.assert_not_called()
    assert run.statuses == {}


def test_a_rejected_row_does_not_stop_the_others(register):
    rows = [make_row('bad', file_name='missing.gff3.gz'), make_row('good')]

    run = register(rows, webin_cli_results=[VALIDATED, SUBMITTED])

    assert run.statuses['bad']['status'] == 'rejected'
    assert run.statuses['good'] == {'status': 'accepted', 'accession': 'ERZ12345678', 'error': ''}
    assert run.result['status'] is False  # one row failed


def test_each_row_has_its_own_webin_cli_folder(register):
    run = register([make_row('first'), make_row('second')],
                   webin_cli_results=[(1, 'ERROR return code 1'), (1, 'ERROR return code 1')])

    output_dirs = [Path(c.kwargs['output_dir']) for c in run.webin_cli.call_args_list]
    assert output_dirs == [run.work_dir / 'first', run.work_dir / 'second']


# ── Preparing the rows (_prepare_decoupled_annotation_submission) ───────

@pytest.fixture
def schemas():
    with mock.patch.object(ena_submission, 'SinglecellSchemas') as singlecell_schemas:
        singlecell_schemas.return_value.get_key_map.return_value = ({COMPONENT: IDENTIFIER, 'study': 'study_id'}, {})
        yield singlecell_schemas


def make_singlecell(rows, study_accession=STUDY_ACCESSION):
    return {
        'schema_name': 'COPO_READ', 'checklist_id': 'version_tol', 'study_id': STUDY_ID,
        'components': {'study': [{'study_id': STUDY_ID, 'accession_ena': study_accession}], COMPONENT: rows},
    }


def test_rows_get_the_studys_ena_accession(schemas):
    row = make_row()
    del row['study_accession']

    data_df, identifier = ena_submission._prepare_decoupled_annotation_submission(make_singlecell([row]), COMPONENT)

    assert identifier == IDENTIFIER
    assert data_df['study_accession'].tolist() == [STUDY_ACCESSION]
    assert data_df[IDENTIFIER].tolist() == ['gff3file1']


def test_no_rows_are_prepared_until_the_study_has_an_accession(schemas):
    data_df, _ = ena_submission._prepare_decoupled_annotation_submission(
        make_singlecell([make_row()], study_accession=''), COMPONENT)

    assert data_df.empty
    ena_submission.l.error.assert_called_once()


# ── Routing (process_pending_submission_ena) ────────────────────────────

def test_gff3_rows_are_submitted_with_webin_cli_without_waiting_for_reads():
    # The study has GFF3 annotations but no reads
    singlecell = make_singlecell([make_row()])
    singlecell['_id'] = 'singlecell1'
    repositories = pd.DataFrame.from_dict({
        'study': {'ena': 'study'},
        'read_file': {'ena': 'read'},
        'sequencing_annotation': {'ena': 'sequencing_annotation'},
        COMPONENT: {'ena': 'decoupled_annotation'},
    }, orient='index')
    prepared = (pd.DataFrame([make_row()]), IDENTIFIER)

    with mock.patch.object(ena_submission, 'Submission') as submission, \
            mock.patch.object(ena_submission, 'Singlecell') as singlecell_da, \
            mock.patch.object(ena_submission, 'SinglecellSchemas') as singlecell_schemas, \
            mock.patch.object(ena_submission, 'EnaSubmissionHelper') as helper_class, \
            mock.patch.object(ena_submission, '_resolve_submitter_credentials'), \
            mock.patch.object(ena_submission, '_prepare_decoupled_annotation_submission',
                              return_value=prepared) as prepare:
        submission.return_value.get_pending_submission.return_value = [
            {'_id': 'sub1', 'profile_id': PROFILE_ID, 'study': [STUDY_ID]}]
        singlecell_da.return_value.get_collection_handle.return_value.find_one.return_value = singlecell
        singlecell_schemas.return_value.get_schema.return_value = {'study': []}
        singlecell_schemas.return_value.get_term_mapping.return_value = {}
        singlecell_schemas.return_value.get_submission_repository.return_value = repositories
        helper = helper_class.return_value
        for step in ('get_submission_xml', 'get_edit_submission_xml', 'register_project'):
            getattr(helper, step).return_value = {'status': True, 'value': ''}

        ena_submission.process_pending_submission_ena()

    prepare.assert_called_once_with(singlecell=singlecell, component_name=COMPONENT)
    helper.register_decoupled_annotation.assert_called_once_with(
        identifier=IDENTIFIER, component_data_df=prepared[0],
        singlecell_id='singlecell1', component_name=COMPONENT)
    helper.register_files.assert_not_called()
    helper.register_sequencing_annotation.assert_not_called()


# ── Checked when the manifest is uploaded (SequencingAnnotationGff3FileValidator) ──

@pytest.fixture
def validate_sheet():
    '''Run the upload validator with S3 mocked.

    Yields a function taking the sheet's file names and the files already in S3
    ({file name: content}). It returns (errors, flag).
    '''
    from src.apps.copo_single_cell_submission.utils.validator import SingleCellSchemaValidators as validators

    with mock.patch.object(validators, 'S3Connection') as s3_connection:
        s3obj = s3_connection.return_value

        def run(file_names, uploaded=None, component=COMPONENT):
            uploaded = uploaded or {}
            s3obj.list_objects.return_value = [{'Key': name} for name in uploaded]

            def get_object(Bucket, Key, Range):
                assert Bucket == PROFILE_ID  # each profile's files are in its own bucket
                end = int(Range.split('-')[1])
                return {'Body': io.BytesIO(uploaded[Key][:end + 1])}

            s3obj.s3_client.get_object.side_effect = get_object
            errors, _, flag, _ = validators.SequencingAnnotationGff3FileValidator(
                profile_id=PROFILE_ID, fields=None, data=pd.DataFrame({COMPONENT: file_names}),
                errors=[], warnings=[], flag=True, component=component, isupdate=False).validate()
            return errors, flag

        run.s3obj = s3obj
        yield run


def test_valid_uploaded_gff3_file_passes(validate_sheet):
    assert validate_sheet([GFF3_FILE], uploaded={GFF3_FILE: gzip.compress(VALID_GFF3)}) == ([], True)


def test_file_not_uploaded_yet_is_left_for_the_submission_check(validate_sheet):
    # Data files may be uploaded after the manifest; register_decoupled_annotation checks them
    assert validate_sheet([GFF3_FILE]) == ([], True)
    validate_sheet.s3obj.s3_client.get_object.assert_not_called()


@pytest.mark.parametrize('file_name', ['genes.gff3', 'genes.gff.gz', 'genes.gff3.bz2'])
def test_file_name_must_end_in_gff3_gz(validate_sheet, file_name):
    errors, flag = validate_sheet([file_name])

    assert flag is False
    assert errors == [f'Sheet <strong>{COMPONENT}</strong>, row <strong>2</strong>: '
                      + MESSAGES['invalid_file_extension'].format(
                          file_name=file_name, valid_exts='<ul><li>.gff3.gz</li></ul>')]
    validate_sheet.s3obj.list_objects.assert_not_called()  # no S3 call for a bad name


def test_uploaded_file_not_really_gzipped_is_reported(validate_sheet):
    errors, flag = validate_sheet([GFF3_FILE], uploaded={GFF3_FILE: VALID_GFF3})

    assert flag is False
    assert errors == [f'Sheet <strong>{COMPONENT}</strong>, row <strong>2</strong>: '
                      + MESSAGES['invalid_compressed_file'].format(file_name=GFF3_FILE, file_extension='.gz')]


def test_uploaded_file_with_invalid_gff3_header_is_reported_with_its_row(validate_sheet):
    no_species = VALID_GFF3.replace(b'##species https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/68724\n', b'')

    errors, flag = validate_sheet(['ok.gff3.gz', 'bad.gff3.gz'], uploaded={
        'ok.gff3.gz': gzip.compress(VALID_GFF3), 'bad.gff3.gz': gzip.compress(no_species)})

    assert flag is False
    assert len(errors) == 1
    assert errors[0].startswith(f'Sheet <strong>{COMPONENT}</strong>, row <strong>3</strong>: '
                                '<strong>bad.gff3.gz</strong> is not a valid GFF3 file:<br>')
    assert "'##species'" in errors[0]


def test_s3_is_listed_once_for_the_whole_sheet(validate_sheet):
    validate_sheet(['a.gff3.gz', 'b.gff3.gz', 'c.gff3.gz'])

    validate_sheet.s3obj.list_objects.assert_called_once_with(PROFILE_ID)


def test_other_worksheets_are_not_checked(validate_sheet):
    assert validate_sheet(['genes.gff3'], component='sequencing_annotation_file') == ([], True)
