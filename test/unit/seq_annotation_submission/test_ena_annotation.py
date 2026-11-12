'''Unit tests for saving and submitting decoupled (GFF3) annotations.

When the form is saved (validate_annotation), a decoupled annotation must:

- have exactly one file,
- not already have been submitted to ENA (ENA rejects a second submission
  with the same name),
- be a real gzip file, checked from its first bytes in S3,
- start with the GFF3 header lines ENA needs.

Its file is only downloaded to COPO, not uploaded to ENA by FTP, because
Webin-CLI uploads it during submission.

Celery then waits for the download and submits it with Webin-CLI
(-context annotation). The ERZ accession is saved on success; otherwise
Webin-CLI's errors are saved and shown to the user.

S3, MongoDB and Webin-CLI are all mocked.

To execute these tests, run: $ pytest test/unit/seq_annotation_submission/test_ena_annotation.py
'''

import gzip
import io
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from django.test import override_settings

from common.ena_utils.FileTransferUtils import TransferStatus
from common.utils import helpers
from common.validators.ena_validators.validation_messages import MESSAGES
from src.apps.copo_seq_annotation_submission.utils import EnaAnnotation

PROFILE_ID = 'profile1'
ANNOTATION_ID = 'annotation1'
GCA_ACCESSION = 'GCA_977927465.1'
GFF3_FILE = 'genes.gff3.gz'
VALID_GFF3 = (
    b'##gff-version 3\n'
    b'##sequence-region chr1 1 15000\n'
    b'##species https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/68737\n'
    b'chr1\t.\tgene\t100\t900\t.\t+\t.\tID=gene1;locus_tag=CANAUR_000001\n'
)


@pytest.fixture(autouse=True)
def silent_loggers():
    # Logger writes logs/<date>.log into the repo
    with mock.patch.object(EnaAnnotation, 'l', mock.MagicMock()), \
            mock.patch.object(helpers, 'l', mock.MagicMock()):
        yield


def make_formset(*file_names, file_type='gff3'):
    # validate_annotation only reads each row's cleaned_data
    return [SimpleNamespace(cleaned_data={'file': f, 'type': file_type}) for f in file_names]


def make_form_data(annotation_type='decoupled_annotation'):
    return {
        'study': 'PRJEB101635',
        'title': 'Golden jackal genes',
        'annotation_type': annotation_type,
        'description': 'Gene models for mCanAur2',
        'primary_id': GCA_ACCESSION if annotation_type == 'decoupled_annotation' else '',
        'sample': '' if annotation_type == 'decoupled_annotation' else 'SAMEA120419578',
    }


def serve_s3_file(s3obj, content):
    '''Make the fake S3 connection return content for ranged GETs, like real S3.'''
    def get_object(Bucket, Key, Range):
        end = int(Range.split('-')[1])
        return {'Body': io.BytesIO(content[:end + 1])}

    s3obj.s3_client.get_object.side_effect = get_object


# ── Saving the form (validate_annotation) ───────────────────────────────

@pytest.fixture
def save_mocks(tmp_path):
    '''Mock S3 and MongoDB for validate_annotation. Yields the mocks to check calls on.'''
    with override_settings(LOCAL_UPLOAD_PATH=str(tmp_path)), \
            mock.patch.object(EnaAnnotation, 's3') as s3, \
            mock.patch.object(EnaAnnotation, 'Submission') as submission, \
            mock.patch.object(EnaAnnotation, 'SequenceAnnotation') as seq_annotation, \
            mock.patch.object(EnaAnnotation, 'DataFile') as data_file, \
            mock.patch.object(EnaAnnotation, 'tx') as tx, \
            mock.patch.object(EnaAnnotation, 'htags'), \
            mock.patch.object(EnaAnnotation, 'generate_additional_columns'), \
            mock.patch.object(EnaAnnotation, 'notify_annotation_status'):
        s3obj = s3.return_value
        s3obj.check_for_s3_bucket.return_value = True
        s3obj.check_s3_bucket_for_files.side_effect = lambda bucket_name, file_list: (
            {f: 'etag1' for f in file_list}, None)
        serve_s3_file(s3obj, gzip.compress(VALID_GFF3))

        submission.return_value.get_collection_handle.return_value.find_one.return_value = {'_id': 'sub1'}
        submission.return_value.make_seq_annotation_submission_uploading.return_value = {'status': 'success'}
        seq_annotation.return_value.get_record.return_value = None
        seq_annotation.return_value.get_all_records_columns.return_value = []
        seq_annotation.return_value.save_record.return_value = {'_id': ANNOTATION_ID}
        data_file.return_value.get_all_records_columns.return_value = []
        data_file.return_value.get_collection_handle.return_value.find_one_and_update.return_value = {'_id': 'file1'}

        yield SimpleNamespace(s3obj=s3obj, seq_annotation=seq_annotation.return_value, tx=tx)


def test_valid_decoupled_annotation_is_saved_and_scheduled(save_mocks):
    form_data = make_form_data()

    result = EnaAnnotation.validate_annotation(form_data, make_formset(GFF3_FILE), PROFILE_ID)

    assert 'error' not in result, result
    saved = save_mocks.seq_annotation.save_record.call_args.kwargs
    assert saved['annotation_type'] == 'decoupled_annotation'
    assert saved['primary_id'] == GCA_ACCESSION
    assert saved['filenames'] == [GFF3_FILE]


@pytest.mark.parametrize('annotation_type, file_name, file_type, local_only', [
    ('decoupled_annotation', GFF3_FILE, 'gff3', True),  # Webin-CLI uploads it
    ('sequence_annotation', 'features.tab.gz', 'tab', False),  # COPO uploads it by FTP
])
def test_file_transfer_depends_on_annotation_type(save_mocks, annotation_type, file_name, file_type, local_only):
    EnaAnnotation.validate_annotation(
        make_form_data(annotation_type), make_formset(file_name, file_type=file_type), PROFILE_ID)

    save_mocks.tx.make_transfer_record.assert_called_once_with(
        file_id='file1', submission_id='sub1', no_remote_location=local_only)


def test_missing_annotation_type_is_saved_as_sequence_annotation(save_mocks):
    form_data = make_form_data('sequence_annotation')
    form_data.pop('annotation_type')

    EnaAnnotation.validate_annotation(form_data, make_formset('features.tab.gz', file_type='tab'), PROFILE_ID)

    assert save_mocks.seq_annotation.save_record.call_args.kwargs['annotation_type'] == 'sequence_annotation'


def test_decoupled_annotation_with_two_files_is_rejected(save_mocks):
    result = EnaAnnotation.validate_annotation(
        make_form_data(), make_formset(GFF3_FILE, 'other.gff3.gz'), PROFILE_ID)

    assert result == {'error': 'A decoupled annotation must have exactly one GFF3 file'}
    save_mocks.s3obj.check_for_s3_bucket.assert_not_called()  # rejected before S3 is checked


def test_submitted_decoupled_annotation_cannot_be_edited(save_mocks):
    save_mocks.seq_annotation.get_record.return_value = {'_id': ANNOTATION_ID, 'accession': 'ERZ123'}

    result = EnaAnnotation.validate_annotation(
        make_form_data(), make_formset(GFF3_FILE), PROFILE_ID, seq_annotation_id=ANNOTATION_ID)

    assert result == {'error': 'This annotation has already been submitted to ENA and cannot be changed'}
    save_mocks.seq_annotation.save_record.assert_not_called()


def test_unsubmitted_decoupled_annotation_can_be_edited(save_mocks):
    save_mocks.seq_annotation.get_record.return_value = {'_id': ANNOTATION_ID, 'accession': ''}

    result = EnaAnnotation.validate_annotation(
        make_form_data(), make_formset(GFF3_FILE), PROFILE_ID, seq_annotation_id=ANNOTATION_ID)

    assert 'error' not in result, result


@pytest.mark.parametrize('content', [
    VALID_GFF3,  # uncompressed GFF3 renamed to .gz
    b'',  # empty file
])
def test_gff3_file_not_really_gzipped_is_rejected(save_mocks, content):
    serve_s3_file(save_mocks.s3obj, content)

    result = EnaAnnotation.validate_annotation(make_form_data(), make_formset(GFF3_FILE), PROFILE_ID)

    assert result == {'error': MESSAGES['invalid_compressed_file'].format(
        file_name=GFF3_FILE, file_extension='.gz')}


def test_gff3_header_problems_are_reported(save_mocks):
    serve_s3_file(save_mocks.s3obj, gzip.compress(b'##gff-version 3\nchr1\t.\tgene\t1\t9\t.\t+\t.\tID=g1\n'))

    result = EnaAnnotation.validate_annotation(make_form_data(), make_formset(GFF3_FILE), PROFILE_ID)

    assert result['error'].startswith(f'<strong>{GFF3_FILE}</strong> is not a valid GFF3 file')
    assert "'##sequence-region'" in result['error']
    assert "'##species'" in result['error']
    save_mocks.seq_annotation.save_record.assert_not_called()


# ── Waiting for the file (update_seq_annotation_submission_pending) ─────

@pytest.fixture
def pending_mocks():
    '''Mock MongoDB for update_seq_annotation_submission_pending, for one annotation with one file.'''
    with mock.patch.object(EnaAnnotation, 'Submission') as submission, \
            mock.patch.object(EnaAnnotation, 'SequenceAnnotation') as seq_annotation, \
            mock.patch.object(EnaAnnotation, 'EnaFileTransfer') as ena_file_transfer, \
            mock.patch.object(EnaAnnotation.tx, 'get_transfer_status') as get_transfer_status:
        submission.return_value.get_seq_annotation_file_uploading.return_value = [
            {'_id': 'sub1', 'profile_id': PROFILE_ID, 'seq_annotations': [ANNOTATION_ID]}]

        def run(annotation_type, transfer_record, transfer_status=None):
            seq_annotation.return_value.get_record.return_value = {
                '_id': ANNOTATION_ID, 'annotation_type': annotation_type, 'files': ['file1']}
            ena_file_transfer.return_value.get_collection_handle.return_value.find_one.return_value = transfer_record
            get_transfer_status.return_value = transfer_status

            EnaAnnotation.update_seq_annotation_submission_pending()

            # True if the submission was moved on to 'pending', ready to submit
            return submission.return_value.update_seq_annotation_submission_pending.called

        yield run


@pytest.mark.parametrize('transfer_status, ready', [
    (TransferStatus.DOWNLOADED_TO_LOCAL, True),
    (TransferStatus.DOWNLOADING_TO_LOCAL, False),
    (TransferStatus.CHECKING_FOR_DOWNLOAD, False),
    (TransferStatus.FAILED, False),  # FAILED (10) is higher than DOWNLOADED_TO_LOCAL (1)
])
def test_decoupled_annotation_waits_for_the_local_download(pending_mocks, transfer_status, ready):
    assert pending_mocks('decoupled_annotation', {'status': 'pending'}, transfer_status) is ready


@pytest.mark.parametrize('status, ready', [('complete', True), ('pending', False)])
def test_sequence_annotation_waits_for_the_ftp_upload(pending_mocks, status, ready):
    assert pending_mocks('sequence_annotation', {'status': status}) is ready


def test_missing_transfer_record_does_not_crash(pending_mocks):
    assert pending_mocks('decoupled_annotation', None) is True
    EnaAnnotation.l.error.assert_called_once_with('file not found file1')


# ── Submitting with Webin-CLI (submit_decoupled_annotation) ─────────────

@pytest.fixture
def submit_mocks(tmp_path):
    '''Mock MongoDB and Webin-CLI for submit_decoupled_annotation.

    Yields a function taking the Webin-CLI results for each run, in order, and an
    optional report to write to the work folder during validation, like Webin-CLI does.
    '''
    work_dir = tmp_path / 'ena_annotation_files' / PROFILE_ID / ANNOTATION_ID
    record = {
        '_id': ANNOTATION_ID,
        'annotation_type': 'decoupled_annotation',
        'title': 'Golden_jackal_genes',
        'study': 'PRJEB101635',
        'primary_id': GCA_ACCESSION,
        'description': 'Gene models for mCanAur2',
        'files': ['file1'],
    }

    with override_settings(MEDIA_ROOT=str(tmp_path)), \
            mock.patch.object(EnaAnnotation, 'DataFile') as data_file, \
            mock.patch.object(EnaAnnotation, 'EnaSubmissionHelper') as helper, \
            mock.patch.object(EnaAnnotation, 'Submission') as submission, \
            mock.patch.object(EnaAnnotation, 'SequenceAnnotation') as seq_annotation, \
            mock.patch.object(EnaAnnotation, 'notify_annotation_status') as notify:
        data_file.return_value.get_record.return_value = {'file_location': f'/local_uploads/{GFF3_FILE}'}

        def run(*webin_cli_results, report=None):
            results = iter(webin_cli_results)

            def run_webin_cli(manifest_path, context, action, output_dir=None):
                if report is not None and action == 'validate':
                    (Path(output_dir) / 'manifest.txt.report').write_text(report)
                return next(results)

            helper.return_value.run_webin_cli.side_effect = run_webin_cli
            EnaAnnotation.submit_decoupled_annotation(record, {'_id': 'sub1', 'profile_id': PROFILE_ID})
            return SimpleNamespace(
                work_dir=work_dir,
                webin_cli=helper.return_value.run_webin_cli,
                submission=submission.return_value,
                seq_annotation=seq_annotation.return_value,
                notify=notify,
            )

        yield run


def test_manifest_is_written_for_webin_cli(submit_mocks):
    result = submit_mocks((1, 'ERROR return code 1'))

    assert (result.work_dir / 'manifest.txt').read_text() == (
        f'NAME\tGolden_jackal_genes_{ANNOTATION_ID}\n'
        'STUDY\tPRJEB101635\n'
        'ANALYSIS_TYPE\tDECOUPLED_ANNOTATION\n'
        f'PRIMARY_ID\t{GCA_ACCESSION}\n'
        f'GFF3\t/local_uploads/{GFF3_FILE}\n'
        'DESCRIPTION\tGene models for mCanAur2\n'
    )


def test_successful_submission_saves_the_erz_accession(submit_mocks):
    result = submit_mocks((0, 'The submission has been validated successfully.'),
                          (0, 'The submission has been completed successfully. The following analysis accession was assigned to the submission: ERZ12345678'))

    assert [c.args[1:3] for c in result.webin_cli.call_args_list] == [
        ('annotation', 'validate'), ('annotation', 'submit')]
    assert all(c.kwargs['output_dir'] == result.work_dir for c in result.webin_cli.call_args_list)
    result.seq_annotation.add_accession.assert_called_once_with(ANNOTATION_ID, 'ERZ12345678')
    result.submission.add_annotation_accessions.assert_called_once_with('sub1', 'ERZ12345678')
    result.submission.update_seq_annotation_submission.assert_called_once_with(
        sub_id='sub1', seq_annotation_id=ANNOTATION_ID)
    result.seq_annotation.update_seq_annotation_error.assert_not_called()


def test_failed_validation_saves_webin_cli_errors_and_does_not_submit(submit_mocks):
    report = ('ERROR: Invalid PRIMARY_ID: GCA_977927465.1\n'
              '\tat uk.ac.ebi.ena.webin.cli.WebinCli.main(WebinCli.java:1)\n'
              'WARNING: Missing locus_tag\n')

    result = submit_mocks((2, 'ERROR return code 2'), report=report)

    assert [c.args[2] for c in result.webin_cli.call_args_list] == ['validate']
    expected_error = 'ERROR return code 2<br>ERROR: Invalid PRIMARY_ID: GCA_977927465.1<br>WARNING: Missing locus_tag'
    result.seq_annotation.update_seq_annotation_error.assert_called_once_with([ANNOTATION_ID], None, expected_error)
    result.notify.assert_called_with(data={'profile_id': PROFILE_ID}, msg=expected_error,
                                     action='error', html_id='annotation_info')
    result.seq_annotation.add_accession.assert_not_called()
    # Removed from the pending list so Celery does not retry it every 10 seconds
    result.submission.update_seq_annotation_submission.assert_called_once()


def test_submit_without_an_accession_in_the_output_is_an_error(submit_mocks):
    result = submit_mocks((0, 'validated'), (0, 'no accession here'))

    result.seq_annotation.add_accession.assert_not_called()
    result.seq_annotation.update_seq_annotation_error.assert_called_once_with([ANNOTATION_ID], None, 'no accession here')


def test_reports_from_an_earlier_attempt_are_not_shown_again(submit_mocks, tmp_path):
    stale_report = tmp_path / 'ena_annotation_files' / PROFILE_ID / ANNOTATION_ID / 'manifest.txt.report'
    stale_report.parent.mkdir(parents=True)
    stale_report.write_text('ERROR: an error from last time\n')

    result = submit_mocks((1, 'ERROR return code 1'))

    result.seq_annotation.update_seq_annotation_error.assert_called_once_with(
        [ANNOTATION_ID], None, 'ERROR return code 1')


# ── Routing in Celery (process_seq_annotation_pending_submission) ───────

def test_decoupled_annotations_are_submitted_with_webin_cli_not_xml():
    sub = {'_id': 'sub1', 'profile_id': PROFILE_ID, 'seq_annotations': [ANNOTATION_ID]}
    record = {'_id': ANNOTATION_ID, 'annotation_type': 'decoupled_annotation'}

    with mock.patch.object(EnaAnnotation, 'Submission') as submission, \
            mock.patch.object(EnaAnnotation, 'SequenceAnnotation') as seq_annotation, \
            mock.patch.object(EnaAnnotation, 'notify_annotation_status'), \
            mock.patch.object(EnaAnnotation, 'submit_decoupled_annotation') as submit_decoupled, \
            mock.patch.object(EnaAnnotation, 'build_analysis_dom') as build_analysis_dom, \
            mock.patch.object(EnaAnnotation, 'submit_ena_dtol_v2') as submit_xml:
        submission.return_value.get_seq_annotation_pending_submission.return_value = [sub]
        seq_annotation.return_value.get_record.return_value = record

        EnaAnnotation.process_seq_annotation_pending_submission()

    submit_decoupled.assert_called_once_with(record, sub)
    build_analysis_dom.assert_not_called()
    submit_xml.assert_not_called()
