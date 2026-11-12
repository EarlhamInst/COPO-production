'''Unit tests for assembly file checks and ENA error reporting.

Assemblies are submitted to ENA with Webin-CLI from either the assembly
form under ASG, ERGA or DToL profiles or from the  `assembly_file` 
worksheet in a reads manifest under Biodata profiles.

Checks include:

- The file type must be one ENA accepts, e.g. AGP is no longer accepted.
- The file name must have an extension accepted for its field, e.g. a
  FASTA file must end in .fasta.gz, .fa.gz, etc.
- ENA requires assembly data files to be gzip compressed, so only .gz
  names are accepted (bzip2 is not documented by ENA for assemblies).
- A file named .gz must actually be gzip compressed,
  which is checked from its first bytes (in S3 for the assembly form
  and in the downloaded reads manifest file for the 
  `assembly_file` worksheet).

ENA's Webin-CLI will reject the manifest.txt file and report errors 
such as Unknown field: AGP or Invalid FASTA file suffix:. These 
errors are saved against the assembly and displayed to the user.

S3, MongoDB and Webin-CLI are all mocked.

To execute these tests, run: $ pytest test/unit/assembly_submission/test_ena_assembly.py
'''

import bz2
import gzip
import io
import json
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from django.test import override_settings

from common.ena_utils import ena_helper
from common.utils import helpers
from common.utils.helpers import get_file_header
from common.validators.ena_validators.validation_messages import MESSAGES
from src.apps.copo_assembly_submission import forms
from src.apps.copo_assembly_submission.utils import EnaAssembly, assembly_utils

# The assembly fields COPO knows about. In the app these are loaded into
# MongoDB from this file, so the unit tests read the file directly.
ASSEMBLY_SCHEMA_FILE = (Path(__file__).resolve().parents[3]
                        / 'common/schemas/copo/uimodels/mappings/isa_mappings/assembly.json')
ASSEMBLY_SCHEMA = {'schema_dict': json.loads(ASSEMBLY_SCHEMA_FILE.read_text())['properties']}

PROFILE_ID = 'profile1'
FASTA_CONTENT = b'>seq1\nACGT\n'
GZIP_SIGNATURE = b'\x1f\x8b'
BZIP2_SIGNATURE = b'BZh'


@pytest.fixture(autouse=True)
def silent_assembly_logger():
    # EnaAssembly and get_file_header log through Logger, which writes logs/<date>.log into the repo
    with mock.patch.object(EnaAssembly, 'l', mock.MagicMock()), \
            mock.patch.object(EnaAssembly, 'Logger', mock.MagicMock()), \
            mock.patch.object(helpers, 'l', mock.MagicMock()):
        yield


def make_s3obj(content=None, error=None):
    '''Fake S3 connection holding one file, served by ranged GET like real S3.'''
    s3obj = mock.Mock()

    def get_object(Bucket, Key, Range):
        if error:
            raise error
        end = int(Range.split('-')[1])
        return {'Body': io.BytesIO(content[:end + 1])}

    s3obj.s3_client.get_object.side_effect = get_object
    return s3obj


def make_form(**files):
    form = {'assemblyname': 'my assembly'}
    form.update({field: '' for field in EnaAssembly.ASSEMBLY_FILE_FIELDS})
    form.update(files)
    return form


# ── AGP is no longer accepted ───────────────────────────────────────────

def test_agp_is_not_an_assembly_file_field():
    assert 'agp' not in EnaAssembly.ASSEMBLY_FILE_FIELDS
    assert 'agp' not in EnaAssembly.ASSEMBLY_FIELD_EXT_MAP


def test_assembly_form_has_no_agp_field():
    assert 'agp' not in forms.AssemblyForm.base_fields


def test_assembly_schema_has_no_agp_field():
    field_ids = [f['id'] for f in ASSEMBLY_SCHEMA['schema_dict']]

    assert 'copo.assembly.agp' not in field_ids


# ── File extensions ─────────────────────────────────────────────────────

@pytest.mark.parametrize('field, file_name', [
    ('fasta', 'genome.fasta.gz'),
    ('fasta', 'genome.fa.gz'),
    ('fasta', 'genome.fna.gz'),
    ('flatfile', 'genome.embl.gz'),
    ('chromosome_list', 'chromosomes.txt.gz'),
    ('unlocalised_list', 'unlocalised.txt.gz'),
])
def test_valid_extensions_are_accepted(field, file_name):
    assert any(file_name.endswith(ext) for ext in EnaAssembly.ASSEMBLY_FIELD_EXT_MAP[field])


@pytest.mark.parametrize('field, file_name', [
    ('fasta', 'genome.fasta'),
    ('fasta', 'genome.txt.gz'),
    ('fasta', 'genome.fasta.zip'),
    ('chromosome_list', 'chromosomes.txt'),
    ('unlocalised_list', 'unlocalised.txt'),
    ('flatfile', 'genome.embl'),
    # ENA only accepts gzip for assemblies
    ('fasta', 'genome.fa.bz2'),
    ('flatfile', 'genome.embl.bz2'),
    ('chromosome_list', 'chromosomes.txt.bz2'),
    ('unlocalised_list', 'unlocalised.txt.bz2'),
])
def test_invalid_extensions_are_rejected(field, file_name):
    assert not any(file_name.endswith(ext) for ext in EnaAssembly.ASSEMBLY_FIELD_EXT_MAP[field])


@pytest.fixture
def validate_mocks(tmp_path):
    '''Mock everything validate_assembly touches before and during the file checks.'''
    with override_settings(MEDIA_ROOT=str(tmp_path)), \
            mock.patch.object(EnaAssembly, 'Assembly') as assembly, \
            mock.patch.object(EnaAssembly, 'Submission') as submission, \
            mock.patch.object(EnaAssembly, 's3') as s3:
        assembly.return_value.execute_query.return_value = []  # An assembly name is unique
        submission.return_value.get_collection_handle.return_value.find_one.return_value = {'_id': 'sub1'}
        yield s3.return_value


@pytest.mark.parametrize('field, file_name', [
    ('fasta', 'genome.fasta'),
    ('fasta', 'genome.txt.gz'),
    ('chromosome_list', 'chromosomes.txt'),
])
def test_validate_assembly_reports_invalid_extension(validate_mocks, field, file_name):
    result = EnaAssembly.validate_assembly(make_form(**{field: file_name}), PROFILE_ID, None)

    assert result['error'].startswith(f'Invalid file <strong>{file_name}</strong>')
    for ext in EnaAssembly.ASSEMBLY_FIELD_EXT_MAP[field]:
        assert f'<li>{ext}</li>' in result['error']
    validate_mocks.check_for_s3_bucket.assert_not_called()  # Rejected before S3 is checked


# Compressed file contents

def test_gzip_header_matches_gzip_signature():
    s3obj = make_s3obj(gzip.compress(FASTA_CONTENT))

    assert get_file_header(s3obj, PROFILE_ID, 'genome.fasta.gz').startswith(GZIP_SIGNATURE)


def test_bzip2_header_matches_bzip2_signature():
    s3obj = make_s3obj(bz2.compress(FASTA_CONTENT))

    assert get_file_header(s3obj, PROFILE_ID, 'genome.fasta.bz2').startswith(BZIP2_SIGNATURE)


def test_only_the_first_bytes_are_requested():
    s3obj = make_s3obj(gzip.compress(FASTA_CONTENT))

    header = get_file_header(s3obj, PROFILE_ID, 'genome.fasta.gz')

    assert len(header) == 3
    s3obj.s3_client.get_object.assert_called_once_with(
        Bucket=PROFILE_ID, Key='genome.fasta.gz', Range='bytes=0-2')


def test_renamed_plain_text_does_not_match_gzip_signature():
    s3obj = make_s3obj(FASTA_CONTENT)

    assert not get_file_header(s3obj, PROFILE_ID, 'genome.fasta.gz').startswith(GZIP_SIGNATURE)


@pytest.mark.parametrize('code', ['InvalidRange', 'NoSuchKey', 'AccessDenied'])
def test_s3_error_returns_empty_header(code):
    # e.g. `InvalidRange` is returned for a ranged `GET` of an empty file
    error = ClientError({'Error': {'Code': code}}, 'GetObject')

    assert get_file_header(make_s3obj(error=error), PROFILE_ID, 'genome.fasta.gz') == b''


@pytest.mark.parametrize('file_name, content', [
    ('genome.fasta.gz', FASTA_CONTENT),  # Plain text renamed to .gz
    ('genome.fasta.gz', bz2.compress(FASTA_CONTENT)),  # bzip2 renamed to .gz
    ('genome.fasta.gz', b''),  # empty file
])
def test_validate_assembly_reports_file_not_really_compressed(validate_mocks, file_name, content):
    s3obj = validate_mocks
    s3obj.check_for_s3_bucket.return_value = True
    s3obj.check_s3_bucket_for_files.return_value = ({file_name: 'etag1'}, None)
    if content:
        s3obj.s3_client.get_object.side_effect = make_s3obj(content).s3_client.get_object.side_effect
    else:
        s3obj.s3_client.get_object.side_effect = ClientError({'Error': {'Code': 'InvalidRange'}}, 'GetObject')

    result = EnaAssembly.validate_assembly(make_form(fasta=file_name), PROFILE_ID, None)

    assert result == {'error': MESSAGES['invalid_compressed_file'].format(
        file_name=file_name, file_extension=Path(file_name).suffix)}


# Errors reported by ENA

@pytest.fixture
def pending_submission(tmp_path):
    '''Run process_assembly_pending_submission for one assembly, with Webin-CLI mocked.

    Yields a function taking the assembly record and the Webin-CLI validation
    result, which returns the mocks so tests can check what was written and shown.
    '''
    assembly_dir = tmp_path / 'ena_assembly_files' / PROFILE_ID
    assembly_dir.mkdir(parents=True)

    with override_settings(MEDIA_ROOT=str(tmp_path), MEDIA_URL='/media/'), \
            mock.patch.object(EnaAssembly, 'Assembly') as assembly, \
            mock.patch.object(EnaAssembly, 'Submission') as submission, \
            mock.patch.object(EnaAssembly, 'EnaSubmissionHelper') as helper, \
            mock.patch.object(EnaAssembly, 'ghlper') as ghlper:
        assembly.return_value.get_schema.return_value = ASSEMBLY_SCHEMA
        submission.return_value.get_assembly_pending_submission.return_value = [
            {'_id': 'sub1', 'profile_id': PROFILE_ID, 'assemblies': ['assembly1']}]

        def run(record, return_code, output, report=None):
            assembly.return_value.get_record.return_value = record
            helper.return_value.validate_assembly.return_value = (return_code, output)
            if report is not None:
                # ENA Webin-CLI writes its validation errors next to the manifest
                (assembly_dir / 'manifest.txt.report').write_text(report)

            EnaAssembly.process_assembly_pending_submission()

            return mock.Mock(
                manifest=(assembly_dir / 'manifest.txt').read_text(),
                assembly=assembly.return_value,
                helper=helper.return_value,
                ghlper=ghlper,
            )

        yield run


def make_assembly_record(**extra):
    record = {
        '_id': 'assembly1',
        'assemblyname': 'my_assembly',
        'submission_type': 'genome',
        'fasta': 'genome.fasta.gz',
    }
    record.update(extra)
    return record


def test_agp_on_an_old_assembly_is_not_written_to_the_manifest(pending_submission):
    # Fail validation so the test stops after the manifest is written
    result = pending_submission(make_assembly_record(agp='scaffolds.agp.gz'), 1, 'ERROR return code 1')

    assert 'FASTA\t' in result.manifest
    assert 'AGP' not in result.manifest


@pytest.mark.parametrize('ena_error', [
    'ERROR: Unknown field: AGP',
    'ERROR: Invalid FASTA file suffix: genome.fasta',
    'ERROR: Invalid CHROMOSOME_LIST file suffix: chromosomes.txt',
])
def test_ena_validation_error_is_shown_to_the_user(pending_submission, ena_error):
    output = 'Submission(s) validation failed ERROR return code 2'

    result = pending_submission(make_assembly_record(), 2, output, report=ena_error)

    expected_error = f'{output} {ena_error}'
    result.assembly.update_assembly_error.assert_called_once_with(
        assembly_ids=['assembly1'], msg=expected_error)
    result.ghlper.notify_assembly_status.assert_any_call(
        data={'profile_id': PROFILE_ID}, msg=expected_error, action='error', html_id='assembly_info')
    result.helper.submit_assembly.assert_not_called()  # Nothing is submitted after a failed validation


# `assembly_file` worksheet in a reads manifest under Biodata profiles
#
# Assemblies under Biodata profiles are listed in the `assembly_file` worksheet of
# the reads manifest, with one row per file and its type (FASTA, ...). The
# sheet is checked when the manifest is uploaded and again when the
# assemblies are submitted to ENA (in the `register_assembly` function).

# File types in the `assembly_file_type` dropdown menu from the reads main schema spreadshsheet
WORKSHEET_FILE_TYPES = ['FASTA', 'FLATFILE', 'CHROMOSOME_LIST', 'UNLOCALISED_LIST']


@pytest.mark.parametrize('file_type, file_name', [
    ('FASTA', 'genome.fasta.gz'),
    ('FASTA', 'genome.fa.gz'),
    ('fasta', 'genome.fna.gz'),
    ('FLATFILE', 'genome.embl.gz'),
    ('CHROMOSOME_LIST', 'chromosomes.txt.gz'),
    ('UNLOCALISED_LIST', 'unlocalised.txt.gz'),
])
def test_valid_worksheet_assembly_file_has_no_error(file_type, file_name):
    assert assembly_utils.get_assembly_file_error(file_type, file_name) == ''


def test_agp_worksheet_assembly_file_is_rejected():
    error = assembly_utils.get_assembly_file_error('AGP', 'scaffolds.agp.gz')

    assert error.startswith('Assembly file type <strong>AGP</strong> of file <strong>scaffolds.agp.gz</strong>')
    for file_type in ('FASTA', 'FLATFILE', 'CHROMOSOME_LIST', 'UNLOCALISED_LIST'):
        assert f'<li>{file_type}</li>' in error
    assert '<li>AGP</li>' not in error


@pytest.mark.parametrize('file_type, file_name', [
    ('FASTA', 'genome.fasta'),
    ('FASTA', 'genome.txt.gz'),
    ('CHROMOSOME_LIST', 'chromosomes.txt'),
    ('FLATFILE', 'genome.embl'),
])
def test_worksheet_assembly_file_with_invalid_extension_is_rejected(file_type, file_name):
    error = assembly_utils.get_assembly_file_error(file_type, file_name)

    assert error.startswith(f'Invalid file <strong>{file_name}</strong>')


# Checked when the manifest is uploaded

def run_assembly_file_validator(rows, component='assembly_file'):
    from src.apps.copo_single_cell_submission.utils.validator.SingleCellSchemaValidators import (
        AssemblyFileValidator)

    data = pd.DataFrame(rows, columns=['assembly_file_type', 'assembly_file'])
    schema = {'assembly_file_type': {'choice': WORKSHEET_FILE_TYPES}}
    errors, _, flag, _ = AssemblyFileValidator(
        profile_id=PROFILE_ID, fields=None, data=data, errors=[], warnings=[], flag=True,
        schema=schema, component=component, isupdate=False).validate()
    return errors, flag


def test_worksheet_validator_accepts_valid_files():
    errors, flag = run_assembly_file_validator([
        ('FASTA', 'genome.fasta.gz'),
        ('CHROMOSOME_LIST', 'chromosomes.txt.gz'),
    ])

    assert flag is True
    assert errors == []


@pytest.mark.parametrize('file_type, file_name, expected_error', [
    ('FASTA', 'genome.fasta', 'Invalid file <strong>genome.fasta</strong>'),
    ('CHROMOSOME_LIST', 'chromosomes.txt', 'Invalid file <strong>chromosomes.txt</strong>'),
])
def test_worksheet_validator_reports_invalid_file_with_its_row(file_type, file_name, expected_error):
    errors, flag = run_assembly_file_validator([
        ('FASTA', 'genome.fasta.gz'),
        (file_type, file_name),
    ])

    assert flag is False
    assert len(errors) == 1
    # Row 1 is the header, so the second data row is row 3
    assert errors[0].startswith(f'Sheet <strong>assembly_file</strong>, row <strong>3</strong>: {expected_error}')


@pytest.mark.parametrize('file_type, file_name', [
    ('NOT_A_TYPE', 'genome.fasta'),  # Reported by `IncorrectValueValidator`
    ('AGP', 'scaffolds.agp.gz'),  # Not in the dropdown menu, so reported by `IncorrectValueValidator`
    ('', 'genome.fasta'),  # Reported by `MandatoryValuesValidator`
    ('FASTA', ''),  # Reported by `MandatoryValuesValidator`
])
def test_worksheet_validator_leaves_other_errors_to_other_validators(file_type, file_name):
    errors, flag = run_assembly_file_validator([(file_type, file_name)])

    assert flag is True
    assert errors == []


def test_worksheet_validator_ignores_other_sheets():
    errors, flag = run_assembly_file_validator([('AGP', 'scaffolds.agp')], component='sequencing_annotation')

    assert flag is True
    assert errors == []


# Checked when the assembly is submitted to ENA

@pytest.fixture
def register_assembly(tmp_path):
    '''Run EnaSubmissionHelper.register_assembly for one assembly, with Webin-CLI mocked.

    Yields a function taking the assembly_file rows and the files downloaded
    to COPO ({file name: content}), which returns the mocks so tests can check
    what was written to the manifest and shown to the user.
    '''
    download_dir = tmp_path / 'downloads'
    download_dir.mkdir()

    with mock.patch.object(ena_helper, 'Assembly') as assembly, \
            mock.patch.object(ena_helper, 'EnaFileTransfer') as ena_file_transfer, \
            mock.patch.object(ena_helper, 'Singlecell') as singlecell, \
            mock.patch.object(ena_helper, 'ghlper'), \
            mock.patch.object(ena_helper, 'notify_submission_status'), \
            mock.patch.object(ena_helper.tempfile, 'gettempdir', return_value=str(tmp_path)):
        assembly.return_value.get_schema.return_value = ASSEMBLY_SCHEMA

        helper = ena_helper.EnaSubmissionHelper.__new__(ena_helper.EnaSubmissionHelper)
        helper.profile_id = PROFILE_ID
        helper.submission_id = 'sub1'
        # Fail ENA Webin-CLI validation so the test stops after the manifest is written
        helper.validate_assembly = mock.Mock(return_value=(1, 'ERROR return code 1'))
        helper.submit_assembly = mock.Mock()

        def run(file_rows, downloaded_files):
            transfers = []
            for file_name, content in downloaded_files.items():
                local_path = download_dir / file_name
                local_path.write_bytes(content)
                transfers.append({'local_path': str(local_path)})
            ena_file_transfer.return_value.get_all_records_columns.return_value = transfers

            assembly_df = pd.DataFrame([{
                'study_id': 'study1', 'assembly_id': 'assembly1', 'assemblyname': 'my_assembly',
                'study_accession': 'PRJEB1', 'biosampleAccession': 'SAMEA12345', 'coverage': '30',
            }])
            file_df = pd.DataFrame(
                [('assembly1', t, f) for t, f in file_rows],
                columns=['assembly_id', 'assembly_file_type', 'assembly_file'])

            result = helper.register_assembly(
                identifier='assembly_id', assembly_component_data_df=assembly_df,
                assembly_file_data_df=file_df, parent_map={'assembly_file': {'assembly': 'assembly_id'}},
                singlecell_id='singlecell1')

            manifest_path = tmp_path / PROFILE_ID / 'study1' / 'assembly1' / 'manifest.txt'
            return mock.Mock(
                result=result,
                manifest=manifest_path.read_text() if manifest_path.exists() else None,
                download_dir=download_dir,
                helper=helper,
                update_component_status=singlecell.return_value.update_component_status,
            )

        yield run


def assert_assembly_rejected(run, expected_error):
    assert run.result['status'] is False
    assert expected_error in run.result['message']
    run.update_component_status.assert_called_once_with(
        id='singlecell1', component='assembly', identifier='assembly_id', identifier_value='assembly1',
        repository='ena', status_column_value={'status': 'rejected', 'error': expected_error})
    run.helper.validate_assembly.assert_not_called()  # Rejected before ENA Webin-CLI is run
    assert run.manifest is None


def test_valid_worksheet_assembly_files_are_written_to_the_manifest(register_assembly):
    run = register_assembly(
        [('FASTA', 'genome.fasta.gz'), ('CHROMOSOME_LIST', 'chromosomes.txt.gz')],
        {'genome.fasta.gz': gzip.compress(FASTA_CONTENT),
         'chromosomes.txt.gz': gzip.compress(b'chr1\t1\tChromosome\n')})

    assert f'FASTA\t{run.download_dir / "genome.fasta.gz"}\n' in run.manifest
    assert f'CHROMOSOME_LIST\t{run.download_dir / "chromosomes.txt.gz"}\n' in run.manifest
    run.helper.validate_assembly.assert_called_once()


def test_agp_worksheet_assembly_file_is_rejected_at_submission(register_assembly):
    # e.g. A manifest uploaded before the `AGP` data file type was rejected on upload
    run = register_assembly(
        [('FASTA', 'genome.fasta.gz'), ('AGP', 'scaffolds.agp.gz')],
        {'genome.fasta.gz': gzip.compress(FASTA_CONTENT), 'scaffolds.agp.gz': gzip.compress(b'agp')})

    assert_assembly_rejected(run, assembly_utils.get_assembly_file_error('AGP', 'scaffolds.agp.gz'))


def test_worksheet_assembly_file_with_invalid_extension_is_rejected_at_submission(register_assembly):
    run = register_assembly([('FASTA', 'genome.fasta')], {'genome.fasta': FASTA_CONTENT})

    assert_assembly_rejected(run, assembly_utils.get_assembly_file_error('FASTA', 'genome.fasta'))


@pytest.mark.parametrize('file_name, content', [
    ('genome.fasta.gz', FASTA_CONTENT),  # Plain text renamed to .gz
    ('genome.fasta.gz', bz2.compress(FASTA_CONTENT)),  # bzip2 renamed to .gz
    ('genome.fasta.gz', b''),  # Empty file
])
def test_worksheet_assembly_file_not_really_compressed_is_rejected_at_submission(
        register_assembly, file_name, content):
    run = register_assembly([('FASTA', file_name)], {file_name: content})

    assert_assembly_rejected(run, MESSAGES['invalid_compressed_file'].format(
        file_name=file_name, file_extension=Path(file_name).suffix))


def test_worksheet_assembly_file_not_downloaded_is_rejected_at_submission(register_assembly):
    run = register_assembly([('FASTA', 'genome.fasta.gz')], {})

    assert_assembly_rejected(run, MESSAGES['missing_assembly_file'].format(file_name='genome.fasta.gz'))


def test_all_invalid_worksheet_assembly_files_are_reported_together(register_assembly):
    run = register_assembly(
        [('AGP', 'scaffolds.agp.gz'), ('FASTA', 'genome.fasta')],
        {'scaffolds.agp.gz': gzip.compress(b'agp'), 'genome.fasta': FASTA_CONTENT})

    assert_assembly_rejected(run, '<br>'.join([
        assembly_utils.get_assembly_file_error('AGP', 'scaffolds.agp.gz'),
        assembly_utils.get_assembly_file_error('FASTA', 'genome.fasta'),
    ]))
