'''Unit tests for assembly file checks and ENA error reporting.

Assemblies are submitted to ENA with Webin-CLI. Checks include:

- The file name must have an extension accepted for its field, e.g. a
  FASTA file must end in .fasta.gz, .fa.gz, etc.
- A file named .gz or .bz2 must actually be gzip or bzip2 compressed,
  which is checked from its first bytes in S3.

ENA’s Webin-CLI will reject the manifest.txt file and report errors 
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

import pytest
from botocore.exceptions import ClientError
from django.test import override_settings

from common.validators.ena_validators.validation_messages import MESSAGES
from src.apps.copo_assembly_submission import forms
from src.apps.copo_assembly_submission.utils import EnaAssembly

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
    # EnaAssembly logs through Logger, which writes logs/<date>.log into the repo
    with mock.patch.object(EnaAssembly, 'l', mock.MagicMock()), \
            mock.patch.object(EnaAssembly, 'Logger', mock.MagicMock()):
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
    ('fasta', 'genome.fa.bz2'),
    ('fasta', 'genome.fna.gz'),
    ('flatfile', 'genome.embl.gz'),
    ('chromosome_list', 'chromosomes.txt.gz'),
    ('unlocalised_list', 'unlocalised.txt.bz2'),
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

    assert EnaAssembly._get_file_header(s3obj, PROFILE_ID, 'genome.fasta.gz').startswith(GZIP_SIGNATURE)


def test_bzip2_header_matches_bzip2_signature():
    s3obj = make_s3obj(bz2.compress(FASTA_CONTENT))

    assert EnaAssembly._get_file_header(s3obj, PROFILE_ID, 'genome.fasta.bz2').startswith(BZIP2_SIGNATURE)


def test_only_the_first_bytes_are_requested():
    s3obj = make_s3obj(gzip.compress(FASTA_CONTENT))

    header = EnaAssembly._get_file_header(s3obj, PROFILE_ID, 'genome.fasta.gz')

    assert len(header) == 3
    s3obj.s3_client.get_object.assert_called_once_with(
        Bucket=PROFILE_ID, Key='genome.fasta.gz', Range='bytes=0-2')


def test_renamed_plain_text_does_not_match_gzip_signature():
    s3obj = make_s3obj(FASTA_CONTENT)

    assert not EnaAssembly._get_file_header(s3obj, PROFILE_ID, 'genome.fasta.gz').startswith(GZIP_SIGNATURE)


@pytest.mark.parametrize('code', ['InvalidRange', 'NoSuchKey', 'AccessDenied'])
def test_s3_error_returns_empty_header(code):
    # e.g. `InvalidRange` is returned for a ranged `GET` of an empty file
    error = ClientError({'Error': {'Code': code}}, 'GetObject')

    assert EnaAssembly._get_file_header(make_s3obj(error=error), PROFILE_ID, 'genome.fasta.gz') == b''


@pytest.mark.parametrize('file_name, content', [
    ('genome.fasta.gz', FASTA_CONTENT),  # Plain text renamed to .gz
    ('genome.fasta.bz2', gzip.compress(FASTA_CONTENT)),  # gzip renamed to .bz2
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
