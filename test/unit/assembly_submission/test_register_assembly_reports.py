'''Unit tests for showing Webin-CLI's errors when a reads manifest assembly fails.

register_assembly writes each assembly's manifest to its own folder and passes
that folder to Webin-CLI as its output folder. When Webin-CLI fails, the
ERROR and WARNING lines of every report in that folder are saved on the
assembly (get_webin_cli_report), whichever return code Webin-CLI gave. The
folder is emptied first, so reports from an earlier attempt are not shown again.

MongoDB and Webin-CLI are mocked.

To execute these tests, run: $ pytest test/unit/assembly_submission/test_register_assembly_reports.py
'''

import gzip
import json
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from common.ena_utils import ena_helper

PROFILE_ID = 'profile1'
ASSEMBLY_SCHEMA_FILE = (Path(__file__).resolve().parents[3]
                        / 'common/schemas/copo/uimodels/mappings/isa_mappings/assembly.json')
ASSEMBLY_SCHEMA = {'schema_dict': json.loads(ASSEMBLY_SCHEMA_FILE.read_text())['properties']}


@pytest.fixture
def register_assembly(tmp_path):
    '''Run register_assembly for one valid assembly, with Webin-CLI validation mocked.

    Yields a function taking Webin-CLI's validation result and the reports it writes
    ({relative path: text}). It returns the submission folder and the mocks.
    '''
    submission_folder = tmp_path / PROFILE_ID / 'study1' / 'assembly1'
    local_fasta = tmp_path / 'downloads' / 'genome.fasta.gz'
    local_fasta.parent.mkdir()
    local_fasta.write_bytes(gzip.compress(b'>seq1\nACGT\n'))

    with mock.patch.object(ena_helper, 'Assembly') as assembly, \
            mock.patch.object(ena_helper, 'EnaFileTransfer') as ena_file_transfer, \
            mock.patch.object(ena_helper, 'Singlecell') as singlecell, \
            mock.patch.object(ena_helper, 'ghlper'), \
            mock.patch.object(ena_helper, 'notify_submission_status'), \
            mock.patch.object(ena_helper.tempfile, 'gettempdir', return_value=str(tmp_path)):
        assembly.return_value.get_schema.return_value = ASSEMBLY_SCHEMA
        ena_file_transfer.return_value.get_all_records_columns.return_value = [{'local_path': str(local_fasta)}]

        helper = ena_helper.EnaSubmissionHelper.__new__(ena_helper.EnaSubmissionHelper)
        helper.profile_id = PROFILE_ID
        helper.submission_id = 'sub1'
        helper.submit_assembly = mock.Mock()

        def run(return_code, output, reports=None):
            def validate_assembly(file_path, submission_type, output_dir=None):
                # Webin-CLI writes its reports to the output folder
                for relative_path, text in (reports or {}).items():
                    report_path = Path(output_dir) / relative_path
                    report_path.parent.mkdir(parents=True, exist_ok=True)
                    report_path.write_text(text)
                return return_code, output

            helper.validate_assembly = mock.Mock(side_effect=validate_assembly)
            result = helper.register_assembly(
                identifier='assembly_id',
                assembly_component_data_df=pd.DataFrame([{
                    'study_id': 'study1', 'assembly_id': 'assembly1', 'assemblyname': 'my_assembly',
                    'study_accession': 'PRJEB1', 'biosampleAccession': 'SAMEA12345', 'coverage': '30',
                }]),
                assembly_file_data_df=pd.DataFrame(
                    [('assembly1', 'FASTA', 'genome.fasta.gz')],
                    columns=['assembly_id', 'assembly_file_type', 'assembly_file']),
                parent_map={'assembly_file': {'assembly': 'assembly_id'}},
                singlecell_id='singlecell1')
            return mock.Mock(result=result, submission_folder=submission_folder, helper=helper,
                             update_component_status=singlecell.return_value.update_component_status)

        yield run


def saved_error(run):
    return run.update_component_status.call_args.kwargs['status_column_value']['error']


def test_webin_cli_gets_the_submission_folder_as_its_output_folder(register_assembly):
    run = register_assembly(1, 'ERROR return code 1')

    assert run.helper.validate_assembly.call_args.kwargs['output_dir'] == str(run.submission_folder)


def test_manifest_errors_are_saved_on_the_assembly(register_assembly):
    # Return code 2: Webin-CLI writes manifest.txt.report next to the manifest
    run = register_assembly(2, 'ERROR return code 2',
                            {'manifest.txt.report': 'ERROR: Unknown field: AGP\n'})

    assert saved_error(run) == 'ERROR return code 2<br>ERROR: Unknown field: AGP'
    assert run.result['status'] is False


def test_file_validation_errors_are_saved_without_stack_traces(register_assembly):
    # Return code 3: reports go in <context>/<name>/validate/
    report = ('ERROR: Invalid FASTA file suffix: genome.fasta\n'
              '\tat uk.ac.ebi.ena.webin.cli.WebinCli.main(WebinCli.java:1)\n'
              'WARNING: Short sequence\n')
    run = register_assembly(3, 'ERROR return code 3',
                            {'genome/study1_assembly1_my_assembly/validate/webin-cli.report': report})

    assert saved_error(run) == ('ERROR return code 3<br>ERROR: Invalid FASTA file suffix: genome.fasta'
                                '<br>WARNING: Short sequence')


def test_failure_without_reports_saves_the_output(register_assembly):
    run = register_assembly(1, 'ERROR return code 1')

    assert saved_error(run) == 'ERROR return code 1'


def test_reports_from_an_earlier_attempt_are_not_shown_again(register_assembly, tmp_path):
    stale_report = tmp_path / PROFILE_ID / 'study1' / 'assembly1' / 'manifest.txt.report'
    stale_report.parent.mkdir(parents=True)
    stale_report.write_text('ERROR: an error from last time\n')

    run = register_assembly(1, 'ERROR return code 1')

    assert saved_error(run) == 'ERROR return code 1'
    assert not stale_report.exists()
