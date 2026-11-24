'''Unit tests for Webin-CLI's file upload

Webin-CLI is run with -ascp, so it uploads files with Aspera. Webin-CLI returns
'Failed to upload files using Aspera' when upload via Aspera fails so, the same 
command is run again without `-ascp` so that Webin-CLI uses FTP.

To execute these tests, run: $ pytest test/unit/ena_utils/test_webin_cli_aspera_fallback.py
'''

import subprocess
from unittest import mock

import pytest

from common.ena_utils import ena_helper

PASSWORD = 's3cret'
ASPERA_FAILED = (
    b'WARN : Aspera upload failed. Client exit code : 1\n'
    b'ascp: failed to authenticate, exiting.\n'
    b'ERROR: Failed to upload files using Aspera. Failed to upload files to server because of a system error.\n'
)
SUBMITTED = b'The following analysis accession was assigned to the submission: ERZ12345678\n'


@pytest.fixture
def webin_cli():
    '''Run run_webin_cli with Webin-CLI mocked.

    Yields a function taking what each Webin-CLI run does, in order: output bytes for
    success, or (return code, output bytes) for failure. It returns run_webin_cli's
    result and the commands that were run.
    '''
    helper = ena_helper.EnaSubmissionHelper.__new__(ena_helper.EnaSubmissionHelper)
    helper.user_token = 'Webin-1'
    helper.pass_word = PASSWORD
    helper.logging_debug = mock.Mock()

    with mock.patch.object(ena_helper.subprocess, 'check_output') as check_output:
        def run(*webin_cli_runs, action='submit'):
            results = iter(webin_cli_runs)

            def run_command(cmd, **kwargs):
                result = next(results)
                if isinstance(result, tuple):
                    raise subprocess.CalledProcessError(result[0], cmd, output=result[1])
                return result

            check_output.side_effect = run_command
            result = helper.run_webin_cli('/work/manifest.txt', 'annotation', action, output_dir='/work')
            commands = [c.args[0] for c in check_output.call_args_list]
            return result, commands

        run.helper = helper
        yield run


def test_files_are_uploaded_with_aspera_first(webin_cli):
    (return_code, output), commands = webin_cli(SUBMITTED)

    assert return_code == 0
    assert 'ERZ12345678' in output
    assert len(commands) == 1
    assert commands[0].endswith(' -submit -ascp')


def test_failed_aspera_upload_is_retried_with_ftp(webin_cli):
    (return_code, output), commands = webin_cli((1, ASPERA_FAILED), SUBMITTED)

    assert return_code == 0
    assert 'ERZ12345678' in output
    assert commands[0].endswith(' -submit -ascp')
    assert commands[1].endswith(' -submit')  # without -ascp, Webin-CLI uploads with FTP
    assert commands[1] == commands[0][:-len(' -ascp')]


def test_ftp_errors_are_returned_when_the_retry_also_fails(webin_cli):
    (return_code, output), commands = webin_cli((1, ASPERA_FAILED), (1, b'ERROR: Failed to upload files to FTP server.\n'))

    assert len(commands) == 2
    assert return_code == 1
    assert output == 'ERROR: Failed to upload files to FTP server.\n ERROR return code 1'


@pytest.mark.parametrize('first_run', [
    pytest.param((2, b'ERROR: Invalid PRIMARY_ID\n'), id='other_error'),
    pytest.param(b'The submission has been validated successfully.\n', id='success'),
])
def test_other_results_are_not_retried(webin_cli, first_run):
    _, commands = webin_cli(first_run)

    assert len(commands) == 1


def test_password_is_not_logged_on_either_attempt(webin_cli):
    webin_cli((1, ASPERA_FAILED), SUBMITTED)

    logged = ' '.join(str(c.args[0]) for c in webin_cli.helper.logging_debug.call_args_list)
    assert PASSWORD not in logged
    assert 'trying again with FTP' in logged
