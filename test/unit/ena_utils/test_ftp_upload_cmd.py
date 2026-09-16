"""Unit tests for resuming FTP uploads to ENA in common.ena_utils.generic_helper.

A failed upload of a 600GB read file used to restart from byte 0, so a
transfer that died at 90% cost another full transfer. _remote_ftp_size asks
ENA how much it already holds, and _build_ftp_upload_cmd adds curl's -C -
(append from that offset) when there is something to resume from.
"""

from unittest import mock

import pytest

from common.ena_utils.generic_helper import _build_ftp_upload_cmd, _remote_ftp_size

FILE_PATH = '/copo/local_uploads/profile1/reads.fastq.gz'
FTP_URL = 'ftp://webin2.ebi.ac.uk/submission1/reads/reads.fastq.gz'
NETRC = '/tmp/copo_netrc_test'
HOST = 'webin2.ebi.ac.uk'
REMOTE_FILE = 'submission1/reads/reads.fastq.gz'


def test_upload_from_scratch_does_not_resume():
    cmd = _build_ftp_upload_cmd(FILE_PATH, FTP_URL, NETRC, resume_from=0)

    assert '-C' not in cmd


@pytest.mark.parametrize('resume_from', [1, 1024, 616392221743])
def test_upload_resumes_when_ena_already_holds_bytes(resume_from):
    cmd = _build_ftp_upload_cmd(FILE_PATH, FTP_URL, NETRC, resume_from=resume_from)

    assert cmd[-2:] == ['-C', '-']


def test_command_always_carries_the_upload_essentials():
    cmd = _build_ftp_upload_cmd(FILE_PATH, FTP_URL, NETRC, resume_from=0)

    assert cmd[0] == 'curl'
    assert cmd[cmd.index('-T') + 1] == FILE_PATH
    assert cmd[cmd.index('--netrc-file') + 1] == NETRC
    assert FTP_URL in cmd
    assert '--ftp-create-dirs' in cmd
    assert cmd[cmd.index('--retry') + 1] == '3'


def test_resume_keeps_the_upload_essentials():
    cmd = _build_ftp_upload_cmd(FILE_PATH, FTP_URL, NETRC, resume_from=99)

    assert cmd[cmd.index('-T') + 1] == FILE_PATH
    assert cmd[cmd.index('--netrc-file') + 1] == NETRC
    assert FTP_URL in cmd


def test_size_probe_returns_the_bytes_ena_already_holds():
    ftp = mock.MagicMock()
    ftp.size.return_value = 583400353848

    with mock.patch('common.ena_utils.generic_helper.ftplib.FTP', return_value=ftp):
        assert _remote_ftp_size(HOST, REMOTE_FILE, 'Webin-1', 'pw') == 583400353848

    ftp.size.assert_called_once_with(REMOTE_FILE)
    # SIZE is undefined in ASCII mode on vsftpd, which is what Webin runs.
    ftp.voidcmd.assert_called_once_with('TYPE I')


def test_size_probe_treats_a_missing_remote_file_as_nothing_to_resume():
    """ftplib raises error_perm ("550 Could not get file size") for a file that
    is not there yet -- the normal case on a first upload."""
    import ftplib as real_ftplib

    ftp = mock.MagicMock()
    ftp.size.side_effect = real_ftplib.error_perm('550 Could not get file size.')

    with mock.patch('common.ena_utils.generic_helper.ftplib.FTP', return_value=ftp):
        assert _remote_ftp_size(HOST, REMOTE_FILE, 'Webin-1', 'pw') == 0


def test_size_probe_survives_a_connection_failure():
    with mock.patch('common.ena_utils.generic_helper.ftplib.FTP',
                    side_effect=OSError('timed out')):
        assert _remote_ftp_size(HOST, REMOTE_FILE, 'Webin-1', 'pw') == 0


def test_size_probe_survives_a_login_refusal():
    """Webin refused logins (curl rc=67) for half an hour on 16 Sep; a probe
    failure must not stop the upload attempt, only disable resume."""
    import ftplib as real_ftplib

    ftp = mock.MagicMock()
    ftp.login.side_effect = real_ftplib.error_perm('530 Login incorrect.')

    with mock.patch('common.ena_utils.generic_helper.ftplib.FTP', return_value=ftp):
        assert _remote_ftp_size(HOST, REMOTE_FILE, 'Webin-1', 'pw') == 0


def test_size_probe_closes_the_control_connection():
    ftp = mock.MagicMock()
    ftp.size.return_value = 10

    with mock.patch('common.ena_utils.generic_helper.ftplib.FTP', return_value=ftp):
        _remote_ftp_size(HOST, REMOTE_FILE, 'Webin-1', 'pw')

    assert ftp.quit.called or ftp.close.called
