"""Unit tests for the curl argv builder used by FTP uploads to ENA.

A failed upload of a 600GB read file used to restart from byte 0, so a
transfer that died at 90% cost another full transfer. _build_ftp_upload_cmd
adds curl's -C - (append from the remote file's current size) whenever ENA
already holds part of the file.
"""

import pytest

from common.ena_utils.generic_helper import _build_ftp_upload_cmd

FILE_PATH = '/copo/local_uploads/profile1/reads.fastq.gz'
FTP_URL = 'ftp://webin2.ebi.ac.uk/submission1/reads/reads.fastq.gz'
NETRC = '/tmp/copo_netrc_test'


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
