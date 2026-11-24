'''Unit tests for calling ENA without the Webin password on a command line.'''

from unittest import mock

import pytest

from common.ena_utils import ena_requests


@pytest.fixture
def xml_files(tmp_path):
    submission = tmp_path / 'submission.xml'
    sample = tmp_path / 'sample.xml'
    submission.write_bytes(b'<SUBMISSION/>')
    sample.write_bytes(b'<SAMPLE_SET/>')
    return {'SUBMISSION': str(submission), 'SAMPLE': str(sample)}


def test_xml_files_are_posted_as_a_multipart_form_with_basic_auth(xml_files):
    with mock.patch.object(ena_requests.requests, 'post') as post:
        response = ena_requests.post_xml_files('https://ena/submit', 'Webin-1', 'secret', xml_files)

    assert response is post.return_value
    assert post.call_args.args == ('https://ena/submit',)
    kwargs = post.call_args.kwargs
    assert kwargs['auth'] == ('Webin-1', 'secret')
    assert kwargs['timeout'] == ena_requests.ENA_REQUEST_TIMEOUT
    assert {name: (file_name, content_type) for name, (file_name, _, content_type) in kwargs['files'].items()} == {
        'SUBMISSION': ('submission.xml', 'application/xml'),
        'SAMPLE': ('sample.xml', 'application/xml'),
    }


def test_xml_files_are_closed_after_posting(xml_files):
    with mock.patch.object(ena_requests.requests, 'post') as post:
        ena_requests.post_xml_files('https://ena/submit', 'Webin-1', 'secret', xml_files)

    assert all(handle.closed for _, handle, _ in post.call_args.kwargs['files'].values())


def test_xml_files_are_closed_when_the_request_fails(xml_files):
    with mock.patch.object(ena_requests.requests, 'post', side_effect=ena_requests.requests.ConnectionError) as post, \
            pytest.raises(ena_requests.requests.ConnectionError):
        ena_requests.post_xml_files('https://ena/submit', 'Webin-1', 'secret', xml_files)

    assert all(handle.closed for _, handle, _ in post.call_args.kwargs['files'].values())


def test_get_sends_the_credentials_as_basic_auth():
    with mock.patch.object(ena_requests.requests, 'get') as get:
        ena_requests.get('https://ena/samples/SAMEA1', 'Webin-1', 'secret', timeout=(30, 300))

    get.assert_called_once_with('https://ena/samples/SAMEA1', auth=('Webin-1', 'secret'), timeout=(30, 300))


def test_webin_cli_reads_the_password_from_an_environment_variable(monkeypatch):
    monkeypatch.setenv('PATH', '/usr/bin')

    env = ena_requests.webin_cli_env('secret')

    assert env[ena_requests.WEBIN_CLI_PASSWORD_ENV] == 'secret'
    assert env['PATH'] == '/usr/bin'  # the rest of the environment is kept
    assert ena_requests.WEBIN_CLI_PASSWORD_OPTION == f'-passwordEnv={ena_requests.WEBIN_CLI_PASSWORD_ENV}'
    assert 'secret' not in ena_requests.WEBIN_CLI_PASSWORD_OPTION
