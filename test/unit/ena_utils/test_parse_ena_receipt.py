'''Unit tests for handling ENA receipts that are not XML.

ENA returns an empty body or an HTML/plain-text error page when it encounters an HTTP 
error instead of an XML receipt. As such, these tests check that such responses are
reported as a clear error message instead of a crash in the lxml.
'''

from unittest import mock

import pytest

from common.ena_utils import ena_helper
from common.ena_utils.ena_helper import EnaSubmissionHelper

RECEIPT_OK = b'<?xml version="1.0" encoding="UTF-8"?><RECEIPT success="true"><PROJECT accession="PRJEB1" alias="x:STUDY001" status="PRIVATE"/></RECEIPT>'


@pytest.fixture
def helper():
    # Skip __init__, which loads the profile from the database
    obj = EnaSubmissionHelper.__new__(EnaSubmissionHelper)
    obj.submission_id = 'sub1'
    obj.profile_id = 'profile1'
    obj.user_token = 'Webin-1'
    obj.pass_word = 'secret'
    obj.logging_debug = mock.Mock()
    obj.logging_info = mock.Mock()
    obj.logging_error = mock.Mock()
    return obj


def test_xml_receipt_is_parsed(helper):
    root, error = helper.parse_ena_receipt(RECEIPT_OK, action='register the study', http_code='200')
    assert error is None
    assert root.get('success') == 'true'
    assert root.find('PROJECT').get('accession') == 'PRJEB1'


def test_xml_receipt_without_an_http_status_is_parsed(helper):
    root, error = helper.parse_ena_receipt(RECEIPT_OK, action='register the study')
    assert error is None
    assert root.get('success') == 'true'


def test_empty_receipt_with_401_points_to_credentials(helper):
    root, error = helper.parse_ena_receipt(b'', action='register the study', http_code='401')
    assert root is None
    assert 'empty response (HTTP 401)' in error
    assert 'register the study' in error
    assert 'Webin username and password' in error


def test_html_receipt_is_reported_with_preview(helper):
    html = b'<!DOCTYPE html>\n<html><body><h1>Service   Unavailable</h1></body></html>'
    root, error = helper.parse_ena_receipt(html, action='register the samples', http_code='503')
    assert root is None
    assert 'unexpected response (HTTP 503)' in error
    assert 'Service Unavailable' in error
    assert 'try again later' in error


def test_plain_text_receipt_is_reported(helper):
    root, error = helper.parse_ena_receipt(b'Unauthorized', action='register the study', http_code='200')
    assert root is None
    assert 'HTTP 200' in error
    assert 'Unauthorized' in error


@pytest.mark.parametrize('body, http_code, expected', [
    (b'', 401, 'empty response (HTTP 401)'),
    (b'<html>Maintenance</html>', 503, 'unexpected response (HTTP 503)'),
])
def test_process_project_rejects_study_instead_of_crashing(helper, tmp_path, body, http_code, expected):
    study = {'study_id': 'STUDY001'}
    helper.write_xml_file = mock.Mock(return_value={'status': True, 'value': str(tmp_path / 'study.xml')})

    with mock.patch.object(helper, 'submit_xml_to_ena', return_value=(body, str(http_code))), \
            mock.patch.object(ena_helper, 'Singlecell') as singlecell, \
            mock.patch.object(ena_helper.ghlper, 'update_submission_status') as update_status:
        result = helper.process_project(str(tmp_path), root=None, submission_xml_path='submission.xml',
                                        study=study, singlecell_id='sc1')

    assert result['status'] is False
    assert expected in result['message']

    status = singlecell.return_value.update_component_status.call_args.kwargs['status_column_value']
    assert status['status'] == 'rejected'
    assert expected in status['error']

    assert update_status.call_args.kwargs['status'] == 'error'
    assert expected in update_status.call_args.kwargs['message']


def test_xml_files_are_posted_to_ena_with_requests(helper, tmp_path):
    submission = tmp_path / 'submission.xml'
    sample = tmp_path / 'sample.xml'
    submission.write_bytes(b'<SUBMISSION/>')
    sample.write_bytes(b'<SAMPLE_SET/>')
    response = mock.Mock(content=RECEIPT_OK, status_code=200)

    with mock.patch.object(ena_helper.requests, 'post', return_value=response) as post, \
            mock.patch.object(ena_helper.subprocess, 'check_output') as check_output:
        body, http_code = helper.submit_xml_to_ena({'SUBMISSION': str(submission), 'SAMPLE': str(sample)})

    assert (body, http_code) == (RECEIPT_OK, '200')
    check_output.assert_not_called()  # no command line that `ps` could show the password on
    kwargs = post.call_args.kwargs
    assert post.call_args.args == (ena_helper.ena_service,)
    assert kwargs['auth'] == ('Webin-1', 'secret')
    assert kwargs['timeout'] == ena_helper.ENA_SUBMIT_TIMEOUT
    assert {name: (file_name, content_type) for name, (file_name, _, content_type) in kwargs['files'].items()} == {
        'SUBMISSION': ('submission.xml', 'application/xml'),
        'SAMPLE': ('sample.xml', 'application/xml'),
    }
    assert all(handle.closed for _, handle, _ in kwargs['files'].values())


def test_xml_files_are_closed_when_the_request_fails(helper, tmp_path):
    submission = tmp_path / 'submission.xml'
    submission.write_bytes(b'<SUBMISSION/>')
    opened = []
    real_open = open

    def tracking_open(*args, **kwargs):
        handle = real_open(*args, **kwargs)
        opened.append(handle)
        return handle

    with mock.patch('builtins.open', side_effect=tracking_open), \
            mock.patch.object(ena_helper.requests, 'post', side_effect=ena_helper.requests.Timeout('slow')), \
            pytest.raises(ena_helper.requests.Timeout):
        helper.submit_xml_to_ena({'SUBMISSION': str(submission)})

    assert opened and all(handle.closed for handle in opened)
