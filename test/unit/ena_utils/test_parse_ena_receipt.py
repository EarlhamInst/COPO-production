'''Unit tests for handling ENA receipts that are not XML.

curl is called without `-f`, so it exits with 0 even when ENA returns an HTTP error.
ENA then sends back an empty body or an HTML/plain-text error page. These tests check
that such replies are reported as a clear error instead of crashing in lxml.
'''

from unittest import mock

import pytest

from common.ena_utils import ena_helper
from common.ena_utils.ena_helper import CURL_HTTP_CODE_MARKER, EnaSubmissionHelper

RECEIPT_OK = b'<?xml version="1.0" encoding="UTF-8"?><RECEIPT success="true"><PROJECT accession="PRJEB1" alias="x:STUDY001" status="PRIVATE"/></RECEIPT>'


def curl_output(body, http_code):
    return body + f'{CURL_HTTP_CODE_MARKER}{http_code}|'.encode()


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
    root, error = helper.parse_ena_receipt(curl_output(RECEIPT_OK, 200), action='register the study')
    assert error is None
    assert root.get('success') == 'true'
    assert root.find('PROJECT').get('accession') == 'PRJEB1'


def test_xml_receipt_without_http_code_is_parsed(helper):
    root, error = helper.parse_ena_receipt(RECEIPT_OK, action='register the study')
    assert error is None
    assert root.get('success') == 'true'


def test_empty_receipt_with_401_points_to_credentials(helper):
    root, error = helper.parse_ena_receipt(curl_output(b'', 401), action='register the study')
    assert root is None
    assert 'empty response (HTTP 401)' in error
    assert 'register the study' in error
    assert 'Webin username and password' in error


def test_html_receipt_is_reported_with_preview(helper):
    html = b'<!DOCTYPE html>\n<html><body><h1>Service   Unavailable</h1></body></html>'
    root, error = helper.parse_ena_receipt(curl_output(html, 503), action='register the samples')
    assert root is None
    assert 'unexpected response (HTTP 503)' in error
    assert 'Service Unavailable' in error
    assert 'try again later' in error


def test_plain_text_receipt_is_reported(helper):
    root, error = helper.parse_ena_receipt(curl_output(b'Unauthorized', 200), action='register the study')
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

    with mock.patch.object(ena_helper.subprocess, 'check_output', return_value=curl_output(body, http_code)), \
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
