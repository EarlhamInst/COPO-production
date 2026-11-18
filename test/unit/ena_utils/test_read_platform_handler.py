'''Unit tests for updating the sequencing platforms and instrument models from ENA.

A Celery task downloads ENA's SRA.common.xsd from EBI's FTP server and saves
its platforms and instrument models for the read dropdowns. EBI's server is
sometimes slow, so:

- timeouts and connection errors are retried before giving up,
- HTTP errors (e.g. 404) are not retried, because they will not fix themselves,
- a failed download keeps the platforms saved last time, without trying to
  parse a missing schema.

The network and MongoDB are mocked.

To execute these tests, run: $ pytest test/unit/ena_utils/test_read_platform_handler.py
'''

from unittest import mock

import pytest
import requests

from common.ena_utils import EnaReadPlatformHandler as handler_module

SCHEMA_URL = 'https://ftp.ebi.ac.uk/pub/databases/ena/doc/xsd/sra_1_5/SRA.common.xsd'
SCHEMA = '''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:com="SRA.common" targetNamespace="SRA.common">
    <xs:complexType name="PlatformType">
        <xs:choice>
            <xs:element name="LS454">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="INSTRUMENT_MODEL" type="com:type454Model"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:choice>
    </xs:complexType>
    <xs:simpleType name="type454Model">
        <xs:restriction base="xs:string">
            <xs:enumeration value="454 GS"/>
            <xs:enumeration value="454 GS FLX"/>
            <xs:enumeration value="unspecified"/>
        </xs:restriction>
    </xs:simpleType>
</xs:schema>'''


def response(text=SCHEMA, status_code=200):
    resp = mock.Mock(text=text, status_code=status_code)
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f'{status_code} error')
    return resp


@pytest.fixture(autouse=True)
def no_waiting_or_logging():
    # Retries must not really sleep, and Logger writes logs/<date>.log into the repo
    with mock.patch.object(handler_module.get_url_text.retry, 'sleep', lambda seconds: None), \
            mock.patch.object(handler_module, 'l', mock.MagicMock()):
        yield


@pytest.fixture
def requests_get():
    with mock.patch.object(handler_module.requests, 'get') as get:
        yield get


@pytest.fixture
def platform_collection():
    with mock.patch.object(handler_module, 'EnaReadPlatformCollection') as collection:
        yield collection.return_value.get_collection_handle.return_value


def saved_platforms(collection):
    return {c.args[0]['platform']: c.args[1]['$set']['instruments'] for c in collection.find_one_and_update.call_args_list}


# ── Downloading the schema ──────────────────────────────────────────────

def test_schema_is_downloaded_with_connect_and_read_timeouts(requests_get):
    requests_get.return_value = response()

    assert handler_module.get_url_text(SCHEMA_URL) == SCHEMA
    requests_get.assert_called_once_with(SCHEMA_URL, timeout=handler_module.REQUEST_TIMEOUT)


@pytest.mark.parametrize('error', [
    requests.ReadTimeout('Read timed out. (read timeout=20)'),
    requests.ConnectTimeout('Connection timed out'),
    requests.ConnectionError('Connection refused'),
])
def test_timeouts_and_connection_errors_are_retried(requests_get, error):
    requests_get.side_effect = [error, error, response()]

    assert handler_module.get_url_text(SCHEMA_URL) == SCHEMA
    assert requests_get.call_count == 3


def test_gives_up_after_the_last_attempt(requests_get):
    requests_get.side_effect = requests.ReadTimeout('Read timed out.')

    with pytest.raises(requests.ReadTimeout):
        handler_module.get_url_text(SCHEMA_URL)
    assert requests_get.call_count == handler_module.MAX_ATTEMPTS


def test_http_errors_are_not_retried(requests_get):
    requests_get.return_value = response(status_code=404)

    with pytest.raises(requests.HTTPError):
        handler_module.get_url_text(SCHEMA_URL)
    assert requests_get.call_count == 1


# ── Updating the saved platforms ────────────────────────────────────────

def test_platforms_and_instruments_are_saved(requests_get, platform_collection):
    requests_get.return_value = response()

    handler_module.EnaReadPlatformHandler().update_platform()

    # 'unspecified' is left out of the dropdown
    assert saved_platforms(platform_collection) == {'LS454': ['454 GS', '454 GS FLX']}


def test_failed_download_keeps_the_saved_platforms_and_logs_one_error(requests_get, platform_collection):
    requests_get.side_effect = requests.ReadTimeout('Read timed out.')

    with mock.patch.object(handler_module.EnaReadPlatformHandler, '_parse_platform') as parse_platform:
        handler_module.EnaReadPlatformHandler().update_platform()

    parse_platform.assert_not_called()  # previously parsed None and logged a second TypeError
    platform_collection.find_one_and_update.assert_not_called()
    handler_module.l.exception.assert_called_once()
    assert 'Request failed after 3 attempts' in handler_module.l.exception.call_args.args[0]
