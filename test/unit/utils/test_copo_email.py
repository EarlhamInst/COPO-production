'''Unit tests for not emailing reserved test domains.

Domains such as `.invalid` and `example.com` are reserved for testing,
so any mail to them can never be delivered. The Playwright
tests share EDP profiles with 'copo-playwright@example.invalid' but Office 365
refused that address and the error broke every EDP profile save in the tests.
The tests now drop reserved addresses before connecting to the mail server.
'''

from unittest import mock

import pytest
from django.test import override_settings

from common.utils import copo_email
from common.utils.copo_email import CopoEmail, is_email_configured, is_reserved_email_address


@pytest.mark.parametrize('address', [
    'copo-playwright@example.invalid',
    'someone@example.com',
    'someone@EXAMPLE.ORG',
    'someone@mail.example.net',
    'someone@site.test',
    'someone@machine.localhost',
    'someone@company.example',
])
def test_reserved_addresses(address):
    assert is_reserved_email_address(address)


@pytest.mark.parametrize('address', [
    'admin@test.com',  # a real domain that only looks like a test one
    'someone@examples.com',
    'someone@invalid.org',
])
def test_real_addresses(address):
    assert not is_reserved_email_address(address)


@pytest.fixture
def smtp():
    # A server with email set up, and a stand-in for its mail server
    with override_settings(MAIL_SERVER='smtp.earlham.test', MAIL_SERVER_PORT='587'), \
            mock.patch.object(copo_email.smtplib, 'SMTP') as smtp, \
            mock.patch.object(copo_email, 'l'):
        yield smtp


def test_only_real_recipients_are_emailed(smtp):
    sent = CopoEmail().send(to=['copo-playwright@example.invalid', 'team@earlham.ac.uk'], sub='Subject', content='Body')

    assert sent is True
    assert smtp.return_value.sendmail.call_args.args[1] == ['team@earlham.ac.uk']


def test_no_connection_when_every_recipient_is_reserved(smtp):
    sent = CopoEmail().send(to=['copo-playwright@example.invalid'], sub='Subject', content='Body')

    assert sent is True
    smtp.assert_not_called()


# --- Servers without email set up (e.g. CI) ---

@pytest.mark.parametrize('mail_server, configured', [('smtp.earlham.test', True), ('', False)])
def test_email_is_set_up_when_there_is_a_mail_server(mail_server, configured):
    with override_settings(MAIL_SERVER=mail_server):
        assert is_email_configured() is configured


def test_skips_sending_when_email_is_not_set_up():
    with override_settings(MAIL_SERVER=''), \
            mock.patch.object(copo_email.smtplib, 'SMTP') as smtp, \
            mock.patch.object(copo_email, 'l'):
        sent = CopoEmail().send(to=['team@earlham.ac.uk'], sub='Subject', content='Body')

    assert sent is False
    smtp.assert_not_called()
