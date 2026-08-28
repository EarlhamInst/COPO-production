"""Unit tests for common.validators.helpers.

These cover the two pure functions in that module. Everything else in it calls
out to ENA, OLS or NCBI and belongs in a later batch with patched HTTP clients.
"""

# Test suite for validator helper functions: validate_date, clean_str, and check_biocollection
# - validate_date: Tests date validation with various formats and past/future dates
# - clean_str: Tests string cleaning including whitespace removal
# - check_biocollection: Tests biocollection voucher registration checking with mocked HTTP responses

from datetime import date, timedelta
from unittest import mock
import pytest

from common.validators.helpers import clean_str, validate_date, check_biocollection


def test_validate_date_accepts_a_past_date():
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    assert validate_date(yesterday) is None


@pytest.mark.parametrize(
    'bad_date',
    [
        '01-01-2020',   # right parts, wrong order
        '2020/01/01',   # right order, wrong separator
        '2020-13-01',   # month 13
        '2020-02-30',   # day does not exist in February
        'not-a-date',
        '',
    ],
)
def test_validate_date_rejects_malformed_dates(bad_date):
    with pytest.raises(ValueError, match='should be YYYY-MM-DD'):
        validate_date(bad_date)


@pytest.mark.parametrize(
    'offset_days',
    [
        0,      # today: the check is strictly-in-the-past, so today fails too
        1,      # tomorrow
        365,    # next year
    ],
)
def test_validate_date_rejects_today_and_future_dates(offset_days):
    # Computed relative to today rather than hardcoded, so the test stays
    # correct as time passes and does not need a clock-freezing library.
    future = (date.today() + timedelta(days=offset_days)).strftime('%Y-%m-%d')

    with pytest.raises(AssertionError, match='date is in the future'):
        validate_date(future)


@pytest.mark.parametrize(
    'raw, expected',
    [
        (' Homo sapiens ', 'Homo sapiens'),  # non-breaking spaces
        ('  padded  ', 'padded'),
        ('\tHomo sapiens\n', 'Homo sapiens'),
        ('Homo sapiens', 'Homo sapiens'),              # already clean
        (123, '123'),                                  # coerces non-strings
    ],
)
def test_clean_str_strips_surrounding_whitespace(raw, expected):
    assert clean_str(raw) == expected


@pytest.mark.xfail(strict=True, reason='clean_str() does not remove zero-width spaces. This is known, and this is an xfail test')
def test_clean_str_removes_zero_width_space():
    assert clean_str('Homo sapiens\u200b') == 'Homo sapiens'

def test_check_biocollection_rejects_unregistered_voucher():
    fake_response = mock.Mock(
        status_code=200,
        **{'json.return_value': {'success': False}},
    )

    with mock.patch('common.validators.helpers.requests.get', return_value=fake_response):
        assert check_biocollection('V12345', 'type1') is False


def test_check_biocollection_accepts_registered_voucher():
    fake_response = mock.Mock(
        status_code=200,
        **{'json.return_value': {'success': True}},
    )

    with mock.patch('common.validators.helpers.requests.get', return_value=fake_response):
        assert check_biocollection('V12345', 'type1') is True