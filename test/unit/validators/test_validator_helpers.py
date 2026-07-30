"""Unit tests for common.validators.helpers.

These cover the two pure functions in that module. Everything else in it calls
out to ENA, OLS or NCBI and belongs in a later batch with patched HTTP clients.
"""

from datetime import date, timedelta

import pytest

from common.validators.helpers import clean_str, validate_date


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
