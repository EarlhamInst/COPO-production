"""Unit tests for parsing curl's FTP upload progress in common.ena_utils.generic_helper.

curl -# prints a progress bar ending in a one-decimal percentage, e.g. "23.7%".
_PCT_RE previously captured only the digits immediately before '%', so it read
the tenths digit and users saw progress cycle 0-9% every ~0.1% of the file.
"""

import pytest

from common.ena_utils.generic_helper import _PCT_RE


@pytest.mark.parametrize(
    'line, expected',
    [
        ('#                                                                         0.0%', 0),
        ('######                                                                    9.7%', 9),
        ('##################                                                       23.7%', 23),
        ('############### 21.0%', 21),
        ('########################################################################100.0%', 100),
        ('  45%', 45),
        ('100%', 100),
    ],
)
def test_pct_re_reads_whole_percentage_from_curl_progress(line, expected):
    match = _PCT_RE.search(line)

    assert match is not None
    assert int(match.group(1)) == expected


def test_pct_re_ignores_lines_without_a_percentage():
    assert _PCT_RE.search('curl: (28) Operation timed out') is None
