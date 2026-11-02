'''Unit tests for checking manifest values against a field's regex.

A regex cell in a schema spreadsheet that is empty or only has whitespace
means that the field has no regex. It must not be treated as an empty
pattern, which no value can match.
'''

from unittest import mock

import pandas as pd
import pytest

from src.apps.copo_single_cell_submission.utils.validator import SingleCellSchemaValidators
from src.apps.copo_single_cell_submission.utils.validator.SingleCellSchemaValidators import (
    IncorrectValueValidator,
)

DESCRIPTION = 'A detailed description of the project, its goals and approach.'


def validate(term_regex, value=DESCRIPTION):
    schema = {
        'description': {
            'term_name': 'description',
            'term_label': 'Description',
            'term_type': 'string',
            'term_regex': term_regex,
        }
    }
    validator = IncorrectValueValidator(
        profile_id='profile_id',
        fields=[],
        data=pd.DataFrame({'description': [value]}),
        errors=[],
        warnings=[],
        flag=True,
        schema=schema,
        component='study',
    )
    # No samples in the profile so, no database is needed
    with mock.patch.object(SingleCellSchemaValidators, 'Sample') as sample:
        sample.return_value.get_all_records_columns.return_value = []
        return validator.validate()


@pytest.mark.parametrize('term_regex', ['', ' ', '  \t', float('nan'), None])
def test_blank_regex_accepts_any_value(term_regex):
    errors, _, flag, _ = validate(term_regex)

    assert errors == []
    assert flag is True


def test_regex_rejects_value_that_does_not_match():
    errors, _, flag, _ = validate(r'^[A-Z]+$')

    assert len(errors) == 1
    assert 'Description' in errors[0]
    assert flag is False


def test_regex_surrounded_by_whitespace_is_stripped():
    errors, _, flag, _ = validate(' ^[A-Z]+$ ', value='ABC')

    assert errors == []
    assert flag is True
