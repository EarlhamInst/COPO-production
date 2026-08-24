"""Regression test for MandatoryValuesValidator.

isnull() and isna() are aliases of the same pandas check, not two different
tests -- extending null_rows with both meant a single blank cell landed in
the list twice, and the researcher saw the same missing value reported as
two separate errors.
"""

import pandas as pd

from common.validators.ena_validators.ena_checklist_validators import (
    MandatoryValuesValidator,
)

CHECKLIST = {
    "fields": {
        "SAMPLE_TITLE": {
            "mandatory": "mandatory",
            "name": "Sample Title",
            "label": "Sample Title",
        }
    }
}


def _make_validator(values):
    data = pd.DataFrame({"SAMPLE_TITLE": values})
    return MandatoryValuesValidator(
        profile_id="test-profile",
        fields=[],
        data=data,
        errors=[],
        warnings=[],
        flag=True,
        checklist=CHECKLIST,
    )


def test_reports_a_blank_cell_once_not_twice():
    validator = _make_validator([None])

    errors, warnings, flag, _ = validator.validate()

    assert flag is False
    assert len(errors) == 1


def test_reports_an_empty_string_cell_once():
    validator = _make_validator([""])

    errors, warnings, flag, _ = validator.validate()

    assert flag is False
    assert len(errors) == 1


def test_accepts_a_populated_cell():
    validator = _make_validator(["a title"])

    errors, warnings, flag, _ = validator.validate()

    assert flag is True
    assert errors == []
