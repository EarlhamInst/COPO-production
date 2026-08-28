"""Regression test for ATaxonIdMustBeIntegerValidator.

Previously a no-op: it called row.get("TAXON_ID", "") inside a try/except
ValueError, but a plain dict-style .get() can never raise ValueError, so the
except clause -- the validator's entire reason for existing -- was dead code.
A non-numeric TAXON_ID passed validation silently, no matter what it was.
"""

import pandas as pd

from src.apps.copo_dtol_upload.utils.tol_validators.taxon_validators import (
    ATaxonIdMustBeIntegerValidator,
)


def _make_validator(taxon_ids):
    data = pd.DataFrame({"TAXON_ID": taxon_ids})
    return ATaxonIdMustBeIntegerValidator(
        profile_id="test-profile",
        fields=[],
        data=data,
        errors=[],
        warnings=[],
        flag=True,
    )


def test_rejects_non_numeric_taxon_id():
    validator = _make_validator(["not-a-number"])

    errors, warnings, flag = validator.validate()

    assert flag is False
    assert len(errors) == 1


def test_accepts_numeric_taxon_id():
    validator = _make_validator(["9606"])

    errors, warnings, flag = validator.validate()

    assert flag is True
    assert errors == []


def test_ignores_blank_taxon_id():
    # Missingness is a different validator's concern (mandatory-field checks) --
    # this one should stay silent on an absent value rather than double-report it
    # as "non numeric".
    validator = _make_validator([""])

    errors, warnings, flag = validator.validate()

    assert flag is True
    assert errors == []
