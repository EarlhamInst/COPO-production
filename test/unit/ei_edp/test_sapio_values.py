'''Unit tests for swapping LIMS manifest values for the values Sapio expects.

Some answers in a LIMS manifest are worded differently from the matching value
in Sapio's picklist (e.g. 'UK-GDPR' in the manifest is 'GDPR' in Sapio). Those
values are listed per field in `COPO_TO_SAPIO_VALUES` and swapped before they
are submitted. Any other value is submitted as it is.
'''

from unittest import mock

import pytest

from src.apps.ei_edp.utils.lims.sapio import adapter

REGULATIONS = 'applicable_regulations'


@pytest.mark.parametrize('copo_value, sapio_value', [
    ('NONE', 'Not relevant'),
    ('UK-GDPR', 'GDPR'),
    ('EU-GDPR', 'GDPR'),
    # Typed values are matched regardless of case or surrounding spaces
    ('uk-gdpr', 'GDPR'),
    (' None ', 'Not relevant'),
])
def test_swaps_listed_values_for_sapio_values(copo_value, sapio_value):
    assert adapter._to_sapio_value(REGULATIONS, copo_value) == sapio_value


@pytest.mark.parametrize('copo_value, sapio_value', [
    ('UK-GDPR; CITES', 'GDPR; CITES'),
    ('CITES;EU-GDPR', 'CITES; GDPR'),
    # Answers that become the same Sapio value are only submitted once
    ('UK-GDPR; EU-GDPR', 'GDPR'),
    ('UK-GDPR; EU-GDPR; CITES;', 'GDPR; CITES'),
])
def test_swaps_each_of_several_answers_typed_in(copo_value, sapio_value):
    assert adapter._to_sapio_value(REGULATIONS, copo_value) == sapio_value


@pytest.mark.parametrize('value', [
    'CITES', 'HUMAN TISSUE ACT', 'CITES; HUMAN TISSUE ACT', '', None, 2.5
])
def test_submits_values_that_are_not_listed_as_they_are(value):
    assert adapter._to_sapio_value(REGULATIONS, value) == value


def test_only_swaps_values_for_the_field_they_are_listed_under():
    # 'None' means 'Not relevant' for regulations, but not for fields without it listed
    assert adapter._to_sapio_value('storage_buffer', 'None') == 'None'


def test_mapping_can_be_extended_for_other_fields():
    with mock.patch.dict(adapter.COPO_TO_SAPIO_VALUES, {'sample_return': {'YES': 'Yes'}}):
        assert adapter._to_sapio_value('sample_return', 'YES') == 'Yes'
