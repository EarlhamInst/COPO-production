"""Unit tests for validating ontology values when OLS can't be reached.

A manifest value that can't be checked against EBI's Ontology Lookup Service
(OLS) must not be reported as invalid, since it may well be valid. The
manifest is rejected with one message saying that OLS couldn't be reached,
and no more lookups are made since each would wait for OLS to time out.
"""

from unittest import mock

import pandas as pd

from common.validators import helpers
from common.validators.ena_validators.ena_checklist_validators import OntologyValidator
from src.apps.copo_single_cell_submission.utils.validator import SingleCellSchemaValidators
from src.apps.copo_single_cell_submission.utils.validator.SingleCellSchemaValidators import (
    IncorrectValueValidator,
)

UNAVAILABLE = helpers.OntologyLookupUnavailable('OLS returned HTTP 503')


def test_sample_manifest_reports_ols_outage_once():
    checklist = {
        'fields': {
            'developmental_stage': {
                'type': 'ONTOLOGY', 'label': 'Developmental Stage', 'ontology': 'UBERON:0000105',
            },
            'organism_part': {
                'type': 'ONTOLOGY', 'label': 'organism part', 'ontology': 'UBERON:0001062',
            },
        }
    }
    data = pd.DataFrame({
        'developmental_stage': ['embryo stage', 'adult'],
        'organism_part': ['apex of heart', 'liver'],
    })
    validator = OntologyValidator(
        profile_id='profile_id', fields=[], data=data, errors=[], warnings=[], flag=True,
        checklist=checklist,
    )

    with mock.patch(
        'common.validators.ena_validators.ena_checklist_validators.checkOntologyTerm',
        side_effect=UNAVAILABLE,
    ) as check:
        errors, _, flag, _ = validator.validate()

    assert check.call_count == 1
    assert flag is False
    assert len(errors) == 1
    assert 'Ontology Lookup Service (OLS) could not be reached' in errors[0]
    assert 'Invalid value' not in errors[0]


def test_single_cell_manifest_reports_ols_outage_once():
    schema = {
        'organism_part': {
            'term_name': 'organism_part', 'term_label': 'Organism Part', 'term_type': 'ontology',
            'term_reference': 'UBERON:0001062',
        },
        'taxon_id': {
            'term_name': 'taxon_id', 'term_label': 'Taxon ID', 'term_type': 'ontology',
            'term_reference': 'NCBITaxon',
        },
    }
    data = pd.DataFrame({'organism_part': ['apex of heart', 'liver'], 'taxon_id': ['9606', '10090']})
    validator = IncorrectValueValidator(
        profile_id='profile_id', fields=[], data=data, errors=[], warnings=[], flag=True,
        schema=schema, component='sample',
    )

    with mock.patch.object(SingleCellSchemaValidators, 'Sample') as sample, \
            mock.patch.object(
                SingleCellSchemaValidators, 'checkOntologyTerm', side_effect=UNAVAILABLE
            ) as check_ontology, \
            mock.patch.object(SingleCellSchemaValidators, 'checkNCBITaxonTerm') as check_taxon:
        sample.return_value.get_all_records_columns.return_value = []
        errors, _, flag, _ = validator.validate()

    assert check_ontology.call_count == 1
    check_taxon.assert_not_called()
    assert flag is False
    assert len(errors) == 1
    assert 'Ontology Lookup Service (OLS) could not be reached' in errors[0]
