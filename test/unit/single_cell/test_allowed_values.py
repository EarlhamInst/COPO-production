'''Unit tests for reading the dropdown menu options from a schema spreadsheet.

The `allowed_values` sheet lists the options for each dropdown menu field.
Options such as 'None' or 'NA' are real answers, so they must not be treated
as missing values and dropped. Only empty cells are left out.
'''

import pandas as pd

from src.apps.copo_single_cell_submission.utils.SingleCellSchemasHandler import (
    SingleCellSchemasHandler,
)

CHECKLIST = 'version_tissue'


def write_schema(path, allowed_values):
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(allowed_values).to_excel(writer, sheet_name='allowed_values', index=False)
        pd.DataFrame({'key': ['sample'], 'name': ['Sample']}).to_excel(
            writer, sheet_name='components', index=False
        )
        pd.DataFrame({'key': [CHECKLIST], 'name': ['Tissue manifest']}).to_excel(
            writer, sheet_name='checklists', index=False
        )
        pd.DataFrame({
            'component_name': ['sample', 'sample'],
            'term_name': ['sample_id', 'preservative'],
            'term_label': ['Sample ID', 'Preservative'],
            'term_type': ['string', 'suggested_enum'],
            'term_regex': [None, None],
            'identifier': [True, False],
            'referenced_component': [None, None],
            CHECKLIST: ['M', 'M'],
        }).to_excel(writer, sheet_name='data', index=False)


def read_enums(tmp_path, allowed_values):
    path = tmp_path / 'schema.xlsx'
    write_schema(path, allowed_values)
    handler = SingleCellSchemasHandler()
    return handler._parseSchemas('TEST_SCHEMA', handler._loadSchemas(path))['enums']


def test_keeps_options_that_pandas_would_treat_as_missing(tmp_path):
    enums = read_enums(tmp_path, {
        'preservative': ['RNAlater', 'EtOH', 'None', None],
        'other_field': ['NA', 'N/A', 'null', 'NULL'],
    })

    assert enums['preservative'] == ['RNAlater', 'EtOH', 'None']
    assert enums['other_field'] == ['NA', 'N/A', 'null', 'NULL']


def test_leaves_out_empty_cells(tmp_path):
    # The shorter column is padded with empty cells in the spreadsheet
    enums = read_enums(tmp_path, {
        'preservative': ['RNAlater', 'EtOH', 'None'],
        'other_field': ['Yes', None, None],
    })

    assert enums['other_field'] == ['Yes']
