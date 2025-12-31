MESSAGES = {
    'invalid_column_value_with_list': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        '<details><summary>Expected values</summary>{valid_values}</details>'),

    'invalid_column_value_generic': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected {expected_value}'),
                            
    'invalid_column_value_ontology': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected value from ontology <strong>{ontology_name}</strong>'),

    'mismatched_value': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'It does not match with the Taxon ID of your biosample <strong>{biosampeAccession}</strong>',),
         
    'missing_value': ('Missing data detected in column <strong>{column_name}</strong> at row <strong>{row}</strong>.'),

    "biosampleAccession_validation_exception": (
        'Biosample Accession <strong>{biosampleAccession}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong> '
        'could not be validated due to an unexpected error. Please try again later or contact support if the issue persists.'
    ),
}

                                                