MESSAGES = {
    'invalid_column_value_with_list': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        '<details><summary>Expected values</summary>{valid_values}</details>'),

    'invalid_column_value_generic': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected {expected_value}'),

    'invalid_column_value_regex': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected: {field_description}<details><summary>regex pattern</summary>{regex_pattern}</details> ' ),

    'invalid_column_value_ontology': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected value from ontology <strong>{ontology_name}</strong>'),

    'mismatched_value': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'It does not match with the Taxon ID of your biosample <strong>{biosampleAccession}</strong>',),
         
    'missing_value': ('Missing data detected in column <strong>{column_name}</strong> at row <strong>{row}</strong>.'),

    "biosampleAccession_validation_exception": (
        'Biosample Accession <strong>{biosampleAccession}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong> '
        'could not be validated due to an unexpected error. Please try again later or contact support if the issue persists.'),

    'form_validation_error': "Some information is missing or incorrect. Please check the top of the form for a summary of errors.",

    'duplicate_file_error': "File <strong>{file_name}</strong> has been specified multiple times in the form. Each file should be unique.",
}

                                                
