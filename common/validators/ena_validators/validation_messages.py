MESSAGES = {
    "biosampleAccession_validation_exception": (
        'Biosample Accession <strong>{biosampleAccession}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong> '
        'could not be validated due to an unexpected error. Please try again later or contact support if the issue persists.'),
        
    'duplicate_file_error': "File <strong>{file_name}</strong> has been specified multiple times in the form. Each file should be unique.",
    
    'form_validation_error': "Some information is missing or incorrect. Please check the top of the form for a summary of errors.",
     
    'invalid_column_value_with_list': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        '<details><summary>Expected values</summary>{valid_values}</details>'),

    'invalid_column_value_generic': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected {expected_value}'),

    'invalid_column_value_regex': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected: {field_description}<details><summary>regex pattern</summary>{regex_pattern}</details>'),

    'invalid_column_value_ontology': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'Expected value from ontology <strong><a href="{ontology_link}" target="_blank">{ontology_name}</a></strong>'),
    
    'invalid_compressed_file': '<strong>{file_name}</strong> is not a valid {file_extension} compressed file',
    
    'invalid_file_extension': (
        'Invalid file <strong>{file_name}</strong>.<br>'
        '<details><summary>Expected file extensions</summary>{valid_exts}</details>'),

    'ontology_lookup_unavailable': (
        'The values in column <strong>{column_name}</strong> could not be checked against ontology '
        '<strong>{ontology_name}</strong> because the Ontology Lookup Service (OLS) could not be reached. '
        'Please try uploading the manifest again later.'),

    'mismatched_value': (
        'Invalid value <strong>{invalid_value}</strong> in column <strong>{column_name}</strong> at row <strong>{row}</strong>.<br>'
        'It does not match with the Taxon ID of your biosample <strong>{biosampleAccession}</strong>'),
    
    'missing_annotation_file': (
        'Annotation file <strong>{file_name}</strong> could not be found for submission. '
        'Please upload it to COPO and try again.'),
    'missing_assembly_file': (
        'Assembly file <strong>{file_name}</strong> could not be found for submission. '
        'Please upload it to COPO and try again.'),
            
    'missing_value': 'Missing data detected in column <strong>{column_name}</strong> at row <strong>{row}</strong>.',
    
    'file_type_extension_mismatch': (
        'File <strong>{file_name}</strong> does not match its file type <strong>{file_type}</strong>.<br>'
        '<details><summary>Expected file extensions for {file_type}</summary>{valid_exts}</details>'),

    'unsupported_sequence_annotation_file_type': (
        'Sequence annotation file type <strong>{file_type}</strong> of file <strong>{file_name}</strong> is not accepted by ENA.<br>'
        '<details><summary>Accepted file types</summary>{valid_types}</details>'),

    'unsupported_assembly_file_type': (
        'Assembly file type <strong>{file_type}</strong> of file <strong>{file_name}</strong> is not accepted by ENA.<br>'
        '<details><summary>Accepted file types</summary>{valid_types}</details>'),
}

                                                
