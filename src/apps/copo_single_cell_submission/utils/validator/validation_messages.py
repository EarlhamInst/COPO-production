MESSAGES = {
    'validation_msg_invalid_enum': (
        f'''
        Sheet <strong>{{component}}</strong>: Invalid value <strong>{{value}}</strong>
        in column <strong>{{column}}</strong> at row <strong>{{row}}</strong>.<br>
        Expected one of
        <details class='valid-enum'>
            <summary class='valid-enum-trigger'>{{num_values}} valid values (click to view).</summary>
            <div class='valid-enum-container'>
                <h3 class='valid-enum-title'>Valid values</h3>{{content}}
            </div>
        </details>
        '''
    ),
    'missing_column': (
        "Sheet <strong>{component}</strong>: Mandatory column <strong>{field_name}</strong> is missing."
    ),
    'missing_value': (
        "Sheet <strong>{component}</strong>: Missing data in column <strong>{column_name}</strong> at row <strong>{line_no}</strong>."
    ),
    'invalid_column_value_with_list': (
        "Sheet <strong>{component}</strong>: Invalid value <strong>{invalid_value}</strong> "
        "in column <strong>{column_name}</strong> at row <strong>{line_no}</strong>.<br>"
        "Expected one of:{valid_values}"
    ),
    'invalid_column_value_regex': (
        "Sheet <strong>{component}</strong>: Invalid value <strong>{invalid_value}</strong> "
        "in column <strong>{column_name}</strong> at row <strong>{line_no}</strong>.<br>"
        "{field_description}"
    ),
    'invalid_column_value_ontology': (
        "Sheet <strong>{component}</strong>: Invalid value <strong>{invalid_value}</strong> "
        "in column <strong>{column_name}</strong> at row <strong>{line_no}</strong>.<br>"
        "Expected a valid <strong>{ontology_name}</strong> term."
    ),
    'mismatched_value': (
        "Sheet <strong>{component}</strong>: Value <strong>{invalid_value}</strong> "
        "in column <strong>{column_name}</strong> at row <strong>{line_no}</strong> "
        "does not match the expected value for biosample accession <strong>{biosampleAccession}</strong>."
    ),
    'biosampleAccession_validation_exception': (
        "Sheet <strong>{component}</strong>: Could not validate biosample accession "
        "<strong>{biosampleAccession}</strong> in column <strong>{column_name}</strong> "
        "at row <strong>{line_no}</strong>. Invalid ENA sample."
    ),
    'invalid_column_value_generic': (
        "Sheet <strong>{component}</strong>: Invalid value <strong>{invalid_value}</strong> "
        "in column <strong>{column_name}</strong> at row <strong>{line_no}</strong>. "
        "Expected {expected_value}."
    ),
    'identifier_column_not_unique': (
        "Sheet <strong>{component}</strong>: Column <strong>{column_name}</strong> must be unique — "
        "value <strong>{invalid_value}</strong> appears more than once."
    ),
    'invalid_column': (
        "Sheet <strong>{component}</strong>: Invalid column <strong>{column_name}</strong>."
    ),
    'missing_ontology_term': (
        "Sheet <strong>{component}</strong>: Ontology term reference is missing for column "
        "<strong>{column_name}</strong>."
    ),
    'missing_referenced_value': (
        "Sheet <strong>{component}</strong>: Value <strong>{invalid_value}</strong> "
        "in column <strong>{column_name}</strong> at row <strong>{line_no}</strong> "
        "is not found in the referenced sheet <strong>{referenced_component}</strong> "
        "(column <strong>{reference_column_name}</strong>)."
    ),
    'incorrect_manifest': (
        "The uploaded manifest does not match the expected format. "
        "Please check that you have selected the correct manifest type."
    ),
}
