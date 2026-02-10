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
}
