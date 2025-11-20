COMPONENT_NAME = 'seqannotation'


def get_tour_config():
    return {
        'order': [
            'component_table',
            'component_legend',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'add_record_button',
            'edit_record_button',
            'delete_record_button',
            'submit_record_button',
            'profile_title',
            'new_component_title_button',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'message_overrides': {
            'component_legend': {
                'title': 'Data submission status legend',
                'content': (
                    'This legend explains the meaning of different colours that highlight the rows in the table.<br><br>'
                    'Hover over each <i class="fa fa-info-circle"></i> for detailed information.'
                ),
                'placement': 'left',
            },
            'new_component_title_button': {
                'title': 'Add sequence annotation',
                'content': 'Use this button to open a form to add sequence annotations.',
            },
        },
        'stages': {
            'overview': [
                'getting_started',
                'profile_title',
                'new_component_title_button',
                'quick_tour_title_button',
            ],
            'creation': [
                'component_table',
                'component_legend',
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
        },
    }
