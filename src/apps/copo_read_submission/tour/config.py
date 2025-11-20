COMPONENT_NAME = 'read'


def get_tour_config():
    return {
        'order': [
            'component_table',
            'component_legend',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'delete_record_button',
            'submit_record_button',
            'profile_title',
            'component_options',
            'download_blank_manifest_title_button',
            'new_spreadsheet_title_button',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'stages': {
            'overview': [
                'getting_started',
                'profile_title',
                'component_options',
                'download_blank_manifest_title_button',
                'new_spreadsheet_title_button',
                'quick_tour_title_button',
            ],
            'creation': [
                'component_table',
                'component_legend',
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
            'release': [
                'release_profile',
            ],
        },
    }
