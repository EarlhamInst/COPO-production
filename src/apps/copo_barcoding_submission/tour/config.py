COMPONENT_NAME = 'taggedseq'


def get_tour_config():
    return {
        'order': [
            'component_table',
            'component_legend',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'download_tagged_seq_manifest_record_button',
            'download_manifest_record_button',
            'delete_record_button',
            'submit_record_button',
            'profile_title',
            'component_options',
            'download_blank_manifest_title_button',
            'new_spreadsheet_title_button',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'message_overrides': {
            'component_legend': {
                'title': 'Data submission status legend',
                'content': (
                    'This legend explains the meaning of different colours that highlight the rows in the table.<br><br>'
                    'Hover over each <i class="fa fa-info-circle"></i> for detailed information.<br><br>'
                    '<div class="shepherd-note">To track the status of your data submissions, refer '
                    'to the <strong>STATUS</strong> column in the table.'
                ),
                'placement': 'left',
            },
        },
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
        },
    }
