COMPONENT_NAME = 'general_sample'


def get_tour_config():
    return {
        'order': [
            'component_table',
            'profile_title',
            'component_options',
            'download_blank_manifest_title_button',
            'new_spreadsheet_title_button',
            'quick_tour_title_button',
            'download_manifest_record_button',
            'delete_record_button',
            'submit_general_sample_ena_button',
            'profile_component_icon_navigation_pane',
        ],
        'messages': {
            'download_general_sample_manifest_button': {
                'title': 'Download sample manifest',
                'content': (
                    'Use this button to download a spreadsheet with the data you previously uploaded.<br><br>'
                    'Select <strong>one record</strong> in the table first. The download will include all samples linked '
                    "to that record's manifest ID.<br><br>"
                    '<p class="shepherd-note">The terms <i>manifest</i> and <i>spreadsheet</i> are often used interchangeably.'
                ),
            },
            'submit_general_sample_ena_button': {
                'title': 'Submit samples to ENA',
                'content': (
                    'Click this button to submit samples to European Nucleotide Archive (ENA), '
                    'a public repository.<br><br>'
                    'Select <strong>at least one record</strong> in the table first. The submission will include '
                    'all selected sample records.<br><br>'
                    '<p class="shepherd-note"> A public repository is a database that stores and shares '
                    'research data with the global scientific community.</p>'
                ),
            },
        },
        'message_overrides': {
            'component_table': {
                'title': 'Uploaded samples',
                'content': 'View and manage the samples uploaded in this data table.',
                'placement': 'right',
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
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
        },
    }
