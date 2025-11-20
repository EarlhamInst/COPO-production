COMPONENT_NAME = 'sample'


def get_tour_config():
    return {
        'order': [
            'component_table',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'download_manifest_record_button',
            'download_permits_button',
            'view_images_button',
            'profile_title',
            'accept_reject_samples_title_button',
            'download_blank_manifest_title_button',
            'download_sop_title_button',
            'new_samples_button',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'messages': {
            'new_samples_button': {
                'title': 'Add (or update) samples',
                'content': (
                    'Use this button to upload a sample spreadsheet to <b>add new samples</b> or <b>update existing ones</b>.<br><br>'
                    'The system automatically detects and processes new versus existing samples.<br><br>'
                    '<p class="shepherd-note">New samples must be on a separate spreadsheet. The terms <i>manifest</i> and '
                    '<i>spreadsheet</i> are often used interchangeably.</p>'
                ),
            },
        },
        'message_overrides': {
            'component_table': {
                'title': 'Uploaded samples',
                'content': (
                    'View and manage the uploaded samples uploaded in this data table.<br><br>'
                    'The sample manager is notified of new or updated submissions and their review decisions '
                    'will appear in the <strong>Status</strong> column and the corresponding date will appear in the '
                    '<strong>Approval Date</strong> column.<br><br>'
                    'Any errors encountered during processing will be listed in the <strong>Errors</strong> column.<br><br>'
                    '<p class="shepherd-note">The <strong>Biosample Accession</strong> column will display the primary '
                    'identifier for the samples once they are approved with additional accessions (such as '
                    '<strong>SRA Accession</strong> and <strong>Submission Accession</strong>) displayed in '
                    'their respective columns.</p>'
                ),
                'placement': 'right',
            },
            'download_manifest_record_button': {
                'title': 'Download sample manifest',
                'content': (
                    'Use this button to download a sample spreadsheet with the data you previously uploaded.<br><br>'
                    'Select <strong>one record</strong> in the table first. The download will include all samples linked '
                    "to that record's manifest ID.<br><br>"
                    '<p class="shepherd-note">The terms <i>manifest</i> and <i>spreadsheet</i> are often used interchangeably. '
                    'To check the manifest ID of a record, refer to the <strong>Manifest Identifier</strong> column in the data table.</p>'
                ),
            },
        },
        'stages': {
            'overview': [
                'getting_started',
                'profile_title',
                'accept_reject_samples_title_button',
                'download_blank_manifest_title_button',
                'download_sop_title_button',
                'new_samples_button',
                'quick_tour_title_button',
            ],
            'creation': [
                'component_table',
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
        },
    }
