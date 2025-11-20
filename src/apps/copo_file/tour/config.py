COMPONENT_NAME = 'files'


def get_tour_config():
    return {
        'order': [
            'component_table',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'add_file_record_button_local',
            'add_file_record_button_terminal',
            'delete_record_button',
            'new_file_button_local',
            'new_file_button_terminal',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'messages': {
            'add_file_record_button_local': {
                'title': 'Add data files from local system',
                'content': (
                    'Use this button to upload data files to COPO from your local computer system. '
                    'The total maximum upload size is 2 GB.<br><br>'
                    'This button performs the same action as the '
                    '<button class="circular tiny ui icon primary button no-click"><i class="icon desktop"></i>'
                    '</button> button located at the top left of the page. It is provided here for convenience.'
                ),
            },
            'add_file_record_button_terminal': {
                'title': 'Add data files via terminal',
                'content': (
                    'Use this button to upload data files to COPO via the terminal.<br><br>'
                    'This button performs the same action as the '
                    '<button class="circular tiny ui icon primary button no-click"><i class="icon terminal sign"></i>'
                    '</button> button located at the top left of the page. It is provided here for convenience.'
                ),
            },
            'new_file_button_local': {
                'title': 'Add data files from local system',
                'content': (
                    'Use this button to upload data files to COPO from your local computer system. '
                    'The total maximum upload size is 2 GB.<br><br>'
                    'These files may include <i>FASTA</i> files, flat, <i>BAM</i>, <i>CRAM</i> or multi-fastq '
                    'files relevant to the research object you plan to upload.<br><br> '
                    '<p class="shepherd-note"> Refer to <span class="hover-text" '
                    'title="European Nucleotide Archive">ENA</span> documentation for '
                    '<a href="https://ena-docs.readthedocs.io/en/latest/submit/fileprep/assembly.html" '
                    'target="_blank" rel="noopener noreferrer">assembly</a> and '
                    '<a href="https://ena-docs.readthedocs.io/en/latest/submit/fileprep/reads.html#accepted-read-data-formats" '
                    'target="_blank" rel="noopener noreferrer">raw read</a> data file format guidelines.</p>'
                ),
            },
            'new_file_button_terminal': {
                'title': 'Add data files via terminal',
                'content': (
                    'Use this button to upload data files to COPO via the terminal.<br><br>'
                    'These files may include <i>FASTA</i> files, flat, <i>BAM</i>, <i>CRAM</i> or multi-fastq '
                    'files relevant to the research object you plan to upload.<br><br>'
                    '<p class="shepherd-note"> Refer to <span class="hover-text" '
                    'title="European Nucleotide Archive">ENA</span> documentation for '
                    '<a href="https://ena-docs.readthedocs.io/en/latest/submit/fileprep/assembly.html" '
                    'target="_blank" rel="noopener noreferrer">assembly</a> and '
                    '<a href="https://ena-docs.readthedocs.io/en/latest/submit/fileprep/reads.html#accepted-read-data-formats" '
                    'target="_blank" rel="noopener noreferrer">raw read</a> data file format guidelines.</p>'
                ),
            },
        },
        'message_overrides': {
            'component_table': {
                'title': 'Uploaded data files',
                'content': 'This table displays the data files that you have uploaded.',
                'placement': 'right',
            }
        },
        'stages': {
            'overview': [
                'getting_started',
                'new_file_button_local',
                'new_file_button_terminal',
                'quick_tour_title_button',
                'profile_component_icon_navigation_pane',
            ],
            'creation': [
                'component_table',
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
        },
    }
