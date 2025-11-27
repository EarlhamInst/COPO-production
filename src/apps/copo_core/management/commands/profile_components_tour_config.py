# This dictionary includes the tour configuration for profile components,
# including their order, messages, stages and any overridden messages.
COMPONENTS_TOUR_CONFIG = {
    'accessions': {
        'order': [
            'component_table',
            'export_csv_button',
            'sample_accessions_tab',
            'other_accessions_tab',
            'component_legend',
            'profile_title',
            'accept_reject_samples_title_button',
            'accession_dashboard_title_button',
            'tol_inspect_title_button',
            'tol_inspect_gal_title_button',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'messages': {
            'accession_dashboard_title_button': {
                'title': 'Accessions dashboard',
                'content': (
                    'This button will grant you access to the Accessions dashboard where you can view the '
                    'accessions assigned for all types of submissions.<br><br>'
                ),
            },
            'other_accessions_tab': {
                'title': 'Other accessions tab',
                'content': (
                    'This tab relates to the following types of accessions. '
                    'They are assigned automatically upon submission.<br><br>'
                    '<ol><li>Assembly</li><li>Experiment</li><li>Project</li>'
                    '<li>Run</li><li>Sequence annotation</li>'
                    '</ol>'
                    '<p class="shepherd-note">Accessions are unique identifiers assigned to submissions to '
                    'track their publication in public repositories.</p>'
                ),
            },
            'sample_accessions_tab': {
                'title': 'Sample accessions tab',
                'content': (
                    'This displays accessions assigned to submitted samples.<br><br>'
                    'Accessions are assigned when samples are approved by a sample manager '
                    '(for Tree of Life samples) or automatically upon submission '
                    'for all other types.<br><br>'
                    '<p class="shepherd-note">Accessions are unique identifiers assigned to submissions to '
                    'track their publication in public repositories.</p>'
                ),
            },
            'tol_inspect_title_button': {
                'title': 'Inspect Tree of Life data',
                'content': (
                    'Click this button to access the Tree of Life data inspection page, where you can review '
                    'and validate your submissions before final submission.<br><br>'
                    'This feature helps ensure that your data meets the required standards.<br><br>'
                    '<p class="shepherd-note">The Tree of Life project aims to sequence and assemble the genomes of all eukaryotic life on Earth.</p>'
                ),
            },
            'tol_inspect_gal_title_button': {
                'title': 'Inspect Tree of Life data by GAL',
                'content': (
                    'Click this button to access the Tree of Life data inspection page, where you can review '
                    'and validate your submissions based on Genome Acquisition Lab (GAL) identifiers.<br><br>'
                    'This feature helps ensure that your data meets the required standards before final submission.<br><br>'
                    '<p class="shepherd-note">GAL identifiers are unique codes assigned to genome assemblies '
                    'within the Tree of Life project.</p>'
                ),
            },
        },
        'message_overrides': {
            'component_legend': {
                'title': 'Accessions legend',
                'content': (
                    'This legend explains the different statuses and symbols used in the Accessions Dashboard.<br><br>'
                    'Refer to this legend to understand the meaning of various indicators related to your sample accessions.<br><br>'
                    '<p class="shepherd-note">Understanding the legend will help you navigate and interpret the information presented in the Accessions Dashboard effectively.</p>'
                ),
                'placement': 'left',
            },
            'component_table': {
                'title': 'Accessions',
                'content': (
                    'View submissions as well as their accessions made in this data table.<br><br>'
                    '<p class="shepherd-note">You can switch between viewing sample accessions and other types of accessions using the tabs above the table.</p>'
                ),
                'placement': 'right',
            },
        },
        'stages': {
            'overview': [
                'getting_started',
                'profile_title',
                'accept_reject_samples_title_button',
                'accession_dashboard_title_button',
                'tol_inspect_title_button',
                'tol_inspect_gal_title_button',
                'quick_tour_title_button',
            ],
        },
    },
    'assembly': {
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
        'messages': {
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>SUBMISSION ERROR</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>STUDY</strong></li>'
                    '<li><strong>SAMPLE</strong></li>'
                    '<li><strong>RUN REF</strong></li>'
                    '<li><strong>ACCESSION</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
                'placement': 'right',
            },
        },
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
                'title': 'Add assembly',
                'content': 'Use this button to open a form to add an assembly.',
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
            'release': [
                'component_table_with_accessions',
                'release_profile',
            ],
        },
    },
    'files': {
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
    },
    'general_sample': {
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
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>STATUS</strong></li>'
                    '<li><strong>ERROR</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>SRA ACCESSION</strong></li>'
                    '<li><strong>BIOSAMPLE ACCESSION</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
                'placement': 'right',
            },
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
            'release': [
                'component_table_with_accessions',
                'release_profile',
            ],
        },
    },
    'profile': {
        'order': [
            'component_options',
            'new_component_title_button',
            'quick_tour_title_button',
            'profile_grid',
            'profile_addtl_info_button',
            'profile_component_buttons_menu',
            'profile_options_icon',
            'sort_profiles_button',
            'component_legend',
        ],
        'messages': {
            'profile_addtl_info_button': {
                'title': 'Profile details',
                'content': 'Click this button to view additional details about a profile such as profile owner name, associated profile type, sequencing centre and study status.',
            },
            'profile_component_buttons_menu': {
                'title': 'Profile components',
                'content': (
                    'Components represent different research objects that form part of a project or study.<br><br>'
                    "Click any of the components (e.g. Manage Sample metadata) to access a particular component's page."
                ),
                'placement': 'right',
            },
            'profile_grid': {
                'title': 'Work profiles',
                'content': (
                    'This area displays your profiles after they have been created.<br><br>'
                    'If no profiles exist, a <i>Getting started</i> overview of this page is shown instead.'
                ),
                'placement': 'right',
            },
            'profile_options_icon': {
                'title': 'Profile options',
                'content': (
                    'Click to view options to:<br>'
                    '<ul><li>Edit profiles</li>'
                    '<li>Delete profiles</li>'
                    '<li>Release studies (also known as projects or profiles) '
                    'to make the metadata publicly accessible in repositories like '
                    '<span class="hover-text" title="European Nucleotide Archive">ENA</span><br><br>'
                    '<span class="shepherd-note">Read more about '
                    '<a href="https://copo-docs.readthedocs.io/en/latest/profile/releasing-profiles.html" '
                    'target="_blank">Releasing Profiles (Studies)</a>.</span>'
                    '</li></ul>'
                ),
            },
            'sort_profiles_button': {
                'title': 'Sort profiles',
                'content': (
                    'Use this dropdown to sort the displayed profiles in ascending or descending '
                    'order based on different criteria such as date created, profile title or type.<br><br>'
                    'Sorting helps you organise and locate profiles more efficiently.'
                ),
            },
        },
        'message_overrides': {
            'component_legend': {
                'title': 'Profile types legend',
                'content': (
                    'This legend attributes the different profile types that  '
                    'you have created to their corresponding colours.<br><br>'
                    'Hover over each <i class="fa fa-info-circle"></i> to '
                    'view the full name of the profile.'
                ),
                'placement': 'left',
            },
            'new_component_title_button': {
                'title': 'Create a profile',
                'content': (
                    "A profile is a collection of 'research objects' or components.<br><br>"
                    'Use this button to open a form to create a profile, providing details '
                    'such as name and description.'
                ),
            },
            'component_options': {
                'title': 'Profile type options',
                'content': "Select a profile type from this dropdown to begin creating your project's profile.",
            },
        },
        'stages': {
            'overview': [
                'getting_started',
                'component_options',
                'new_component_title_button',
                'quick_tour_title_button',
            ],
            'creation': [
                'profile_addtl_info_button',
                'profile_component_buttons_menu',
                'profile_options_icon',
                'profile_grid',
                'component_legend',
                'quick_tour_title_button',
            ],
        },
    },
    'read': {
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
        'messages': {
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>ENA FILE UPLOAD STATUS</strong></li>'
                    '<li><strong>STATUS</strong></li>'
                    '<li><strong>ENA FILE PROCESSING STATUS</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>RUN ACCESSION</strong></li>'
                    '<li><strong>EXPERIMENT ACCESSION</strong></li>'
                    '<li><strong>STUDY ACCESSION</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
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
                'component_legend',
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
            'release': [
                'component_table_with_accessions',
                'release_profile',
            ],
        },
    },
    'reads_schema': {
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
        'messages': {
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>ENA FILE UPLOAD STATUS</strong></li>'
                    '<li><strong>STATUS</strong></li>'
                    '<li><strong>ENA FILE PROCESSING STATUS</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>RUN ACCESSION</strong></li>'
                    '<li><strong>EXPERIMENT ACCESSION</strong></li>'
                    '<li><strong>STUDY ACCESSION</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
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
                'component_legend',
                'profile_component_icon_navigation_pane',
                'quick_tour_title_button',
            ],
            'release': [
                'component_table_with_accessions',
                'release_profile',
            ],
        },
    },
    'rembi': {
        'order': [
            'component_table',
            'component_legend',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'download_manifest_record_button',
            'delete_record_button',
            'submit_record_button',
            'publish_record_button',
            'submit_record_button_zenodo',
            'publish_record_button_zenodo',
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
        },
    },
    'sample': {
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
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>Status</strong></li>'
                    '<li><strong>Approval Date</strong></li>'
                    '<li><strong>Error</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>Biosample Accession</strong></li>'
                    '<li><strong>SRA Accession</strong></li>'
                    '<li><strong>Submission Accession</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
                'placement': 'right',
            },
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
            'release': [
                'component_table_with_accessions',
            ],
        },
    },
    'seqannotation': {
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
        'messages': {
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>SUBMISSION ERROR</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>STUDY</strong></li>'
                    '<li><strong>SAMPLE</strong></li>'
                    '<li><strong>RUN</strong></li>'
                    '<li><strong>EXPERIMENT</strong></li>'
                    '<li><strong>ACCESSION</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
                'placement': 'right',
            },
        },
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
            'release': [
                'component_table_with_accessions',
                'release_profile',
            ],
        },
    },
    'singlecell': {
        'order': [
            'component_table',
            'component_legend',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'download_manifest_record_button',
            'delete_record_button',
            'submit_record_button',
            'publish_record_button',
            'submit_record_button_zenodo',
            'publish_record_button_zenodo',
            'profile_title',
            'component_options',
            'download_blank_manifest_title_button',
            'new_spreadsheet_title_button',
            'quick_tour_title_button',
            'profile_component_icon_navigation_pane',
        ],
        'messages': {
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>Status For Ena</strong></li>'
                    '<li><strong>Status For Zenodo</strong></li>'
                    '<li><strong>Error For Ena</strong></li>'
                    '<li><strong>Error For Zenodo</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>Accession For Ena</strong></li>'
                    '<li><strong>Accession For Zenodo</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
                'placement': 'top',
            },
            'publish_study': {
                'title': 'Publish study',
                'content': (
                    'After submitting the data, you can publish the study to any of the following '
                    'public repositories to make it publicly accessible:<br>'
                    '<ul><li><strong>European Nucleotide Archive (ENA)</strong> using '
                    '<button class="tiny ui basic teal button publish-btn no-click">'
                    '<i class="fa fa-info-circle"></i>&nbsp;Publish to ENA</button></li>'
                    '<li><strong>Zenodo</strong> using '
                    '<button class="tiny ui basic blue button publish-btn no-click">'
                    '<i class="fa fa-info-circle"></i>&nbsp;Publish to Zenodo</button></li>'
                    '</ul><br>'
                    'Select <strong>one record</strong> under the <strong>STUDY</strong> tab in the data table '
                    'then, click any of the buttons to publish the study. '
                    'The publication will include all data related to the selected record matching the study '
                    'ID in the <strong>Study ID</strong> column.<br><br>'
                    '<p class="shepherd-note">Publishing your study is an important step in sharing your '
                    'research with the scientific community. A public repository is a database that stores and shares '
                    'research data with the global scientific community.</p>'
                ),
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
            'publish': ['component_table_with_accessions', 'publish_study'],
        },
    },
    'stx_fish': {
        'order': [
            'component_table',
            'component_legend',
            'select_all_button',
            'select_filtered_button',
            'clear_selection_button',
            'export_csv_button',
            'download_manifest_record_button',
            'delete_record_button',
            'submit_record_button',
            'publish_record_button',
            'submit_record_button_zenodo',
            'publish_record_button_zenodo',
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
        },
    },
    'taggedseq': {
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
        'messsages': {
            'component_table_with_accessions': {
                'title': 'Submitted data with accessions',
                'content': (
                    'View the submission status and the accessions assigned to the submissions in the data table.<br><br>'
                    'To view the <strong>status</strong> of the submissions, refer to any of the following columns:'
                    '<ul><li><strong>STATUS</strong></li>'
                    '<li><strong>ERROR</strong></li>'
                    '</ul><br>'
                    'To view the <strong>accessions</strong>, refer to any of the following columns:'
                    '<ul><li><strong>ACCESSION</strong></li>'
                    '</ul>'
                    '<p class="shepherd-note">An accession is a unique identifier assigned to the submitted data. '
                    'The accessions can be used to reference the submitted data in public repositories.</p>'
                ),
                'placement': 'right',
            },
        },
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
            'release': [
                'component_table_with_accessions',
                'release_profile',
            ],
        },
    },
}
