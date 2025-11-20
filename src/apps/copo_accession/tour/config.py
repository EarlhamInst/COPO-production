COMPONENT_NAME = 'accessions'


def get_tour_config():
    return {
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
    }
