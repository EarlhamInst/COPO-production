COMPONENT_NAME = 'profile'


def get_tour_config():
    return {
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
                    '<span class="hover-text" title="European Nucleotide Archive">ENA</span>'
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
    }
