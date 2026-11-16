'''Hooks that let a profile type change how shared pages and tasks behave.

Shared code (e.g. the Samples page and the broker) calls the hooks of a
profile's type rather than checking for particular profile types itself:

    hooks = get_profile_type_hooks(profile.get('type'))
    result = hooks.check_sample_submission(profile, sample_ids)

`ProfileTypeHooks` holds the default behaviour which changes nothing. A
profile type that needs something different subclasses it, overrides only the
hooks it needs and is listed in `PROFILE_TYPE_HOOKS`.

The classes are listed by dotted path, like the profile type's pre-save and
post-save actions, so that this module doesn't import the apps that extend it.
'''

from django.utils.module_loading import import_string

SUCCESS = {'status': 'success', 'message': ''}

# Profile types whose hooks differ from the defaults
PROFILE_TYPE_HOOKS = {
    'ei_edp': 'src.apps.ei_edp.utils.profile_type_hooks.EDPProfileTypeHooks',
}


class ProfileTypeHooks:
    '''The default hooks, used by profile types not in `PROFILE_TYPE_HOOKS`.

    `request_data` is the request's GET or POST data (a dict-like object).
    Checks return `{'status': 'success' | 'warning' | 'error', 'message': ...}`.
    '''

    # --- Work profiles page ---

    def can_create_profile(self, user):
        '''Checks whether users can create profiles for the profile type. The profile type is
        only offered to users who can. Users that a profile is shared with
        can only work on it.'''
        return True

    def get_profile_card_fields(self):
        '''Profile fields that `get_profile_card` reads.

        The Work profiles page only loads the fields that every card shows so,
        any others that a profile type's card needs have to be listed here.
        '''
        return []

    def get_profile_card(self, profile):
        '''Extra details for the profile's card on the Work profiles page.

        Returns a dict with:
        - `details`: rows added to the card's 'View more details' popover,
          each `{'label', 'value', 'id' (optional), 'url' (optional)}`
        - `data_attributes`: `data-*` attributes added to the card (without the
          'data-' prefix) e.g. for the card's action buttons to read
        '''
        return {'details': [], 'data_attributes': {}}

    # --- Samples page ---

    def get_sample_page_context(self, profile):
        '''Extra template context for the Samples page.

        Known keys:
        - `manifest_group`: a dropdown menu that chooses what a sample manifest
          is for. `{'param': <request parameter>, 'label': ..., 'options':
          [{'value', 'text', 'is_default'}]}`. The chosen value is sent with
          the blank manifest download, its check and the manifest upload.
        - `use_profile_blank_manifest`: True if the blank sample manifest is
          generated for the profile (see `get_blank_sample_manifest_row_limit`)
          rather than downloaded as the checklist's standard template.
        '''
        return {}

    def check_blank_sample_manifest(self, profile, request_data):
        # Whether a blank sample manifest can be downloaded
        return SUCCESS

    def get_blank_sample_manifest_row_limit(self, profile, request_data):
        '''How many rows of a blank sample manifest can be filled in.

        Returns `(row_count, error_message)`. A `row_count` of `None`
        means no limit. A non-empty `error_message` stops the download.
        '''
        return None, ''

    def prepare_sample_upload(self, profile, request_data):
        '''Get ready to validate an uploaded sample manifest.

        Returns a dict with:
        - `validators`: extra validator classes for the manifest
        - `new_sample_fields`: fields to set on the samples it creates
        - `error`: a message that stops the upload, if not empty
        '''
        return {'validators': [], 'new_sample_fields': {}, 'error': ''}

    def check_sample_submission(self, profile, sample_ids):
        # Whether the selected samples can be submitted
        return SUCCESS


def get_profile_type_hooks(profile_type):
    hooks_class_path = PROFILE_TYPE_HOOKS.get(str(profile_type or '').lower().strip())
    hooks_class = import_string(hooks_class_path) if hooks_class_path else ProfileTypeHooks
    return hooks_class()


def get_all_profile_card_fields():
    '''The profile fields that the cards of any profile type read.

    The Work profiles page loads profiles of several types in one query, before
    their types are known so, it loads the fields that any type's card needs.
    '''
    hooks = [ProfileTypeHooks()] + [
        get_profile_type_hooks(profile_type) for profile_type in PROFILE_TYPE_HOOKS
    ]
    return sorted({field for hook in hooks for field in hook.get_profile_card_fields()})
