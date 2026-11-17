'''Unit tests for the hooks that let EDP profiles change shared pages and tasks.

Shared code asks a profile type's hooks (`get_profile_type_hooks`) and a
schema's handler (`get_schema_class`) rather than checking for EDP itself.
The defaults change nothing. Under EDP profiles, a manifest is for one of the
profile's batches of samples.
'''

from unittest import mock

import pytest

from src.apps.copo_core.profile_type_hooks import (
    ProfileTypeHooks,
    get_all_profile_card_fields,
    get_profile_type_hooks,
)
from src.apps.copo_single_cell_submission.utils.single_cell_utils import (
    SchemaClassType,
    get_schema_class,
)
from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES
from src.apps.ei_edp.utils.profile_type_hooks import EDPProfileTypeHooks

SUCCESS = {'status': 'success', 'message': ''}
BATCHES = [
    {'batch_id': 'b1', 'no_of_samples': 2, 'sample_type': 'Tissue', 'container_type': 'Plate',
     'library_type': '', 'lims_sample_record_ids': []},
    {'batch_id': 'b2', 'no_of_samples': 3, 'sample_type': 'Total RNA', 'container_type': 'Tube',
     'library_type': '', 'lims_sample_record_ids': []},
]
PROFILE = {'_id': 'p1', 'type': 'ei_edp', 'sample_batches': BATCHES}


@pytest.mark.parametrize('profile_type, hooks_class', [
    ('ei_edp', EDPProfileTypeHooks),
    (' EI_EDP ', EDPProfileTypeHooks),
    ('genomics', ProfileTypeHooks),
    ('', ProfileTypeHooks),
    (None, ProfileTypeHooks),
])
def test_registry(profile_type, hooks_class):
    assert type(get_profile_type_hooks(profile_type)) is hooks_class


def test_default_hooks_change_nothing():
    hooks = ProfileTypeHooks()
    profile = {'_id': 'p1', 'type': 'genomics'}

    assert hooks.can_create_profile(mock.Mock()) is True
    assert hooks.get_profile_card_fields() == []
    assert hooks.get_profile_card(profile) == {'details': [], 'data_attributes': {}}
    assert hooks.get_sample_page_context(profile) == {}
    assert hooks.check_blank_sample_manifest(profile, {}) == SUCCESS
    assert hooks.get_blank_sample_manifest_row_limit(profile, {}) == (None, '')
    assert hooks.prepare_sample_upload(profile, {}) == {
        'validators': [], 'new_sample_fields': {}, 'error': ''
    }
    assert hooks.check_sample_submission(profile, ['s1']) == SUCCESS


def test_default_schema_handler_changes_nothing():
    handler = get_schema_class('COPO_SINGLE_CELL', SchemaClassType.HANDLER)()
    profile = {'_id': 'p1', 'sample_type': 'Tissue'}

    assert handler.get_checklist_filter_values(profile) == [profile]
    assert handler.get_page_context(profile, [(profile, ['c1'])]) == {}
    assert handler.check_blank_manifest(profile, {}) == SUCCESS
    assert handler.check_manifest_upload(profile, {}) == SUCCESS


# --- EDP: a sample manifest is for one of the profile's batches ---

def test_sample_upload_needs_a_batch_when_there_are_several():
    upload = EDPProfileTypeHooks().prepare_sample_upload(PROFILE, {})

    assert upload == {'validators': [], 'new_sample_fields': {}, 'error': MESSAGES['select_sample_batch']}


def test_sample_upload_is_checked_against_and_tagged_with_its_batch():
    upload = EDPProfileTypeHooks().prepare_sample_upload(PROFILE, {'batch_id': 'b2'})

    assert upload['error'] == ''
    assert upload['new_sample_fields'] == {'edp_batch_id': 'b2'}
    [validator] = upload['validators']
    assert validator.batch_id == 'b2'


def test_blank_sample_manifest_rows_are_limited_to_the_batchs_samples_left():
    with mock.patch.object(edp_utils, 'get_samples_left_to_upload', return_value=2) as samples_left:
        result = EDPProfileTypeHooks().get_blank_sample_manifest_row_limit(PROFILE, {'batch_id': 'b2'})

    assert result == (2, '')
    samples_left.assert_called_once_with(PROFILE, 'b2')


def test_samples_page_starts_with_the_first_batch_with_samples_left():
    described_batches = [
        {'batch_id': 'b1', 'name': 'Batch 1', 'samples_left': 0},
        {'batch_id': 'b2', 'name': 'Batch 2', 'samples_left': 3},
    ]
    with mock.patch.object(edp_utils, 'describe_upload_batches', return_value=described_batches):
        context = EDPProfileTypeHooks().get_sample_page_context(PROFILE)

    assert context['use_profile_blank_manifest'] is True
    group = context['manifest_group']
    assert group['param'] == 'batch_id'
    assert [(option['value'], option['text'], option['is_default']) for option in group['options']] == [
        ('b1', 'Batch 1 - 0 left to upload', False),
        ('b2', 'Batch 2 - 3 left to upload', True),
    ]


def test_lims_page_offers_each_batch_its_own_checklists():
    handler = get_schema_class('EI_EDP', SchemaClassType.HANDLER)()
    described_batches = [
        {'batch_id': 'b1', 'name': 'Batch 1', 'samples_left': 0},
        {'batch_id': 'b2', 'name': 'Batch 2', 'samples_left': 3},
    ]

    filter_values = handler.get_checklist_filter_values(PROFILE)
    assert filter_values == [{**batch, 'related_pip': ''} for batch in BATCHES]

    with mock.patch.object(edp_utils, 'describe_upload_batches', return_value=described_batches):
        context = handler.get_page_context(
            PROFILE, [(BATCHES[0], ['tissue']), (BATCHES[1], ['rna', 'rna_v2'])]
        )

    assert [(option['value'], option['checklist_ids']) for option in context['manifest_group']['options']] == [
        ('b1', 'tissue'),
        ('b2', 'rna,rna_v2'),
    ]


def test_lims_blank_manifest_check_passes_the_chosen_batch():
    handler = get_schema_class('EI_EDP', SchemaClassType.HANDLER)()
    with mock.patch.object(edp_utils, 'check_edp_samples_submitted', return_value=SUCCESS), \
            mock.patch.object(edp_utils, 'check_lims_blank_manifest', return_value=SUCCESS) as check:
        assert handler.check_blank_manifest(PROFILE, {'batch_id': 'b2'}) == SUCCESS

    check.assert_called_once_with(profile=PROFILE, batch_id='b2')


# --- EDP: the profile's card on the Work profiles page ---

def test_card_shows_the_sapio_project_links_each_batchs_jira_pip_and_says_whether_samples_are_registered():
    batches = [{**BATCHES[0], 'related_pip': 'PIP-12'}, {**BATCHES[1], 'related_pip': 'PIP-13'}]
    card = EDPProfileTypeHooks().get_profile_card(
        {**PROFILE, 'id': 'p1', 'sample_batches': batches, 'sapio_project_id': 'PR0001'}
    )

    assert card['details'] == [
        {'label': 'Sapio Project ID', 'value': 'PR0001', 'id': 'sapio_project_id_p1'},
        {
            'label': 'Batch 1 Jira PIP',
            'value': 'PIP-12',
            'id': 'related_pip_p1_1',
            'url': 'https://earlham-institute.atlassian.net/browse/PIP-12',
        },
        {
            'label': 'Batch 2 Jira PIP',
            'value': 'PIP-13',
            'id': 'related_pip_p1_2',
            'url': 'https://earlham-institute.atlassian.net/browse/PIP-13',
        },
    ]
    assert card['data_attributes'] == {'has-sample-batches': 'true'}


def test_card_of_a_profile_with_one_batch_says_jira_pip():
    card = EDPProfileTypeHooks().get_profile_card(
        {'id': 'p1', 'type': 'ei_edp', 'sample_batches': [{**BATCHES[0], 'related_pip': 'PIP-12'}]}
    )

    assert [(detail['label'], detail['value']) for detail in card['details']] == [('Jira PIP', 'PIP-12')]


def test_card_reads_only_fields_that_the_profiles_page_loads():
    # The Work profiles page only loads the fields that cards ask for so, a
    # card built from just those fields must match one built from the full profile
    profile = {
        **PROFILE,
        'id': 'p1',
        'title': 'Profile',
        'related_pip': 'PIP-12',
        'sapio_project_id': 'PR0001',
        'related_enq': 'ENQ-1',
        'budget_holder': 'Budget person',
        'user_email': 'user@example.com',
    }
    loaded_fields = {'id', 'type'} | set(get_all_profile_card_fields())
    loaded_profile = {field: value for field, value in profile.items() if field in loaded_fields}

    hooks = EDPProfileTypeHooks()
    assert hooks.get_profile_card(loaded_profile) == hooks.get_profile_card(profile)


def test_card_counts_batch_fields_that_have_not_been_moved_into_a_batch():
    # Profiles made before batches existed keep their batch fields on the profile
    profile = {'id': 'p1', 'type': 'ei_edp', 'no_of_samples': '2', 'sample_type': 'Tissue',
               'container_type': 'Tube'}
    loaded_fields = {'id', 'type'} | set(get_all_profile_card_fields())
    loaded_profile = {field: value for field, value in profile.items() if field in loaded_fields}

    card = EDPProfileTypeHooks().get_profile_card(loaded_profile)

    assert card['data_attributes'] == {'has-sample-batches': 'true'}


def test_card_of_a_profile_without_a_pip_or_batches():
    card = EDPProfileTypeHooks().get_profile_card({'id': 'p1', 'type': 'ei_edp'})

    assert card == {'details': [], 'data_attributes': {'has-sample-batches': 'false'}}


# --- EDP: who can create EDP profiles ---

@pytest.mark.parametrize('is_manager', [True, False])
def test_only_edp_managers_can_create_edp_profiles(is_manager):
    user = mock.Mock()
    with mock.patch.object(edp_utils, 'is_edp_manager', return_value=is_manager) as is_edp_manager:
        assert EDPProfileTypeHooks().can_create_profile(user) is is_manager
    is_edp_manager.assert_called_once_with(user)


def test_profile_types_a_user_cannot_create_are_not_offered():
    from src.apps.copo_core import utils as core_utils

    profile_types = [
        mock.Mock(type='genomics', description='Genomics', is_deprecated=False, is_permission_required=False),
        mock.Mock(type='ei_edp', description='EDP', is_deprecated=False, is_permission_required=True),
    ]
    user = mock.Mock()
    user.groups.filter.return_value.values_list.return_value = ['ei_edp_users']
    with mock.patch.object(core_utils.ProfileType, 'objects') as objects, \
            mock.patch.object(edp_utils, 'is_edp_manager', return_value=False):
        objects.all.return_value = profile_types
        options = core_utils.get_all_profile_types_for_options_for_user(user)

    assert options == [{'value': 'genomics', 'label': 'Genomics'}]


def test_card_shows_the_related_enq_and_budget_holder_but_not_user_emails():
    card = EDPProfileTypeHooks().get_profile_card({
        'id': 'p1',
        'type': 'ei_edp',
        'related_enq': 'ENQ-1',
        'sapio_project_id': 'PR0001',
        'budget_holder': 'Budget person',
        'user_email': 'user@example.com',
        'sample_batches': [{**BATCHES[0], 'related_pip': 'PIP-12'}],
    })

    assert [(detail['label'], detail['value']) for detail in card['details']] == [
        ('Related ENQ', 'ENQ-1'),
        ('Sapio Project ID', 'PR0001'),
        ('Jira PIP', 'PIP-12'),
        ('Budget holder', 'Budget person'),
    ]
