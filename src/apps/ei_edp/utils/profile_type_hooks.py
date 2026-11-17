'''
Samples are registered under an EDP profile in batches so, a sample manifest
is for one of the profile's batches, chosen on the Samples page, and the
batch's sample count limits how many samples can be uploaded and submitted.
'''

from src.apps.copo_core.profile_type_hooks import ProfileTypeHooks

from . import edp_utils
from .constants import (
    BATCH_FIELDS,
    JIRA_BROWSE_URL,
    PROFILE_BUDGET_HOLDER,
    PROFILE_RELATED_ENQ,
    PROFILE_RELATED_PIP,
    PROFILE_SAMPLE_BATCHES,
    PROFILE_SAPIO_PROJECT_ID,
    SAMPLE_BATCH_ID,
)
from .edp_validation_messages import MESSAGES
from .edp_validators import SampleCountValidator
from .sample_batches import get_sample_batches

# The request parameter that carries the chosen batch
BATCH_PARAM = 'batch_id'


def get_batch_manifest_group(described_batches, label, option_text, default_batch_id=''):
    '''A `manifest_group` dropdown menu for choosing one of the profile's batches.

    `described_batches` comes from `edp_utils.describe_upload_batches` and
    `option_text(batch)` gives each option's text.
    '''
    return {
        'param': BATCH_PARAM,
        'label': label,
        'options': [
            {
                'value': batch['batch_id'],
                'text': option_text(batch),
                'is_default': batch['batch_id'] == default_batch_id,
                **({'checklist_ids': batch['checklist_ids']} if 'checklist_ids' in batch else {}),
            }
            for batch in described_batches
        ],
    }


class EDPProfileTypeHooks(ProfileTypeHooks):

    def can_create_profile(self, user):
        # Only EDP managers assigned to the `ei_edp_managers` Django group can
        # create EDP profiles and share them with users
        return edp_utils.is_edp_manager(user)

    def get_profile_card_fields(self):
        # The sample batch fields are read when a profile's batch fields have not been
        # moved into a batch as yet and the profile's Jira PIP stands in for that
        # of batches saved before each batch had one
        return [
            PROFILE_RELATED_ENQ,
            PROFILE_SAPIO_PROJECT_ID,
            PROFILE_BUDGET_HOLDER,
            PROFILE_RELATED_PIP,
            PROFILE_SAMPLE_BATCHES,
            *BATCH_FIELDS,
        ]

    def get_profile_card(self, profile):
        details = []
        profile_id = profile.get('id', '')
        related_enq = profile.get(PROFILE_RELATED_ENQ)
        if related_enq:
            details.append(
                {'label': 'Related ENQ', 'value': related_enq, 'id': f'related_enq_{profile_id}'}
            )
            
        sapio_project_id = profile.get(PROFILE_SAPIO_PROJECT_ID)
        if sapio_project_id:
            details.append(
                {
                    'label': 'Sapio Project ID',
                    'value': sapio_project_id,
                    'id': f"sapio_project_id_{profile.get('id', '')}",
                }
            )
        batches = get_sample_batches(profile)
        for number, batch in enumerate(batches, start=1):
            related_pip = batch.get(PROFILE_RELATED_PIP)
            if not related_pip:
                continue
            # Each batch's PIP is shown as a link to the PIP in Jira
            details.append(
                {
                    'label': f'Batch {number} Jira PIP' if len(batches) > 1 else 'Jira PIP',
                    'value': related_pip,
                    'id': f"related_pip_{profile.get('id', '')}_{number}",
                    'url': JIRA_BROWSE_URL.format(issue=related_pip),
                }
            )
        budget_holder = profile.get(PROFILE_BUDGET_HOLDER)
        if budget_holder:
            details.append(
                {'label': 'Budget holder', 'value': budget_holder, 'id': f'budget_holder_{profile_id}'}
            )
        return {
            'details': details,
            # The 'Register samples' button reads 'Add more samples'
            # once the profile has a batch (see edp_sample_batches.js)
            'data_attributes': {'has-sample-batches': 'true' if batches else 'false'},
        }

    def get_sample_page_context(self, profile):
        described_batches = edp_utils.describe_upload_batches(profile)
        # Start with the first batch that still has samples left to upload
        default_batch_id = next(
            (batch['batch_id'] for batch in described_batches if batch['samples_left']), ''
        )
        return {
            'use_profile_blank_manifest': True,
            'manifest_group': get_batch_manifest_group(
                described_batches,
                'The batch of samples that a manifest is for',
                lambda batch: f"{batch['name']} - {batch['samples_left']} left to upload",
                default_batch_id,
            ),
        }

    def check_blank_sample_manifest(self, profile, request_data):
        return edp_utils.check_blank_sample_manifest(
            profile=profile, batch_id=request_data.get(BATCH_PARAM, '')
        )

    def get_blank_sample_manifest_row_limit(self, profile, request_data):
        # Only as many rows as there are samples left to upload for the batch
        batch_id = edp_utils.get_upload_batch_id(profile, request_data.get(BATCH_PARAM, ''))
        if not batch_id:
            return None, MESSAGES['select_sample_batch']
        return edp_utils.get_samples_left_to_upload(profile, batch_id), ''

    def prepare_sample_upload(self, profile, request_data):
        # The manifest cannot add more samples than are left to upload for the
        # batch and, the samples it creates belong to the batch
        batch_id = edp_utils.get_upload_batch_id(profile, request_data.get(BATCH_PARAM, ''))
        if not batch_id:
            return {'validators': [], 'new_sample_fields': {}, 'error': MESSAGES['select_sample_batch']}
        return {
            'validators': [SampleCountValidator.for_batch(batch_id)],
            'new_sample_fields': {SAMPLE_BATCH_ID: batch_id},
            'error': '',
        }

    def check_sample_submission(self, profile, sample_ids):
        return edp_utils.check_sample_submission(profile=profile, sample_ids=sample_ids)
