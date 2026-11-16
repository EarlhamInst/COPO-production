import operator
import uuid
from functools import reduce

from bson import ObjectId

from django.contrib.auth.models import Group
from django.db.models import Q
from sapiopylib.rest.utils.recordmodel.PyRecordModel import PyRecordModel

from common.dal.copo_da import CopoGroup
from common.dal.profile_da import Profile
from common.dal.sample_da import Sample
from common.schemas.utils.data_utils import render_html_list
from common.utils import helpers
from common.utils.logger import Logger
from src.apps.copo_core.models import User
from src.apps.copo_single_cell_submission.utils.da import Singlecell, SinglecellSchemas

from .EDPSchemaHandler import get_pending_batch_samples
from .constants import (
    BATCH_DATE_MODIFIED,
    BATCH_FIELDS,
    BATCH_ID,
    BATCH_LIMS_SAMPLE_RECORD_IDS,
    BIOSAMPLE_ACCESSION,
    EDP_PROFILE_TYPE,
    EDP_SCHEMA_NAME,
    EDP_USERS_GROUP,
    PREMADE_LIBRARY_SAMPLE_TYPES,
    PROFILE_CONTAINER_TYPE,
    PROFILE_LIBRARY_TYPE,
    PROFILE_NO_OF_SAMPLES,
    PROFILE_SAMPLE_TYPE,
    PROFILE_USER_EMAIL,
    PROFILE_USER_EMAIL_TOKEN,
    PROFILE_SAMPLE_BATCHES,
    PROFILE_SAPIO_PROJECT_ID,
    PROFILE_TITLE,
    PROFILE_TYPE,
    PROFILE_USER_ID,
    SAMPLE_BATCH_ID,
    STUDY_ID,
)
from .edp_validation_messages import MESSAGES
from .email import Email
from .lims import get_lims_adapter
from .sample_batches import (
    clean_batch_fields,
    find_batch,
    find_batch_by_study_id,
    get_requested_sample_count,
    get_sample_batches,
    move_profile_fields_into_batch,
    new_batch,
    resolve_batch_id,
)

# Separator used when a profile owner enters multiple user emails
USER_EMAIL_SPLITTER = ';'

l = Logger()


def get_sapio_sample_type_options():
    # Sample-type choices for the profile form dropdown, sourced from the LIMS.
    return get_lims_adapter().get_sample_type_options()


def _add_to_edp_django_group(user_ids, action='add'):
    '''
    Adds users to the 'ei_edp_users' Django group
    by their IDs if they are not a member.
    '''
    if not user_ids:
        return

    edp_django_group, _ = Group.objects.get_or_create(name=EDP_USERS_GROUP)

    if action == 'add':
        edp_django_group.user_set.add(*user_ids)
    elif action == 'remove':
        edp_django_group.user_set.remove(*user_ids)
    else:
        l.error(
            "Error occurred when adding user to EDP group. Invalid action. Use 'add' or 'remove'."
        )


def pre_save_edp_profile(auto_fields, **kwargs):
    '''Validate EDP profile fields before saving their details to the database

    - A new profile must arrive with its first batch of samples, which is
      entered in the profile form's batch fields. The profile cannot created
      without it.
      
    - An existing profile's batches are changed through `add_sample_batch` and
      `update_sample_batch` so, any batch fields sent with the profile form are
      ignored. If the profile already has a linked LIMS project, verify that
      the project still exists.
    '''
    target_id = kwargs.get('target_id', '')
    if not target_id:
        cleaned_fields, errors = clean_batch_fields(
            {field: auto_fields.get(f'copo.profile.{field}', '') for field in BATCH_FIELDS}
        )
        if errors:
            return {
                'status': 'error',
                'message': ' '.join([MESSAGES['first_batch_required'], *errors]),
            }
        # Saved as tidied up. post_save_edp_profile moves them into the first batch.
        for field, value in cleaned_fields.items():
            auto_fields[f'copo.profile.{field}'] = str(value)
        return {'status': 'success'}

    for field in BATCH_FIELDS:
        auto_fields.pop(f'copo.profile.{field}', None)

    profile = Profile().get_record(target_id)
    return get_lims_adapter().validate_profile_change(profile)


def _share_profile(profile, email_obj):
    '''Share the profile with its user email addresses.

    Users that already exist in COPO get an email notification immediately;
    users that don't have a COPO account yet get a token-based invite link.
    Only addresses that the profile isn't shared with yet are emailed.
    '''
    current_user = helpers.get_current_user()

    user_email = profile.get(PROFILE_USER_EMAIL, "")
    if user_email:
        emails = [
            email.strip()
            for email in user_email.split(USER_EMAIL_SPLITTER)
            if email.strip()
        ]

        # Ensure a COPO group exists for this profile (used to control access)
        group_id = None
        groups = CopoGroup().get_group_by_profile(profile_id=profile["_id"])
        if not groups:
            group_id = CopoGroup().create_group_for_profile(
                profile_id=profile["_id"],
                group_name=profile[PROFILE_TITLE],
                owner_id=profile[PROFILE_USER_ID],
            )
        else:
            group_id = groups[0]["_id"]
        current_shared_users = CopoGroup().get_users_for_group_info(group_id=group_id)

        # Case-insensitivity is considered in email addresses provided
        emails_in_lowercase = {
            email.lower(): email for email in emails
        } 
        
        # Users currently in the group who are no longer in the email list should be removed
        incorrect_shared_user_ids = [
            str(user["id"])
            for user in current_shared_users
            if (user.get('email') or '').lower() not in emails_in_lowercase
        ]
        users = User.objects.filter(
            reduce(
                operator.or_,
                (Q(email__iexact=email) for email in emails_in_lowercase),
                Q(pk__in=[]),
            )
        ).values('id', 'email', 'first_name')
        existing_user_emails = {user['email'].lower() for user in users}
        missing_user_emails = {
            email
            for lower_email, email in emails_in_lowercase.items()
            if lower_email not in existing_user_emails
        }
        if users:
            current_shared_user_ids = {user['id'] for user in current_shared_users}
            # Add any new users who are not already in the group (and aren't the profile owner)
            new_shared_user = {
                str(user['id']): user
                for user in users
                if user['id'] not in current_shared_user_ids
                and user['email'].lower() != (current_user.email or '').lower()
            }

            if new_shared_user:
                # Add users to the shared profile group
                CopoGroup().add_users_to_group(
                    group_id=group_id, user_ids=list(new_shared_user.keys())
                )

                # Add users to the 'ei_edp_users' Django group (by IDs)
                _add_to_edp_django_group(list(map(int, new_shared_user.keys())))

                # Send an email about the shared profile
                email_obj.notify_shared_profile_to_existing_user(
                    profile, new_shared_user.values()
                )

        # Remove users who are no longer in the email list from the shared profile group
        CopoGroup().remove_users_from_group(
            group_id=group_id, user_ids=incorrect_shared_user_ids
        )

        # Remove users from the 'ei_edp_users' Django group
        _add_to_edp_django_group(
            list(map(int, incorrect_shared_user_ids)), action='remove'
        )

        if missing_user_emails:
            # Generate a UUID token per email address so uninvited users can claim access
            # via the join_shared_profile view without needing an existing COPO account
            user_email_tokens = {
                str(uuid.uuid4()): email for email in missing_user_emails
            }
            Profile().get_collection_handle().update_one(
                {"_id": profile["_id"]},
                {"$set": {PROFILE_USER_EMAIL_TOKEN: user_email_tokens}},
            )
            email_obj.notify_shared_profile_to_non_existent_user(
                profile, user_email_tokens
            )



def _save_batch_record_ids(profile, batch_sample_record_ids):
    # Link each batch to its samples in the LIMS
    batches = get_sample_batches(profile)
    for batch in batches:
        if batch[BATCH_ID] in batch_sample_record_ids:
            batch[BATCH_LIMS_SAMPLE_RECORD_IDS] = batch_sample_record_ids[batch[BATCH_ID]]
    profile[PROFILE_SAMPLE_BATCHES] = batches
    Profile().get_collection_handle().update_one(
        {'_id': profile['_id']}, {'$set': {PROFILE_SAMPLE_BATCHES: batches}}
    )


def _undo_new_profile(profile, project_id):
    # A new profile is pointless without its LIMS project and samples so,
    # neither the COPO profile nor the partly created LIMS project is kept
    message = MESSAGES['sapio_create_failed']
    if project_id:
        result = get_lims_adapter().delete_project(project_id)
        if result.get('status') != 'success':
            message = MESSAGES['sapio_create_cleanup_failed'].format(project_id=project_id)
    Profile().get_collection_handle().delete_one({'_id': profile['_id']})
    return {'status': 'error', 'message': message}


def post_save_edp_profile(profile, new_record=False, previous_record=None):
    """Sync the saved EDP profile to the LIMS, then share it by email.

    1. A new profile's first batch (saved on the profile by the form) is moved
       into the profile's `sample_batches` list. So are the batch fields of
       profiles saved before batches existed.

    2. LIMS project sync - creates the project and each batch's samples when a
       profile is first saved, or updates the project and reconciles each
       batch's samples on later saves. The samples of each batch are linked to it.

       If the sync fails, a new profile is removed from COPO along with its
       partly created LIMS project and, an existing profile's previous values
       are restored. Either way, nothing is emailed.

    3. COPO profile sharing - each newly added user email address is added to
       the COPO group for this profile and emailed.
    """
    move_profile_fields_into_batch(profile, Profile().get_collection_handle())

    # The adapter returns the (possibly newly minted) project id, which is
    # persisted on the profile so subsequent saves update rather than recreate.
    result = get_lims_adapter().sync_project(profile)
    project_id = result.get("project_id", "")

    if result.get("status") != "success":
        if new_record:
            return _undo_new_profile(profile, project_id)
        if previous_record:
            Profile().get_collection_handle().replace_one(
                {'_id': profile['_id']}, previous_record
            )
            return {'status': 'error', 'message': MESSAGES['sapio_update_failed_not_saved']}
        return {'status': result.get('status', 'warning'), 'message': result.get('message', '')}

    if project_id and project_id != profile.get(PROFILE_SAPIO_PROJECT_ID, ""):
        profile[PROFILE_SAPIO_PROJECT_ID] = project_id
        Profile().get_collection_handle().update_one(
            {"_id": profile["_id"]}, {"$set": {PROFILE_SAPIO_PROJECT_ID: project_id}}
        )
    _save_batch_record_ids(profile, result.get('batch_sample_record_ids') or {})

    email_obj = Email()
    if profile.get(PROFILE_USER_EMAIL, ""):
        _share_profile(profile, email_obj)

    messages = []
    status = 'success'
    if email_obj.failed_emails:
        status = 'warning'
        messages.append(
            MESSAGES['profile_share_failed'].format(
                emails=render_html_list(sorted(email_obj.failed_emails))
            )
        )

    if email_obj.skipped_emails:
        messages.append(
            MESSAGES['profile_email_not_sent'].format(
                emails=render_html_list(sorted(email_obj.skipped_emails))
            )
        )

    if messages:
        return {'status': status, 'message': ' '.join(messages)}

    return {"status": "success"}


def _get_edp_profile(profile_id):
    profile = Profile().get_record(profile_id)
    if not profile or profile.get(PROFILE_TYPE) != EDP_PROFILE_TYPE:
        return None
    move_profile_fields_into_batch(profile, Profile().get_collection_handle())
    return profile


def _save_sample_batches(profile, batches):
    # Sync the profile's batches to the LIMS before saving them in COPO.
    # If the sync fails, any samples it created are removed and the batches
    # are left as they were.
    previous_batches = get_sample_batches(profile)
    profile[PROFILE_SAMPLE_BATCHES] = batches
    adapter = get_lims_adapter()
    result = adapter.sync_project(profile)
    if result.get('status') != 'success':
        adapter.delete_samples(result.get('created_sample_record_ids') or [])
        profile[PROFILE_SAMPLE_BATCHES] = previous_batches
        return {'status': 'error', 'message': MESSAGES['batch_saved_but_lims_failed']}

    _save_batch_record_ids(profile, result.get('batch_sample_record_ids') or {})
    return {'status': 'success', 'message': '', PROFILE_SAMPLE_BATCHES: profile[PROFILE_SAMPLE_BATCHES]}


def add_sample_batch(profile_id, fields):
    '''Register another batch of samples under an existing EDP profile.

    `fields` maps each of `BATCH_FIELDS` to the value entered. No emails
    are sent since the profile is already shared.
    '''
    profile = _get_edp_profile(profile_id)
    if not profile:
        return {'status': 'error', 'message': MESSAGES['not_edp_profile'].format(profile_title='')}

    cleaned_fields, errors = clean_batch_fields(fields)
    if errors:
        return {'status': 'error', 'message': ' '.join(errors)}

    return _save_sample_batches(profile, get_sample_batches(profile) + [new_batch(cleaned_fields)])


def get_sample_batch_form_options():
    '''Options for the fields of the dialog that registers a batch of samples.

    The sample types come from the LIMS (which raises `LIMSUnavailable` if it
    can't be reached) and the other options from the EDP profile schema.
    '''
    schema = {
        field['id'].split('.')[-1]: field
        for field in Profile().get_component_schema(profile_type=EDP_PROFILE_TYPE)
    }

    def listed_options(field):
        options = schema.get(field, {}).get('option_values')
        return options if isinstance(options, list) else []

    # Each field's label, 'required' flag and help tip come from the profile
    # schema so, the dialogue's labels match those of the profile form. The
    # hard-coded labels are only used if the schema lacks a field or its label.
    fields = {}
    for field, fallback_label in BATCH_FIELDS.items():
        schema_field = schema.get(field, {})
        fields[field] = {
            'label': schema_field.get('label') or fallback_label,
            'required': str(schema_field.get('required', '')).strip().lower() == 'true',
            'help_tip': schema_field.get('help_tip', ''),
        }

    return {
        'fields': fields,
        'labels': {field: details['label'] for field, details in fields.items()},
        'sample_type_options': get_sapio_sample_type_options(),
        'container_type_options': listed_options(PROFILE_CONTAINER_TYPE),
        'library_type_options': listed_options(PROFILE_LIBRARY_TYPE),
        # The library type field is only shown for these sample types
        'premade_library_sample_types': list(PREMADE_LIBRARY_SAMPLE_TYPES),
    }


def get_submitted_batch_sample_counts(profile):
    # A batch is locked once any of its samples have been submitted, whether
    # to ENA from COPO or to the LIMS with a LIMS manifest. The larger of the
    # two counts is reported since they are mostly the same samples.
    lims_counts = get_lims_adapter().get_submitted_sample_counts(profile)
    copo_counts = count_copo_batch_samples(profile)
    return {
        batch[BATCH_ID]: max(
            lims_counts.get(batch[BATCH_ID], {}).get('submitted', 0),
            copo_counts.get(batch[BATCH_ID], {}).get('submitted', 0),
        )
        for batch in get_sample_batches(profile)
    }


def describe_sample_batches(profile_id):
    '''The EDP profile's batches as shown in the dialog, oldest first.

    Each batch says whether it is locked i.e. whether any of its samples
    have been submitted. Returns `None` if the profile isn't an EDP profile.
    '''
    profile = _get_edp_profile(profile_id)
    if not profile:
        return None

    submitted_sample_counts = get_submitted_batch_sample_counts(profile)
    described_batches = []
    for batch in get_sample_batches(profile):
        submitted_sample_count = submitted_sample_counts.get(batch[BATCH_ID], 0)
        date_created = batch.get('date_created')
        described_batches.append(
            {
                BATCH_ID: batch[BATCH_ID],
                **{field: batch.get(field, '') for field in BATCH_FIELDS},
                'date_created': date_created.strftime('%d %b %Y') if date_created else '',
                'submitted_sample_count': submitted_sample_count,
                'locked': bool(submitted_sample_count),
            }
        )
    return described_batches


def update_sample_batch(profile_id, batch_id, fields):
    '''Change a batch of samples under an EDP profile.

    A batch can be changed until any of its samples have been submitted.
    '''
    profile = _get_edp_profile(profile_id)
    if not profile:
        return {'status': 'error', 'message': MESSAGES['not_edp_profile'].format(profile_title='')}

    batch = find_batch(profile, batch_id)
    if not batch:
        return {'status': 'error', 'message': MESSAGES['batch_not_found']}

    submitted_sample_count = get_submitted_batch_sample_counts(profile).get(batch_id, 0)
    if submitted_sample_count:
        return {
            'status': 'error',
            'message': MESSAGES['batch_locked'].format(
                submitted_sample_count=submitted_sample_count,
                total_sample_count=batch.get(PROFILE_NO_OF_SAMPLES, 0),
            ),
        }

    cleaned_fields, errors = clean_batch_fields(fields)
    if errors:
        return {'status': 'error', 'message': ' '.join(errors)}

    # Samples already uploaded for the batch in COPO aren't removed by
    # changing the batch so, the batch must keep room for them
    uploaded_sample_count = count_copo_batch_samples(profile).get(batch_id, {}).get('uploaded', 0)
    if cleaned_fields[PROFILE_NO_OF_SAMPLES] < uploaded_sample_count:
        return {
            'status': 'error',
            'message': MESSAGES['batch_count_below_uploaded'].format(
                uploaded_sample_count=uploaded_sample_count
            ),
        }

    updated_batch = {**batch, **cleaned_fields, BATCH_DATE_MODIFIED: helpers.get_datetime()}
    batches = [
        updated_batch if existing_batch[BATCH_ID] == batch_id else existing_batch
        for existing_batch in get_sample_batches(profile)
    ]
    return _save_sample_batches(profile, batches)


def post_delete_edp_profile(profile):
    """Delete the linked Sapio Project when an EDP profile is deleted in COPO.

    Uses recursive_delete=True so Sapio cleans up all child records
    (Samples, Plates, etc.) along with the Project.
    """
    if profile.get(PROFILE_SAPIO_PROJECT_ID, ""):
        return get_lims_adapter().delete_project(profile[PROFILE_SAPIO_PROJECT_ID])
    return {"status": "success"}


def submit_lims_manifest_to_sapio(profile_id, study_id):
    '''Push submission data from COPO into the linked Sapio project.

    Reads the manifest data stored in the `SingleCellCollection` and maps each
    field to its corresponding Sapio field using the `sapio_name` column in the
    schema (format: "<SapioDataType>:<FieldName>", e.g. "Sample:C_LibraryType").

    Study-level fields are written to the Sapio `Project` record and broadcast to
    all samples. Sample-level fields are matched by SampleId to the correct
    Sapio `Sample` record.
    '''
    profile = Profile().get_record(profile_id)
    profile_title = profile.get(PROFILE_TITLE, '')
    
    if not profile:
        return {
            "status": "error",
            "message": MESSAGES['profile_not_found'].format(profile_id=profile_id, profile_title=profile_title),
        }
        
    # Retrieve documents for the given profile 
    # and study from the `SingleCellCollection`
    records = (
        Singlecell()
        .get_collection_handle()
        .find_one({"profile_id": profile_id, STUDY_ID: study_id})
    )
    if not records:
        return {
            "status": "error",
            "message": MESSAGES['submission_not_found'].format(
                profile_id=profile_id, profile_title=profile_title, study_id=study_id
            ),
        }

    record_components = records.get("components", {})
    schemas = SinglecellSchemas().get_schema(
        schema_name=records["schema_name"], target_id=records["checklist_id"]
    )

    # Each batch has its own LIMS manifest, told apart by its study ID, but
    # they all go to the profile's LIMS project
    if not find_batch_by_study_id(profile, study_id):
        return {
            "status": "error",
            "message": MESSAGES['submission_not_found'].format(
                profile_id=profile_id, profile_title=profile_title, study_id=study_id
            ),
        }

    # The COPO term --> LIMS field mapping and the write itself are the adapter's
    # responsibility
    result = get_lims_adapter().submit_manifest(
        profile[PROFILE_SAPIO_PROJECT_ID], schemas, record_components
    )

    # Record the outcome of a record submission in the
    # `Status`, `Accession` or `Error` columns
    # As regards Sapio, its submission accession is Sapio's
    # record ID of the project
    status = result.get('status', 'error')
    status_column_value = {
        'status': 'accepted' if status == 'success' else 'rejected',
        'error': '' if status == 'success' else result.get('message', ''),
    }
    if status != 'error':
        record_id = result.get('record_id', '')
        status_column_value['accession'] = str(record_id) if record_id else ''

    Singlecell().update_component_status(
        id=records['_id'],
        component='study',
        identifier=STUDY_ID,
        identifier_value=study_id,
        repository='sapio',
        status_column_value=status_column_value,
    )
    return result


def check_lims_blank_manifest(profile, batch_id=''):
    # A warning is displayed if a user tries to download a blank 
    # LIMS manifest when all the samples in the chosen batch
    # have already been submitted.
    # Otherwise, the blank manifest is downloaded.
    sapio_project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, '')
    if not sapio_project_id:
        return {
            'status': 'error',
            'message': MESSAGES['missing_sapio_project'],
        }

    batch_id = resolve_batch_id(profile, batch_id)
    if not batch_id:
        return {'status': 'error', 'message': MESSAGES['select_sample_batch']}

    schemas = (
        SinglecellSchemas()
        .get_collection_handle()
        .find_one({'name': EDP_SCHEMA_NAME})['schemas']
    )
    
    if not get_pending_batch_samples(profile, batch_id, schemas):
        return {'status': 'warning', 'message': MESSAGES['all_samples_submitted']}
    
    return {'status': 'success', 'message': ''}


def _batch_sample_query(profile, batch_id):
    # COPO samples uploaded for the batch. Samples uploaded before
    # batches existed have no batch and belong to the first batch.
    batches = get_sample_batches(profile)
    query = {
        'profile_id': str(profile['_id']),
        'deleted': helpers.get_not_deleted_flag(),
    }
    if batches and batches[0][BATCH_ID] == batch_id:
        query['$or'] = [
            {SAMPLE_BATCH_ID: batch_id},
            {SAMPLE_BATCH_ID: {'$exists': False}},
            {SAMPLE_BATCH_ID: {'$in': ['', None]}},
        ]
    else:
        query[SAMPLE_BATCH_ID] = batch_id
    return query


def get_sample_batch_id(profile, sample):
    # The batch that a COPO sample was uploaded for
    batches = get_sample_batches(profile)
    return sample.get(SAMPLE_BATCH_ID) or (batches[0][BATCH_ID] if batches else '')


def get_samples_left_to_upload(profile, batch_id=None):
    # The number of samples that can still be uploaded under an EDP profile
    # i.e. the number of samples registered (in the batch, if one is given)
    # less those that have already been uploaded
    if batch_id:
        batch = find_batch(profile, batch_id)
        if not batch:
            return 0
        requested_sample_count = int(batch.get(PROFILE_NO_OF_SAMPLES) or 0)
        query = _batch_sample_query(profile, batch_id)
    else:
        requested_sample_count = get_requested_sample_count(profile)
        query = {
            'profile_id': str(profile['_id']),
            'deleted': helpers.get_not_deleted_flag(),
        }
    uploaded_sample_count = Sample().get_collection_handle().count_documents(query)
    return max(requested_sample_count - uploaded_sample_count, 0)


def get_upload_batch_id(profile, batch_id=''):
    # The batch that a sample manifest is for ('' if one must be chosen)
    return resolve_batch_id(profile, batch_id)


def describe_upload_batches(profile):
    # The batches offered on the Samples and LIMS manifest pages with the
    # number of samples left to upload for each. Only COPO is read, not the LIMS.
    move_profile_fields_into_batch(profile, Profile().get_collection_handle())
    uploaded_counts = count_copo_batch_samples(profile)
    described_batches = []
    for number, batch in enumerate(get_sample_batches(profile), start=1):
        no_of_samples = int(batch.get(PROFILE_NO_OF_SAMPLES) or 0)
        samples_left = max(no_of_samples - uploaded_counts[batch[BATCH_ID]]['uploaded'], 0)
        library_type = f", {batch[PROFILE_LIBRARY_TYPE]}" if batch.get(PROFILE_LIBRARY_TYPE) else ''
        described_batches.append(
            {
                BATCH_ID: batch[BATCH_ID],
                'samples_left': samples_left,
                'name': (
                    f"Batch {number}: {no_of_samples} x {batch.get(PROFILE_SAMPLE_TYPE, '')}{library_type} "
                    f"({batch.get(PROFILE_CONTAINER_TYPE, '')})"
                ),
            }
        )
    return described_batches


def count_copo_batch_samples(profile):
    # The number of COPO samples uploaded for each batch and how many of
    # them have been submitted to ENA (or are queued for submission)
    counts = {batch[BATCH_ID]: {'uploaded': 0, 'submitted': 0} for batch in get_sample_batches(profile)}
    samples = Sample().get_collection_handle().find(
        {'profile_id': str(profile['_id']), 'deleted': helpers.get_not_deleted_flag()},
        {SAMPLE_BATCH_ID: 1, BIOSAMPLE_ACCESSION: 1, 'status': 1},
    )
    for sample in samples:
        batch_counts = counts.get(get_sample_batch_id(profile, sample))
        if batch_counts is None:
            continue
        batch_counts['uploaded'] += 1
        if sample.get(BIOSAMPLE_ACCESSION) or sample.get('status') in ('processing', 'sending'):
            batch_counts['submitted'] += 1
    return counts


def check_blank_sample_manifest(profile, batch_id=None):
    # A warning is displayed if a user tries to download a blank sample
    # manifest under an EDP profile when all the samples registered in the
    # chosen batch have already been uploaded.
    # Otherwise, the blank manifest is downloaded.
    if profile.get(PROFILE_TYPE) != EDP_PROFILE_TYPE:
        return {'status': 'success', 'message': ''}

    if not get_samples_left_to_upload(profile, batch_id):
        return {'status': 'warning', 'message': MESSAGES['all_samples_uploaded']}

    return {'status': 'success', 'message': ''}

def check_sample_submission(profile, sample_ids):
    # An error is displayed if a user tries to submit more samples of a batch
    # under an EDP profile than the number of samples registered in the batch.
    # A sample counts as submitted once it has a BioSample accession or it
    # is queued for submission. Selected samples that have already been
    # submitted are submitted again as updates so, they are not counted again.
    profile_type = profile.get(PROFILE_TYPE)
    
    if profile_type != EDP_PROFILE_TYPE:
        return {'status': 'success', 'message': ''}

    profile_id = str(profile['_id'])
    selected_sample_ids = set(map(str, sample_ids))

    samples = list(
        Sample().get_collection_handle().find(
            {
                'profile_id': profile_id,
                'deleted': helpers.get_not_deleted_flag(),
                '$or': [
                    {BIOSAMPLE_ACCESSION: {'$exists': True, '$nin': ['', None]}},
                    {'status': {'$in': ['processing', 'sending']}},
                    {'_id': {'$in': [ObjectId(sample_id) for sample_id in selected_sample_ids if ObjectId.is_valid(sample_id)]}},
                ],
            },
            {'_id': 1, SAMPLE_BATCH_ID: 1, BIOSAMPLE_ACCESSION: 1, 'status': 1},
        )
    )

    for batch in get_sample_batches(profile):
        batch_id = batch[BATCH_ID]
        batch_samples = [sample for sample in samples if get_sample_batch_id(profile, sample) == batch_id]
        submitted_sample_ids = {
            str(sample['_id'])
            for sample in batch_samples
            if sample.get(BIOSAMPLE_ACCESSION) or sample.get('status') in ('processing', 'sending')
        }
        new_sample_count = len(
            ({str(sample['_id']) for sample in batch_samples} & selected_sample_ids)
            - submitted_sample_ids
        )
        requested_sample_count = int(batch.get(PROFILE_NO_OF_SAMPLES) or 0)
        samples_left_to_submit = max(requested_sample_count - len(submitted_sample_ids), 0)
        if new_sample_count > samples_left_to_submit:
            return {
                'status': 'error',
                'message': MESSAGES['too_many_samples_to_submit'].format(
                    requested_sample_count=requested_sample_count,
                    submitted_sample_count=len(submitted_sample_ids),
                    samples_left_to_submit=samples_left_to_submit,
                ),
            }

    return {'status': 'success', 'message': ''}


def check_edp_samples_submitted(profile):
    # A warning is displayed if a user tries to download a blank LIMS
    # manifest or upload a LIMS manifest before any samples have been
    # submitted under an EDP profile since a LIMS manifest describes
    # those samples.
    # Otherwise, the user can proceed with the download or upload.
    profile_id = str(profile['_id'])
    submitted_samples = Sample().get_collection_handle().count_documents(
        {
            'profile_id': profile_id,
            'deleted': helpers.get_not_deleted_flag(),
            BIOSAMPLE_ACCESSION: {'$exists': True, '$nin': ['', None]},
        },
        limit=1,
    )
    if not submitted_samples:
        return {'status': 'warning', 'message': MESSAGES['no_samples_submitted']}

    return {'status': 'success', 'message': ''}


def join_shared_edp_profile(profile, token):
    """Allow a customer to join an EDP profile they've been invited to.

    Called from the join_shared_profile view when a user follows the token link
    sent by Email.notify_shared_profile_to_non_existent_user.

    If the user has no email address yet (e.g. a new ORCID-only account), the
    token is used to look up and assign their email before adding them to the
    COPO group.

    In addition, if the customer is not already a member of the 'ei_edp_users' group,
    the customer is added to it so that the EDP profile can be accessed in the
    profile type dropdown menu.
    """
    type = profile[PROFILE_TYPE]
    if type != EDP_PROFILE_TYPE:
        return {
            "status": "error",
            "message": MESSAGES['not_edp_profile'].format(profile_title=profile[PROFILE_TITLE]),
        }
    user = helpers.get_current_user()
    if user.id == profile[PROFILE_USER_ID]:
        return {"status": "error", "message": MESSAGES['profile_owner_cannot_join']}

    if user.email == '' or user.email is None:
        # New user with no email - resolve their address from the invite token
        email = profile.get(PROFILE_USER_EMAIL_TOKEN, {}).get(token, "")
        if not email:
            return {
                "status": "error",
                "message": MESSAGES['user_not_authorised_to_join'],
            }
        user.email = email
        user.save()

    user_email = profile.get(PROFILE_USER_EMAIL, "")
    if user_email:
        emails = [
            email.strip()
            for email in user_email.split(USER_EMAIL_SPLITTER)
            if email.strip()
        ]

        if user.email.lower() in {email.lower() for email in emails}:
            groups = CopoGroup().get_group_by_profile(profile_id=profile["_id"])
            if not groups:
                group_id = CopoGroup().create_group_for_profile(
                    profile_id=profile["_id"],
                    group_name=profile[PROFILE_TITLE],
                    owner_id=profile[PROFILE_USER_ID],
                )
            else:
                group_id = groups[0]["_id"]

            # Add user to the shared profile group
            CopoGroup().add_user_to_group(group_id=group_id, user_id=str(user.id))

            # Add user to the 'ei_edp_users' Django group by ID
            _add_to_edp_django_group([user.id])

            return {"status": "success"}
        return {
            "status": "error",
            "message": MESSAGES['customer_not_authorised_to_join'].format(email=user.email),
        }
