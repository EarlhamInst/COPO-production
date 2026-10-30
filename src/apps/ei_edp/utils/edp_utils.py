import operator
import uuid
from functools import reduce

from django.contrib.auth.models import Group
from django.db.models import Q
from sapiopylib.rest.utils.recordmodel.PyRecordModel import PyRecordModel

from common.dal.copo_da import CopoGroup
from common.dal.profile_da import Profile
from common.schemas.utils.data_utils import render_html_list
from common.utils import helpers
from common.utils.logger import Logger
from src.apps.copo_core.models import User
from src.apps.copo_single_cell_submission.utils.da import Singlecell, SinglecellSchemas

from .EDPSchemaHandler import get_pending_samples
from .edp_validation_messages import MESSAGES
from .email import Email
from .lims import get_lims_adapter

# Separator used when a profile owner enters multiple customer emails
CUSTOMER_EMAIL_SPLITTER = ';'
EDP_SCHEMA_NAME = 'EI_EDP'

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

    edp_django_group, _ = Group.objects.get_or_create(name='ei_edp_users')

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

    - The requested sample count is a non-negative whole number.
    - If the profile already has a linked Sapio project, verify that the project
      still exists in Sapio and the requested sample count does not drop below the
      number of samples that have already been submitted (in other words, users
      cannot silently delete submitted samples).
    '''
    raw_sample_count = auto_fields.get('copo.profile.no_of_samples', '')
    try:
        requested_sample_count = int(str(raw_sample_count).strip() or 0)
    except ValueError:
        requested_sample_count = -1
    if requested_sample_count < 0:
        return {
            'status': 'error',
            'message': MESSAGES['invalid_sample_count'].format(sample_count=raw_sample_count),
        }

    target_id = kwargs.get("target_id", "")
    if not target_id:
        return {"status": "success"}
    profile = Profile().get_record(target_id)

    # LIMS-specific validation (e.g. can't drop below committed samples) is
    # delegated to the active adapter.
    return get_lims_adapter().validate_profile_change(profile, requested_sample_count)


def post_save_edp_profile(profile):
    """Sync the saved EDP profile to Sapio after it has been written to the database.

    This function does two things:

    1. COPO profile sharing - if the profile has a `customer_emails` field,
       ensures each email address is added to the COPO group for this profile.
       Users that already exist in COPO get an email notification immediately;
       users that don't have a COPO account yet get a token-based invite link.

    2. Sapio project sync - creates a new Sapio Project record (and a matching
       set of Sample records) when a profile is first saved, or updates the
       existing project and reconciles sample counts on subsequent saves.
       Samples are assigned to 96-well plates; new plates are created
       automatically if needed.

    If EDP profiles fail to be shared via email, their project creations in Sapio
    will still go ahead. All this occurs after the profile has been saved in COPO.
    """
    current_user = helpers.get_current_user()
    email_obj = Email()

    # --- COPO profile sharing ---
    customer_emails = profile.get("customer_emails", "")
    if customer_emails:
        emails = [
            email.strip()
            for email in customer_emails.split(CUSTOMER_EMAIL_SPLITTER)
            if email.strip()
        ]

        # Ensure a COPO group exists for this profile (used to control access)
        group_id = None
        groups = CopoGroup().get_group_by_profile(profile_id=profile["_id"])
        if not groups:
            group_id = CopoGroup().create_group_for_profile(
                profile_id=profile["_id"],
                group_name=profile["title"],
                owner_id=profile["user_id"],
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
            customer_emails_tokens = {
                str(uuid.uuid4()): email for email in missing_user_emails
            }
            Profile().get_collection_handle().update_one(
                {"_id": profile["_id"]},
                {"$set": {"customer_emails_tokens": customer_emails_tokens}},
            )
            email_obj.notify_shared_profile_to_non_existent_user(
                profile, customer_emails_tokens
            )

    # --- LIMS project sync ---
    # Runs whether or not emails have been successfully sent about the created profiles
    # Delegate project/sample synchronisation to the active LIMS adapter. The
    # adapter returns the (possibly newly minted) project id, which we persist
    # on the profile so subsequent saves update rather than recreate. The id is
    # returned even on partial failure, so a retry finds the existing project.
    result = get_lims_adapter().sync_project(profile)
    project_id = result.get("project_id", "")
    if project_id and project_id != profile.get("sapio_project_id", ""):
        profile["sapio_project_id"] = project_id
        Profile().get_collection_handle().update_one(
            {"_id": profile["_id"]}, {"$set": {"sapio_project_id": project_id}}
        )

    messages = []
    status = 'success'
    if result.get("status") != "success":
        status = result.get('status', 'warning')
        messages.append(result.get('message', ''))

    if email_obj.failed_emails:
        if status == 'success':
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


def post_delete_edp_profile(profile):
    """Delete the linked Sapio Project when an EDP profile is deleted in COPO.

    Uses recursive_delete=True so Sapio cleans up all child records
    (Samples, Plates, etc.) along with the Project.
    """
    if profile.get("sapio_project_id", ""):
        return get_lims_adapter().delete_project(profile["sapio_project_id"])
    return {"status": "success"}


def submit_edp_to_sapio(profile_id, study_id):
    '''Push submission data from COPO into the linked Sapio project.

    Reads the manifest data stored in the `SingleCellCollection` and maps each
    field to its corresponding Sapio field using the `sapio_name` column in the
    schema (format: "<SapioDataType>:<FieldName>", e.g. "Sample:C_LibraryType").

    Study-level fields are written to the Sapio `Project` record and broadcast to
    all samples. Sample-level fields are matched by SampleId to the correct
    Sapio `Sample` record.
    '''
    profile = Profile().get_record(profile_id)
    profile_title = profile.get('title', '')
    
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
        .find_one({"profile_id": profile_id, "study_id": study_id})
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

    # The COPO term --> LIMS field mapping and the write itself are the adapter's
    # responsibility; here study_id is the LIMS project identifier.
    result = get_lims_adapter().submit_manifest(
        study_id, schemas, record_components
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
        identifier='study_id',
        identifier_value=study_id,
        repository='sapio',
        status_column_value=status_column_value,
    )
    return result


def check_lims_blank_manifest(profile_id):
    # A warning is displayed if a user tries to download a blank 
    # LIMS manifest when all the requested samples from the EDP profile 
    # level have already been submitted.
    # Otherwise, the blank manifest is downloaded.
    profile = Profile().get_record(profile_id) or {}
    sapio_project_id = profile.get('sapio_project_id', '')
    if not sapio_project_id:
        return {
            'status': 'error',
            'message': MESSAGES['missing_sapio_project'],
        }

    schemas = (
        SinglecellSchemas()
        .get_collection_handle()
        .find_one({'name': EDP_SCHEMA_NAME})['schemas']
    )
    
    if not get_pending_samples(sapio_project_id, schemas):
        return {'status': 'warning', 'message': MESSAGES['all_samples_submitted']}
    
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
    type = profile["type"]
    if type != "ei_edp":
        return {
            "status": "error",
            "message": MESSAGES['not_edp_profile'].format(profile_title=profile['title']),
        }
    user = helpers.get_current_user()
    if user.id == profile["user_id"]:
        return {"status": "error", "message": MESSAGES['profile_owner_cannot_join']}

    if user.email == '' or user.email is None:
        # New user with no email - resolve their address from the invite token
        email = profile.get("customer_emails_tokens", {}).get(token, "")
        if not email:
            return {
                "status": "error",
                "message": MESSAGES['user_not_authorised_to_join'],
            }
        user.email = email
        user.save()

    customer_emails = profile.get("customer_emails", "")
    if customer_emails:
        emails = [
            email.strip()
            for email in customer_emails.split(CUSTOMER_EMAIL_SPLITTER)
            if email.strip()
        ]

        if user.email.lower() in {email.lower() for email in emails}:
            groups = CopoGroup().get_group_by_profile(profile_id=profile["_id"])
            if not groups:
                group_id = CopoGroup().create_group_for_profile(
                    profile_id=profile["_id"],
                    group_name=profile["title"],
                    owner_id=profile["user_id"],
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
