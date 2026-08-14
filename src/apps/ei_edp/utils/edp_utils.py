from common.utils.logger import Logger
from common.dal.profile_da import Profile
from src.apps.copo_single_cell_submission.utils.da import Singlecell, SinglecellSchemas
from src.apps.copo_core.models import User
from common.dal.copo_da import CopoGroup
from common.utils import helpers
import uuid
from .email import Email
from .lims import get_lims_adapter

# Separator used when a profile owner enters multiple customer emails in a single field
CUSTOMER_EMAIL_SPLITTER = ";"

l = Logger()

def get_sapio_sample_type_options():
    # Sample-type choices for the profile form dropdown, sourced from the LIMS.
    return get_lims_adapter().get_sample_type_options()

def pre_save_edp_profile(auto_fields, **kwargs):
    """Validate EDP profile fields before saving.

    Called by the COPO profile save machinery before writing to the database.
    Returns {"status": "error", "message": ...} to abort the save, or
    {"status": "success"} to allow it.

    Checks:
    - If the profile already has a linked Sapio project, verifies the project
      still exists and that the requested sample count doesn't drop below the
      number of samples that already have customer names assigned (i.e. can't
      silently delete committed samples).
    """
    target_id = kwargs.get("target_id","")
    if not target_id:
        return {"status": "success"}
    profile = Profile().get_record(target_id)
    requested_sample_count = auto_fields.get("copo.profile.no_of_samples", [])

    # LIMS-specific validation (e.g. can't drop below committed samples) is
    # delegated to the active adapter.
    return get_lims_adapter().validate_profile_change(profile, requested_sample_count)

def post_save_edp_profile(profile):
    """Sync the saved EDP profile to Sapio after it has been written to the database.

    This function does two things:

    1. COPO profile sharing — if the profile has a `customer_emails` field,
       ensures each email address is added to the COPO group for this profile.
       Users that already exist in COPO get an email notification immediately;
       users that don't have a COPO account yet get a token-based invite link.

    2. Sapio project sync — creates a new Sapio Project record (and a matching
       set of Sample records) when a profile is first saved, or updates the
       existing project and reconciles sample counts on subsequent saves.
       Samples are assigned to 96-well plates; new plates are created
       automatically if needed.

    Returns {"status": "success"} on success, or
    {"status": "warning", "message": ...} if the profile saved to COPO but the
    Sapio sync failed (so the caller can show a partial-success message).
    """
    project_record = None

    current_user = helpers.get_current_user()

    # --- COPO profile sharing ---
    customer_emails = profile.get("customer_emails","")
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
            group_id = CopoGroup().create_group_for_profile(profile_id=profile["_id"], group_name=profile["title"], owner_id=profile["user_id"])
        else:
            group_id = groups[0]["_id"]
        current_shared_users = CopoGroup().get_users_for_group_info(group_id=group_id)

        missing_user_emails = set(emails)
        # Users currently in the group who are no longer in the email list should be removed
        incorrect_shared_user_ids = [str(user["id"]) for user in current_shared_users if user.get("email","") not in emails]
        users = User.objects.filter(email__in = emails).values('id', 'email','first_name')
        if users:
            missing_user_emails = set(emails) - {user["email"] for user in users}
            # Add any new users who aren't already in the group (and aren't the profile owner)
            new_shared_user = {str(user['id']) : user for user in users if
                                    user['id'] not in current_shared_users
                                    and user['email'] != current_user.email
                                }
            if new_shared_user:
                CopoGroup().add_users_to_group(group_id=group_id, user_ids=list(new_shared_user.keys()))
                Email().notify_shared_profile_to_existing_user(profile, new_shared_user.values())

        CopoGroup().remove_users_from_group(group_id=group_id, user_ids=incorrect_shared_user_ids)

        if missing_user_emails:
            # Generate a UUID token per email address so uninvited users can claim access
            # via the join_shared_profile view without needing an existing COPO account
            customer_emails_tokens = { str(uuid.uuid4()):email for email in missing_user_emails}
            Profile().get_collection_handle().update_one({"_id":profile["_id"]},{"$set":{"customer_emails_tokens": customer_emails_tokens}})
            Email().notify_shared_profile_to_non_existent_user(profile, customer_emails_tokens)

    # --- LIMS project sync ---
    # Delegate project/sample synchronisation to the active LIMS adapter. The
    # adapter returns the (possibly newly minted) project id, which we persist
    # on the profile so subsequent saves update rather than recreate. The id is
    # returned even on partial failure, so a retry finds the existing project.
    result = get_lims_adapter().sync_project(profile)
    project_id = result.get("project_id", "")
    if project_id and project_id != profile.get("sapio_project_id", ""):
        profile["sapio_project_id"] = project_id
        Profile().get_collection_handle().update_one(
            {"_id": profile["_id"]}, {"$set": {"sapio_project_id": project_id}})

    if result.get("status") != "success":
        return {"status": result.get("status", "warning"),
                "message": result.get("message", "")}

    return {"status": "success"}


def post_delete_edp_profile(profile):
    """Delete the linked Sapio Project when an EDP profile is deleted in COPO.

    Uses recursive_delete=True so Sapio cleans up all child records
    (Samples, Plates, etc.) along with the Project.
    """
    if profile.get("sapio_project_id",""):
        return get_lims_adapter().delete_project(profile["sapio_project_id"])
    return {"status": "success"}

def submit_edp_to_sapio(profile_id, study_id):
    """Push single-cell submission data from COPO into the linked Sapio project.

    Reads the manifest data stored in the Singlecell collection and maps each
    field to its corresponding Sapio field using the `sapio_name` column in the
    schema (format: "<SapioDataType>:<FieldName>", e.g. "Sample:C_LibraryType").

    Study-level fields are written to the Sapio Project record and broadcast to
    all samples. Sample-level fields are matched by SampleId to the correct
    Sapio Sample record.
    """
    profile = Profile().get_record(profile_id)
    if not profile:
        return {"status": "error", "message": f"Profile {profile_id} not found."}

    singlecell = Singlecell().get_collection_handle().find_one({"profile_id": profile_id, "study_id": study_id})
    if not singlecell:
        return {"status": "error", "message": f"Singlecell submission for profile {profile_id} and study {study_id} not found."}

    singlecell_components = singlecell.get("components",{})
    schemas = SinglecellSchemas().get_schema(schema_name=singlecell["schema_name"], target_id=singlecell["checklist_id"])

    # The COPO term → LIMS field mapping and the write itself are the adapter's
    # responsibility; here study_id is the LIMS project identifier.
    return get_lims_adapter().submit_manifest(study_id, schemas, singlecell_components)


def join_shared_edp_profile(profile, token):
    """Allow a customer to join an EDP profile they've been invited to.

    Called from the join_shared_profile view when a user follows the token link
    sent by Email.notify_shared_profile_to_non_existent_user.

    If the user has no email address yet (e.g. a new ORCID-only account), the
    token is used to look up and assign their email before adding them to the
    COPO group.
    """
    type = profile["type"]
    if type != "ei_edp":
        return {"status": "error", "message": f"Profile {profile['_id']} is not an EDP profile."}
    user = helpers.get_current_user()
    if user.id == profile["user_id"]:
        return {"status": "error", "message": f"Profile owner cannot join the profile."}

    if user.email == '' or user.email is None:
        # New user with no email — resolve their address from the invite token
        email = profile.get("customer_emails_tokens", {}).get(token, "")
        if not email:
            return {"status": "error", "message": f"User is not authorised to join the profile."}
        user.email = email
        user.save()

    customer_emails = profile.get("customer_emails","")
    if customer_emails:
        emails = [email.strip() for email in customer_emails.split(";")
                   if email.strip()]

        if user.email in emails:
            groups = CopoGroup().get_group_by_profile(profile_id=profile["_id"])
            if not groups:
                group_id = CopoGroup().create_group_for_profile(profile_id=profile["_id"], group_name=profile["title"], owner_id=profile["user_id"])
            else:
                group_id = groups[0]["_id"]
            CopoGroup().add_user_to_group(group_id=group_id, user_id=str(user.id))
            return {"status": "success"}
        return {
            "status": "error",
            "message": f"Customer with email '{user.email}' is not authorised to join the profile.",
        }
