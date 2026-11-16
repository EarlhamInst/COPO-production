'''Sample batches under EDP profiles.

Samples are registered under an EDP profile in batches. Each batch has its own
number of samples, sample type, container type and library type so, a profile
(i.e. one LIMS project with one Related ENQ) can hold samples of more than one
sample type. A batch can be edited until any of its samples have been submitted.
'''

import uuid

from common.utils import helpers

from .constants import (
    BATCH_DATE_CREATED,
    BATCH_DATE_MODIFIED,
    BATCH_FIELDS,
    BATCH_ID,
    BATCH_LIMS_SAMPLE_RECORD_IDS,
    CONTAINER_TYPES,
    PREMADE_LIBRARY_SAMPLE_TYPES,
    PROFILE_CONTAINER_TYPE,
    PROFILE_LIBRARY_TYPE,
    PROFILE_NO_OF_SAMPLES,
    PROFILE_SAMPLE_BATCHES,
    PROFILE_SAMPLE_TYPE,
    PROFILE_SAPIO_PROJECT_ID,
)
from .edp_validation_messages import MESSAGES


def is_premade_library(sample_type):
    return str(sample_type or '').strip().lower() in {
        premade_sample_type.lower() for premade_sample_type in PREMADE_LIBRARY_SAMPLE_TYPES
    }


def clean_batch_fields(fields):
    '''Check the batch fields that a user entered and tidy them up.

    `fields` maps each of `BATCH_FIELDS` to the value entered. Returns
    `(cleaned_fields, errors)`; `errors` is a list of messages and is
    empty when the fields are valid.
    '''
    errors = []

    raw_sample_count = fields.get(PROFILE_NO_OF_SAMPLES, '')
    try:
        # The form's field is free text so, the count can arrive as a string
        no_of_samples = int(str(raw_sample_count).strip())
    except (TypeError, ValueError):
        no_of_samples = 0
    if no_of_samples < 1:
        errors.append(MESSAGES['invalid_batch_sample_count'].format(sample_count=raw_sample_count))

    sample_type = str(fields.get(PROFILE_SAMPLE_TYPE) or '').strip()
    if not sample_type:
        errors.append(MESSAGES['missing_batch_field'].format(field=BATCH_FIELDS[PROFILE_SAMPLE_TYPE]))

    # Container types used to be saved in lower case
    container_type = str(fields.get(PROFILE_CONTAINER_TYPE) or '').strip().capitalize()
    if container_type not in CONTAINER_TYPES:
        errors.append(MESSAGES['missing_batch_field'].format(field=BATCH_FIELDS[PROFILE_CONTAINER_TYPE]))

    # The library type is hidden for other sample types so, any value left over is dropped
    library_type = ''
    if is_premade_library(sample_type):
        library_type = str(fields.get(PROFILE_LIBRARY_TYPE) or '').strip()
        if not library_type:
            errors.append(
                MESSAGES['missing_batch_field'].format(field=BATCH_FIELDS[PROFILE_LIBRARY_TYPE])
            )

    cleaned_fields = {
        PROFILE_NO_OF_SAMPLES: no_of_samples,
        PROFILE_SAMPLE_TYPE: sample_type,
        PROFILE_CONTAINER_TYPE: container_type,
        PROFILE_LIBRARY_TYPE: library_type,
    }
    return cleaned_fields, errors


def new_batch(cleaned_fields):
    now = helpers.get_datetime()
    return {
        BATCH_ID: uuid.uuid4().hex,
        **cleaned_fields,
        BATCH_LIMS_SAMPLE_RECORD_IDS: [],
        BATCH_DATE_CREATED: now,
        BATCH_DATE_MODIFIED: now,
    }


def batch_from_profile_fields(profile):
    '''A batch made from the batch fields saved on the profile itself.

    EDP profiles used to keep a single sample count and type. Their samples
    in the LIMS aren't linked to a batch yet so, the batch's record IDs are
    left as `None` for the next LIMS sync to claim them. A new profile's
    first batch arrives the same way, through the profile form.
    Returns `None` if the profile has no batch fields saved.
    '''
    if not any(str(profile.get(field) or '').strip() for field in BATCH_FIELDS):
        return None

    try:
        no_of_samples = int(str(profile.get(PROFILE_NO_OF_SAMPLES) or 0).strip() or 0)
    except ValueError:
        no_of_samples = 0

    sample_type = str(profile.get(PROFILE_SAMPLE_TYPE) or '').strip()
    batch = new_batch(
        {
            PROFILE_NO_OF_SAMPLES: no_of_samples,
            PROFILE_SAMPLE_TYPE: sample_type,
            PROFILE_CONTAINER_TYPE: str(profile.get(PROFILE_CONTAINER_TYPE) or '').strip().capitalize(),
            PROFILE_LIBRARY_TYPE: (
                str(profile.get(PROFILE_LIBRARY_TYPE) or '').strip()
                if is_premade_library(sample_type)
                else ''
            ),
        }
    )
    batch[BATCH_LIMS_SAMPLE_RECORD_IDS] = None
    # The same batch ID every time the profile is read, until the batch is saved
    batch[BATCH_ID] = uuid.uuid5(uuid.NAMESPACE_OID, str(profile.get('_id', ''))).hex
    return batch


def get_sample_batches(profile):
    '''The profile's sample batches, oldest first.

    A profile whose batch fields haven't been moved into a batch yet
    (see `move_profile_fields_into_batch`) is read as one batch.
    '''
    batches = profile.get(PROFILE_SAMPLE_BATCHES)
    if batches:
        return list(batches)
    batch = batch_from_profile_fields(profile)
    return [batch] if batch else []


def find_batch(profile, batch_id):
    return next(
        (batch for batch in get_sample_batches(profile) if batch.get(BATCH_ID) == batch_id),
        None,
    )


def resolve_batch_id(profile, batch_id=''):
    '''The batch that a manifest is for.

    Returns the given batch if the profile has it. Otherwise, a profile with
    only one batch uses that batch. Returns '' if a batch must be chosen.
    '''
    if batch_id:
        return batch_id if find_batch(profile, batch_id) else ''
    batches = get_sample_batches(profile)
    return batches[0][BATCH_ID] if len(batches) == 1 else ''


def get_batch_study_id(profile, batch_id):
    '''The study ID of a batch's LIMS manifest.

    Each batch has its own LIMS manifest (so that batches of different
    sample types can use different LIMS checklists), told apart by its
    study ID. The first batch's study ID is the LIMS project ID, as it was
    before batches existed, and later batches add their number e.g. PR0001-B2.
    '''
    project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, '')
    for number, batch in enumerate(get_sample_batches(profile), start=1):
        if batch[BATCH_ID] == batch_id:
            return project_id if number == 1 else f'{project_id}-B{number}'
    return ''


def find_batch_by_study_id(profile, study_id):
    study_id = str(study_id or '').strip()
    if not study_id or not profile.get(PROFILE_SAPIO_PROJECT_ID):
        return None
    return next(
        (
            batch
            for batch in get_sample_batches(profile)
            if get_batch_study_id(profile, batch[BATCH_ID]) == study_id
        ),
        None,
    )


def get_requested_sample_count(profile):
    # The number of samples registered across all of the profile's batches
    return sum(int(batch.get(PROFILE_NO_OF_SAMPLES) or 0) for batch in get_sample_batches(profile))


def move_profile_fields_into_batch(profile, profile_collection):
    '''Move the batch fields saved on the profile into its `sample_batches` list.

    Does nothing if the profile already has batches or no batch fields.
    The profile is updated in place and in the database. Returns `True`
    if the profile was changed.
    '''
    if profile.get(PROFILE_SAMPLE_BATCHES):
        return False
    batch = batch_from_profile_fields(profile)
    if not batch:
        return False

    profile_collection.update_one(
        {'_id': profile['_id']},
        {
            '$set': {PROFILE_SAMPLE_BATCHES: [batch]},
            '$unset': {field: '' for field in BATCH_FIELDS},
        },
    )
    profile[PROFILE_SAMPLE_BATCHES] = [batch]
    for field in BATCH_FIELDS:
        profile.pop(field, None)
    return True
