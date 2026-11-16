# Bring existing EDP profiles (and their Sapio projects) up to date with the
# changes made to EDP profiles over time.
#
# Each EDP profile goes through these parts, in order:
#
#   1. Renamed profile fields: values saved under a field's old name are moved
#      to its new name, e.g. 'JIRA ticket number' became 'Related ENQ'.
#
#   2. Removed profile fields: fields that are no longer used are deleted.
#
#   3. Sample batches: samples are now registered in batches so, the number of
#      samples, sample type, container type and library type saved on the
#      profile are moved into one batch (container types in title case).
#
#   4. Saved LIMS manifests: values saved under a renamed LIMS manifest field
#      are moved to its new name, e.g. 'customer_email' became 'user_email'.
#
#   5. Sapio project (skipped with --skip-lims): the project is linked to its
#      department, its samples' container types are put in title case,
#      plated samples are given the well elements that colour them in Sapio's
#      Plate View and, each batch is linked to its Sapio samples.
#
#   6. Missing values: values that can't be filled in automatically (e.g. the
#      Jira PIP) are reported so that someone can add them to the profile.
#
# Every part only changes what is out of date, so the command can be run
# again safely. Samples are never created or removed in Sapio.
#
# SAFETY: dry run by default. Pass --commit to save the changes.
#
# Options:
# 
# Dry run by default. It reads COPO and Sapio and reports the changes per profile.
# --commit saves the changes.
# --skip-lims leaves Sapio alone. The batch is then linked to its samples on the next Sapio save.
# --profile-id <id> lets you try a single profile first.

from django.core.management import BaseCommand

from common.dal.profile_da import Profile
from common.utils.helpers import get_not_deleted_flag
from src.apps.copo_single_cell_submission.utils.da import Singlecell
from src.apps.ei_edp.utils.constants import (
    BATCH_FIELDS,
    BATCH_ID,
    BATCH_LIMS_SAMPLE_RECORD_IDS,
    EDP_PROFILE_TYPE,
    EDP_SCHEMA_NAME,
    PROFILE_BUDGET_HOLDER,
    PROFILE_CONTAINER_TYPE,
    PROFILE_RELATED_ENQ,
    PROFILE_RELATED_PIP,
    PROFILE_SAMPLE_BATCHES,
    PROFILE_SAPIO_PROJECT_ID,
    PROFILE_USER_EMAIL,
    PROFILE_USER_EMAIL_TOKEN,
    USER_EMAIL,
)
from src.apps.ei_edp.utils.sample_batches import batch_from_profile_fields

# Part 1: profile fields that were renamed, as {old name: new name}
RENAMED_PROFILE_FIELDS = {
    'jira_ticket_number': PROFILE_RELATED_ENQ,  # 'JIRA ticket number' -> 'Related ENQ'
    'budget_user': PROFILE_BUDGET_HOLDER,  # 'Budget user' -> 'Budget holder'
    'customer_emails': PROFILE_USER_EMAIL,  # 'Customer emails' -> 'User email address'
    'customer_emails_tokens': PROFILE_USER_EMAIL_TOKEN,
    'tube_or_plate': PROFILE_CONTAINER_TYPE,  # 'Tube or plate' -> 'Container type'
}

# Part 2: profile fields that are no longer used. The Sapio plates are
# found from the Sapio project rather than saved on the profile.
REMOVED_PROFILE_FIELDS = ['plates', 'sapio_plate_ids']

# Part 4: study fields of saved LIMS manifests that were renamed, as {old name: new name}
RENAMED_LIMS_STUDY_FIELDS = {
    'customer_email': USER_EMAIL,  # 'customer' became 'user'
}

# Part 6: profile fields (with their form labels) that have to be filled in by someone
REQUIRED_PROFILE_FIELDS = {
    PROFILE_RELATED_ENQ: 'Related ENQ',
    PROFILE_RELATED_PIP: 'Jira PIP',
    PROFILE_BUDGET_HOLDER: 'Budget holder',
}


def is_blank(value):
    return value is None or (isinstance(value, str) and not value.strip())


class ProfileChanges:
    '''The changes to save to a profile, applied to it in memory straight away
    so that each part sees what the parts before it changed.'''

    def __init__(self, profile):
        self.profile = profile
        self.original_fields = set(profile)
        self.to_set = {}
        self.to_unset = set()
        self.notes = []

    def set(self, field, value):
        self.profile[field] = value
        self.to_set[field] = value
        self.to_unset.discard(field)

    def unset(self, field):
        self.profile.pop(field, None)
        self.to_set.pop(field, None)
        # Only fields that are saved in the database need unsetting there
        if field in self.original_fields:
            self.to_unset.add(field)

    def has_changes(self):
        return bool(self.to_set or self.to_unset)

    def save(self, collection):
        update = {}
        if self.to_set:
            update['$set'] = self.to_set
        if self.to_unset:
            update['$unset'] = {field: '' for field in self.to_unset}
        if update:
            collection.update_one({'_id': self.profile['_id']}, update)


class Command(BaseCommand):
    help = (
        'Bring existing EDP profiles and their Sapio projects up to date '
        '(renamed fields, sample batches, Sapio department and plate colours). '
        'Dry run unless --commit is given.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--commit',
            action='store_true',
            help='Save the changes. Without this the command only reports what it would do.',
        )
        parser.add_argument(
            '--skip-lims',
            action='store_true',
            help='Only update COPO and leave the Sapio projects as they are (part 5).',
        )
        parser.add_argument(
            '--profile-id',
            default='',
            help='Only update this profile e.g. to try the command on one profile first.',
        )

    def handle(self, *args, **options):
        commit = options['commit']
        profile_collection = Profile().get_collection_handle()
        query = {'type': EDP_PROFILE_TYPE, 'deleted': get_not_deleted_flag()}
        profiles = list(profile_collection.find(query))
        if options['profile_id']:
            profiles = [profile for profile in profiles if str(profile['_id']) == options['profile_id']]

        lims = None
        if not options['skip_lims']:
            # Imported here so that --skip-lims never connects to the LIMS
            from src.apps.ei_edp.utils.lims import get_lims_adapter

            lims = get_lims_adapter()

        changed_count = 0
        for profile in profiles:
            changes = ProfileChanges(profile)
            self.rename_profile_fields(changes)
            self.remove_profile_fields(changes)
            self.move_sample_fields_into_batch(changes)
            lims_manifest_count = self.rename_lims_study_fields(profile, commit)
            if lims:
                self.upgrade_lims_project(lims, changes, commit)
            self.report_missing_values(changes)

            if changes.has_changes() or lims_manifest_count:
                changed_count += 1
            if commit:
                changes.save(profile_collection)
            self.report(profile, changes, lims_manifest_count)

        verb = 'Updated' if commit else 'Dry run: would update'
        self.stdout.write(
            (self.style.SUCCESS if commit else self.style.WARNING)(
                f'{verb} {changed_count} of {len(profiles)} EDP profile(s).'
                + ('' if commit else ' Pass --commit to save.')
            )
        )

    # --- Part 1: renamed profile fields ---
    def rename_profile_fields(self, changes):
        for old_field, new_field in RENAMED_PROFILE_FIELDS.items():
            if old_field not in changes.profile:
                continue
            old_value = changes.profile[old_field]
            new_value = changes.profile.get(new_field)
            if is_blank(new_value):
                changes.set(new_field, old_value)
                changes.notes.append(f'{old_field} -> {new_field}')
            elif not is_blank(old_value) and old_value != new_value:
                # The new field was filled in since so, it is kept
                changes.notes.append(
                    f'{old_field} removed; kept the newer {new_field} value {new_value!r} over {old_value!r}'
                )
            else:
                changes.notes.append(f'{old_field} removed (already in {new_field})')
            changes.unset(old_field)

    # --- Part 2: removed profile fields ---
    def remove_profile_fields(self, changes):
        for field in REMOVED_PROFILE_FIELDS:
            if field in changes.profile:
                changes.unset(field)
                changes.notes.append(f'{field} removed')

    # --- Part 3: sample batches ---
    def move_sample_fields_into_batch(self, changes):
        if changes.profile.get(PROFILE_SAMPLE_BATCHES):
            # Batch fields can't be left over on a profile with batches
            for field in BATCH_FIELDS:
                if field in changes.profile:
                    changes.unset(field)
            return
        batch = batch_from_profile_fields(changes.profile)
        if not batch:
            changes.notes.append('no sample fields to move into a batch')
            return
        changes.set(PROFILE_SAMPLE_BATCHES, [batch])
        for field in BATCH_FIELDS:
            changes.unset(field)
        changes.notes.append(
            'moved into batch 1: '
            + ', '.join(f'{label}: {batch[field] or "-"}' for field, label in BATCH_FIELDS.items())
        )

    # --- Part 4: saved LIMS manifests ---
    def rename_lims_study_fields(self, profile, commit):
        # The study rows of the profile's saved LIMS manifests. Returns
        # how many saved manifests have (or would have) changed.
        collection = Singlecell().get_collection_handle()
        changed_count = 0
        for record in collection.find(
            {'profile_id': str(profile['_id']), 'schema_name': EDP_SCHEMA_NAME},
            {'components.study': 1},
        ):
            study_rows = record.get('components', {}).get('study', [])
            is_changed = False
            for row in study_rows:
                for old_field, new_field in RENAMED_LIMS_STUDY_FIELDS.items():
                    if old_field not in row:
                        continue
                    old_value = row.pop(old_field)
                    if is_blank(row.get(new_field)):
                        row[new_field] = old_value
                    is_changed = True
            if is_changed:
                changed_count += 1
                if commit:
                    collection.update_one(
                        {'_id': record['_id']}, {'$set': {'components.study': study_rows}}
                    )
        return changed_count

    # --- Part 5: Sapio project ---
    def upgrade_lims_project(self, lims, changes, commit):
        profile = changes.profile
        if not profile.get(PROFILE_SAPIO_PROJECT_ID):
            changes.notes.append('no Sapio project to upgrade')
            return

        result = lims.upgrade_project(profile, dry_run=not commit)
        if result.get('status') != 'success':
            changes.notes.append(f"Sapio not upgraded: {result.get('message', '')}")
            return

        if result.get('department') == 'linked':
            changes.notes.append('Sapio project linked to its department')
        elif result.get('department') == 'department_not_found':
            changes.notes.append("Sapio department not found so, the project isn't linked to it")
        if result.get('container_types_fixed'):
            changes.notes.append(f"{result['container_types_fixed']} Sapio sample container type(s) put in title case")
        if result.get('well_elements_added') or result.get('well_elements_removed'):
            changes.notes.append(
                f"Sapio plate well elements: {result.get('well_elements_added', 0)} added, "
                f"{result.get('well_elements_removed', 0)} removed"
            )

        # Link batches that aren't linked to their Sapio samples yet
        # (i.e. those moved into a batch in part 3)
        batches = profile.get(PROFILE_SAMPLE_BATCHES) or []
        record_ids = result.get('batch_sample_record_ids') or {}
        if any(batch.get(BATCH_LIMS_SAMPLE_RECORD_IDS) is None for batch in batches):
            linked_batches = [
                {
                    **batch,
                    BATCH_LIMS_SAMPLE_RECORD_IDS: record_ids.get(batch[BATCH_ID], [])
                    if batch.get(BATCH_LIMS_SAMPLE_RECORD_IDS) is None
                    else batch[BATCH_LIMS_SAMPLE_RECORD_IDS],
                }
                for batch in batches
            ]
            changes.set(PROFILE_SAMPLE_BATCHES, linked_batches)
            changes.notes.append(
                'batches linked to their Sapio samples: '
                + ', '.join(
                    f"batch {number}: {len(batch[BATCH_LIMS_SAMPLE_RECORD_IDS])} sample(s)"
                    for number, batch in enumerate(linked_batches, start=1)
                )
            )

    # --- Part 6: missing values ---
    def report_missing_values(self, changes):
        missing_labels = [
            label for field, label in REQUIRED_PROFILE_FIELDS.items() if is_blank(changes.profile.get(field))
        ]
        if missing_labels:
            changes.notes.append(
                f"needs filling in by editing the profile: {', '.join(missing_labels)}"
            )

    def report(self, profile, changes, lims_manifest_count):
        self.stdout.write(f"'{profile.get('title')}' ({profile['_id']})")
        notes = list(changes.notes)
        if lims_manifest_count:
            notes.append(f'{lims_manifest_count} saved LIMS manifest(s): customer_email -> user_email')
        for note in notes or ['up to date']:
            self.stdout.write(f'  - {note}')
