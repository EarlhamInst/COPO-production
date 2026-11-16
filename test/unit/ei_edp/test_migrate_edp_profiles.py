'''Unit tests for the command that brings existing EDP profiles up to date.

The command renames and removes old profile fields, moves the sample fields
into a batch, renames old fields in saved LIMS manifests, upgrades the Sapio
project and reports values that someone has to fill in. It only saves the
changes when it is given --commit.
'''

from io import StringIO
from unittest import mock

import pytest
from bson import ObjectId
from django.core.management import call_command

from src.apps.copo_core.management.commands import migrate_edp_profiles as command_module

PROJECT_ID = 'PR0001'


def old_profile(**fields):
    return {
        '_id': ObjectId(),
        'title': 'Old profile',
        'type': 'ei_edp',
        'sapio_project_id': PROJECT_ID,
        'jira_ticket_number': 'ENQ-1',
        'budget_user': 'Budget person',
        'customer_emails': 'user@earlham.ac.uk',
        'tube_or_plate': 'plate',
        'plates': ['P1'],
        'no_of_samples': '2',
        'sample_type': 'Tissue',
        **fields,
    }


def run(profile, *args, lims_result=None, lims_records=()):
    profile_collection = mock.Mock()
    profile_collection.find.return_value = [profile]
    singlecell_collection = mock.Mock()
    singlecell_collection.find.return_value = list(lims_records)
    lims = mock.Mock()
    lims.upgrade_project.side_effect = lambda p, dry_run: lims_result or {
        'status': 'success',
        'department': 'linked',
        'container_types_fixed': 2,
        'well_elements_added': 2,
        'well_elements_removed': 0,
        'batch_sample_record_ids': {b['batch_id']: [11, 12] for b in p['sample_batches']},
    }
    output = StringIO()
    with mock.patch.object(command_module, 'Profile') as profile_da, \
            mock.patch.object(command_module, 'Singlecell') as singlecell_da, \
            mock.patch('src.apps.ei_edp.utils.lims.get_lims_adapter', return_value=lims):
        profile_da.return_value.get_collection_handle.return_value = profile_collection
        singlecell_da.return_value.get_collection_handle.return_value = singlecell_collection
        call_command('migrate_edp_profiles', *args, stdout=output)
    return output.getvalue(), profile_collection, singlecell_collection, lims


def saved_update(profile_collection):
    [(_, update), _] = profile_collection.update_one.call_args
    return update


def test_dry_run_reports_but_saves_nothing():
    output, profile_collection, singlecell_collection, lims = run(old_profile())

    assert 'jira_ticket_number -> related_enq' in output
    assert 'Dry run: would update 1 of 1' in output
    profile_collection.update_one.assert_not_called()
    singlecell_collection.update_one.assert_not_called()
    assert lims.upgrade_project.call_args.kwargs == {'dry_run': True}


def test_commit_renames_removes_and_moves_fields_into_a_linked_batch():
    output, profile_collection, _, lims = run(old_profile(), '--commit')

    update = saved_update(profile_collection)
    assert update['$set']['related_enq'] == 'ENQ-1'
    assert update['$set']['budget_holder'] == 'Budget person'
    assert update['$set']['user_email'] == 'user@earlham.ac.uk'
    [batch] = update['$set']['sample_batches']
    assert batch['no_of_samples'] == 2
    assert batch['sample_type'] == 'Tissue'
    assert batch['container_type'] == 'Plate'
    assert batch['lims_sample_record_ids'] == [11, 12]
    # The container type was renamed then moved into the batch so,
    # only the fields saved under their old names are unset
    assert set(update['$unset']) == {
        'jira_ticket_number', 'budget_user', 'customer_emails', 'tube_or_plate',
        'plates', 'no_of_samples', 'sample_type',
    }
    assert lims.upgrade_project.call_args.kwargs == {'dry_run': False}
    assert 'Sapio project linked to its department' in output
    assert 'Sapio plate well elements: 2 added, 0 removed' in output


def test_newer_value_is_kept_over_an_old_one():
    output, profile_collection, _, _ = run(old_profile(related_enq='ENQ-2'), '--commit', '--skip-lims')

    update = saved_update(profile_collection)
    assert 'related_enq' not in update['$set']
    assert 'jira_ticket_number' in update['$unset']
    assert "kept the newer related_enq value 'ENQ-2'" in output


def test_skip_lims_leaves_sapio_alone():
    output, profile_collection, _, lims = run(old_profile(), '--commit', '--skip-lims')

    lims.upgrade_project.assert_not_called()
    [batch] = saved_update(profile_collection)['$set']['sample_batches']
    assert batch['lims_sample_record_ids'] is None  # linked by the next Sapio sync


def test_saved_lims_manifests_are_renamed():
    record = {'_id': 'r1', 'components': {'study': [{'study_id': PROJECT_ID, 'customer_email': 'a@b.c'}]}}

    output, _, singlecell_collection, _ = run(old_profile(), '--commit', '--skip-lims', lims_records=[record])

    singlecell_collection.update_one.assert_called_once_with(
        {'_id': 'r1'}, {'$set': {'components.study': [{'study_id': PROJECT_ID, 'user_email': 'a@b.c'}]}}
    )
    assert '1 saved LIMS manifest(s): customer_email -> user_email' in output


def test_up_to_date_profile_is_left_alone():
    profile = {
        '_id': ObjectId(), 'title': 'New profile', 'type': 'ei_edp', 'sapio_project_id': PROJECT_ID,
        'related_enq': 'ENQ-1', 'related_pip': 'PIP-1', 'budget_holder': 'Budget person',
        'sample_batches': [{'batch_id': 'b1', 'no_of_samples': 1, 'lims_sample_record_ids': [5]}],
    }
    lims_result = {'status': 'success', 'department': 'already_linked', 'batch_sample_record_ids': {'b1': [5]}}

    output, profile_collection, _, _ = run(profile, '--commit', lims_result=lims_result)

    profile_collection.update_one.assert_not_called()
    assert 'up to date' in output
    assert 'Updated 0 of 1' in output


def test_values_that_must_be_filled_in_are_reported():
    output, _, _, _ = run(old_profile(), '--skip-lims')

    assert 'needs filling in by editing the profile: Jira PIP' in output


def test_sapio_failure_is_reported_and_copo_is_still_updated():
    output, profile_collection, _, _ = run(
        old_profile(), '--commit', lims_result={'status': 'error', 'message': 'Sapio Project PR0001 not found.'}
    )

    assert 'Sapio not upgraded: Sapio Project PR0001 not found.' in output
    assert saved_update(profile_collection)['$set']['related_enq'] == 'ENQ-1'
