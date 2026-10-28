'''Unit tests for sharing an EDP profile by email.

Saving an EDP profile shares it with the customer email addresses and emails
each of them. A failed email used to raise straight out of the save: the
profile was already stored in COPO, but the Sapio sync that runs next never
happened, and the user only saw "Errors occurred". Now every address ends up
as one of:

- sent
- skipped: email isn't set up on this server (e.g. CI). The save still
  succeeds, with a note that no email was sent.
- failed: sending failed. The save still syncs Sapio, but reports a warning
  naming the addresses that weren't emailed.
'''

from unittest import mock

import pytest
from bson import ObjectId

from src.apps.ei_edp.utils import edp_utils
from src.apps.ei_edp.utils import email as email_module
from src.apps.ei_edp.utils.email import Email

PROFILE = {'_id': ObjectId(), 'title': 'Customer project', 'user_id': 1}
USERS = [
    {'id': 2, 'email': 'good@earlham.ac.uk', 'first_name': 'Good'},
    {'id': 3, 'email': 'bad@earlham.ac.uk', 'first_name': 'Bad'},
]


def send_fails_for(*bad_addresses):
    # CopoEmail.send returns True once it has sent the email
    def send(to, **kwargs):
        if to[0] in bad_addresses:
            raise RuntimeError('550 mailbox unavailable')
        return True
    return send


@pytest.fixture
def copo_email():
    with mock.patch.object(email_module, 'CopoEmail') as copo_email, \
            mock.patch.object(email_module, 'logger'):
        copo_email.return_value.send.return_value = True
        yield copo_email.return_value


def test_existing_users_all_emailed(copo_email):
    email = Email()
    email.notify_shared_profile_to_existing_user(PROFILE, USERS)

    assert copo_email.send.call_count == 2
    assert email.failed_emails == []
    assert email.skipped_emails == []


def test_one_failed_email_does_not_stop_the_others(copo_email):
    copo_email.send.side_effect = send_fails_for('bad@earlham.ac.uk')

    email = Email()
    email.notify_shared_profile_to_existing_user(PROFILE, USERS)

    assert copo_email.send.call_count == 2
    assert email.failed_emails == ['bad@earlham.ac.uk']


def test_failed_invitation_to_new_user_is_recorded(copo_email):
    copo_email.send.side_effect = send_fails_for('new@earlham.ac.uk')
    tokens = {'token1': 'new@earlham.ac.uk', 'token2': 'other@earlham.ac.uk'}

    email = Email()
    with mock.patch.object(Email, '_get_uri', return_value='https://copo.test'), \
            mock.patch.object(email_module, 'reverse', return_value='/join'):
        email.notify_shared_profile_to_non_existent_user(PROFILE, tokens)

    assert email.failed_emails == ['new@earlham.ac.uk']


def test_emails_are_skipped_when_email_is_not_set_up(copo_email):
    # CopoEmail.send returns False when the server has no mail server
    copo_email.send.return_value = False

    email = Email()
    email.notify_shared_profile_to_existing_user(PROFILE, USERS)

    assert email.skipped_emails == ['good@earlham.ac.uk', 'bad@earlham.ac.uk']
    assert email.failed_emails == []


# --- The profile save carries on and reports what wasn't emailed ---

@pytest.fixture
def save():
    # post_save_edp_profile with COPO's groups, users and database stubbed
    # out, for a profile shared with one address that has no COPO account
    with mock.patch.object(edp_utils, 'helpers'), \
            mock.patch.object(edp_utils, 'CopoGroup') as copo_group, \
            mock.patch.object(edp_utils, 'User') as user, \
            mock.patch.object(edp_utils, 'Profile'), \
            mock.patch.object(edp_utils, '_add_to_edp_django_group'), \
            mock.patch.object(edp_utils, 'Email') as email, \
            mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter:
        copo_group.return_value.get_group_by_profile.return_value = [{'_id': 'group1'}]
        copo_group.return_value.get_users_for_group_info.return_value = []
        user.objects.filter.return_value.values.return_value = []
        lims = get_lims_adapter.return_value
        lims.sync_project.return_value = {'status': 'success', 'project_id': 'PR0001'}

        def run(failed_emails=(), skipped_emails=(), sync_result=None):
            email.return_value.failed_emails = list(failed_emails)
            email.return_value.skipped_emails = list(skipped_emails)
            if sync_result:
                lims.sync_project.return_value = sync_result
            profile = {**PROFILE, 'customer_emails': 'new@earlham.ac.uk', 'sapio_project_id': 'PR0001'}
            return edp_utils.post_save_edp_profile(profile), lims

        yield run


FAILED_MESSAGE = (
    'Profile has been saved but, it could not be shared to the following email addresses:<br>'
    '<ul><li>new@earlham.ac.uk</li></ul>Please check the email addresses or contact the COPO team.'
)
SKIPPED_MESSAGE = (
    'An email address was not set up on this server so, an email was not sent about the '
    'created profile to:<br><ul><li>new@earlham.ac.uk</li></ul>'
)


def test_success_when_every_email_is_sent(save):
    result, lims = save()

    assert result == {'status': 'success'}
    lims.sync_project.assert_called_once()


def test_skipped_email_is_still_a_success_with_a_note(save):
    # e.g. CI, where email isn't set up
    result, lims = save(skipped_emails=['new@earlham.ac.uk'])

    lims.sync_project.assert_called_once()
    assert result == {'status': 'success', 'message': SKIPPED_MESSAGE}


def test_failed_email_still_syncs_sapio_and_warns(save):
    result, lims = save(failed_emails=['new@earlham.ac.uk'])

    lims.sync_project.assert_called_once()
    assert result == {'status': 'warning', 'message': FAILED_MESSAGE}


def test_sapio_failure_and_failed_email_are_both_reported(save):
    result, _ = save(
        failed_emails=['new@earlham.ac.uk'],
        sync_result={'status': 'warning', 'project_id': 'PR0001', 'message': 'Sapio sync failed.'},
    )

    assert result == {'status': 'warning', 'message': 'Sapio sync failed. ' + FAILED_MESSAGE}


def test_sapio_failure_is_not_hidden_by_a_skipped_email(save):
    result, _ = save(
        skipped_emails=['new@earlham.ac.uk'],
        sync_result={'status': 'warning', 'project_id': 'PR0001', 'message': 'Sapio sync failed.'},
    )

    assert result == {'status': 'warning', 'message': 'Sapio sync failed. ' + SKIPPED_MESSAGE}


def test_only_newly_added_users_are_emailed():
    # Saving the profile again shouldn't re-email users it is already shared with
    with mock.patch.object(edp_utils, 'helpers') as helpers, \
            mock.patch.object(edp_utils, 'CopoGroup') as copo_group, \
            mock.patch.object(edp_utils, 'User') as user, \
            mock.patch.object(edp_utils, 'Profile'), \
            mock.patch.object(edp_utils, '_add_to_edp_django_group'), \
            mock.patch.object(edp_utils, 'Email') as email, \
            mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter:
        helpers.get_current_user.return_value.email = 'owner@earlham.ac.uk'
        copo_group.return_value.get_group_by_profile.return_value = [{'_id': 'group1'}]
        copo_group.return_value.get_users_for_group_info.return_value = [USERS[0]]
        user.objects.filter.return_value.values.return_value = USERS
        email.return_value.failed_emails = []
        email.return_value.skipped_emails = []
        get_lims_adapter.return_value.sync_project.return_value = {'status': 'success'}

        profile = {**PROFILE, 'customer_emails': 'good@earlham.ac.uk;bad@earlham.ac.uk'}
        edp_utils.post_save_edp_profile(profile)

    copo_group.return_value.add_users_to_group.assert_called_once_with(
        group_id='group1', user_ids=['3']
    )
    _, emailed_users = email.return_value.notify_shared_profile_to_existing_user.call_args.args
    assert list(emailed_users) == [USERS[1]]


# --- Email addresses are matched case-insensitively ---

@pytest.fixture
def share():
    # post_save_edp_profile saved by owner@earlham.ac.uk, with COPO's groups,
    # users and database stubbed out
    with mock.patch.object(edp_utils, 'helpers') as helpers, \
            mock.patch.object(edp_utils, 'CopoGroup') as copo_group, \
            mock.patch.object(edp_utils, 'User') as user, \
            mock.patch.object(edp_utils, 'Profile'), \
            mock.patch.object(edp_utils, '_add_to_edp_django_group'), \
            mock.patch.object(edp_utils, 'Email') as email, \
            mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter:
        helpers.get_current_user.return_value.email = 'owner@earlham.ac.uk'
        copo_group.return_value.get_group_by_profile.return_value = [{'_id': 'group1'}]
        email.return_value.failed_emails = []
        email.return_value.skipped_emails = []
        get_lims_adapter.return_value.sync_project.return_value = {'status': 'success'}

        def run(customer_emails, existing_users=(), shared_users=()):
            copo_group.return_value.get_users_for_group_info.return_value = list(shared_users)
            user.objects.filter.return_value.values.return_value = list(existing_users)
            profile = {**PROFILE, 'customer_emails': customer_emails}
            edp_utils.post_save_edp_profile(profile)
            return email.return_value, copo_group.return_value

        yield run


def test_owner_entering_their_email_in_different_case_is_not_emailed(share):
    owner = {'id': 1, 'email': 'owner@earlham.ac.uk', 'first_name': 'Owner'}

    email, copo_group = share('Owner@Earlham.ac.uk', existing_users=[owner])

    email.notify_shared_profile_to_non_existent_user.assert_not_called()
    email.notify_shared_profile_to_existing_user.assert_not_called()
    copo_group.add_users_to_group.assert_not_called()


def test_existing_user_entered_in_different_case_is_not_invited_to_create_an_account(share):
    email, copo_group = share('GOOD@earlham.ac.uk', existing_users=[USERS[0]])

    email.notify_shared_profile_to_non_existent_user.assert_not_called()
    copo_group.add_users_to_group.assert_called_once_with(group_id='group1', user_ids=['2'])


def test_shared_user_entered_in_different_case_stays_in_the_group(share):
    _, copo_group = share(
        'Good@Earlham.ac.uk', existing_users=[USERS[0]], shared_users=[USERS[0]]
    )

    copo_group.remove_users_from_group.assert_called_once_with(group_id='group1', user_ids=[])


def test_same_new_address_in_different_cases_is_invited_once(share):
    email, _ = share('new@earlham.ac.uk; NEW@earlham.ac.uk')

    _, tokens = email.notify_shared_profile_to_non_existent_user.call_args.args
    assert len(tokens) == 1


def test_new_user_is_invited_at_the_address_as_entered(share):
    email, _ = share('New.User@Earlham.ac.uk')

    _, tokens = email.notify_shared_profile_to_non_existent_user.call_args.args
    assert list(tokens.values()) == ['New.User@Earlham.ac.uk']


def test_blank_customer_emails_do_not_crash_the_save(share):
    email, copo_group = share(' ; ')

    email.notify_shared_profile_to_non_existent_user.assert_not_called()
    email.notify_shared_profile_to_existing_user.assert_not_called()
    copo_group.add_users_to_group.assert_not_called()


# --- Joining a shared profile from an invite link ---

@pytest.fixture
def join():
    # join_shared_edp_profile for a logged-in user with id 2, with COPO's
    # groups stubbed out
    with mock.patch.object(edp_utils, 'helpers') as helpers, \
            mock.patch.object(edp_utils, 'CopoGroup') as copo_group, \
            mock.patch.object(edp_utils, '_add_to_edp_django_group'):
        copo_group.return_value.get_group_by_profile.return_value = [{'_id': 'group1'}]

        def run(user_email, customer_emails):
            helpers.get_current_user.return_value.id = 2
            helpers.get_current_user.return_value.email = user_email
            profile = {**PROFILE, 'type': 'ei_edp', 'customer_emails': customer_emails}
            return edp_utils.join_shared_edp_profile(profile, 'token1'), copo_group.return_value

        yield run


def test_user_whose_email_differs_in_case_can_join(join):
    result, copo_group = join('good@earlham.ac.uk', 'Good@Earlham.ac.uk')

    assert result == {'status': 'success'}
    copo_group.add_user_to_group.assert_called_once_with(group_id='group1', user_id='2')


def test_user_not_in_customer_emails_cannot_join(join):
    result, copo_group = join('other@earlham.ac.uk', 'Good@Earlham.ac.uk')

    assert result['status'] == 'error'
    copo_group.add_user_to_group.assert_not_called()
