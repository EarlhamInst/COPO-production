'''Unit tests for the feedback from deleting a profile.

Deleting an EDP profile in COPO also deletes its Sapio project, via the
profile type's post-delete action. When that failed, the action's warning was
discarded, so the user saw "Profile deleted!" while the Sapio project was left
behind with nothing in COPO pointing to it. The warning now reaches the page.
'''

from unittest import mock

import pytest
from bson import ObjectId

from common.dal import profile_da
from common.dal.profile_da import Profile

PROFILE_ID = str(ObjectId())
SAPIO_WARNING = {
    'status': 'warning',
    'message': 'Profile has been deleted but it failed to be removed from Sapio.',
}


@pytest.fixture
def delete():
    # Profile.validate_and_delete with the database and profile types stubbed
    # out, for an EDP profile whose post-delete action returns `post_delete`
    with mock.patch.object(profile_da, 'handle_dict') as handle_dict, \
            mock.patch.object(profile_da, 'ProfileType') as profile_type, \
            mock.patch.object(profile_da, 'get_class') as get_class, \
            mock.patch.object(Profile, 'get_record', return_value={'_id': PROFILE_ID, 'type': 'ei_edp'}), \
            mock.patch.object(Profile, 'get_collection_handle') as collection:
        profile_type.objects.filter.return_value.first.return_value.post_delete_action = (
            'src.apps.ei_edp.utils.edp_utils.post_delete_edp_profile'
        )

        def run(references=0, post_delete=None):
            handle_dict.__getitem__.return_value.count_documents.return_value = references
            get_class.return_value.post_delete_edp_profile.return_value = post_delete
            return Profile().validate_and_delete(PROFILE_ID), collection.return_value

        yield run


def test_deleted(delete):
    result, collection = delete(post_delete={'status': 'success'})

    assert result == {'status': 'success', 'message': 'Profile deleted!'}
    collection.delete_one.assert_called_once()


def test_sapio_warning_reaches_the_page(delete):
    result, collection = delete(post_delete=SAPIO_WARNING)

    assert result == SAPIO_WARNING
    collection.delete_one.assert_called_once()


def test_profile_with_samples_or_manifests_is_not_deleted(delete):
    result, collection = delete(references=1)

    assert result['status'] == 'error'
    collection.delete_one.assert_not_called()
