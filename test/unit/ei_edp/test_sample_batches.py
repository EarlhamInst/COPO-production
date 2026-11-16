'''Unit tests for sample batches under EDP profiles.

Samples are registered under an EDP profile in batches, each with its own
number of samples, sample type, container type and library type. A new profile
is only created along with its first batch. A batch can be changed until any of
its samples have been submitted (their `C_CustomerSampleName` field is set in
Sapio). If the Sapio sync fails, nothing is kept: a new profile is removed
along with its partly created Sapio project, and a batch change is undone.
'''

from unittest import mock

import pytest
from bson import ObjectId

from src.apps.ei_edp.utils import edp_utils, sample_batches
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES
from src.apps.ei_edp.utils.lims.sapio import adapter as sapio_adapter
from src.apps.ei_edp.utils.lims.sapio.adapter import SapioAdapter, _group_samples_by_batch

PROJECT_ID = 'PR0001'
TISSUE = {
    'no_of_samples': '3', 'sample_type': 'Tissue', 'container_type': 'plate', 'library_type': '',
    'related_pip': 'PIP-1',
}


def batch(batch_id, record_ids, **fields):
    return {
        'batch_id': batch_id,
        'no_of_samples': len(record_ids or []),
        'sample_type': 'Tissue',
        'container_type': 'Plate',
        'library_type': '',
        'lims_sample_record_ids': record_ids,
        **fields,
    }


# --- Checking the fields that users enter ---

def test_valid_fields_are_tidied_up():
    cleaned, errors = sample_batches.clean_batch_fields(
        {'no_of_samples': ' 3 ', 'sample_type': ' Tissue ', 'container_type': 'plate', 'library_type': '',
         'related_pip': ' pip-12 '}
    )

    assert errors == []
    assert cleaned == {
        'no_of_samples': 3,
        'sample_type': 'Tissue',
        'container_type': 'Plate',
        'library_type': '',
        'related_pip': 'PIP-12',
    }


def test_jira_pip_is_required():
    _, errors = sample_batches.clean_batch_fields({**TISSUE, 'related_pip': ' '})

    assert errors == [MESSAGES['missing_batch_field'].format(field='Jira PIP')]


@pytest.mark.parametrize('pip', ['1234', 'PIP1234', 'ENQ-1234', 'PIP-12a'])
def test_jira_pip_must_look_like_a_pip(pip):
    _, errors = sample_batches.clean_batch_fields({**TISSUE, 'related_pip': pip})

    assert errors == [MESSAGES['invalid_batch_pip'].format(pip=pip.upper())]


@pytest.mark.parametrize('raw', ['', '0', '-1', '2.5', 'abc', None])
def test_number_of_samples_must_be_1_or_more(raw):
    _, errors = sample_batches.clean_batch_fields({**TISSUE, 'no_of_samples': raw})

    assert errors == [MESSAGES['invalid_batch_sample_count'].format(sample_count=raw)]


@pytest.mark.parametrize('field, label', [
    ('sample_type', 'Sample type'),
    ('container_type', 'Container type'),
])
def test_sample_and_container_types_are_required(field, label):
    _, errors = sample_batches.clean_batch_fields({**TISSUE, field: ''})

    assert errors == [MESSAGES['missing_batch_field'].format(field=label)]


def test_library_type_is_dropped_for_other_sample_types():
    # The field is hidden for other sample types so, a value left over is ignored
    cleaned, errors = sample_batches.clean_batch_fields(
        {**TISSUE, 'library_type': 'PacBio premade library'}
    )

    assert errors == []
    assert cleaned['library_type'] == ''


@pytest.mark.parametrize('sample_type', ['Pre-made library', 'pre-made library pool'])
def test_library_type_is_required_for_premade_libraries(sample_type):
    _, errors = sample_batches.clean_batch_fields({**TISSUE, 'sample_type': sample_type})
    assert errors == [MESSAGES['missing_batch_field'].format(field='Library type')]

    cleaned, errors = sample_batches.clean_batch_fields(
        {**TISSUE, 'sample_type': sample_type, 'library_type': 'Illumina premade library'}
    )
    assert errors == []
    assert cleaned['library_type'] == 'Illumina premade library'


# --- Reading a profile's batches ---

def test_profile_fields_are_read_as_one_batch():
    # i.e. a profile saved before batches existed
    batches = sample_batches.get_sample_batches({**TISSUE, 'library_type': 'ignored'})

    assert len(batches) == 1
    assert batches[0]['no_of_samples'] == 3
    assert batches[0]['container_type'] == 'Plate'
    assert batches[0]['library_type'] == ''
    # Linked to its Sapio samples by the next sync
    assert batches[0]['lims_sample_record_ids'] is None


def test_profile_without_batches_or_fields_has_no_batches():
    assert sample_batches.get_sample_batches({'title': 'p'}) == []
    assert sample_batches.get_requested_sample_count({'title': 'p'}) == 0


def test_requested_sample_count_adds_up_every_batch():
    profile = {'sample_batches': [batch('b1', [1, 2]), batch('b2', [3, 4, 5])]}

    assert sample_batches.get_requested_sample_count(profile) == 5


def test_profile_fields_are_moved_into_a_batch():
    profile = {'_id': 'p1', **TISSUE}
    collection = mock.Mock()

    assert sample_batches.move_profile_fields_into_batch(profile, collection)

    (query, update), _ = collection.update_one.call_args
    assert query == {'_id': 'p1'}
    assert update['$unset'] == {
        'no_of_samples': '', 'sample_type': '', 'container_type': '', 'library_type': '',
        'related_pip': '',
    }
    assert update['$set']['sample_batches'][0]['sample_type'] == 'Tissue'
    assert update['$set']['sample_batches'][0]['related_pip'] == 'PIP-1'
    assert 'sample_type' not in profile
    assert 'related_pip' not in profile
    assert len(profile['sample_batches']) == 1


def test_a_jira_pip_alone_is_not_moved_into_a_batch():
    # The Jira PIP used to be a profile field so, it doesn't mean the profile
    # has samples to register
    collection = mock.Mock()
    profile = {'_id': 'p1', 'related_pip': 'PIP-1'}

    assert not sample_batches.move_profile_fields_into_batch(profile, collection)
    collection.update_one.assert_not_called()


def test_batches_saved_before_each_had_a_jira_pip_use_the_profiles():
    profile = {
        '_id': 'p1',
        'related_pip': 'PIP-1',
        'sample_batches': [batch('b1', [1]), batch('b2', [2], related_pip='PIP-2')],
    }

    assert [b['related_pip'] for b in sample_batches.get_sample_batches(profile)] == ['PIP-1', 'PIP-2']
    # The saved batches are left as they are
    assert 'related_pip' not in profile['sample_batches'][0]


def test_profile_with_batches_is_not_moved_again():
    collection = mock.Mock()
    profile = {'_id': 'p1', **TISSUE, 'sample_batches': [batch('b1', [1])]}

    assert not sample_batches.move_profile_fields_into_batch(profile, collection)
    collection.update_one.assert_not_called()


# --- Pairing batches with their Sapio samples ---

class FakeSample:
    def __init__(self, record_id, customer_name=''):
        self.record_id = record_id
        self.customer_name = customer_name

    def get_field_value(self, field):
        return self.customer_name if field == 'C_CustomerSampleName' else None


def test_moved_batch_claims_the_samples_no_other_batch_has():
    samples = [FakeSample(record_id) for record_id in (1, 2, 3, 4)]
    batches = [batch('old', None), batch('new', [4])]

    paired = {
        b['batch_id']: [sample.record_id for sample in batch_samples]
        for b, batch_samples in _group_samples_by_batch(batches, samples)
    }

    assert paired == {'old': [1, 2, 3], 'new': [4]}


def test_batch_samples_missing_from_sapio_are_left_out():
    samples = [FakeSample(1)]

    [(_, batch_samples)] = _group_samples_by_batch([batch('b1', [1, 2])], samples)

    assert [sample.record_id for sample in batch_samples] == [1]


def adapter_with_project(samples):
    project = mock.Mock()
    project.get_children_of_type.return_value = samples
    sapio = mock.Mock()
    sapio.return_value.dataRecordManager.query_data_records.return_value.result_list = (
        [object()] if samples is not None else []
    )
    sapio.return_value.inst_man.add_existing_record.return_value = project
    return sapio


def test_submitted_samples_are_counted_per_batch():
    samples = [FakeSample(1, 'cust1'), FakeSample(2), FakeSample(3), FakeSample(4)]
    profile = {
        'sapio_project_id': PROJECT_ID,
        'sample_batches': [batch('b1', [1, 2]), batch('b2', [3, 4])],
    }

    with mock.patch.object(sapio_adapter, 'Sapio', adapter_with_project(samples)):
        counts = SapioAdapter.__new__(SapioAdapter).get_submitted_sample_counts(profile)

    assert counts == {'b1': {'submitted': 1, 'total': 2}, 'b2': {'submitted': 0, 'total': 2}}


def test_missing_sapio_project_is_an_error():
    with mock.patch.object(sapio_adapter, 'Sapio', adapter_with_project(None)):
        result = SapioAdapter.__new__(SapioAdapter).validate_profile_change(
            {'sapio_project_id': PROJECT_ID}
        )

    assert result == {
        'status': 'error',
        'message': MESSAGES['sapio_project_not_found'].format(project_id=PROJECT_ID),
    }


# --- Removing samples from a batch ---

def resize(batch_samples, no_of_samples):
    project = mock.Mock()
    created = []
    return SapioAdapter.__new__(SapioAdapter)._resize_batch(
        project, [], batch('b1', [], no_of_samples=no_of_samples), batch_samples, created
    ), project


def test_excess_samples_are_removed_from_the_end():
    samples = [FakeSample(1), FakeSample(2), FakeSample(3)]

    (kept, removed), project = resize(samples, 1)

    assert [sample.record_id for sample in kept] == [1]
    assert {sample.record_id for sample in removed} == {2, 3}
    project.remove_children.assert_called_once()


def test_submitted_samples_are_never_removed():
    samples = [FakeSample(1), FakeSample(2, 'cust2'), FakeSample(3, 'cust3')]

    (kept, removed), _ = resize(samples, 2)

    assert [sample.record_id for sample in kept] == [2, 3]
    assert [sample.record_id for sample in removed] == [1]


# --- Saving a profile ---

@pytest.fixture
def pre_save():
    # Saved by an EDP manager unless a test says otherwise
    with mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter, \
            mock.patch.object(edp_utils, 'is_edp_manager', return_value=True), \
            mock.patch.object(edp_utils, 'Profile') as profile_da:
        profile_da.return_value.get_record.return_value = {'sapio_project_id': PROJECT_ID}
        lims = get_lims_adapter.return_value
        lims.validate_profile_change.return_value = {'status': 'success'}
        yield lims


def form_fields(**fields):
    return {f'copo.profile.{field}': value for field, value in fields.items()}


def test_new_profile_needs_its_first_batch(pre_save):
    result = edp_utils.pre_save_edp_profile(form_fields(title='p'))

    assert result['status'] == 'error'
    assert result['message'].startswith(MESSAGES['first_batch_required'])
    pre_save.validate_profile_change.assert_not_called()


def test_new_profile_first_batch_is_tidied_up(pre_save):
    auto_fields = form_fields(**TISSUE)

    assert edp_utils.pre_save_edp_profile(auto_fields) == {'status': 'success'}
    assert auto_fields['copo.profile.container_type'] == 'Plate'
    pre_save.validate_profile_change.assert_not_called()


def test_profile_form_cannot_change_an_existing_profiles_batches(pre_save):
    auto_fields = form_fields(title='p', **TISSUE)

    assert edp_utils.pre_save_edp_profile(auto_fields, target_id='p1') == {'status': 'success'}
    assert auto_fields == {'copo.profile.title': 'p'}
    pre_save.validate_profile_change.assert_called_once_with({'sapio_project_id': PROJECT_ID})


@pytest.fixture
def post_save():
    # post_save_edp_profile with the database, emails and Sapio stubbed out
    with mock.patch.object(edp_utils, 'Profile') as profile_da, \
            mock.patch.object(edp_utils, '_share_profile') as share_profile, \
            mock.patch.object(edp_utils, 'Email') as email, \
            mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter:
        email.return_value.failed_emails = []
        email.return_value.skipped_emails = []
        lims = get_lims_adapter.return_value
        collection = profile_da.return_value.get_collection_handle.return_value

        def run(sync_result, **kwargs):
            lims.sync_project.return_value = sync_result
            lims.delete_project.return_value = {'status': 'success'}
            profile = {'_id': ObjectId(), 'user_email': 'new@earlham.ac.uk', **TISSUE}
            result = edp_utils.post_save_edp_profile(profile, **kwargs)
            return result, profile, lims, collection, share_profile

        yield run


def test_new_profile_is_synced_then_shared(post_save):
    result, profile, lims, collection, share_profile = post_save(
        {'status': 'success', 'project_id': PROJECT_ID, 'batch_sample_record_ids': {}},
        new_record=True,
    )
    batch_id = profile['sample_batches'][0]['batch_id']

    assert result == {'status': 'success'}
    synced_profile = lims.sync_project.call_args.args[0]
    assert [b['batch_id'] for b in synced_profile['sample_batches']] == [batch_id]
    share_profile.assert_called_once()
    collection.delete_one.assert_not_called()


def test_first_batch_is_linked_to_its_sapio_samples(post_save):
    def sync(profile):
        batch_id = profile['sample_batches'][0]['batch_id']
        return {'status': 'success', 'project_id': PROJECT_ID, 'batch_sample_record_ids': {batch_id: [7, 8, 9]}}

    with mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter, \
            mock.patch.object(edp_utils, 'Profile'), \
            mock.patch.object(edp_utils, '_share_profile'), \
            mock.patch.object(edp_utils, 'Email') as email:
        email.return_value.failed_emails = []
        email.return_value.skipped_emails = []
        get_lims_adapter.return_value.sync_project.side_effect = sync
        profile = {'_id': ObjectId(), **TISSUE}
        edp_utils.post_save_edp_profile(profile, new_record=True)

    assert profile['sapio_project_id'] == PROJECT_ID
    assert profile['sample_batches'][0]['lims_sample_record_ids'] == [7, 8, 9]


def test_new_profile_is_removed_when_sapio_fails(post_save):
    result, profile, lims, collection, share_profile = post_save(
        {'status': 'warning', 'project_id': PROJECT_ID, 'message': 'failed'}, new_record=True
    )

    assert result == {'status': 'error', 'message': MESSAGES['sapio_create_failed']}
    lims.delete_project.assert_called_once_with(PROJECT_ID)
    collection.delete_one.assert_called_once_with({'_id': profile['_id']})
    share_profile.assert_not_called()


def test_leftover_sapio_project_is_named_when_it_cannot_be_removed(post_save):
    with mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter, \
            mock.patch.object(edp_utils, 'Profile'):
        lims = get_lims_adapter.return_value
        lims.sync_project.return_value = {'status': 'warning', 'project_id': PROJECT_ID}
        lims.delete_project.return_value = {'status': 'warning', 'message': 'nope'}
        result = edp_utils.post_save_edp_profile({'_id': ObjectId(), **TISSUE}, new_record=True)

    assert result == {
        'status': 'error',
        'message': MESSAGES['sapio_create_cleanup_failed'].format(project_id=PROJECT_ID),
    }


def test_edited_profile_is_restored_when_sapio_fails(post_save):
    previous_record = {'_id': 'p1', 'title': 'before'}
    result, profile, _, collection, share_profile = post_save(
        {'status': 'warning', 'project_id': PROJECT_ID, 'message': 'failed'},
        previous_record=previous_record,
    )

    assert result == {'status': 'error', 'message': MESSAGES['sapio_update_failed_not_saved']}
    collection.replace_one.assert_called_once_with({'_id': profile['_id']}, previous_record)
    collection.delete_one.assert_not_called()
    share_profile.assert_not_called()


# --- Adding and changing batches ---

@pytest.fixture
def batches():
    profile = {
        '_id': ObjectId(),
        'type': 'ei_edp',
        'sapio_project_id': PROJECT_ID,
        'sample_batches': [batch('b1', [1, 2])],
    }
    with mock.patch.object(edp_utils, 'Profile') as profile_da, \
            mock.patch.object(edp_utils, 'Sample') as sample_da, \
            mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter:
        profile_da.return_value.get_record.return_value = profile
        # No samples have been uploaded in COPO
        sample_da.return_value.get_collection_handle.return_value.find.return_value = []
        lims = get_lims_adapter.return_value
        lims.get_submitted_sample_counts.return_value = {'b1': {'submitted': 0, 'total': 2}}
        lims.sync_project.side_effect = lambda p: {
            'status': 'success',
            'project_id': PROJECT_ID,
            'batch_sample_record_ids': {b['batch_id']: [100] for b in p['sample_batches']},
        }
        yield profile, lims, profile_da.return_value.get_collection_handle.return_value


def test_batch_is_added(batches):
    profile, lims, collection = batches

    result = edp_utils.add_sample_batch(str(profile['_id']), {**TISSUE, 'sample_type': 'Total RNA'})

    assert result['status'] == 'success'
    saved_batches = collection.update_one.call_args.args[1]['$set']['sample_batches']
    assert [b['sample_type'] for b in saved_batches] == ['Tissue', 'Total RNA']
    assert saved_batches[1]['lims_sample_record_ids'] == [100]


def test_invalid_batch_is_not_added(batches):
    profile, lims, collection = batches

    result = edp_utils.add_sample_batch(str(profile['_id']), {**TISSUE, 'no_of_samples': '0'})

    assert result['status'] == 'error'
    lims.sync_project.assert_not_called()
    collection.update_one.assert_not_called()


def test_added_batch_is_undone_when_sapio_fails(batches):
    profile, lims, collection = batches
    lims.sync_project.side_effect = None
    lims.sync_project.return_value = {
        'status': 'warning', 'project_id': PROJECT_ID, 'created_sample_record_ids': [5, 6]
    }

    result = edp_utils.add_sample_batch(str(profile['_id']), TISSUE)

    assert result == {'status': 'error', 'message': MESSAGES['batch_saved_but_lims_failed']}
    lims.delete_samples.assert_called_once_with([5, 6])
    collection.update_one.assert_not_called()
    assert [b['batch_id'] for b in profile['sample_batches']] == ['b1']


def test_batch_is_changed_before_its_samples_are_submitted(batches):
    profile, lims, collection = batches

    result = edp_utils.update_sample_batch(
        str(profile['_id']), 'b1', {**TISSUE, 'no_of_samples': '5', 'container_type': 'tube'}
    )

    assert result['status'] == 'success'
    [saved_batch] = collection.update_one.call_args.args[1]['$set']['sample_batches']
    assert saved_batch['no_of_samples'] == 5
    assert saved_batch['container_type'] == 'Tube'


def test_batch_with_submitted_samples_is_locked(batches):
    profile, lims, collection = batches
    lims.get_submitted_sample_counts.return_value = {'b1': {'submitted': 1, 'total': 2}}

    result = edp_utils.update_sample_batch(str(profile['_id']), 'b1', TISSUE)

    assert result == {
        'status': 'error',
        'message': MESSAGES['batch_locked'].format(submitted_sample_count=1, total_sample_count=2),
    }
    lims.sync_project.assert_not_called()


def test_unknown_batch_is_an_error(batches):
    profile, lims, _ = batches

    result = edp_utils.update_sample_batch(str(profile['_id']), 'nope', TISSUE)

    assert result == {'status': 'error', 'message': MESSAGES['batch_not_found']}


# --- Plates ---

class FakePlatedSample(FakeSample):
    def __init__(self, record_id, plate_id=None, column=None, row=None):
        super().__init__(record_id)
        self.fields = {'PlateId': plate_id, 'ColPosition': column, 'RowPosition': row}

    def get_field_value(self, field):
        return self.fields.get(field, super().get_field_value(field))

    def set_field_value(self, field, value):
        self.fields[field] = value


class FakePlate:
    def __init__(self, plate_id, samples=()):
        self.plate_id = plate_id
        self.samples = list(samples)

    def get_field_value(self, field):
        return self.plate_id if field == 'PlateId' else None

    def get_children_of_type(self, data_type):
        return list(self.samples)

    def add_children(self, samples):
        self.samples.extend(samples)

    def remove_children(self, samples):
        self.samples = [sample for sample in self.samples if sample not in samples]


def test_batch_samples_only_fill_their_own_batchs_plates():
    # Plate P1 holds another batch's sample. Plate P2 holds one of this batch's
    # samples so, the new samples go on P2 rather than P1.
    other_batch_sample = FakePlatedSample(1, 'P1', '1', 'A')
    placed_sample = FakePlatedSample(2, 'P2', '1', 'A')
    new_samples = [FakePlatedSample(3), FakePlatedSample(4)]
    plates = [FakePlate('P1', [other_batch_sample]), FakePlate('P2', [placed_sample])]

    left_unplated = SapioAdapter.__new__(SapioAdapter)._place_batch_on_plates(
        mock.Mock(), plates, [placed_sample, *new_samples], 'Tissue'
    )

    assert not left_unplated
    assert [sample.record_id for sample in plates[0].samples] == [1]
    assert [sample.record_id for sample in plates[1].samples] == [2, 3, 4]
    assert [(s.fields['ColPosition'], s.fields['RowPosition']) for s in new_samples] == [
        ('1', 'B'), ('1', 'C')
    ]


def test_tube_batch_is_taken_off_plates_without_touching_other_batches():
    other_batch_sample = FakePlatedSample(1, 'P1', '1', 'A')
    tube_sample = FakePlatedSample(2, 'P1', '1', 'B')
    plates = [FakePlate('P1', [other_batch_sample, tube_sample])]

    SapioAdapter.__new__(SapioAdapter)._take_batch_off_plates(plates, [tube_sample])

    assert plates[0].samples == [other_batch_sample]
    assert tube_sample.fields == {'PlateId': None, 'ColPosition': None, 'RowPosition': None}
    assert other_batch_sample.fields['PlateId'] == 'P1'


# --- The batch dialog ---

def test_dialog_options_come_from_sapio_and_the_profile_schema():
    schema = [
        {'id': 'copo.profile.container_type', 'option_values': [{'value': 'Tube', 'label': 'Tube'}]},
        {'id': 'copo.profile.library_type', 'option_values': [{'value': 'PacBio', 'label': 'PacBio'}]},
        {'id': 'copo.profile.sample_type', 'option_values': {'callback': {}}},
    ]
    with mock.patch.object(edp_utils, 'Profile') as profile_da, \
            mock.patch.object(edp_utils, 'get_sapio_sample_type_options') as sample_types:
        profile_da.return_value.get_component_schema.return_value = schema
        sample_types.return_value = [{'value': 'Tissue', 'label': 'Tissue'}]
        options = edp_utils.get_sample_batch_form_options()

    assert options['sample_type_options'] == [{'value': 'Tissue', 'label': 'Tissue'}]
    assert options['container_type_options'] == [{'value': 'Tube', 'label': 'Tube'}]
    assert options['library_type_options'] == [{'value': 'PacBio', 'label': 'PacBio'}]
    assert options['premade_library_sample_types'] == ['Pre-made library', 'Pre-made library pool']


def test_dialog_field_labels_come_from_the_profile_schema():
    schema = [
        {
            'id': 'copo.profile.no_of_samples',
            'label': 'Number of samples',
            'required': 'true',
            'help_tip': 'Please input the number of samples',
        },
        {'id': 'copo.profile.library_type', 'label': 'Library type', 'required': 'false', 'help_tip': ''},
        {'id': 'copo.profile.sample_type', 'label': 'Kind of sample', 'required': True},
    ]
    with mock.patch.object(edp_utils, 'Profile') as profile_da, \
            mock.patch.object(edp_utils, 'get_sapio_sample_type_options', return_value=[]):
        profile_da.return_value.get_component_schema.return_value = schema
        options = edp_utils.get_sample_batch_form_options()

    assert options['fields']['no_of_samples'] == {
        'label': 'Number of samples',
        'required': True,
        'help_tip': 'Please input the number of samples',
    }
    assert options['fields']['library_type']['required'] is False
    assert options['fields']['sample_type']['required'] is True
    assert options['labels']['sample_type'] == 'Kind of sample'
    # A field missing from the schema falls back to its hard-coded label
    assert options['fields']['container_type'] == {
        'label': 'Container type',
        'required': False,
        'help_tip': '',
    }
    assert options['labels']['container_type'] == 'Container type'


def test_dialog_lists_batches_with_their_lock(batches):
    profile, lims, _ = batches
    lims.get_submitted_sample_counts.return_value = {'b1': {'submitted': 2, 'total': 2}}

    [described] = edp_utils.describe_sample_batches(str(profile['_id']))

    assert described['batch_id'] == 'b1'
    assert described['locked'] is True
    assert described['submitted_sample_count'] == 2
    # The LIMS record IDs stay on the server
    assert 'lims_sample_record_ids' not in described


def test_dialog_lists_nothing_for_other_profile_types(batches):
    profile, _, _ = batches
    profile['type'] = 'genomics'

    assert edp_utils.describe_sample_batches(str(profile['_id'])) is None


def test_batch_with_samples_submitted_to_ena_is_locked(batches):
    profile, lims, _ = batches
    with mock.patch.object(edp_utils, 'Sample') as sample_da:
        sample_da.return_value.get_collection_handle.return_value.find.return_value = [
            {'_id': 's1', 'edp_batch_id': 'b1', 'biosampleAccession': 'SAMEA1'},
        ]
        result = edp_utils.update_sample_batch(str(profile['_id']), 'b1', TISSUE)

    assert result['status'] == 'error'
    assert result['message'] == MESSAGES['batch_locked'].format(
        submitted_sample_count=1, total_sample_count=2
    )
    lims.sync_project.assert_not_called()


def test_batch_cannot_be_smaller_than_its_uploaded_samples(batches):
    profile, lims, _ = batches
    with mock.patch.object(edp_utils, 'Sample') as sample_da:
        sample_da.return_value.get_collection_handle.return_value.find.return_value = [
            {'_id': 's1', 'edp_batch_id': 'b1'},
            {'_id': 's2'},  # uploaded before batches existed, so in the first batch
        ]
        result = edp_utils.update_sample_batch(
            str(profile['_id']), 'b1', {**TISSUE, 'no_of_samples': '1'}
        )

    assert result == {
        'status': 'error',
        'message': MESSAGES['batch_count_below_uploaded'].format(uploaded_sample_count=2),
    }
    lims.sync_project.assert_not_called()


def test_profile_fields_are_read_as_the_same_batch_every_time():
    # Otherwise samples uploaded before batches existed never match their batch
    profile = {'_id': ObjectId(), **TISSUE}

    first_read = sample_batches.get_sample_batches(profile)[0]['batch_id']

    assert sample_batches.get_sample_batches(profile)[0]['batch_id'] == first_read
    assert sample_batches.get_sample_batches({'_id': ObjectId(), **TISSUE})[0]['batch_id'] != first_read


# --- Each batch has its own LIMS manifest ---

STUDY_PROFILE = {
    'sapio_project_id': PROJECT_ID,
    'sample_batches': [batch('b1', [1]), batch('b2', [2]), batch('b3', [3])],
}


@pytest.mark.parametrize('batch_id, study_id', [
    ('b1', PROJECT_ID),  # as before batches existed
    ('b2', f'{PROJECT_ID}-B2'),
    ('b3', f'{PROJECT_ID}-B3'),
    ('nope', ''),
])
def test_batch_study_ids(batch_id, study_id):
    assert sample_batches.get_batch_study_id(STUDY_PROFILE, batch_id) == study_id
    if study_id:
        assert sample_batches.find_batch_by_study_id(STUDY_PROFILE, study_id)['batch_id'] == batch_id


@pytest.mark.parametrize('study_id', ['', 'PR9999', f'{PROJECT_ID}-B4', f'{PROJECT_ID}-B'])
def test_unknown_study_ids_have_no_batch(study_id):
    assert sample_batches.find_batch_by_study_id(STUDY_PROFILE, study_id) is None


def test_later_batchs_manifest_is_sent_to_the_profiles_sapio_project():
    with mock.patch.object(edp_utils, 'Profile') as profile_da, \
            mock.patch.object(edp_utils, 'Singlecell') as singlecell_da, \
            mock.patch.object(edp_utils, 'SinglecellSchemas'), \
            mock.patch.object(edp_utils, 'get_lims_adapter') as get_lims_adapter:
        profile_da.return_value.get_record.return_value = {**STUDY_PROFILE, 'title': 'p'}
        singlecell_da.return_value.get_collection_handle.return_value.find_one.return_value = {
            '_id': 'r1', 'components': {}, 'schema_name': 'EI_EDP', 'checklist_id': 'c1'
        }
        get_lims_adapter.return_value.submit_manifest.return_value = {'status': 'success', 'record_id': 7}

        edp_utils.submit_lims_manifest_to_sapio('p1', f'{PROJECT_ID}-B2')

    assert get_lims_adapter.return_value.submit_manifest.call_args.args[0] == PROJECT_ID


# --- The batches offered on the Samples and LIMS manifest pages ---

def test_batch_names_end_with_their_jira_pip():
    profile = {
        '_id': 'p1',
        'sample_batches': [
            batch('b1', [1, 2], related_pip='PIP-1'),
            batch('b2', [3], sample_type='Pre-made library', library_type='PacBio', related_pip='PIP-2'),
        ],
    }
    uploaded_counts = {'b1': {'uploaded': 1, 'submitted': 0}, 'b2': {'uploaded': 0, 'submitted': 0}}
    with mock.patch.object(edp_utils, 'Profile'), \
            mock.patch.object(edp_utils, 'count_copo_batch_samples', return_value=uploaded_counts):
        described = edp_utils.describe_upload_batches(profile)

    assert [(b['name'], b['samples_left']) for b in described] == [
        ('Batch 1: 2 x Tissue (Plate) - PIP-1', 1),
        ('Batch 2: 1 x Pre-made library, PacBio (Plate) - PIP-2', 1),
    ]


# --- Only EDP managers create EDP profiles and register samples ---

class FakeUser:
    def __init__(self, groups, is_authenticated=True):
        self.is_authenticated = is_authenticated
        self.groups = mock.Mock()
        self.groups.filter.side_effect = lambda name__in: mock.Mock(
            exists=lambda: bool(set(groups) & set(name__in))
        )


@pytest.mark.parametrize('groups, expected', [
    (['ei_edp_managers'], True),
    (['data_managers'], True),
    (['ei_edp_users'], False),
    ([], False),
])
def test_edp_managers_and_data_managers_are_edp_managers(groups, expected):
    assert edp_utils.is_edp_manager(FakeUser(groups)) is expected


def test_anonymous_users_are_not_edp_managers():
    assert not edp_utils.is_edp_manager(None)
    assert not edp_utils.is_edp_manager(FakeUser(['ei_edp_managers'], is_authenticated=False))


def test_only_edp_managers_create_edp_profiles(pre_save):
    with mock.patch.object(edp_utils, 'is_edp_manager', return_value=False):
        result = edp_utils.pre_save_edp_profile(form_fields(**TISSUE))

    assert result == {'status': 'error', 'message': MESSAGES['edp_manager_required_for_profile']}


def test_users_who_are_not_edp_managers_can_still_edit_a_shared_profile(pre_save):
    with mock.patch.object(edp_utils, 'is_edp_manager', return_value=False):
        result = edp_utils.pre_save_edp_profile(form_fields(title='Profile'), target_id='p1')

    assert result == {'status': 'success'}
