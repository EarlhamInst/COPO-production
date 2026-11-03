'''Unit tests for downloading a blank sample manifest under an EDP profile.

An EDP profile requests a number of samples so, a blank sample manifest only
lets users fill in as many rows as there are samples left to upload. The rows
below them are greyed out and protected. Users are warned if all the requested
samples have already been uploaded, and an uploaded manifest cannot add more
samples than are left. No more samples can be submitted than the profile requests.
'''

from io import BytesIO
from unittest import mock

import openpyxl
import pandas as pd
import pytest

from common.ena_utils.EnaChecklistHandler import write_manifest
from src.apps.copo_core import broker_da as broker_module
from src.apps.ei_edp.utils import edp_utils, edp_validators
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES

PROFILE_ID = 'profile1'

CHECKLIST = {
    'primary_id': 'ERC000011',
    'name': 'ENA default sample checklist',
    'fields': {
        'organism': {'name': 'organism', 'label': 'Organism', 'mandatory': 'mandatory', 'type': 'TEXT_FIELD'},
        'sex': {
            'name': 'sex',
            'label': 'Sex',
            'mandatory': 'optional',
            'type': 'TEXT_CHOICE_FIELD',
            'choice': ['female', 'male'],
        },
    },
}


def samples_left(requested_sample_count, uploaded_sample_count):
    with mock.patch.object(edp_utils, 'Sample') as sample_da:
        count_documents = sample_da.return_value.get_collection_handle.return_value.count_documents
        count_documents.return_value = uploaded_sample_count
        profile = {'_id': PROFILE_ID, 'no_of_samples': requested_sample_count}
        return edp_utils.get_samples_left_to_upload(profile), count_documents


@pytest.mark.parametrize('requested, uploaded, expected', [
    ('5', 0, 5),
    ('5', 2, 3),
    (' 5 ', 2, 3),  # the form's field is free text
    ('5', 5, 0),
    ('2', 5, 0),  # never negative
    ('', 0, 0),
    (None, 0, 0),
])
def test_samples_left_to_upload(requested, uploaded, expected):
    result, _ = samples_left(requested, uploaded)

    assert result == expected


def test_counts_only_samples_in_the_profile_that_are_not_deleted():
    _, count_documents = samples_left('5', 0)

    query = count_documents.call_args.args[0]
    assert query == {'profile_id': PROFILE_ID, 'deleted': edp_utils.helpers.get_not_deleted_flag()}


def check(profile, samples_left_to_upload=0):
    with mock.patch.object(edp_utils, 'get_samples_left_to_upload', return_value=samples_left_to_upload):
        return edp_utils.check_blank_sample_manifest(profile)


def test_warns_when_all_samples_uploaded():
    result = check({'_id': PROFILE_ID, 'type': 'ei_edp'}, samples_left_to_upload=0)

    assert result == {'status': 'warning', 'message': MESSAGES['all_samples_uploaded']}


def test_proceeds_when_samples_left_to_upload():
    result = check({'_id': PROFILE_ID, 'type': 'ei_edp'}, samples_left_to_upload=3)

    assert result == {'status': 'success', 'message': ''}


def test_other_profile_types_are_not_checked():
    result = check({'_id': PROFILE_ID, 'type': 'genomics'})

    assert result == {'status': 'success', 'message': ''}


def manifest_worksheet(editable_row_count):
    file = BytesIO()
    write_manifest(CHECKLIST, with_read=False, file_path=file, editable_row_count=editable_row_count)
    return openpyxl.load_workbook(BytesIO(file.getvalue())).worksheets[0]


def is_locked(worksheet, row):
    return [worksheet.cell(row, column).protection.locked for column in range(1, 3)]


@pytest.mark.parametrize('editable_row_count', [1, 5])
def test_only_rows_for_samples_left_to_upload_are_editable(editable_row_count):
    worksheet = manifest_worksheet(editable_row_count)

    assert worksheet.protection.sheet
    assert worksheet.protection.password
    assert is_locked(worksheet, 1) == [True, True]  # header
    for row in range(2, editable_row_count + 2):
        assert is_locked(worksheet, row) == [False, False]
    assert is_locked(worksheet, editable_row_count + 2) == [True, True]


def test_rows_below_the_editable_rows_are_greyed_out():
    worksheet = manifest_worksheet(5)

    rules = worksheet.conditional_formatting
    assert [str(rule.sqref) for rule in rules] == ['A7:B1048576']
    assert list(rules)[0].rules[0].dxf.fill.bgColor.rgb.endswith('D3D3D3')


def test_editable_rows_keep_the_text_format():
    worksheet = manifest_worksheet(5)

    assert worksheet.cell(2, 1).number_format == '@'


def test_manifest_is_unprotected_without_a_row_limit():
    worksheet = manifest_worksheet(None)

    assert not worksheet.protection.sheet
    assert not list(worksheet.conditional_formatting)


def validate_upload(sample_names, existing_sample_names=(), samples_left_to_upload=0):
    checklist = {'fields': {**CHECKLIST['fields'], 'sample': {'label': 'Sample', 'mandatory': 'mandatory'}}}
    data = pd.DataFrame({'sample': list(sample_names)})
    profile = {'_id': PROFILE_ID, 'type': 'ei_edp'}
    with mock.patch.object(edp_validators, 'Sample') as sample_da, \
            mock.patch.object(
                edp_validators, 'get_samples_left_to_upload', return_value=samples_left_to_upload
            ) as get_samples_left_to_upload:
        distinct = sample_da.return_value.get_collection_handle.return_value.distinct
        distinct.return_value = list(existing_sample_names)
        # The profile is passed to every validator by `EnaCheckListSpreadsheet`
        errors, _, flag, _ = edp_validators.SampleCountValidator(
            profile_id=PROFILE_ID, profile=profile, fields=None, data=data, errors=[],
            warnings=[], flag=True, checklist=checklist, isupdate=False,
        ).validate()
        get_samples_left_to_upload.assert_called_once_with(profile)
        return errors, flag, distinct


@pytest.mark.parametrize('sample_names, samples_left_to_upload', [
    (['s1', 's2', 's3'], 3),  # exactly the samples left
    (['s1', 's2'], 3),
    (['s1', 's1'], 1),  # the same sample twice is one sample
    (['s1', '', 'nan'], 1),  # blank cells are not samples
])
def test_upload_within_the_samples_left_passes(sample_names, samples_left_to_upload):
    errors, flag, _ = validate_upload(sample_names, samples_left_to_upload=samples_left_to_upload)

    assert flag
    assert errors == []


def test_upload_with_more_samples_than_are_left_fails():
    errors, flag, _ = validate_upload(['s1', 's2', 's3', 's4'], samples_left_to_upload=3)

    assert not flag
    assert errors == [MESSAGES['too_many_samples'].format(new_sample_count=4, samples_left_to_upload=3)]


def test_existing_samples_in_the_upload_are_updates_and_not_counted():
    # 's1' and 's2' were uploaded before so, only 's3' is new
    errors, flag, distinct = validate_upload(
        ['s1', 's2', 's3'], existing_sample_names=['s1', 's2'], samples_left_to_upload=1
    )

    assert flag
    query = distinct.call_args.args[1]
    assert query['profile_id'] == PROFILE_ID
    assert query['deleted'] == edp_utils.helpers.get_not_deleted_flag()
    assert sorted(query['name']['$in']) == ['s1', 's2', 's3']


def test_updating_samples_passes_when_no_samples_are_left():
    errors, flag, _ = validate_upload(['s1', 's2'], existing_sample_names=['s1', 's2'], samples_left_to_upload=0)

    assert flag


def check_submission(selected_sample_ids, submitted_sample_ids=(), requested='3', profile_type='ei_edp'):
    profile = {'_id': PROFILE_ID, 'type': profile_type, 'no_of_samples': requested}
    with mock.patch.object(edp_utils, 'Sample') as sample_da:
        find = sample_da.return_value.get_collection_handle.return_value.find
        find.return_value = [{'_id': sample_id} for sample_id in submitted_sample_ids]
        return edp_utils.check_sample_submission(profile, list(selected_sample_ids)), find


@pytest.mark.parametrize('selected, submitted', [
    (['s1', 's2', 's3'], []),  # exactly the samples requested
    (['s3'], ['s1', 's2']),
    (['s1', 's2', 's3'], ['s1', 's2', 's3']),  # submitted again as updates
])
def test_submission_within_the_requested_samples_passes(selected, submitted):
    result, _ = check_submission(selected, submitted)

    assert result == {'status': 'success', 'message': ''}


@pytest.mark.parametrize('selected, submitted, left', [
    (['s1', 's2', 's3', 's4', 's5'], [], 3),  # 5 pending samples but only 3 requested
    (['s1', 's4'], ['s1', 's2', 's3'], 0),  # 's1' is an update, 's4' is one too many
    (['s3', 's4'], ['s1', 's2'], 1),
])
def test_submission_with_more_samples_than_requested_fails(selected, submitted, left):
    result, _ = check_submission(selected, submitted)

    assert result == {
        'status': 'error',
        'message': MESSAGES['too_many_samples_to_submit'].format(
            requested_sample_count=3,
            submitted_sample_count=len(submitted),
            samples_left_to_submit=left,
        ),
    }


def test_submitted_samples_include_queued_samples():
    _, find = check_submission(['s1'])

    query = find.call_args.args[0]
    assert query['profile_id'] == PROFILE_ID
    assert query['deleted'] == edp_utils.helpers.get_not_deleted_flag()
    assert query['$or'] == [
        {'biosampleAccession': {'$exists': True, '$nin': ['', None]}},
        {'status': {'$in': ['processing', 'sending']}},
    ]


def test_submission_under_other_profile_types_is_not_checked():
    result, _ = check_submission(['s1', 's2', 's3', 's4'], profile_type='genomics')

    assert result == {'status': 'success', 'message': ''}


def submit(profile_type, check_result):
    with mock.patch.object(broker_module, 'DAComponent'), \
            mock.patch.object(broker_module, 'Profile') as profile_da, \
            mock.patch.object(broker_module, 'copo_sample') as copo_sample, \
            mock.patch.object(
                broker_module.edp_utils, 'check_sample_submission', return_value=check_result
            ) as check_sample_submission:
        profile = {'_id': PROFILE_ID, 'type': profile_type}
        profile_da.return_value.get_record.return_value = profile
        copo_sample.submit_sample.return_value = {'status': 'success', 'message': 'queued'}
        broker = broker_module.BrokerDA(
            context={}, component='sample', profile_id=PROFILE_ID, target_ids=['s1'], request_dict={}
        )
        feedback = broker.do_submit_sample()['action_feedback']
        return feedback, profile, check_sample_submission, copo_sample.submit_sample


def test_edp_submission_over_the_requested_samples_is_not_submitted():
    error = {'status': 'error', 'message': 'too many samples'}
    feedback, profile, check_sample_submission, submit_sample = submit('ei_edp', error)

    assert feedback == error
    check_sample_submission.assert_called_once_with(profile=profile, sample_ids=['s1'])
    submit_sample.assert_not_called()


def test_edp_submission_within_the_requested_samples_is_submitted():
    feedback, _, check_sample_submission, submit_sample = submit(
        'ei_edp', {'status': 'success', 'message': ''}
    )

    assert feedback == {'status': 'success', 'message': 'queued'}
    check_sample_submission.assert_called_once()
    submit_sample.assert_called_once()


@pytest.mark.parametrize('profile_type', ['genomics', 'biodata'])
def test_submission_under_other_profile_types_skips_the_edp_check(profile_type):
    # The sample page is shared by these profile types and sends the same
    # request so, only the profile type decides whether the EDP check runs
    feedback, _, check_sample_submission, submit_sample = submit(
        profile_type, {'status': 'error', 'message': 'too many samples'}
    )

    assert feedback == {'status': 'success', 'message': 'queued'}
    check_sample_submission.assert_not_called()
    submit_sample.assert_called_once()
