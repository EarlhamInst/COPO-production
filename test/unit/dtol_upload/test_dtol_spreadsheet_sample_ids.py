"""Unit tests for storing sample IDs while saving a Tree of Life manifest.

DtolSpreadsheet.save_records and update_records keep each sample's
MongoDB ObjectId in an `_id` column of the manifest DataFrame, so that
images can be linked to their samples afterwards.

In pandas 3, a column holding text is a string column, and storing an
ObjectId in it raises "TypeError: len() of unsized object". This broke
uploading and updating samples under ASG, DToL and ERGA profiles.
"""

import pickle
from unittest import mock

import pandas as pd
import pytest
from bson import ObjectId

from src.apps.copo_dtol_upload.utils import Dtol_Spreadsheet as dtol_module
from src.apps.copo_dtol_upload.utils.Dtol_Spreadsheet import DtolSpreadsheet

PROFILE_ID = 'profile1'
SPECIMENS = ['SPEC1', 'SPEC2']
IMAGE = {'name': 'SPEC2_1.jpg', 'file_name': '/media/SPEC2_1.jpg', 'specimen_id': 'SPEC2'}


def manifest():
    data = pd.DataFrame({
        'RACK_OR_PLATE_ID': ['RACK1', 'RACK1'],
        'TUBE_OR_WELL_ID': ['TUBE1', 'TUBE2'],
        'SPECIMEN_ID': SPECIMENS,
        'TAXON_ID': ['9606', '9606'],
        'SYMBIONT': ['TARGET', 'TARGET'],
    })
    # The same string columns that pandas 3 gives a manifest read from Excel
    assert pd.api.types.is_string_dtype(data['SPECIMEN_ID'])
    return data


def spreadsheet():
    dtol = DtolSpreadsheet.__new__(DtolSpreadsheet)
    dtol.profile_id = PROFILE_ID
    dtol.type = 'DTOL'
    dtol.associated_type = []
    dtol.current_schema_version = '1'
    dtol.vr = {'manifest_data': pickle.dumps(manifest())}
    return dtol


@pytest.fixture
def copo():
    # Stand-ins for the database, request, email and public name service
    request = mock.MagicMock()
    request.session = {'profile_id': PROFILE_ID, 'image_specimen_match': [IMAGE]}
    request.build_absolute_uri.return_value = 'https://copo.test/'

    with mock.patch.object(dtol_module.ThreadLocal, 'get_current_request', return_value=request), \
            mock.patch.object(dtol_module, 'get_current_user'), \
            mock.patch.object(dtol_module, 'Sample') as sample, \
            mock.patch.object(dtol_module, 'DataFile') as data_file, \
            mock.patch.object(dtol_module, 'Profile') as profile, \
            mock.patch.object(dtol_module, 'Email'), \
            mock.patch.object(dtol_module, 'notify_frontend'), \
            mock.patch.object(dtol_module, 'query_public_name_service', return_value=[]), \
            mock.patch.object(dtol_module, 'jp') as jp:
        jp.return_value.find.return_value = []
        profile.return_value.get_record.return_value = {
            'title': 'Profile', 'description': '', 'associated_type': []}
        data_file.return_value.get_records_by_fields.return_value = []
        yield sample, data_file


def test_new_samples_are_saved_and_images_linked_to_their_sample_ids(copo):
    sample, data_file = copo
    sample_ids = {specimen: ObjectId() for specimen in SPECIMENS}

    def save_record(auto_fields, **s):
        return {**s, '_id': sample_ids[s['SPECIMEN_ID']]}

    sample.return_value.save_record.side_effect = save_record

    spreadsheet().save_records()

    assert sample.return_value.save_record.call_count == len(SPECIMENS)
    data_file.return_value.insert_sample_ids.assert_called_once_with(IMAGE['name'], [sample_ids['SPEC2']])


def test_updated_samples_are_saved_and_images_linked_to_their_sample_ids(copo):
    sample, data_file = copo
    recorded = {
        f'RACK1/TUBE{i}': {
            '_id': ObjectId(), 'SPECIMEN_ID': specimen, 'TAXON_ID': '9606', 'SYMBIONT': 'TARGET',
            'RACK_OR_PLATE_ID': 'RACK1', 'TUBE_OR_WELL_ID': f'TUBE{i}', 'status': 'pending',
            'manifest_version': '0', 'associated_tol_project': '',
            'species_list': [{'TAXON_ID': '9606', 'SYMBIONT': 'TARGET'}],
        }
        for i, specimen in enumerate(SPECIMENS, start=1)
    }
    sample.return_value.get_target_by_field.side_effect = lambda field, rack_tube: [recorded[rack_tube]]

    spreadsheet().update_records()

    # `_id` is not compared with, or written to, the recorded sample
    updated_fields = [c.args[0] for c in sample.return_value.update_field.call_args_list]
    assert '_id' not in updated_fields
    data_file.return_value.insert_sample_ids.assert_called_once_with(
        IMAGE['name'], [recorded['RACK1/TUBE2']['_id']])


def test_map_to_dict_reads_a_dataframe_row_by_position():
    data = manifest()

    assert dtol_module.map_to_dict(data.columns, data.iloc[1, :])['SPECIMEN_ID'] == 'SPEC2'
