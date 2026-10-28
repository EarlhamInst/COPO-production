'''Unit tests for syncing Sapio plate well elements for an EDP project.

Sapio's Plate View finds the sample in each well through a
`PlateDesignerWellElement` record. COPO fills plates directly rather than
through Sapio's Plate Designer, so without these records the wells are shown
black and the sample details are missing on hover. After each project sync,
every sample on a plate should have exactly one element, and elements whose
sample is no longer on the plate should be deleted.
'''

from unittest import mock

from src.apps.ei_edp.utils.lims.sapio import adapter as sapio_adapter
from src.apps.ei_edp.utils.lims.sapio.adapter import SapioAdapter


class FakeRecord:
    def __init__(self, record_id, **fields):
        self.record_id = record_id
        self.fields = fields

    def get_field_value(self, field):
        return self.fields.get(field)


class FakePlate:
    def __init__(self, record_id, samples):
        self.record_id = record_id
        self.samples = samples

    def get_children_of_type(self, data_type):
        return self.samples if data_type == 'Sample' else []


def sample(record_id, row, column):
    return FakeRecord(record_id, RowPosition=row, ColPosition=column)


def well_element(record_id, plate_record_id, sample_record_id):
    return FakeRecord(
        record_id, PlateRecordId=plate_record_id, AliquotSampleRecordId=sample_record_id
    )


def page(well_elements, next_page_criteria=None):
    result = mock.Mock()
    result.result_list = well_elements
    result.is_next_page_available = next_page_criteria is not None
    result.next_page_criteria = next_page_criteria
    return result


def sync(plates, *pages):
    # Runs the sync against a Sapio holding the given pages of existing
    # well elements and returns the mocked data record manager
    sapio = mock.Mock()
    data_record_manager = sapio.return_value.dataRecordManager
    data_record_manager.query_data_records.side_effect = list(pages)
    with mock.patch.object(sapio_adapter, 'Sapio', sapio):
        SapioAdapter.__new__(SapioAdapter)._sync_plate_well_elements(plates)
    return data_record_manager


def created_well_elements(data_record_manager):
    if not data_record_manager.add_data_records_with_data.called:
        return []
    kwargs = data_record_manager.add_data_records_with_data.call_args.kwargs
    assert kwargs['data_type_name'] == 'PlateDesignerWellElement'
    return kwargs['field_map_list']


def deleted_well_elements(data_record_manager):
    if not data_record_manager.delete_data_record_list.called:
        return []
    return data_record_manager.delete_data_record_list.call_args.args[0]


def test_no_plates_makes_no_sapio_calls():
    data_record_manager = sync([])

    data_record_manager.query_data_records.assert_not_called()
    data_record_manager.add_data_records_with_data.assert_not_called()
    data_record_manager.delete_data_record_list.assert_not_called()


def test_creates_a_well_element_for_each_sample_without_one():
    plate = FakePlate(100, [sample(1, 'A', '1'), sample(2, 'B', '1')])

    data_record_manager = sync([plate], page([]))

    query = data_record_manager.query_data_records.call_args.kwargs
    assert query['data_type_name'] == 'PlateDesignerWellElement'
    assert query['data_field_name'] == 'PlateRecordId'
    assert query['value_list'] == [100]
    # A sample placed directly on a plate is both the source and the aliquot
    assert created_well_elements(data_record_manager) == [
        {
            'PlateRecordId': 100,
            'RowPosition': 'A',
            'ColPosition': '1',
            'Layer': 1,
            'SourceDataTypeName': 'Sample',
            'SourceRecordId': 1,
            'AliquotSampleRecordId': 1,
            'IsControl': False,
        },
        {
            'PlateRecordId': 100,
            'RowPosition': 'B',
            'ColPosition': '1',
            'Layer': 1,
            'SourceDataTypeName': 'Sample',
            'SourceRecordId': 2,
            'AliquotSampleRecordId': 2,
            'IsControl': False,
        },
    ]
    data_record_manager.delete_data_record_list.assert_not_called()


def test_already_synced_plate_is_left_unchanged():
    plate = FakePlate(100, [sample(1, 'A', '1'), sample(2, 'B', '1')])
    existing = [well_element(500, 100, 1), well_element(501, 100, 2)]

    data_record_manager = sync([plate], page(existing))

    data_record_manager.add_data_records_with_data.assert_not_called()
    data_record_manager.delete_data_record_list.assert_not_called()


def test_only_missing_well_elements_are_created():
    plate = FakePlate(100, [sample(1, 'A', '1'), sample(2, 'B', '1')])

    data_record_manager = sync([plate], page([well_element(500, 100, 1)]))

    assert [
        element['AliquotSampleRecordId']
        for element in created_well_elements(data_record_manager)
    ] == [2]
    data_record_manager.delete_data_record_list.assert_not_called()


def test_well_element_of_a_removed_sample_is_deleted():
    # Sample 2 was removed from the plate when the sample count was reduced
    plate = FakePlate(100, [sample(1, 'A', '1')])
    kept = well_element(500, 100, 1)
    stale = well_element(501, 100, 2)

    data_record_manager = sync([plate], page([kept, stale]))

    assert deleted_well_elements(data_record_manager) == [stale]
    data_record_manager.add_data_records_with_data.assert_not_called()


def test_duplicate_well_elements_for_a_sample_are_deleted():
    plate = FakePlate(100, [sample(1, 'A', '1')])
    first = well_element(500, 100, 1)
    duplicate = well_element(501, 100, 1)

    data_record_manager = sync([plate], page([first, duplicate]))

    assert deleted_well_elements(data_record_manager) == [duplicate]
    data_record_manager.add_data_records_with_data.assert_not_called()


def test_well_elements_are_matched_by_plate_and_sample():
    # The existing element links sample 1 to the other plate, so it doesn't
    # count for plate 100 and is deleted
    plate = FakePlate(100, [sample(1, 'A', '1')])
    other_plate = FakePlate(200, [])
    wrong_plate = well_element(500, 200, 1)

    data_record_manager = sync([plate, other_plate], page([wrong_plate]))

    assert data_record_manager.query_data_records.call_args.kwargs['value_list'] == [100, 200]
    assert deleted_well_elements(data_record_manager) == [wrong_plate]
    assert [
        (element['PlateRecordId'], element['AliquotSampleRecordId'])
        for element in created_well_elements(data_record_manager)
    ] == [(100, 1)]


def test_existing_well_elements_are_read_from_every_page():
    plate = FakePlate(100, [sample(1, 'A', '1'), sample(2, 'B', '1')])
    next_page_criteria = object()

    data_record_manager = sync(
        [plate],
        page([well_element(500, 100, 1)], next_page_criteria),
        page([well_element(501, 100, 2)]),
    )

    assert data_record_manager.query_data_records.call_count == 2
    assert (
        data_record_manager.query_data_records.call_args.kwargs['paging_criteria']
        is next_page_criteria
    )
    data_record_manager.add_data_records_with_data.assert_not_called()
    data_record_manager.delete_data_record_list.assert_not_called()
