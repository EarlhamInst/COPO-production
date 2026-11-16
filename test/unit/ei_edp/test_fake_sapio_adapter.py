'''Unit tests for the fake Sapio used to test the EDP flow without Sapio.

`FakeSapioAdapter` must behave like the Sapio adapter where COPO can see it:
a project is created on first save with one sample per requested sample,
plate batches are placed on 96-well plates, submitted samples can't be
removed, manifests are pre-filled from the samples and written back to them,
and deleting a project deletes its samples.
'''

import copy
import itertools

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from src.apps.ei_edp.utils.constants import (
    BATCH_ID,
    BATCH_LIMS_SAMPLE_RECORD_IDS,
    PROFILE_BUDGET_HOLDER,
    PROFILE_CONTAINER_TYPE,
    PROFILE_NO_OF_SAMPLES,
    PROFILE_RELATED_ENQ,
    PROFILE_SAMPLE_BATCHES,
    PROFILE_SAMPLE_TYPE,
    PROFILE_SAPIO_PROJECT_ID,
)
from src.apps.ei_edp.utils.lims.sapio import fake
from src.apps.ei_edp.utils.lims.sapio.fake import FakeSapioAdapter


class FakeCollection:
    '''The few MongoDB collection operations that the fake Sapio uses.'''

    def __init__(self):
        self.documents = []
        self.ids = itertools.count(1)

    @staticmethod
    def _value(document, path):
        for key in path.split('.'):
            document = (document or {}).get(key)
        return document

    def _matches(self, document, query):
        for path, condition in query.items():
            value = self._value(document, path)
            if isinstance(condition, dict) and '$in' in condition:
                if value not in condition['$in']:
                    return False
            elif value != condition:
                return False
        return True

    def find_one(self, query):
        return next(
            (copy.deepcopy(d) for d in self.documents if self._matches(d, query)), None
        )

    def find(self, query):
        documents = [copy.deepcopy(d) for d in self.documents if self._matches(d, query)]

        class Cursor(list):
            def sort(self, key, direction):
                return sorted(self, key=lambda d: d[key], reverse=direction < 0)

        return Cursor(documents)

    def insert_one(self, document):
        document['_id'] = next(self.ids)
        self.documents.append(copy.deepcopy(document))

    def insert_many(self, documents):
        for document in documents:
            self.insert_one(document)

    def update_one(self, query, update):
        for document in self.documents:
            if self._matches(document, query):
                for path, value in update['$set'].items():
                    *parents, key = path.split('.')
                    target = document
                    for parent in parents:
                        target = target.setdefault(parent, {})
                    target[key] = value
                return

    def delete_many(self, query):
        self.documents = [d for d in self.documents if not self._matches(d, query)]

    def delete_one(self, query):
        self.delete_many(query)

    def find_one_and_update(self, query, update, upsert, return_document):
        document = next((d for d in self.documents if self._matches(d, query)), None)
        if document is None:
            document = {**query, 'value': 0}
            self.documents.append(document)
        document['value'] += update['$inc']['value']
        return copy.deepcopy(document)


@pytest.fixture
def adapter(monkeypatch):
    collections = {}
    monkeypatch.setattr(
        fake, 'get_collection_ref', lambda name: collections.setdefault(name, FakeCollection())
    )
    with override_settings(DEBUG=True):
        yield FakeSapioAdapter()


def batch(batch_id, no_of_samples, container_type='Plate', record_ids=None):
    return {
        BATCH_ID: batch_id,
        PROFILE_NO_OF_SAMPLES: no_of_samples,
        PROFILE_SAMPLE_TYPE: 'Tissue',
        PROFILE_CONTAINER_TYPE: container_type,
        BATCH_LIMS_SAMPLE_RECORD_IDS: record_ids,
    }


def profile(*batches, project_id=''):
    return {
        '_id': 'profile_id',
        PROFILE_RELATED_ENQ: 'ENQ0001',
        PROFILE_BUDGET_HOLDER: 'budget holder',
        PROFILE_SAPIO_PROJECT_ID: project_id,
        PROFILE_SAMPLE_BATCHES: list(batches),
    }


def field(term_name, sapio_name, term_access=None):
    return {'term_name': term_name, 'sapio_name': sapio_name, 'term_access': term_access}


SCHEMAS = {
    'study': [
        field('health_safety', 'Project:C_HandS'),
        field('sample_return', 'Sample:C_SampleReturn'),
    ],
    'sample': [
        field('sample_id', 'Sample:SampleId', 'protected'),
        field('row_position', 'Sample:RowPosition', 'protected'),
        field('column_position', 'Sample:ColPosition', 'protected'),
        field('submitter_sample_reference', 'Sample:C_CustomerSampleName'),
        field('applicable_regulations', 'Sample:C_ApplicableRegulations'),
    ],
}


def test_refuses_to_run_without_debug():
    with override_settings(DEBUG=False), pytest.raises(ImproperlyConfigured):
        FakeSapioAdapter()


def test_first_save_creates_a_project_with_plated_samples(adapter):
    result = adapter.sync_project(profile(batch('b1', 3)))

    assert result['status'] == 'success'
    assert result['project_id'] == 'PR000001'
    assert len(result['batch_sample_record_ids']['b1']) == 3

    project = adapter.read_project(
        'PR000001', ['ProjectName', 'C_BudgetHolder'], ['SampleId', 'PlateId', 'RowPosition', 'ColPosition']
    )
    assert project['exists']
    assert project['project'] == {'ProjectName': 'ENQ0001', 'C_BudgetHolder': 'budget holder'}
    assert [(s['RowPosition'], s['ColPosition']) for s in project['samples']] == [
        ('A', '1'), ('B', '1'), ('C', '1')
    ]
    assert len({s['PlateId'] for s in project['samples']}) == 1


def test_tube_batch_is_not_plated(adapter):
    adapter.sync_project(profile(batch('b1', 2, container_type='Tube')))

    samples = adapter.read_project('PR000001', sample_fields=['PlateId'])['samples']
    assert [s['PlateId'] for s in samples] == [None, None]


def test_lowering_the_count_keeps_submitted_samples(adapter):
    record_ids = adapter.sync_project(profile(batch('b1', 2)))['batch_sample_record_ids']['b1']
    sample_ids = [s['sample_id'] for s in adapter.get_project_samples('PR000001', SCHEMAS)]
    adapter.submit_manifest(
        'PR000001',
        SCHEMAS,
        {'sample': [{'sample_id': sample_ids[0], 'submitter_sample_reference': 'sample1'}]},
    )

    # The unsubmitted sample can be removed
    result = adapter.sync_project(profile(batch('b1', 1, record_ids=record_ids), project_id='PR000001'))
    assert result['status'] == 'success'
    assert result['batch_sample_record_ids']['b1'] == [record_ids[0]]

    # The submitted one can't
    result = adapter.sync_project(
        profile(batch('b1', 0, record_ids=[record_ids[0]]), project_id='PR000001')
    )
    assert result['status'] == 'warning'
    counts = adapter.get_submitted_sample_counts(
        profile(batch('b1', 1, record_ids=[record_ids[0]]), project_id='PR000001')
    )
    assert counts == {'b1': {'submitted': 1, 'total': 1}}


def test_manifest_is_prefilled_from_and_written_to_the_samples(adapter):
    adapter.sync_project(profile(batch('b1', 1)))
    prefilled = adapter.get_batch_samples(
        profile(batch('b1', 1), project_id='PR000001'), 'b1', SCHEMAS
    )
    assert prefilled == [{
        'sample_id': 'FAKE-000001',
        'row_position': 'A',
        'column_position': '1',
        'submitter_sample_reference': None,
        'applicable_regulations': None,
    }]

    result = adapter.submit_manifest(
        'PR000001',
        SCHEMAS,
        {
            'study': [{'health_safety': 'Yes', 'sample_return': 'No'}],
            'sample': [
                # Protected fields such as the row position aren't written
                {'sample_id': 'FAKE-000001', 'row_position': 'H',
                 'submitter_sample_reference': 'sample1', 'applicable_regulations': 'UK-GDPR'},
                {'sample_id': 'NOT-A-SAMPLE', 'submitter_sample_reference': 'sample2'},
            ],
        },
    )

    assert result['status'] == 'warning'
    assert result['skipped_samples'] == ['NOT-A-SAMPLE']
    project = adapter.read_project(
        'PR000001',
        ['RecordId', 'C_HandS'],
        ['RowPosition', 'C_CustomerSampleName', 'C_ApplicableRegulations', 'C_SampleReturn'],
    )
    assert project['project'] == {'RecordId': result['record_id'], 'C_HandS': 'Yes'}
    # Values are swapped for the ones Sapio expects, as the Sapio adapter does
    assert project['samples'] == [{
        'RowPosition': 'A',
        'C_CustomerSampleName': 'sample1',
        'C_ApplicableRegulations': 'GDPR',
        'C_SampleReturn': 'No',
    }]
    assert adapter.get_project_metadata('PR000001') == {
        'health_and_safety': 'Yes', 'sample_return': 'No'
    }


def test_deleting_a_project_deletes_its_samples(adapter):
    adapter.sync_project(profile(batch('b1', 2)))

    assert adapter.delete_project('PR000001') == {'status': 'success'}
    assert adapter.read_project('PR000001') == {'exists': False, 'project': {}, 'samples': []}
    assert adapter.validate_profile_change(profile(project_id='PR000001'))['status'] == 'error'
