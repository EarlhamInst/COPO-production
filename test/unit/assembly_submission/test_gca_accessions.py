'''Unit tests for looking up assemblies' GCA accessions in ENA.

ENA assigns an assembly's GCA accession days after submission. A daily Celery
task (update_assembly_gca_accessions) queries ENA's portal search once per
study and saves 'gca_accession' on the assemblies whose names match. Decoupled
(GFF3) annotations can then choose those assemblies as their PRIMARY ID.

ENA and MongoDB are mocked.

To execute these tests, run: $ pytest test/unit/assembly_submission/test_gca_accessions.py
'''

from unittest import mock

import pytest
import requests

from src.apps.copo_assembly_submission.utils import EnaAssembly

STUDY_ACCESSION = 'PRJEB101635'
# What ENA's portal search returned for this study
PORTAL_RESULTS = [
    {'assembly_name': 'mCanAur2_v2', 'assembly_set_accession': 'GCA_977927465.2', 'accession': 'GCA_977927465'},
    {'assembly_name': 'mCanAur2', 'assembly_set_accession': 'GCA_977927465.1', 'accession': 'GCA_977927465'},
]


@pytest.fixture(autouse=True)
def silent_assembly_logger():
    # Logger writes logs/<date>.log into the repo
    with mock.patch.object(EnaAssembly, 'l', mock.MagicMock()):
        yield


@pytest.fixture
def portal_search():
    with mock.patch.object(EnaAssembly.requests, 'post') as post:
        post.return_value.json.return_value = PORTAL_RESULTS
        yield post


def test_study_assemblies_are_mapped_to_versioned_gca_accessions(portal_search):
    assert EnaAssembly.get_study_assembly_accessions(STUDY_ACCESSION) == {
        'mCanAur2_v2': 'GCA_977927465.2',
        'mCanAur2': 'GCA_977927465.1',
    }
    data = portal_search.call_args.kwargs['data']
    assert data['result'] == 'assembly'
    assert data['query'] == f'study_accession="{STUDY_ACCESSION}"'
    assert data['fields'] == 'assembly_name,assembly_set_accession'


def test_study_without_assemblies_gives_an_empty_map(portal_search):
    portal_search.return_value.json.return_value = []

    assert EnaAssembly.get_study_assembly_accessions('PRJEB000000') == {}


@pytest.fixture
def assembly_records():
    '''Mock the Assembly collection. Yields a function taking the records to return.'''
    with mock.patch.object(EnaAssembly, 'Assembly') as assembly:
        collection = assembly.return_value.get_collection_handle.return_value

        def set_records(*records):
            assembly.return_value.get_all_records_columns.return_value = list(records)
            return collection

        yield set_records


def saved_gca_accessions(collection):
    return {c.args[0]['_id']: c.args[1]['$set']['gca_accession'] for c in collection.update_one.call_args_list}


def test_gca_accession_is_saved_on_assemblies_whose_names_match(portal_search, assembly_records):
    collection = assembly_records(
        {'_id': 'a1', 'study': STUDY_ACCESSION, 'assemblyname': 'mCanAur2'},
        {'_id': 'a2', 'study': STUDY_ACCESSION, 'assemblyname': 'mCanAur2_v2'},
        {'_id': 'a3', 'study': STUDY_ACCESSION, 'assemblyname': 'not_processed_yet'},
    )

    EnaAssembly.update_assembly_gca_accessions()

    assert saved_gca_accessions(collection) == {'a1': 'GCA_977927465.1', 'a2': 'GCA_977927465.2'}
    portal_search.assert_called_once()  # one query for the whole study


def test_assemblies_without_a_study_are_skipped(portal_search, assembly_records):
    collection = assembly_records({'_id': 'a1', 'study': '', 'assemblyname': 'mCanAur2'})

    EnaAssembly.update_assembly_gca_accessions()

    portal_search.assert_not_called()
    collection.update_one.assert_not_called()


def test_a_failed_lookup_does_not_stop_other_studies(portal_search, assembly_records):
    collection = assembly_records(
        {'_id': 'a1', 'study': 'PRJEB_DOWN', 'assemblyname': 'mCanAur2'},
        {'_id': 'a2', 'study': STUDY_ACCESSION, 'assemblyname': 'mCanAur2'},
    )
    ok_response = portal_search.return_value

    def search(*args, **kwargs):
        if 'PRJEB_DOWN' in kwargs['data']['query']:
            raise requests.ConnectionError('ENA unavailable')
        return ok_response

    portal_search.side_effect = search

    EnaAssembly.update_assembly_gca_accessions()

    assert saved_gca_accessions(collection) == {'a2': 'GCA_977927465.1'}
    EnaAssembly.l.error.assert_called_once()
