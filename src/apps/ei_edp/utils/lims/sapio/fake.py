'''A stand-in for Sapio for local testing only.

`FakeSapioAdapter` behaves like `SapioAdapter` without calling Sapio, so the
EDP flow can be tested end to end when Sapio's test instance is unavailable.
It is switched on by setting the `LIMS_ADAPTER` environment variable to
`FAKE_SAPIO_ADAPTER` (below) for copo_web and the Playwright container, and it
refuses to run unless `DEBUG` is on.

Records are kept in the same shape as Sapio's (a `Project` and its `Sample`
records, keyed by Sapio field names) in a MongoDB collection rather than in
memory so that a separate process, such as a Playwright test reading the
project back through `manage.py shell`, sees what the COPO server wrote.

Only what COPO reads back is kept. Sapio's own side effects (departments,
plate records, plate well elements, QC data and index barcodes) are not.
'''

import math
from typing import Any, Dict, List

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from pymongo import ReturnDocument

from common.dal.mongo_util import get_collection_ref

from ...constants import (
    BATCH_ID,
    LIMS_HEALTH_AND_SAFETY,
    LIMS_SAMPLE_RETURN,
    PROFILE_BUDGET_HOLDER,
    PROFILE_CONTAINER_TYPE,
    PROFILE_DESCRIPTION,
    PROFILE_LIBRARY_TYPE,
    PROFILE_NO_OF_SAMPLES,
    PROFILE_RELATED_ENQ,
    PROFILE_SAMPLE_TYPE,
    PROFILE_SAPIO_PROJECT_ID,
    PROFILE_TITLE,
    SAMPLE_ID,
    SAPIO_BUDGET_HOLDER,
    SAPIO_COL_POSITION,
    SAPIO_CONTAINER_TYPE,
    SAPIO_CUSTOMER_SAMPLE_NAME,
    SAPIO_EXEMPLAR_SAMPLE_STATUS,
    SAPIO_EXEMPLAR_SAMPLE_TYPE,
    SAPIO_HEALTH_AND_SAFETY,
    SAPIO_LIBRARY_TYPE,
    SAPIO_NAME,
    SAPIO_OTHER_SAMPLE_ID,
    SAPIO_PLATE_ID,
    SAPIO_PROJECT_DATA_TYPE,
    SAPIO_PROJECT_DESC,
    SAPIO_PROJECT_IDENTIFIER,
    SAPIO_PROJECT_NAME,
    SAPIO_RECORD_ID,
    SAPIO_ROW_POSITION,
    SAPIO_SAMPLE_DATA_TYPE,
    SAPIO_SAMPLE_ID,
    SAPIO_SAMPLE_RETURN,
    SAPIO_STORAGE_UNIT_PATH,
    TERM_ACCESS,
    TERM_NAME,
)
from ...edp_validation_messages import MESSAGES
from ...sample_batches import get_sample_batches
from ..base import LIMSAdapter
from .adapter import PLATE_COLUMNS, PLATE_ROWS, PLATE_WELLS, _group_samples_by_batch, _to_sapio_value

FAKE_SAPIO_ADAPTER = 'src.apps.ei_edp.utils.lims.sapio.fake.FakeSapioAdapter'

RECORDS_COLLECTION = 'FakeSapioRecords'
COUNTERS_COLLECTION = 'FakeSapioCounters'

# Sapio's 'Exemplar Sample Types' picklist
SAMPLE_TYPES = [
    'Agarose plug', 'Amplicon', 'Amplified cDNA', 'Amplified gDNA', 'Bacterial Culture',
    'Buffer', 'cDNA', 'Cell Lysate', 'Cells', 'gDNA', 'Lib. Dil', 'Library',
    'miRNA enriched RNA', 'mRNA enriched RNA', 'OmniC Capture DNA', 'Pool',
    'Pre-made library', 'Pre-made library pool', 'rRNA depleted RNA', 'Synthetic DNA',
    'Tailed amplicon', 'Tissue', 'Total RNA',
]


class _FakeRecord:
    # Gives a stored sample the record ID and field access of a Sapio
    # record model so that it can be grouped by `_group_samples_by_batch`
    def __init__(self, document):
        self.document = document
        self.record_id = document['record_id']

    def get_field_value(self, field):
        return self.document['fields'].get(field)


def _records():
    return get_collection_ref(RECORDS_COLLECTION)


def _next_number(counter):
    return get_collection_ref(COUNTERS_COLLECTION).find_one_and_update(
        {'_id': counter}, {'$inc': {'value': 1}}, upsert=True, return_document=ReturnDocument.AFTER
    )['value']


def _sapio_field_maps(schemas: Dict[str, Any], include_protected=True) -> Dict[str, Dict[str, str]]:
    # Map COPO term_name → Sapio field for each Sapio data type, from the
    # schemas' `sapio_name` column ("Object:Field"), as the Sapio adapter does
    field_maps: Dict[str, Dict[str, str]] = {}
    for component_schema in schemas.values():
        for field in component_schema:
            sapio_name = field.get(SAPIO_NAME)
            if not isinstance(sapio_name, str) or ':' not in sapio_name:
                continue
            if not include_protected and field.get(TERM_ACCESS) == 'protected':
                continue
            sapio_object, sapio_field = sapio_name.split(':', 1)
            field_maps.setdefault(sapio_object, {})[field[TERM_NAME]] = sapio_field
    return field_maps


class FakeSapioAdapter(LIMSAdapter):

    def __init__(self):
        if not settings.DEBUG:
            raise ImproperlyConfigured('FakeSapioAdapter is for local testing only (DEBUG is off)')

    # ------------------------------------------------------------------ #
    # Stored records
    # ------------------------------------------------------------------ #
    def _find_project(self, project_id: str):
        if not project_id:
            return None
        return _records().find_one(
            {'data_type': SAPIO_PROJECT_DATA_TYPE, f'fields.{SAPIO_PROJECT_IDENTIFIER}': project_id}
        )

    def _project_samples(self, project) -> List[Dict[str, Any]]:
        if not project:
            return []
        return list(
            _records()
            .find({'data_type': SAPIO_SAMPLE_DATA_TYPE, 'project_record_id': project['record_id']})
            .sort('record_id', 1)
        )

    def _set_fields(self, record, fields: Dict[str, Any]):
        record['fields'].update(fields)
        _records().update_one(
            {'_id': record['_id']},
            {'$set': {f'fields.{field}': value for field, value in fields.items()}},
        )

    def _add_samples(self, project, batch, count) -> List[Dict[str, Any]]:
        samples = []
        for _ in range(count):
            number = _next_number('sample')
            samples.append({
                'data_type': SAPIO_SAMPLE_DATA_TYPE,
                'record_id': _next_number('record_id'),
                'project_record_id': project['record_id'],
                'fields': {
                    SAPIO_SAMPLE_ID: f'FAKE-{number:06d}',
                    SAPIO_OTHER_SAMPLE_ID: f'{project["fields"][SAPIO_PROJECT_IDENTIFIER]}_{number}',
                    SAPIO_EXEMPLAR_SAMPLE_TYPE: batch.get(PROFILE_SAMPLE_TYPE) or '',
                    SAPIO_CONTAINER_TYPE: (batch.get(PROFILE_CONTAINER_TYPE) or '').capitalize(),
                    SAPIO_LIBRARY_TYPE: batch.get(PROFILE_LIBRARY_TYPE) or '',
                    SAPIO_EXEMPLAR_SAMPLE_STATUS: 'Logged',
                },
            })
        if samples:
            _records().insert_many(samples)
        return samples

    def _place_on_plates(self, project_samples, batch_samples):
        # Fill the batch's own plates first, column by column, then new plates,
        # so that a plate never mixes batches (as the Sapio adapter does)
        unplated = [sample for sample in batch_samples if not sample['fields'].get(SAPIO_PLATE_ID)]
        if not unplated:
            return
        batch_plate_ids = sorted({
            sample['fields'][SAPIO_PLATE_ID]
            for sample in batch_samples
            if sample['fields'].get(SAPIO_PLATE_ID)
        })
        new_plate_ids = [
            f'FAKE-PLATE-{_next_number("plate"):06d}'
            for _ in range(math.ceil(len(unplated) / PLATE_WELLS))
        ]
        for plate_id in batch_plate_ids + new_plate_ids:
            taken_wells = {
                (sample['fields'].get(SAPIO_COL_POSITION), sample['fields'].get(SAPIO_ROW_POSITION))
                for sample in project_samples
                if sample['fields'].get(SAPIO_PLATE_ID) == plate_id
            }
            for column in range(1, PLATE_COLUMNS + 1):
                for row in PLATE_ROWS:
                    if unplated and (str(column), row) not in taken_wells:
                        self._set_fields(unplated.pop(0), {
                            SAPIO_PLATE_ID: plate_id,
                            SAPIO_STORAGE_UNIT_PATH: plate_id,
                            SAPIO_COL_POSITION: str(column),
                            SAPIO_ROW_POSITION: row,
                        })

    def read_project(self, project_id: str, project_fields=(), sample_fields=()) -> Dict[str, Any]:
        '''Return a project and its samples as the Playwright tests read them from Sapio.

        Returns {'exists': bool, 'project': {field: value}, 'samples': [{field: value}, ...]}.
        '''
        def values(record, fields):
            return {
                field: record['record_id'] if field == SAPIO_RECORD_ID else record['fields'].get(field)
                for field in fields
            }

        project = self._find_project(project_id)
        if not project:
            return {'exists': False, 'project': {}, 'samples': []}
        return {
            'exists': True,
            'project': values(project, project_fields),
            'samples': [values(sample, sample_fields) for sample in self._project_samples(project)],
        }

    # ------------------------------------------------------------------ #
    # Form support
    # ------------------------------------------------------------------ #
    def get_sample_type_options(self) -> List[Dict[str, str]]:
        return [{'value': sample_type, 'label': sample_type} for sample_type in SAMPLE_TYPES]

    # ------------------------------------------------------------------ #
    # Project lifecycle
    # ------------------------------------------------------------------ #
    def validate_profile_change(self, profile: Dict[str, Any]) -> Dict[str, str]:
        project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, '')
        if project_id and not self._find_project(project_id):
            return {
                'status': 'error',
                'message': MESSAGES['sapio_project_not_found'].format(project_id=project_id),
            }
        return {'status': 'success'}

    def get_submitted_sample_counts(self, profile: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        samples = self._project_samples(self._find_project(profile.get(PROFILE_SAPIO_PROJECT_ID, '')))
        return {
            batch[BATCH_ID]: {
                'submitted': sum(
                    1 for sample in batch_samples if sample.get_field_value(SAPIO_CUSTOMER_SAMPLE_NAME)
                ),
                'total': len(batch_samples),
            }
            for batch, batch_samples in _group_samples_by_batch(
                get_sample_batches(profile), [_FakeRecord(sample) for sample in samples]
            )
        }

    def delete_samples(self, record_ids: List[int]) -> Dict[str, str]:
        if record_ids:
            _records().delete_many(
                {'data_type': SAPIO_SAMPLE_DATA_TYPE, 'record_id': {'$in': list(record_ids)}}
            )
        return {'status': 'success'}

    def sync_project(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, '')
        project_name = profile.get(PROFILE_RELATED_ENQ) or profile.get(PROFILE_TITLE) or ''
        if not project_name:
            return {'status': 'success', 'project_id': project_id}

        project_fields = {
            SAPIO_PROJECT_NAME: project_name,
            SAPIO_PROJECT_DESC: profile.get(PROFILE_DESCRIPTION) or '',
        }
        if profile.get(PROFILE_BUDGET_HOLDER):
            project_fields[SAPIO_BUDGET_HOLDER] = profile[PROFILE_BUDGET_HOLDER]

        if project_id:
            project = self._find_project(project_id)
            if not project:
                return {
                    'status': 'warning',
                    'project_id': project_id,
                    'message': MESSAGES['sapio_project_update_failed'],
                    'created_sample_record_ids': [],
                }
            self._set_fields(project, project_fields)
        else:
            project_id = f'PR{_next_number("project"):06d}'
            project = {
                'data_type': SAPIO_PROJECT_DATA_TYPE,
                'record_id': _next_number('record_id'),
                'fields': {SAPIO_PROJECT_IDENTIFIER: project_id, **project_fields},
            }
            _records().insert_one(project)

        project_samples = self._project_samples(project)
        batch_sample_record_ids: Dict[str, List[int]] = {}
        created_sample_record_ids: List[int] = []
        samples_to_remove: List[Dict[str, Any]] = []
        for batch, batch_records in _group_samples_by_batch(
            get_sample_batches(profile), [_FakeRecord(sample) for sample in project_samples]
        ):
            batch_samples = [record.document for record in batch_records]

            # Create or remove samples so that the batch has as many as it requests.
            # Only samples without a customer name can be removed.
            no_of_samples = int(batch.get(PROFILE_NO_OF_SAMPLES) or 0)
            if len(batch_samples) < no_of_samples:
                new_samples = self._add_samples(project, batch, no_of_samples - len(batch_samples))
                created_sample_record_ids.extend(sample['record_id'] for sample in new_samples)
                project_samples.extend(new_samples)
                batch_samples.extend(new_samples)
            removable = [
                sample for sample in batch_samples if not sample['fields'].get(SAPIO_CUSTOMER_SAMPLE_NAME)
            ]
            excess = len(batch_samples) - no_of_samples
            if excess > len(removable):
                return {
                    'status': 'warning',
                    'project_id': project_id,
                    'message': MESSAGES['sapio_project_update_failed'],
                    'created_sample_record_ids': created_sample_record_ids,
                }
            removed = removable[len(removable) - excess:] if excess > 0 else []
            samples_to_remove.extend(removed)
            batch_samples = [sample for sample in batch_samples if sample not in removed]

            container_type = (batch.get(PROFILE_CONTAINER_TYPE) or '').capitalize()
            for sample in batch_samples:
                self._set_fields(sample, {
                    SAPIO_EXEMPLAR_SAMPLE_TYPE: batch.get(PROFILE_SAMPLE_TYPE) or '',
                    SAPIO_CONTAINER_TYPE: container_type,
                    SAPIO_LIBRARY_TYPE: batch.get(PROFILE_LIBRARY_TYPE) or '',
                })
            if container_type == 'Plate':
                self._place_on_plates(project_samples, batch_samples)
            else:
                for sample in batch_samples:
                    self._set_fields(sample, {
                        SAPIO_PLATE_ID: None,
                        SAPIO_STORAGE_UNIT_PATH: None,
                        SAPIO_COL_POSITION: None,
                        SAPIO_ROW_POSITION: None,
                    })

            batch_sample_record_ids[batch[BATCH_ID]] = [sample['record_id'] for sample in batch_samples]

        self.delete_samples([sample['record_id'] for sample in samples_to_remove])
        return {
            'status': 'success',
            'project_id': project_id,
            'batch_sample_record_ids': batch_sample_record_ids,
        }

    def delete_project(self, project_id: str) -> Dict[str, str]:
        project = self._find_project(project_id)
        if not project:
            return {'status': 'warning', 'message': MESSAGES['sapio_project_delete_failed']}
        _records().delete_many({'project_record_id': project['record_id']})
        _records().delete_one({'_id': project['_id']})
        return {'status': 'success'}

    # ------------------------------------------------------------------ #
    # Manifest
    # ------------------------------------------------------------------ #
    def _to_term_records(self, samples, schemas) -> List[Dict[str, Any]]:
        sample_field_map = _sapio_field_maps({'sample': schemas.get('sample', [])}).get(
            SAPIO_SAMPLE_DATA_TYPE, {}
        )
        return [
            {term_name: sample['fields'].get(sapio_field) for term_name, sapio_field in sample_field_map.items()}
            for sample in samples
        ]

    def get_project_samples(self, project_id: str, schemas: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self._to_term_records(self._project_samples(self._find_project(project_id)), schemas)

    def get_batch_samples(
        self, profile: Dict[str, Any], batch_id: str, schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        samples = self._project_samples(self._find_project(profile.get(PROFILE_SAPIO_PROJECT_ID, '')))
        for batch, batch_records in _group_samples_by_batch(
            get_sample_batches(profile), [_FakeRecord(sample) for sample in samples]
        ):
            if batch[BATCH_ID] == batch_id:
                return self._to_term_records([record.document for record in batch_records], schemas)
        return []

    def get_project_metadata(self, project_id: str) -> Dict[str, Any]:
        project = self._find_project(project_id)
        if not project:
            return {}
        metadata = {LIMS_HEALTH_AND_SAFETY: project['fields'].get(SAPIO_HEALTH_AND_SAFETY)}
        samples = self._project_samples(project)
        if samples:
            metadata[LIMS_SAMPLE_RETURN] = samples[0]['fields'].get(SAPIO_SAMPLE_RETURN)
        return metadata

    def submit_manifest(
        self, project_id: str, schemas: Dict[str, Any], components: Dict[str, Any]
    ) -> Dict[str, Any]:
        project = self._find_project(project_id)
        if not project:
            return {
                'status': 'error',
                'message': MESSAGES['sapio_project_not_found'].format(project_id=project_id),
            }
        samples = self._project_samples(project)
        samples_by_sample_id = {sample['fields'].get(SAPIO_SAMPLE_ID): sample for sample in samples}

        # Only fields that aren't 'protected' are written, as the Sapio adapter does
        field_maps = _sapio_field_maps(schemas, include_protected=False)
        project_field_map = field_maps.get(SAPIO_PROJECT_DATA_TYPE, {})
        sample_field_map = field_maps.get(SAPIO_SAMPLE_DATA_TYPE, {})

        def mapped_values(row, field_map):
            return {
                field_map[term_name]: _to_sapio_value(term_name, value)
                for term_name, value in row.items()
                if term_name in field_map
            }

        # Study fields are written to the project and, where they
        # map to a sample field, to every sample of the project
        for row in components.get('study', []):
            self._set_fields(project, mapped_values(row, project_field_map))
            sample_values = mapped_values(row, sample_field_map)
            if sample_values:
                for sample in samples:
                    self._set_fields(sample, sample_values)

        skipped_samples = []
        for row in components.get('sample', []):
            sample = samples_by_sample_id.get(row.get(SAMPLE_ID, ''))
            if not sample:
                skipped_samples.append(str(row.get(SAMPLE_ID, '')))
                continue
            self._set_fields(sample, mapped_values(row, sample_field_map))

        if skipped_samples:
            return {
                'status': 'warning',
                'message': MESSAGES['sapio_manifest_samples_skipped'].format(
                    project_id=project_id, sample_ids=', '.join(skipped_samples)
                ),
                'record_id': project['record_id'],
                'skipped_samples': skipped_samples,
            }
        return {
            'status': 'success',
            'message': MESSAGES['sapio_manifest_submitted'].format(project_id=project_id),
            'record_id': project['record_id'],
            'skipped_samples': [],
        }
