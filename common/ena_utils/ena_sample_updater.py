import requests

from collections import defaultdict
from django.conf import settings
from lxml import etree as ET
from pymongo import UpdateOne
from requests_cache import timedelta

from common.dal.sample_da import Sample, Source
from common.schema_versions.lookup.dtol_lookups import DTOL_ENA_MAPPINGS
from common.utils.helpers import get_datetime
from common.utils.logger import Logger

l = Logger()


class EnaSampleUpdater:

    def __init__(self, sample_types=list(), biosample_accessions=list()):
        self.sample_types = sample_types
        self.biosample_accessions = biosample_accessions
        self.cutoff_time = get_datetime() - timedelta(hours=24)
        self.max_accessions = 10000
        self.repository = 'ena'

        additional_output_fields = {'_id': 0, 'biosampleAccession': 1}
        self.additional_projected_fields = {
            'TAXON_ID': {'ena': 'TAXON_ID'},
            'SCIENTIFIC_NAME': {'ena': 'SCIENTIFIC_NAME'},
        }

        self.all_ena_field_mappings = {
            **DTOL_ENA_MAPPINGS,
            **self.additional_projected_fields,
        }
        self.ena_fields = [
            v['ena'] for v in self.all_ena_field_mappings.values() if 'ena' in v
        ]

        self.projection = {
            copo_field: 1 for copo_field in self.all_ena_field_mappings
        } | additional_output_fields

    def _build_filter(self, sample_type_list, biosample_accession_list):
        filter_dict = {
            '$or': [
                {f'last_checked_{self.repository}': {'$exists': False}},
                {f'last_checked_{self.repository}': None},
                {f'last_checked_{self.repository}': {'$lt': self.cutoff_time}},
            ]
        }

        if biosample_accession_list:
            filter_dict['biosampleAccession'] = {'$in': biosample_accession_list}

        if sample_type_list:
            filter_dict.update(
                {
                    'sample_type': {'$in': sample_type_list},
                    'biosampleAccession': {'$exists': True, '$ne': ''},
                }
            )

        return filter_dict

    def _normalise_records(self, record, collection_name):
        if collection_name == 'source':
            collection_name = Source()
        elif collection_name == 'sample':
            collection_name = Sample()

            # Handle cases where TAXON_ID and SCIENTIFIC_NAME is nested in 'species_list' list
            if all(
                k in record for k in ['species_list', 'TAXON_ID', 'SCIENTIFIC_NAME']
            ):
                # Remove the existing TAXON_ID if it exists
                record.pop('species_list', None)

            if 'species_list' in record:
                if 'TAXON_ID' not in record:
                    record['TAXON_ID'] = record['species_list'][0].get(
                        'TAXON_ID', 'N/A'
                    )

                if 'SCIENTIFIC_NAME' not in record:
                    record['SCIENTIFIC_NAME'] = record['species_list'][0].get(
                        'SCIENTIFIC_NAME', 'N/A'
                    )
                record.pop('species_list', None)

                # ToDo: Consider unpacking 'species_list' into the document

        return {
            'accession': record['biosampleAccession'],
            'collection': collection_name,
            'data': record,
        }

    def _split_accessions_by_collection(self, accessions):
        sample = []
        source = []

        for accession in accessions:
            query_dict = {'biosampleAccession': accession}
            if Sample().get_collection_handle().find_one(query_dict, {'id': 1}):
                sample.append(accession)
            elif Source().get_collection_handle().find_one(query_dict, {'id': 1}):
                source.append(accession)
        return sample, source

    def _parse_sample_names(self, sample_node):
        # Note that TAXON_ID and SCIENTIFIC_NAME are not stored as
        # SAMPLE_ATTRIBUTE in ENA, so they have to be
        # handled them separately
        sample_names = {}
        for name_node in sample_node.findall('.//SAMPLE_NAME'):
            for child in name_node:
                if child.tag in self.additional_projected_fields.keys():
                    sample_names[child.tag] = child.text
        return sample_names

    def _parse_sample_attributes(self, sample_node):
        attrs = {}
        for attr in sample_node.findall('.//SAMPLE_ATTRIBUTE'):
            tag = attr.find('TAG').text
            if tag not in self.ena_fields:
                # Skip attributes that are not in the list of ENA fields
                continue
            value = attr.find('VALUE').text
            attrs[tag] = value
        return attrs

    def update_ena_records(self, records):
        try:
            # Retrieve XML format of output of records from repository
            # using primary accession numbers
            accessions = [r['accession'] for r in records]
            payload = {
                'accessions': accessions,
                'expanded': True,
                'annotationOnly': False,
                'lineLimit': 0,
                'download': False,
                'gzip': True,
                'set': True,
                'includeLinks': False,
                'range': 'string',
                'complement': True,
            }

            response = requests.post(
                settings.ENA_BROWSER_API_URL['xml'],
                headers={
                    'accept': 'application/xml',
                    'Content-Type': 'application/json',
                },
                json=payload,
                timeout=30,
            )

            if response.status_code == requests.codes.ok:
                xml_str = response.text
                xml = xml_str.encode('utf-8')
                parser = ET.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
                root = ET.fromstring(xml, parser=parser)

                # The root is SAMPLE_SET so there is no need to do root.find('SAMPLE_SET')
                sample_set = root

                bulk_updates = defaultdict(list)
                db_collection_handles = {}

                for sample_node in sample_set.findall('SAMPLE'):
                    update_dict = {}
                    record_accession = sample_node.attrib['accession']
                    current_record = next(
                        (r for r in records if r['accession'] == record_accession), None
                    )

                    # Database related information for the current record
                    db_collection = current_record['collection']
                    collection_handle = db_collection.get_collection_handle()
                    collection_name = collection_handle.name
                    db_collection_handles[collection_name] = collection_handle

                    # Parse output from ENA's XML string
                    sample_names = self._parse_sample_names(sample_node)
                    attributes = self._parse_sample_attributes(sample_node)

                    # Merge dictionaries. Also written as: {**sample_names, **attributes }
                    record = sample_names | attributes

                    for internal_field, mapping in self.all_ena_field_mappings.items():
                        ena_field = mapping.get('ena')
                        transform = mapping.get('ena_data_function')

                        if transform:
                            new_value = transform(record)
                        else:
                            new_value = record.get(ena_field)

                        if new_value is None:
                            continue

                        current_value = current_record['data'].get(internal_field)
                        if current_value != new_value:
                            update_dict[internal_field] = new_value

                    # Add update query to bulk_updates if there are any changes
                    if update_dict:
                        update_dict[f'last_checked_{self.repository}'] = get_datetime()

                        bulk_updates[collection_name].append(
                            UpdateOne(
                                {'accession': record_accession}, {'$set': update_dict}
                            )
                        )

                # Perform bulk update instead of individual updates to the database
                if bulk_updates:
                    for db_collection_name, updates in bulk_updates.items():
                        db_collection_handles[db_collection_name].bulk_write(updates)
        except Exception as e:
            l.error(f'API call error: {str(e)}')
