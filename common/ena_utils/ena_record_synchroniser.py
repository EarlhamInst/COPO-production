import os
import shutil
import pandas as pd
import requests

from collections import defaultdict
from datetime import timedelta
from django.conf import settings
from lxml import etree as ET
from pathlib import Path
from pymongo import UpdateOne

from common.dal.sample_da import Sample, Source
from common.schema_versions.lookup.dtol_lookups import (
    DTOL_ENA_MAPPINGS,
    TOL_PROFILE_TYPES,
)
from common.schemas.utils.data_utils import join_with_and
from common.utils.copo_email import CopoEmail
from common.utils.helpers import get_datetime
from common.utils.logger import Logger

l = Logger()


class EnaRecordSynchroniser:
    # Purpose: To update samples and sources in the database
    # with the latest information from their corresponding records in ENA

    def __init__(self, sample_types=list(), biosample_accessions=list()):
        self.sample_types = sample_types
        self.biosample_accessions = biosample_accessions
        self.max_accessions = 10000
        self.repository = 'ena'

        # Initialise lists to store update and warning reports
        # if there are any changes to be made to the system records
        self.update_report = []
        self.warning_report = []
        self.sync_report_run_id = None
        self.sync_report_timestamp = None
        # Similar to os.path.join(settings.BASE_DIR, 'reports')
        self.reports_directory = Path(settings.BASE_DIR) / 'reports'
        self.reports_directory_housekeeping_days = 5
        self.sync_report_file_name = f'{self.repository}-sync-report'

        additional_output_fields = {
            '_id': 0,
            'biosampleAccession': 1,
            'COLLECTION_LOCATION': 1,
            'sample_type': 1,
        }
        self.additional_projected_fields = {
            'TAXON_ID': {'ena': 'TAXON_ID'},
            'SCIENTIFIC_NAME': {'ena': 'SCIENTIFIC_NAME'},
        }

        self.all_ena_fields_map = {
            **DTOL_ENA_MAPPINGS,
            **self.additional_projected_fields,
        }

        self.system_fields_mapped_to_bio_material = [
            x
            for x in self.all_ena_fields_map
            if self.all_ena_fields_map[x].get('ena') == 'bio_material'
        ]

        self.ena_fields = [
            v['ena'] for v in self.all_ena_fields_map.values() if 'ena' in v
        ]

        self.projection = {
            copo_field: 1 for copo_field in self.all_ena_fields_map
        } | additional_output_fields

        # Handle the `COLLECTION_LOCATION` field
        # i.e. COLLECTION_LOCATION_1 and COLLECTION_LOCATION_2
        # in the projection dictionary
        self.projection = {
            key: value
            for key, value in self.projection.items()
            if 'COLLECTION_LOCATION' not in key
        }
        self.projection['COLLECTION_LOCATION'] = 1

        # Date related fields based on sample type
        self.sample_type_date_fields_map = {
            'tol_sample_types': {
                'time_updated': None,
                'updated_by': 'system',
                'update_type': 'system',
            },
            'tol_sources': {
                'time_updated': None,
            },
            'isasample': {
                'date_modified': None,
                'updated_by': 'system',
            },
        }

    def _build_filter(self, sample_type_list, biosample_accession_list):
        filter_dict = {}

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

    def _build_date_update_dict(self, sample_type):
        # Get the date fields based on sample type
        update_fields = {}

        match sample_type:
            case x if x in TOL_PROFILE_TYPES:
                update_fields = self.sample_type_date_fields_map['tol_sample_types']
            case x if x.endswith('_specimen') and any(
                x.startswith(prefix) for prefix in TOL_PROFILE_TYPES
            ):
                update_fields = self.sample_type_date_fields_map['tol_sources']
            case 'isasample':
                update_fields = self.sample_type_date_fields_map['isasample']
            case _:
                return update_fields

        # Set the current date for the date fields
        output_dict = {
            field: (
                get_datetime() if field not in ['updated_by', 'update_type'] else value
            )
            for field, value in update_fields.items()
        }

        return output_dict

    def _normalise_records(self, record, collection_name):
        record_type = record.pop('sample_type', None)

        if collection_name == 'source':
            collection_name = Source()
        elif collection_name == 'sample':
            collection_name = Sample()

            # Handle cases where the `TAXON_ID` and `SCIENTIFIC_NAME`
            # fields are nested within the 'species_list' list
            if all(
                k in record for k in ['species_list', 'TAXON_ID', 'SCIENTIFIC_NAME']
            ):
                # Remove the existing `TAXON_ID` field if it exists
                record.pop('species_list', None)

            if 'species_list' in record:
                if 'TAXON_ID' not in record and len(record['species_list']) > 0:
                    record['TAXON_ID'] = record['species_list'][0].get(
                        'TAXON_ID', 'N/A'
                    )

                if 'SCIENTIFIC_NAME' not in record and len(record['species_list']) > 0:
                    record['SCIENTIFIC_NAME'] = record['species_list'][0].get(
                        'SCIENTIFIC_NAME', 'N/A'
                    )
                record.pop('species_list', None)

                # ToDo: Consider unpacking 'species_list' into the document

        return {
            'accession': record['biosampleAccession'],
            'collection': collection_name,
            'record_type': record_type,
            'data': record,
        }

    def _normalise_value(self, value):
        return ' | '.join(part.strip().casefold() for part in value.split('|')).replace(
            '_', ' '
        )

    def _find_matching_ena_field_value(self, system_value, ena_values):
        # Try exact/normalised matching to get exact matches
        for index, ena_value in enumerate(ena_values):
            if self._normalise_value(system_value) == self._normalise_value(ena_value):
                return index, ena_value

        return None, None

    def _get_ena_sample_attribute_values(self, sample_node, tag_name):
        # Handle special case for fields in ENA like `bio_material` field
        # The `bio_material` field is mapped to several fields in the system database
        # namely `DNA_VOUCHER_ID_FOR_BIOBANKING`and `TISSUE_VOUCHER_ID_FOR_BIOBANKING`
        sample_attributes = sample_node.find('SAMPLE_ATTRIBUTES')

        values = [
            attribute.findtext('VALUE')
            for attribute in sample_attributes.findall('SAMPLE_ATTRIBUTE')
            if attribute.findtext('TAG') == tag_name
        ]
        return values

    def _get_system_value(self, system_field, mapping, data):
        '''
        Returns the first non-empty value found from the internal field (COPO field)
        or its equivalent field in ENA

        Cases:
            - DTOL AND ERGA records only have `GAL` and `GAL_SAMPLE_ID` fields in COPO
            - ASG records only have `PARTNER` and `PARTNER_SAMPLE_ID` fields in COPO
            - ENA records only have `GAL` and `GAL_SAMPLE_ID` fields so
                * the `PARTNER` field in COPO is mapped to `GAL` (noted as an alias in `dtol_lookups.py` file) in ENA
                * the `PARTNER_SAMPLE_ID` field in COPO is mapped to `GAL_SAMPLE_ID` (noted as an alias in `dtol_lookups.py` file) in ENA
        '''

        for field in [system_field, *mapping.get('aliases', [])]:
            value = data.get(field)
            if value not in (None, ''):
                return value
        return None

    def _get_collection_location_field_values(
        self, transform_function, sample_node, data
    ):
        '''
        Handle special case for `COLLECTION_LOCATION`

        NB: The `COLLECTION_LOCATION` field in COPO is stored in ENA
        as `geographic location (country and/or sea)` field and
        `geographic location (region and locality)` field

        Example:
        COPO: COLLECTION_LOCATION: 'UNITED KINGDOM | ENGLAND | SOMERSET | BRISTOL | RODWAY COMMON'
        ENA : geographic location (country and/or sea): 'UNITED KINGDOM'
              geographic location (region and locality): 'ENGLAND | SOMERSET | BRISTOL | RODWAY COMMON'
        '''

        system_field = 'COLLECTION_LOCATION'
        # i.e.  'COLLECTION_LOCATION_1'
        collection_location_name_prefix = 'geographic location (country and/or sea)'
        ena_location_value_1 = next(
            iter(
                sample_node.xpath(
                    f'.//SAMPLE_ATTRIBUTE[TAG="{collection_location_name_prefix}"]/VALUE/text()'
                )
            ),
            '',
        )

        # i.e.  'COLLECTION_LOCATION_2'
        collection_location_name_suffix = 'geographic location (region and locality)'
        ena_location_value_2 = next(
            iter(
                sample_node.xpath(
                    f'.//SAMPLE_ATTRIBUTE[TAG="{collection_location_name_suffix}"]/VALUE/text()'
                )
            ),
            '',
        )

        # `system_value` is the value currently stored in the system database
        system_value = data.get(system_field)

        # `ena_value` is the value coming from ENA
        ena_value = f'{ena_location_value_1} | {ena_location_value_2}'.strip()

        # Remove COLLECTION_LOCATION related fields from the mapping to avoid duplication
        # i.e. `COLLECTION_LOCATION_1` and `COLLECTION_LOCATION_2` would be removed
        for location_field in [
            field
            for field in self.all_ena_fields_map
            if transform_function and system_field in field
        ]:
            self.all_ena_fields_map.pop(location_field, None)

        return system_value, ena_value

    def _split_accessions_by_collection(self, accessions):
        # Collection refers to the MongoDB collection where the record is stored
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
        # Note: `TAXON_ID` and `SCIENTIFIC_NAME` are fields that are not
        # stored within the `SAMPLE_ATTRIBUTE` element in the
        # parsed XML from ENA so, they have to be handled separately
        sample_names = {}
        for name_node in sample_node.findall('.//SAMPLE_NAME'):
            for child in name_node:
                if child.tag in self.additional_projected_fields.keys():
                    sample_names[child.tag] = child.text
        return sample_names

    def _parse_sample_attributes(self, sample_node):
        # Function: Get the `SAMPLE_ATTRIBUTE` elements from the parsed XML from ENA
        attrs = {}
        for attr in sample_node.findall('.//SAMPLE_ATTRIBUTE'):
            tag = attr.findtext('TAG')  # Same as tag = attr.find('TAG').text
            if tag not in self.ena_fields:
                # Skip elements that are not in the list of fields
                # that can be submitted to ENA
                continue
            value = attr.findtext('VALUE')  # Same as value = attr.find('VALUE').text
            attrs[tag] = value
        return attrs

    def create_excel_report(self, update_report, warning_report, output_path):
        '''
        Create an Excel report containing:
            - 'Updates' worksheet contains the system updates applied from ENA
            - 'Warnings' worksheet contains records where updates could not be safely applied
        '''
        # Create DataFrames
        updates_df = pd.DataFrame(update_report)
        warnings_df = pd.DataFrame(warning_report)

        # Write DataFrames to Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as f:
            updates_df.to_excel(f, index=False, sheet_name='Updates')
            warnings_df.to_excel(f, index=False, sheet_name='Warnings')

    def send_sync_report_email(
        self, file_path, record_count, update_count, warning_count
    ):
        recipient_email_address = settings.MAIL_ADDRESS  # 'ei.copo@earlham.ac.uk'
        cc_email_addresses = recipient_email_address  # CC to the sender's email address
        email_subject_prefix = (
            ''
            if settings.ENVIRONMENT_TYPE == 'prod'
            else f'[{settings.ENVIRONMENT_TYPE.upper()} SERVER NOTIFICATION] - '
        )
        email_subject = f'{email_subject_prefix}ENA Synchronisation Completed {self.sync_report_timestamp}'
        file_name = os.path.basename(file_path)

        email_body = f'''
        <p>Dear COPO project Team,</p>
        <br><br>
        <p>The European Nucleotide Archive (ENA) synchronisation process has completed successfully.</p>
        <br><br>
        <p>Records processed:{record_count}</p>
        <p>Updates applied: {update_count}</p>
        <p>Warnings: {warning_count}</p>
        <br><br>
        <p>Please find the attached file `{file_name}` for details.</p>
        <br><br><br>
        <p>Best regards,</p>
        <p>Collaborative OPen Omics (COPO) Project Team</p>
        '''

        try:
            CopoEmail().send(
                to=[recipient_email_address],
                sub=email_subject,
                content=email_body,
                html=True,
                cc=[cc_email_addresses],
            )

            # Move file from the 'pending' to 'sent' folder within the 'media/reports' directory
            archive_file_path = file_path.replace('pending', 'sent')
            shutil.move(file_path, archive_file_path)
        except Exception as e:
            l.exception(
                f'Error sending email notification for updates synchronised from {self.repository.upper()}: {e}'
            )

    def remove_old_synced_reports(self):
        '''
        A housekeeping task to delete all .xlsx  or .zip files in the 'reports/pending' folder
        and 'reports/sent' folder older than 30 days that has the prefix 'ena-sync-report'
        '''
        cutoff_time = get_datetime() - timedelta(
            days=self.reports_directory_housekeeping_days
        )

        try:
            for folder in ('pending', 'sent'):
                directory = Path(self.reports_directory) / folder

                for file in directory.glob(f'{self.sync_report_file_name}-*'):
                    if file.suffix in {'.xlsx', '.zip'}:
                        if (
                            get_datetime().fromtimestamp(file.stat().st_mtime)
                            < cutoff_time
                        ):
                            l.debug(f'Deleting file: {file}')
                            file.unlink()
        except Exception as e:
            l.error(f'Error deleting file `{file}`: {e}')

    def send_sync_report(self, record_count):
        if not (self.update_report or self.warning_report):
            return

        current_date = get_datetime().strftime('%Y-%m-%d')
        # Similar code to os.path.join(self.reports_directory, 'pending')
        pending_reports_directory = Path(self.reports_directory) / 'pending'
        pending_reports_directory.mkdir(parents=True, exist_ok=True)

        # Create a 'sent' directory as well
        sent_reports_directory = Path(self.reports_directory) / 'sent'
        sent_reports_directory.mkdir(parents=True, exist_ok=True)

        # The division operator (/) joins the paths.
        file_path = (
            pending_reports_directory
            / f'{self.sync_report_file_name}-{current_date}.xlsx'
        )

        self.create_excel_report(
            self.update_report,
            self.warning_report,
            file_path,
        )

        self.send_sync_report_email(
            file_path,
            record_count,
            len(self.update_report),
            len(self.warning_report),
        )

    def apply_ena_record_changes_to_system_records(self, records):
        # Function: Retrieve records from ENA using biosample accessions
        # and update the corresponding records in the system
        # database if there are any changes
        try:
            # Example command to call to ENA Browser API for retrieving metadata in XML format
            '''
            curl -X 'POST' \
            'https://www.ebi.ac.uk/ena/browser/api/xml' \
            -H 'accept: application/xml' \
            -H 'Content-Type: application/json' \
            -d '{
            "accessions": [
                "SAMEAxxxx",
                "SAMEAxxxx"
            ],
            "expanded": true,
            "annotationOnly": false,
            "lineLimit": 0,
            "download": false,
            "gzip": true,
            "set": true,
            "includeLinks": false,
            "range": "string",
            "complement": true
            }'
            '''
            # Retrieve XML format of output of records from
            # repository using primary accession numbers
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

                # The root is `SAMPLE_SET` so there is no need to do - sample_set = root.find('SAMPLE_SET')
                sample_set = root

                bulk_updates = defaultdict(list)
                db_collection_handles = {}

                for sample_node in sample_set.findall('SAMPLE'):
                    update_dict = {}
                    record_accession = sample_node.attrib['accession']
                    system_record = next(
                        (r for r in records if r['accession'] == record_accession), None
                    )
                    system_record_type = system_record['record_type']

                    # Database information for the current record
                    db_collection = system_record['collection']
                    collection_handle = db_collection.get_collection_handle()
                    collection_name = collection_handle.name
                    db_collection_handles[collection_name] = collection_handle

                    # Parse output from XML string
                    sample_names = self._parse_sample_names(sample_node)
                    attributes = self._parse_sample_attributes(sample_node)

                    # Merge dictionaries. This can also be written as - {**sample_names, **attributes }
                    ena_record = sample_names | attributes

                    remaining_bio_material_fields = (
                        self.system_fields_mapped_to_bio_material.copy()
                    )

                    for system_field, mapping in list(self.all_ena_fields_map.items()):
                        ena_field = mapping.get('ena')
                        transform = mapping.get('ena_data_function')

                        # Account for the removal of COLLECTION_LOCATION
                        # related fields from the mapping
                        if system_field not in self.all_ena_fields_map:
                            continue

                        if transform:
                            # `system_value` is the value currently stored in the system database
                            # `ena_value` is the value coming from ENA
                            if 'COLLECTION_LOCATION' in system_field:
                                system_value, ena_value = (
                                    self._get_collection_location_field_values(
                                        transform, sample_node, system_record['data']
                                    )
                                )
                            else:
                                # The current record's value is transformed using the
                                # function specified by the `ena_data_function` field.
                                system_value = transform(
                                    self._get_system_value(
                                        system_field,
                                        mapping,
                                        system_record['data'],
                                    )
                                )

                                ena_value = ena_record.get(ena_field)
                        elif (
                            ena_field == 'bio_material'
                            and system_field
                            in self.system_fields_mapped_to_bio_material
                            and 'bio_material' in ena_record
                        ):
                            # Handle special case for ENA's `bio_material` field
                            # This field is mapped to several fields in the system database
                            # like `DNA_VOUCHER_ID_FOR_BIOBANKING`and `TISSUE_VOUCHER_ID_FOR_BIOBANKING`

                            # Sometimes, the `bio_material` field is not in ENA's record but the corresponding
                            # fields in the system database have values
                            ena_value = None  # ena_record.get(ena_field)
                            system_value = self._get_system_value(
                                system_field,
                                mapping,
                                system_record['data'],
                            )

                            ena_remaining_values = (
                                self._get_ena_sample_attribute_values(
                                    sample_node, ena_field
                                ).copy()
                            )

                            for field in list(remaining_bio_material_fields):
                                index, value = self._find_matching_ena_field_value(
                                    system_record['data'][field],
                                    ena_remaining_values,
                                )

                                if value is not None:
                                    ena_value = value

                                    # Remove the matched value from the list of remaining values
                                    # and the field from the list of remaining fields
                                    ena_remaining_values.pop(index)
                                    remaining_bio_material_fields.remove(field)

                                if (
                                    len(ena_remaining_values) > 1
                                    and len(remaining_bio_material_fields) > 1
                                ):
                                    l.log(
                                        f'Unable to uniquely map {self.repository.upper()} "bio_material" field values '
                                        f'to system fields for the record matching the accession {record_accession}',
                                    )

                                    self.warning_report.append(
                                        {
                                            'run_id': self.sync_report_run_id,
                                            'timestamp': self.sync_report_timestamp,
                                            'collection': collection_name,
                                            'accession': record_accession,
                                            'record_type': system_record_type,
                                            'system_field': system_field,
                                            'ena_field': ena_field,
                                            'reason': (
                                                'Unable to uniquely map ENA `bio_material` field values'
                                                'because more than one value exists in ENA for the '
                                                '`bio_material` field. This field is mapped to - '
                                                f'{join_with_and(self.system_fields_mapped_to_bio_material)} '
                                                'fields in the system database.'
                                            ),
                                        }
                                    )
                        else:
                            # `system_value` is the value currently stored in the system database
                            # `ena_value` is the value coming from ENA
                            ena_value = ena_record.get(ena_field)
                            system_value = self._get_system_value(
                                system_field,
                                mapping,
                                system_record['data'],
                            )

                        if ena_value is None or system_value is None:
                            continue

                        if system_value != ena_value:
                            # Ignore field values that are the same after normalisation
                            # i.e. values that are identical when compared
                            # case-insensitively and with whitespace ignored
                            if self._normalise_value(
                                system_value
                            ) == self._normalise_value(ena_value):
                                continue

                            update_dict[system_field] = ena_value

                            self.update_report.append(
                                {
                                    'run_id': self.sync_report_run_id,
                                    'timestamp': self.sync_report_timestamp,
                                    'collection': collection_name,
                                    'accession': record_accession,
                                    'record_type': system_record_type,
                                    'system_field': system_field,
                                    'ena_field': ena_field,
                                    'old_system_value': system_value,
                                    'new_value_from_ena': ena_value,
                                }
                            )

                    # Add update query to bulk_updates if there are any changes
                    if update_dict:
                        # Combine the fields to be updated with the 'update' date fields
                        update_dict = {
                            **update_dict,
                            **self._build_date_update_dict(system_record_type),
                        }

                        bulk_updates[collection_name].append(
                            UpdateOne(
                                {'accession': record_accession}, {'$set': update_dict}
                            )
                        )

                # Perform a bulk update instead of individual updates
                # if bulk_updates:
                #     for db_collection_name, updates in bulk_updates.items():
                #         db_collection_handles[db_collection_name].bulk_write(updates)
        except Exception as e:
            l.exception(
                f'Failed to synchronise system records with ENA records: {str(e)}'
            )
