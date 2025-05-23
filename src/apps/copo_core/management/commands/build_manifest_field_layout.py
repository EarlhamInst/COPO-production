'''
This script generates minimal field layout JSON files for each manifest type,
based on the field blocks, column letters, and other attributes defined in the 
Standard Operating Procedure (SOP).
It groups and structures minimal field definitions such as name, label, 
required and type according to their position and role in the spreadsheet layout.

The output JSON files are saved in the 
`/common/schema_versions/isa_mappings/manifest_field_layouts/` directory,
one per manifest type and provide a simplified view of the manifest structure
useful for validation, generation or documentation purposes.

To execute the script, run the following Django command in the terminal:
    $  python manage.py build_manifest_field_layout
'''

import json
import pandas as pd
import warnings

import common.schemas.utils.data_utils as d_utils
from django.conf import settings
from django.core.management import BaseCommand
from openpyxl.utils.cell import get_column_letter, column_index_from_string
from os.path import join, exists
from itertools import groupby

from common.schemas.utils.data_utils import get_copo_schema
from common.schema_versions.lookup.dtol_lookups import (
    DTOL_ENUMS,
    DTOL_RULES,
    DTOL_UNITS,
)
from src.apps.api.enums import ProjectEnum

# Ignore the warning: UserWarning: Unknown extension is not supported and will be removed warn(msg)
warnings.simplefilter('ignore')


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = 'Generate the manifest type fields JSON file for the Tree of Life (ToL) projects'

    def handle(self, *args, **options):
        self.stdout.write('Generating manifest field layout...')

        self.set_data()

        # Generate a json file for each manifest type
        self.generate_manifest_json(self.manifest_map)
        self.stdout.write(
            self.style.SUCCESS(
                f'{d_utils.join_with_and([x.upper() for x in self.manifest_map])} manifest fields JSON files generated successfully!'
            )
        )

    def set_data(self):
        # Create a map with the manifest type
        # and its corresponding latest version
        excluded_manifest_types = ['dtolenv']

        self.manifest_map = {
            x.lower(): settings.MANIFEST_VERSION.get(x.upper(), '')
            for x in ProjectEnum.values()
            if x.lower() not in excluded_manifest_types
        }

        # Blocks
        self.manifest_blocks = {
            'asg': [
                {
                    'start_letter': 'A',
                    'end_letter': 'D',
                    'column_colour': 'green',
                    'block': '1',
                    'block_description': 'Sample submission information including specimen identifier and tube/well identifiers',
                },
                {
                    'start_letter': 'E',
                    'end_letter': 'M',
                    'column_colour': 'yellow',
                    'block': '2',
                    'block_description': 'Taxonomic information including species name, family and common name',
                },
                {
                    'start_letter': 'N',
                    'end_letter': 'R',
                    'column_colour': 'purple',
                    'block': '3',
                    'block_description': 'Biological information of the sample including lifestage, sex, and organism part',
                },
                {
                    'start_letter': 'S',
                    'end_letter': 'T',
                    'column_colour': 'grey',
                    'block': '4',
                    'block_description': 'Details of the submitting GAL and the associated organisational codes',
                },
                {
                    'start_letter': 'U',
                    'end_letter': 'AL',
                    'column_colour': 'blue',
                    'block': '5',
                    'block_description': 'Data on the collector, collection event, and collection localities',
                },
                {
                    'start_letter': 'AM',
                    'end_letter': 'AQ',
                    'column_colour': 'limegreen',
                    'block': '6',
                    'block_description': 'Information on taxonomic identification, taxonomic uncertainty and risks',
                },
                {
                    'start_letter': 'AR',
                    'end_letter': 'AX',
                    'column_colour': 'brown',
                    'block': '7',
                    'block_description': 'Details of the tissue preservation event',
                },
                {
                    'start_letter': 'AY',
                    'end_letter': 'BF',
                    'column_colour': 'teal',
                    'block': '8',
                    'block_description': 'Information on DNA barcoding',
                },
                {
                    'start_letter': 'BG',
                    'end_letter': 'BK',
                    'column_colour': 'red',
                    'block': '9',
                    'block_description': 'Information on vouchering and biobanking',
                },
                {
                    'start_letter': 'BL',
                    'end_letter': 'BP',
                    'column_colour': 'skyblue',
                    'block': '10',
                    'block_description': 'IAdditional information fields including free text field for other important sample notes',
                },
            ],
            'dtol': [
                {
                    'start_letter': 'A',
                    'end_letter': 'D',
                    'column_colour': 'green',
                    'block': '1',
                    'block_description': 'Sample submission information including specimen identifier and tube/well identifiers',
                },
                {
                    'start_letter': 'E',
                    'end_letter': 'M',
                    'column_colour': 'yellow',
                    'block': '2',
                    'block_description': 'Taxonomic information including species name, family and common name',
                },
                {
                    'start_letter': 'N',
                    'end_letter': 'R',
                    'column_colour': 'purple',
                    'block': '3',
                    'block_description': 'Biological information of the sample including lifestage, sex, and organism part',
                },
                {
                    'start_letter': 'S',
                    'end_letter': 'T',
                    'column_colour': 'grey',
                    'block': '4',
                    'block_description': 'Details of the submitting GAL and the associated organisational codes',
                },
                {
                    'start_letter': 'U',
                    'end_letter': 'AL',
                    'column_colour': 'blue',
                    'block': '5',
                    'block_description': 'Data on the collector, collection event, and collection localities',
                },
                {
                    'start_letter': 'AM',
                    'end_letter': 'AQ',
                    'column_colour': 'limegreen',
                    'block': '6',
                    'block_description': 'Information on taxonomic identification, taxonomic uncertainty and risks',
                },
                {
                    'start_letter': 'AR',
                    'end_letter': 'AX',
                    'column_colour': 'brown',
                    'block': '7',
                    'block_description': 'Details of the tissue preservation event',
                },
                {
                    'start_letter': 'AY',
                    'end_letter': 'BF',
                    'column_colour': 'teal',
                    'block': '8',
                    'block_description': 'Information on DNA barcoding',
                },
                {
                    'start_letter': 'BG',
                    'end_letter': 'BK',
                    'column_colour': 'red',
                    'block': '9',
                    'block_description': 'Information on vouchering and biobanking',
                },
                {
                    'start_letter': 'BL',
                    'end_letter': 'BP',
                    'column_colour': 'skyblue',
                    'block': '10',
                    'block_description': 'IAdditional information fields including free text field for other important sample notes',
                },
            ],
            'erga': [
                {
                    'start_letter': 'A',
                    'end_letter': 'F',
                    'column_colour': 'green',
                    'block': '1',
                    'block_description': 'Sample submission information including specimen identifier and tube/well identifiers,as well as information on the sample coordinator',
                },
                {
                    'start_letter': 'G',
                    'end_letter': 'O',
                    'column_colour': 'yellow',
                    'block': '2',
                    'block_description': 'Taxonomic information including species name, family and common name',
                },
                {
                    'start_letter': 'P',
                    'end_letter': 'T',
                    'column_colour': 'purple',
                    'block': '3',
                    'block_description': 'Biological information of the sample including lifestage, sex, and organism part',
                },
                {
                    'start_letter': 'U',
                    'end_letter': 'V',
                    'column_colour': 'grey',
                    'block': '4',
                    'block_description': 'Details of the submitting GAL and the associated organisational codes',
                },
                {
                    'start_letter': 'W',
                    'end_letter': 'AR',
                    'column_colour': 'blue',
                    'block': '5',
                    'block_description': 'Data on the collector, collection event, and collection localities',
                },
                {
                    'start_letter': 'AS',
                    'end_letter': 'AW',
                    'column_colour': 'limegreen',
                    'block': '6',
                    'block_description': 'Information on taxonomic identification, taxonomic uncertainty and risks',
                },
                {
                    'start_letter': 'AX',
                    'end_letter': 'BD',
                    'column_colour': 'brown',
                    'block': '7',
                    'block_description': 'Details of the tissue preservation event',
                },
                {
                    'start_letter': 'BE',
                    'end_letter': 'BI',
                    'column_colour': 'teal',
                    'block': '8',
                    'block_description': 'Information on DNA barcoding',
                },
                {
                    'start_letter': 'BJ',
                    'end_letter': 'BU',
                    'column_colour': 'red',
                    'block': '9',
                    'block_description': 'Information on Biobanking and Vouchering',
                },
                {
                    'start_letter': 'BV',
                    'end_letter': 'CI',
                    'column_colour': 'orange',
                    'block': '10',
                    'block_description': 'Information on regulatory compliances, Indigenous rights, traditional knowledge and permits',
                },
                {
                    'start_letter': 'CJ',
                    'end_letter': 'CM',
                    'column_colour': 'green',
                    'block': '11',
                    'block_description': 'Additional information including a free text field to house other important sample notes',
                },
            ],
        }

    def get_manifest_fields(self, manifest_type):
        schema = get_copo_schema(component='sample')

        # Get project defined field names
        # These are usually field names that are uppercase or uppercase with underscores
        # Retrieving fields based on manifest version is not considered for this task
        # because the fields not pertaining to the latest version can be fetched from the API
        field_map = {
            x['id'].split('.')[-1]: {
                'label': x.get('label', ''),
                'required': (
                    d_utils.convertStringToBoolean(x.get('required', False))
                    if isinstance(x.get('required', False), str)
                    else x.get('required', False)
                ),
                'type': x.get('type', 'string'),
            }
            for x in schema
            if x.get('specifications', [])
            and x.get('specifications', [])[0] == manifest_type
            and 'tol' in x.get('standards', {})
        }
        return field_map

    def generate_manifest_json(self, manifest_map):
        for manifest_type in manifest_map:
            manifest_mapping = self.get_manifest_fields(manifest_type)
            current_manifest_version = manifest_map[manifest_type]
            manifest_file_path = join(
                settings.MEDIA_ROOT,
                'assets',
                'manifests',
                f'{manifest_type.upper()}_MANIFEST_TEMPLATE_v{current_manifest_version}.xlsx',
            )

            if not exists(manifest_file_path):
                self.stdout.write(
                    self.style.ERROR(
                        f"Manifest file not found at '{manifest_file_path}'. Skipping {manifest_type.upper()} manifest."
                    )
                )
                continue

            output = []

            columns = pd.read_excel(manifest_file_path).columns.tolist()
            column_letters = []
            column_blocks = []

            manifest_blocks = self.manifest_blocks.get(manifest_type, [])

            for i in range(len(columns)):
                column_letter = get_column_letter(i + 1)
                column_letters.append({columns[i]: column_letter})
                column_block = [
                    colour_range['block']
                    for colour_range in manifest_blocks
                    if column_index_from_string(colour_range['start_letter'])
                    <= column_index_from_string(column_letter)
                    <= column_index_from_string(colour_range['end_letter'])
                ]

                column_blocks.append({'field': columns[i], 'block': column_block[0]})

            column_blocks_grouped = [
                {key: [g['field'] for g in group]}
                for key, group in groupby(column_blocks, lambda x: x['block'])
            ]

            for d in manifest_blocks:
                fields_lst = []
                output_dict = {}
                output_dict['block'] = d['block']
                output_dict['block_description'] = d['block_description']
                output_dict['block_colour'] = d['column_colour']

                columns = [
                    v
                    for d in column_blocks_grouped
                    for k, v in d.items()
                    if k == output_dict['block']
                ]

                for column_name in columns[0]:
                    element = manifest_mapping.get(column_name, '')
                    fields_dict = {}

                    fields_dict['name'] = column_name.strip()
                    fields_dict['label'] = element.get('label', '')

                    try:
                        fields_dict['column'] = [
                            value
                            for d in column_letters
                            for key, value in d.items()
                            if key == fields_dict['name']
                        ][0]
                    except IndexError:
                        fields_dict['column'] = ''

                    fields_dict['type'] = (
                        element.get('type', 'string')
                        if column_name in manifest_mapping
                        and element.get('type', 'string')
                        else 'string'
                    )
                    fields_dict['required'] = (
                        element.get('required', False)
                        if column_name in manifest_mapping
                        and element.get('required', False)
                        else False
                    )

                    if column_name in DTOL_RULES:
                        for key, value in DTOL_RULES[column_name].items():
                            if key.endswith('regex'):
                                fields_dict['regex'] = value

                    if column_name in DTOL_ENUMS:
                        if manifest_type.upper() in DTOL_ENUMS[column_name]:
                            fields_dict['enum'] = DTOL_ENUMS[column_name][
                                manifest_type.upper()
                            ]
                        else:
                            fields_dict['enum'] = DTOL_ENUMS[column_name]

                    if column_name in DTOL_UNITS:
                        fields_dict['unit'] = DTOL_UNITS[column_name]['ena_unit']

                    fields_lst.append(fields_dict)

                    output_dict['fields'] = fields_lst

                output.append(output_dict)

            file_name = (
                f'{manifest_type}_manifest_fields_v{current_manifest_version}.json'
            )
            file_path = join(
                settings.BASE_DIR,
                'common',
                'schema_versions',
                'isa_mappings',
                'manifest_field_layouts',
                file_name,
            )

            with open(file_path, 'w+') as f:
                print(
                    json.dumps(output, indent=4, sort_keys=False, default=str), file=f
                )
                f.close()
