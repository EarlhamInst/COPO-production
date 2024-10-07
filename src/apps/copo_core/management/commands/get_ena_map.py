from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from common.schema_versions.lookup.dtol_lookups import  DTOL_ENA_MAPPINGS, DTOL_RULES, DTOL_UNITS

import json

lg = settings.LOGGER

class Command(BaseCommand):
    help = 'Get European Nucleotide Archive (ENA) field mapping based on the specified type'

    MAP_TYPES = {
        'name': 'Retrieve the mapping of ENA field names to Tree of Life (ToL) field names ',
        'rules': 'Retrieve the rules associated with ENA field mappings.',
        'unit': 'Retrieve the units for the ENA field mappings.',
    }

    def add_arguments(self, parser):
        parser.add_argument(
            'map_type', 
            type=str, 
            choices=self.MAP_TYPES.keys(),
            help='The type of ENA mapping to retrieve. Options: {}'.format(', '.join(self.MAP_TYPES.keys()))
        )
    
    def handle(self, *args, **kwargs):
        map_type = kwargs['map_type']
        data = None

        if map_type == 'name':
            data = self.get_ena_field_name_map()
        elif map_type == 'rules':
            data = self.get_ena_field_rules()
        elif map_type == 'unit':
            data = self.get_ena_field_unit_map()

        if data is not None:
            # Output the data as a JSON string for use in a script
            self.stdout.write(json.dumps(data, indent=4))
        else:
            error = 'No data found for the specified map type'
            self.stderr.write(self.style.ERROR(error))
            raise CommandError(error)
        
    def get_ena_field_name_map(self):
        serialisable_data = {}
        non_serialisable_data = {}

        # Loop through the dictionary and check each key:value pair
        for key, value in DTOL_ENA_MAPPINGS.items():
            try:
                # Try to serialise the value to check if it's JSON serialisable
                json.dumps(value)
                # If no error, add it to the serialisable_data dictionary
                serialisable_data[key] = value
            except TypeError:
                # If there is a TypeError (e.g., value is a function), add to non_serialisable_data
                non_serialisable_data[key] = value

                if 'ena' in value:
                    serialisable_data[key] = {'ena': value['ena']}

        # Report any non-serialisable data
        if non_serialisable_data:
            lg.exception(f'Non-serialisable data found: {non_serialisable_data}')

        # Output the serialisable data
        return serialisable_data
    
    def get_ena_field_rules(self):
        rules = DTOL_RULES
        return  rules
    
    def get_ena_field_unit_map(self):
        units = DTOL_UNITS
        return  units