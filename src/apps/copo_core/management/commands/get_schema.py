from common.schemas.utils.data_utils import get_copo_schema
from django.core.management.base import BaseCommand
import json

class Command(BaseCommand):
    help = 'Retrieve schema from the database'

    COMPONENT_TYPES = [
        'approval',
        'comment',
        'datafile',
        'duration',
        'environment_variables',
        'hydroponics',
        'material_attribute_value',
        'metadata_template',
        'miappe_rooting_field',
        'miappe_rooting_greenhouse',
        'ontology_annotation',
        'person',
        'phenotypic_variables',
        'publication',
        'sample',
        'soil',
        'source'
    ]
    
    def add_arguments(self, parser):
        parser.add_argument(
            'component', 
            type=str, 
            choices=self.COMPONENT_TYPES,
            help='A key in the schema_dict to be retrieved. Options: {}'.format(', '.join(self.COMPONENT_TYPES))
        )

        parser.add_argument('--as_object', type=bool, default=False,
            help='True returns the schema as an object whose element can be ' \
                'accessed using the "." notation. False for the traditional ' \
                'Python dictionary access')
    
    def handle(self, *args, **kwargs):
        component = kwargs.get('component', str())
        as_object = kwargs.get('as_object', False)
        schema = get_copo_schema(component, as_object)
        # Output the data as a JSON string for use in a script
        self.stdout.write(json.dumps(schema, indent=4))