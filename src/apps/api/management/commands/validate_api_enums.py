'''
Purpose: This command ensures consistency between Swagger API and
backend enums by validating that the enum values defined in the
Swagger YAML file match those defined in the backend code.

To execute run: `python manage.py validate_api_enums`
'''
import os
import yaml
from django.conf import settings
from django.core.management import BaseCommand
from src.apps.api.enums import *


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = 'Validate enum values in Swagger against backend enums'

    def __init__(self):
        super().__init__()

    def handle(self, *args, **options):
        self.stdout.write('Validating enum values in Swagger against backend enums')
        swagger = self.load_swagger_yaml()

        # List of parameters to check
        parameters_to_check = [
            ('AssociatedProjects', AssociatedProjectEnum),
            ('AssociatedProjects_multiple', AssociatedProjectEnum),
            ('ManifestVersions', ProjectVersionsEnum),
            ('Projects', ProjectEnum),
            ('Projects_multiple', ProjectEnum),
            ('ReturnTypes', ReturnTypeEnum),
            ('SampleTypes', ProjectEnum),
        ]

        # Loop through the parameters and perform the check
        for param_name, enum_class in parameters_to_check:
            self.check_enum_in_swagger(swagger, param_name, enum_class)

    def load_swagger_yaml(self):
        # Load the Swagger YAML file
        api_file_path = os.path.join(
            settings.STATIC_ROOT, 'swagger', 'openapi_copo_dtol.yml'
        )

        if not os.path.exists(api_file_path):
            raise FileNotFoundError(
                f'Swagger file not found at {api_file_path}. Please check the path.'
            )

        # Load the Swagger YAML file
        with open(api_file_path) as f:
            swagger = yaml.safe_load(f)
        return swagger

    # Retrieve enum values from the backend
    def get_enum_values_from_backend(self, enum_class):
        return set(enum_class.values())

    # Check if enum values match between the backend and Swagger
    def check_enum_in_swagger(self, swagger_data, schema_name, enum_class):
        # Iterate over parameters to find matching ones by name
        schemas = swagger_data.get('components', {}).get('schemas', {})
        for key, value in schemas.items():
            if key == schema_name and ('enum' in value or 'enum' in value.get('items', {})):
                # Enum values from Swagger
                swagger_enum = set(value['enum']) if 'enum' in value else set(value['items']['enum'])
                # Enum values from backend
                backend_enum = self.get_enum_values_from_backend(enum_class)

                # Check if all the values in the backend enum are present in the Swagger enum
                if not swagger_enum.issubset(backend_enum):
                    self.stdout.write(
                        self.style.ERROR(f"Enum mismatch for schema '{schema_name}'!")
                    )
                    self.stdout.write(f'Swagger enum: {swagger_enum}')
                    self.stdout.write(f'Backend enum: {backend_enum}')

                    # Get items in either swagger_enum or backend_enum but not in both
                    diff = list(set(swagger_enum).symmetric_difference(backend_enum))
                    self.stdout.write(f"The difference is '{diff}'")
                break
            else:
                self.style.ERROR(f"Schema '{schema_name}' not found in Swagger.")
