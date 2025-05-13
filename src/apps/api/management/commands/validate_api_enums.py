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

import common.schemas.utils.data_utils as d_utils
from src.apps.api.enums import *


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = 'Validate enum values in Swagger against backend enums'

    def handle(self, *args, **options):
        self.stdout.write(
            'Validating enum values in Swagger API against backend enums...'
        )
        swagger = self.load_swagger_yaml()

        # List of Swagger API parameters to check
        parameters_to_check = [
            ('AssociatedProjects', AssociatedProjectEnum),
            ('AssociatedProjects_multiple', AssociatedProjectEnum),
            ('ManifestVersions', ProjectVersionsEnum),
            ('Projects', ProjectEnum),
            ('Projects_multiple', ProjectEnum),
            ('ReturnTypes', ReturnTypeEnum),
            ('Sample_long2', SampleFieldsEnum),
            ('SampleTypes', ProjectEnum),
            ('SequencingCentres', SequencingCentreEnum),
            ('Standards', StandardEnum),
            ('SampleAudit_field_query_options', UpdateAuditFieldEnum),
        ]

        # Loop through the parameters and perform the check
        for param_name, enum_class in parameters_to_check:
            self.check_swagger_enums(swagger, param_name, enum_class)

        self.stdout.write(self.style.SUCCESS('\nValidation complete!'))

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
    def retrieve_backend_enums(self, enum_class, schema_name):
        values = enum_class.values()
        if enum_class is ProjectEnum and schema_name != 'SampleTypes':
            values = [v.upper() for v in values]
        return set(values)

    # Check if enum values match between the backend and Swagger
    def check_swagger_enums(self, swagger_data, schema_name, enum_class):
        # Iterate over parameters to find matching ones by name
        schemas = swagger_data.get('components', {}).get('schemas', {})
        for key, value in schemas.items():
            if schema_name not in schemas:
                self.style.ERROR(f"Schema '{schema_name}' not found in Swagger.")
            elif key == schema_name and (
                'enum' in value or 'enum' in value.get('items', {})
            ):
                # Enum values from Swagger
                swagger_enum = (
                    set(value['enum'])
                    if 'enum' in value
                    else set(value['items']['enum'])
                )
                # Enum values from backend
                backend_enum = self.retrieve_backend_enums(enum_class, schema_name)

                # Check if all the values in the backend enum are present in the Swagger enum
                if not swagger_enum.issubset(backend_enum):
                    self.stdout.write(
                        self.style.ERROR(
                            f"\nEnum mismatch for schema '{schema_name}' and enum class '{enum_class.__name__}'!"
                        )
                    )

                    # Get differences in swagger and backend enums
                    backend_only = set(backend_enum) - set(swagger_enum)
                    swagger_only = set(swagger_enum) - set(backend_enum)

                    if backend_only:
                        backend_only = d_utils.join_with_and(sorted(backend_only))
                        self.stdout.write(
                            f"Difference (i.e. items only in backend_enum but not in swagger_enum ): {backend_only}"
                        )

                    if swagger_only:
                        swagger_only = d_utils.join_with_and(sorted(swagger_only))
                        self.stdout.write(
                            f"Difference (i.e. items only in swagger_enum but not in backend_enum ): {swagger_only}"
                        )
                break
