from enum import Enum

from src.apps.ei_edp.utils.EDPSchemaHandler import (
    EDPSchemaHandler,
    EDPSchemaSpreadsheet,
)
from .SingleCellSchemasHandler import (
    SinglecellschemasSpreadsheet,
    SingleCellSchemasHandler,
)

class SchemaClassType(str, Enum):
    HANDLER = 'handler'
    SPREADSHEET = 'spreadsheet'


# Schemas that need their own classes are defined here. 
# Anything that is not listed will use the defaults in 
# the `DEFAULT_SCHEMA_CLASSES` dictionary.
SCHEMA_CLASS_MAP = {
    'EI_EDP': {
        SchemaClassType.HANDLER: EDPSchemaHandler,
        SchemaClassType.SPREADSHEET: EDPSchemaSpreadsheet,
    },
}

DEFAULT_SCHEMA_CLASSES = {
    SchemaClassType.HANDLER: SingleCellSchemasHandler,
    SchemaClassType.SPREADSHEET: SinglecellschemasSpreadsheet,
}


def get_schema_class(schema_name, class_type):
    return SCHEMA_CLASS_MAP.get(schema_name, DEFAULT_SCHEMA_CLASSES)[class_type]
