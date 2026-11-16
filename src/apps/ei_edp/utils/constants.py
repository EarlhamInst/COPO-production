EDP_SCHEMA_NAME = 'EI_EDP' # Type of Single-cell schema for EDP profiles
EDP_PROFILE_TYPE = 'ei_edp'
EDP_USERS_GROUP = 'ei_edp_users' # Django group for EDP users

# Django group for EDP project managers. Only they (and data managers) can create
# EDP profiles and register samples. The users that a profile is shared with
# can only upload the LIMS and samples' metadata.
EDP_MANAGERS_GROUP = 'ei_edp_managers'
DATA_MANAGERS_GROUP = 'data_managers'

# -- EDP profile fields --
PROFILE_TITLE = 'title'
PROFILE_DESCRIPTION = 'description'
PROFILE_TYPE = 'type'
PROFILE_USER_ID = 'user_id'
PROFILE_SAPIO_PROJECT_ID = 'sapio_project_id'
PROFILE_RELATED_ENQ = 'related_enq'

# Jira PIP e.g. PIP-1234. Each batch of samples has its own. It is only
# only stored and displayed in COPO; it is not saved in Sapio.
PROFILE_RELATED_PIP = 'related_pip'
JIRA_PIP_REGEX = r'^PIP-[0-9]+$'
JIRA_BROWSE_URL = 'https://earlham-institute.atlassian.net/browse/{issue}'
PROFILE_BUDGET_HOLDER = 'budget_holder'
PROFILE_NO_OF_SAMPLES = 'no_of_samples'
PROFILE_SAMPLE_TYPE = 'sample_type'
PROFILE_CONTAINER_TYPE = 'container_type'
PROFILE_LIBRARY_TYPE = 'library_type'
PROFILE_USER_EMAIL = 'user_email'
PROFILE_USER_EMAIL_TOKEN = 'user_email_token'
PROFILE_SAMPLE_BATCHES = 'sample_batches'

# -- Sample batches --
# Each time samples are registered under an EDP profile, a batch is added to
# the profile's `sample_batches` list. A batch keeps the fields below plus the
# LIMS record IDs of its samples so that its samples can be told apart from
# those of other batches in the same LIMS project.
BATCH_ID = 'batch_id'
BATCH_DATE_CREATED = 'date_created'
BATCH_DATE_MODIFIED = 'date_modified'
# `None` for a batch moved from the fields an EDP profile used to have, until
# the next LIMS sync claims the project's existing samples for it
BATCH_LIMS_SAMPLE_RECORD_IDS = 'lims_sample_record_ids'

# Batch fields entered by users (with their form labels). They cannot be
# changed once any of the batch's samples have been submitted.
BATCH_FIELDS = {
    PROFILE_NO_OF_SAMPLES: 'Number of samples',
    PROFILE_SAMPLE_TYPE: 'Sample type',
    PROFILE_CONTAINER_TYPE: 'Container type',
    PROFILE_LIBRARY_TYPE: 'Library type',
    PROFILE_RELATED_PIP: 'Jira PIP',
}
# The batch fields that describe its samples i.e. all but the Jira PIP
BATCH_SAMPLE_FIELDS = [field for field in BATCH_FIELDS if field != PROFILE_RELATED_PIP]
# Field on COPO sample records that names the batch the sample was uploaded
# for. Samples uploaded before batches existed have none and belong to the
# profile's first batch.
SAMPLE_BATCH_ID = 'edp_batch_id'
CONTAINER_TYPES = ('Plate', 'Tube')
# A library type only applies to these sample types
PREMADE_LIBRARY_SAMPLE_TYPES = ('Pre-made library', 'Pre-made library pool')

# -- Column names that describe each field in the EDP schema --
TERM_NAME = 'term_name'
TERM_LABEL = 'term_label'
TERM_DESCRIPTION = 'term_description'
TERM_TYPE = 'term_type'
TERM_LENGTH = 'term_length'
TERM_ACCESS = 'term_access'
# Maps a term to its Sapio field in the format "<SapioDataType>:<FieldName>"
SAPIO_NAME = 'sapio_name'
CHOICE = 'choice' # Enum/dropdown menu options 

# -- Fields in LIMS manifests --
STUDY_ID = 'study_id'
USER_EMAIL = 'user_email'
SAMPLE_RETURN = 'sample_return'
HEALTH_SAFETY = 'health_safety'
SAMPLE_ID = 'sample_id'
SUBMITTER_SAMPLE_REFERENCE = 'submitter_sample_reference'
TAXON_ID = 'taxon_id'
SCIENTIFIC_NAME = 'scientific_name'
BIOSAMPLE_ACCESSION = 'biosampleAccession'
PRESERVATIVE = 'preservative'
APPLICABLE_REGULATIONS = 'applicable_regulations'

# Study fields from an EDP profile that correspond to cells
# in a LIMS manifest that must have a value filled in
EDP_PROJECT_STUDY_FIELDS = [USER_EMAIL, SAMPLE_RETURN, HEALTH_SAFETY]

# -- Project-level metadata keys returned by a LIMS adapter --
LIMS_HEALTH_AND_SAFETY = 'health_and_safety'
LIMS_SAMPLE_RETURN = 'sample_return'

# -- Sapio data types --
SAPIO_PROJECT_DATA_TYPE = 'Project'
SAPIO_SAMPLE_DATA_TYPE = 'Sample'
SAPIO_PLATE_DATA_TYPE = 'Plate'
SAPIO_DIRECTORY_DATA_TYPE = 'Directory'
SAPIO_DEPARTMENT_DATA_TYPE = 'VeloxDepartment'
SAPIO_PLATE_WELL_ELEMENT_DATA_TYPE = 'PlateDesignerWellElement'
SAPIO_QC_DATUM_DATA_TYPE = 'QCDatum'
SAPIO_INDEX_BARCODE_DATA_TYPE = 'IndexBarcode'

# -- Sapio fields --
# Shared by more than one Sapio data type
SAPIO_RECORD_ID = 'RecordId'
SAPIO_DATA_RECORD_NAME = 'DataRecordName'
SAPIO_SAMPLE_ID = 'SampleId'
SAPIO_PLATE_ID = 'PlateId'
SAPIO_ROW_POSITION = 'RowPosition'
SAPIO_COL_POSITION = 'ColPosition'

# Project data type fields
SAPIO_PROJECT_IDENTIFIER = 'C_ProjectIdentifier'
SAPIO_PROJECT_NAME = 'ProjectName'
SAPIO_PROJECT_DESC = 'ProjectDesc'
SAPIO_BUDGET_HOLDER = 'C_BudgetHolder'
SAPIO_HEALTH_AND_SAFETY = 'C_HandS'

# Sample data type fields
SAPIO_CUSTOMER_SAMPLE_NAME = 'C_CustomerSampleName'
SAPIO_EXEMPLAR_SAMPLE_TYPE = 'ExemplarSampleType'
SAPIO_EXEMPLAR_SAMPLE_STATUS = 'ExemplarSampleStatus'
SAPIO_CONTAINER_TYPE = 'ContainerType'
SAPIO_LIBRARY_TYPE = 'C_LibraryType'
SAPIO_STORAGE_UNIT_PATH = 'StorageUnitPath'
SAPIO_SAMPLE_RETURN = 'C_SampleReturn'
SAPIO_CONCENTRATION = 'Concentration'
SAPIO_VOLUME = 'Volume'

# Plate data type fields
SAPIO_PLATE_SAMPLE_TYPE = 'PlateSampleType'
SAPIO_PLATE_COLUMNS = 'PlateColumns'
SAPIO_PLATE_ROWS = 'PlateRows'

# PlateDesignerWellElement data type fields
SAPIO_PLATE_RECORD_ID = 'PlateRecordId'
SAPIO_LAYER = 'Layer'
SAPIO_SOURCE_DATA_TYPE_NAME = 'SourceDataTypeName'
SAPIO_SOURCE_RECORD_ID = 'SourceRecordId'
SAPIO_ALIQUOT_SAMPLE_RECORD_ID = 'AliquotSampleRecordId'
SAPIO_IS_CONTROL = 'IsControl'

# QCDatum data type fields
SAPIO_DATUM_TYPE = 'DatumType'
SAPIO_QC_VOLUME = 'C_Volume'

# IndexBarcode data type fields
# NB: In Sapio, an 'IndexBarcode' record is displayed as 'Assigned Index'
SAPIO_INDEX_ID = 'IndexId'
SAPIO_OTHER_SAMPLE_ID = 'OtherSampleId'

# Sample fields whose values are copied onto the
# 'Customer Supplied' `QCDatum` keyed by the `Sample` field
# name with the `QCDatum` field name as the value
# This is because more than one record type in Sapio
# applies to those sample fields.
SAMPLE_TO_QC_DATUM_FIELDS = {
    SAPIO_CONCENTRATION: SAPIO_CONCENTRATION,
    SAPIO_VOLUME: SAPIO_QC_VOLUME,
}

# LIMS manifest values that are swapped for the value Sapio
# expects before they are submitted, keyed by the field's term name.
# Values are matched case-insensitively. Values that are not listed
# are submitted as they are. Extend this when a manifest value
# differs from Sapio's picklist value for the same answer.
COPO_TO_SAPIO_VALUES = {
    APPLICABLE_REGULATIONS: {
        'None': 'Not relevant',
        'UK-GDPR': 'GDPR',
        'EU-GDPR': 'GDPR',
    },
    PRESERVATIVE: {
        'None': 'Not relevant', # This isn't an option in Sapio's picklist for `preservative`.
    },
}
