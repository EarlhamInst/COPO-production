EDP_SCHEMA_NAME = 'EI_EDP' # Type of Single-cell schema for EDP profiles
EDP_PROFILE_TYPE = 'ei_edp'
EDP_USERS_GROUP = 'ei_edp_users' # Django group for EDP users

# Study fields from an EDP profile that correspond to cells
# in a LIMS manifest that must have a value filled in
EDP_PROJECT_STUDY_FIELDS = ['customer_email', 'sample_return', 'health_safety']
