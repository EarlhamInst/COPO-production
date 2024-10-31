from common.schemas.utils.data_utils import convertStringToTitleCase

# Function to determine the class name for a jQuery DataTable column
def set_column_class_name(label, copo_api_labels=list()):
    if label.lower().endswith('accession'):
        return 'ena-accession'
    elif label.lower() in copo_api_labels:
        return 'copo-api'
    return ''

# Function to generate the title for a jQuery DataTable column
def generate_column_title(label, copo_api_labels=list()):
    title_case_label = convertStringToTitleCase(label)
    # Special case for COPO API labels to append 'Link' to the title
    if label.lower() in copo_api_labels:
        return f"{title_case_label.replace('Record Identifier', 'Sample').replace('Manifest Identifier', 'Manifest')} Link"
    return title_case_label