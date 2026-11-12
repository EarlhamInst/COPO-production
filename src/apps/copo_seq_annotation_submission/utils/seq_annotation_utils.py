from common.validators.ena_validators.validation_messages import MESSAGES
from src.apps.copo_assembly_submission.utils.assembly_utils import has_valid_extension, to_html_list

# The file extensions accepted for each file type. The 'sequence_annotation' types are
# sent to ENA as the analysis XML's filetype, with or without gzip; ENA's Webin-CLI
# only accepts gzipped GFF3 for 'decoupled_annotation'.
SEQUENCE_ANNOTATION_FILE_TYPE_EXTS = {
    'gff': ['.gff', '.gff.gz'],
    'tab': ['.tab', '.tab.gz'],
    'fasta': ['.fasta', '.fasta.gz'],
    'bed': ['.bed', '.bed.gz'],
    'gff3': ['.gff3.gz'],
}


def get_file_type_extensions(file_types):
    '''Return the extensions accepted for any of the given file types, in order.'''
    return [ext for file_type in file_types for ext in SEQUENCE_ANNOTATION_FILE_TYPE_EXTS[file_type]]


SEQUENCE_ANNOTATION_TYPES_MAP = {
    'sequence_annotation': {
        'num_form_file_rows': 3,
        'valid_file_extensions': get_file_type_extensions(['gff', 'tab', 'fasta', 'bed']),
        'valid_file_types': [('gff', 'GFF'), ('tab', 'TAB'), ('fasta', 'FASTA'), ('bed', 'BED')],
    },
    'decoupled_annotation': {
        'num_form_file_rows': 1,
        'valid_file_extensions': get_file_type_extensions(['gff3']),
        'valid_file_types': [('gff3', 'GFF3')],
    }
}

SEQUENCE_ANNOTATION_TYPES = list(SEQUENCE_ANNOTATION_TYPES_MAP.keys())


def get_sequence_annotation_file_error(file_name, file_type=None):
    # Check a sequence annotation file's extension against its file type when one is given.
    file_name = str(file_name or '').strip()
    file_type = str(file_type or '').strip().lower()
    route_types = [value for value, _ in SEQUENCE_ANNOTATION_TYPES_MAP['sequence_annotation']['valid_file_types']]

    if not file_type:
        valid_exts = get_file_type_extensions(route_types)
        if has_valid_extension(file_name, valid_exts):
            return ''
        return MESSAGES['invalid_file_extension'].format(file_name=file_name, valid_exts=to_html_list(valid_exts))

    if file_type not in route_types:
        return MESSAGES['unsupported_sequence_annotation_file_type'].format(
            file_type=file_type, file_name=file_name, valid_types=to_html_list(route_types))

    valid_exts = SEQUENCE_ANNOTATION_FILE_TYPE_EXTS[file_type]
    if has_valid_extension(file_name, valid_exts):
        return ''
    return MESSAGES['file_type_extension_mismatch'].format(
        file_name=file_name, file_type=file_type, valid_exts=to_html_list(valid_exts))


def get_assembly_accession_choices(assemblies):
    '''Split assembly records into GCA choices and the names of those still waiting for a GCA.

       NB: ENA assigns GCA accessions days after assemblies have been submitted.
       The `update_assembly_gca_accessions` function later saves the assigned GCA accession 
       to the assembly record.
    '''
    choices = []
    pending = []
    for assembly in assemblies:
        if not assembly.get('accession'):
            continue  # This means that the assembly has not been submitted to ENA as yet
        name = assembly.get('assemblyname', '')
        gca_accession = assembly.get('gca_accession')
        if gca_accession:
            choices.append((gca_accession, f'{name} ({gca_accession})'))
        else:
            pending.append(name)
    return choices, pending