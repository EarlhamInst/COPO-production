from common.validators.ena_validators.validation_messages import MESSAGES

VALID_CHROMOSOME_FILE_EXTS = ['.gz', '.bz2']
VALID_FASTA_FILE_EXTS = ['.fasta.gz', '.fas.gz', '.fsa.gz', '.fna.gz', '.fa.gz',
                         '.fasta.bz2', '.fas.bz2', '.fsa.bz2', '.fna.bz2', '.fa.bz2']

# AGP is no longer accepted by ENA, so it is not listed
ASSEMBLY_FIELD_EXT_MAP = {
    'fasta': VALID_FASTA_FILE_EXTS,
    'flatfile': ['.gz', '.bz2'],
    'chromosome_list': VALID_CHROMOSOME_FILE_EXTS,
    'unlocalised_list': VALID_CHROMOSOME_FILE_EXTS,
}
ASSEMBLY_FILE_FIELDS = list(ASSEMBLY_FIELD_EXT_MAP.keys())

FILE_COMPRESSION_SIGNATURES = {
    '.gz': b'\x1f\x8b',
    '.bz2': b'BZh',
}
FILE_SIGNATURE_LENGTH = max(len(s) for s in FILE_COMPRESSION_SIGNATURES.values())


def has_valid_extension(file_name, valid_exts):
    return any(str(file_name).endswith(ext) for ext in valid_exts)


def to_html_list(values):
    return '<ul>' + ''.join(f'<li>{v}</li>' for v in values) + '</ul>'


def get_expected_signature(file_name):
    '''Return the magic bytes expected for the file's compression extension, or None.'''
    return next((sig for ext, sig in FILE_COMPRESSION_SIGNATURES.items()
                 if str(file_name).endswith(ext)), None)


def matches_expected_file_signature(file_name, header):
    '''True if the file header matches the compression its extension claims.

    Files whose extension does not imply a compression are not checked.
    '''
    expected = get_expected_signature(file_name)
    return expected is None or (header or b'').startswith(expected)


def read_file_header(file_path, n=FILE_SIGNATURE_LENGTH):
    try:
        with open(file_path, 'rb') as f:
            return f.read(n)
    except OSError:
        return b''


def get_assembly_file_error(file_type, file_name):
    '''Check an assembly file's type and extension.

    file_type is the ENA manifest field, e.g. FASTA or CHROMOSOME_LIST
    (case-insensitive). Returns an HTML error message, or '' if the file is valid.
    '''
    file_type = str(file_type or '').strip()
    file_name = str(file_name or '').strip()
    valid_exts = ASSEMBLY_FIELD_EXT_MAP.get(file_type.lower())

    if valid_exts is None:
        return MESSAGES['unsupported_assembly_file_type'].format(
            file_type=file_type, file_name=file_name,
            valid_types=to_html_list(f.upper() for f in ASSEMBLY_FILE_FIELDS))

    if not has_valid_extension(file_name, valid_exts):
        return MESSAGES['invalid_file_extension'].format(
            file_name=file_name, valid_exts=to_html_list(valid_exts))

    return ''
