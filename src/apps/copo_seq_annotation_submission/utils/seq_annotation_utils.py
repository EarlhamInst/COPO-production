SEQUENCE_ANNOTATION_TYPES_MAP = {
    'sequence_annotation': {
        'num_form_file_rows': 3,
        'valid_file_extensions': ['.gff', '.gff.gz', '.tab', '.tab.gz', '.fasta', '.fasta.gz', '.bed', '.bed.gz'],
        'valid_file_types': [('gff', 'GFF'), ('tab', 'TAB'), ('fasta', 'FASTA'), ('bed', 'BED')],
    },
    'decoupled_annotation': {
        'num_form_file_rows': 1,
        'valid_file_extensions': ['.gff3.gz'], # ENA only accepts compressed GFF3
        'valid_file_types': [('gff3', 'GFF3')],
    }
}

SEQUENCE_ANNOTATION_TYPES = list(SEQUENCE_ANNOTATION_TYPES_MAP.keys())