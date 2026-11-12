'''Unit tests for which compressed (and uncompressed) data files COPO accepts.

Each submission type follows what ENA documents for it:

| Submission type | ENA's rule | COPO accepts |
|---|---|---|
| Assembly (Webin-CLI) | "all data files must be compressed with GZIP" [1] | .gz only |
| Sequence annotation (analysis XML) | No compression rule [2] | Uncompressed or .gz |
| Decoupled GFF3 annotation (Webin-CLI) | Not documented [3], but Webin-CLI 11.0.0 rejects uncompressed GFF3 in testing | .gff3.gz only |
| Reads (FASTQ) | "The Fastq files must be compressed using gzip or bzip2." [4] | .gz or .bz2 |

[1] https://ena-docs.readthedocs.io/en/latest/submit/fileprep/assembly.html
[2] https://ena-docs.readthedocs.io/en/latest/submit/analyses/sequence-annotation.html
[3] https://ena-docs.readthedocs.io/en/latest/submit/gff3.html
[4] https://ena-docs.readthedocs.io/en/latest/submit/fileprep/reads.html

So .bz2 is only accepted for reads; every other type rejects it.

To execute these tests, run: $ pytest test/unit/ena_utils/test_ena_file_compression.py
'''

import pandas as pd
import pytest

from src.apps.copo_assembly_submission.forms import AssemblyForm
from src.apps.copo_assembly_submission.utils import assembly_utils
from src.apps.copo_read_submission.utils.ena_validator.ena_seq_validators import FileSuffixValidator
from src.apps.copo_seq_annotation_submission.forms import AnnotationFilesForm
from src.apps.copo_seq_annotation_submission.utils.seq_annotation_utils import SEQUENCE_ANNOTATION_TYPES_MAP

ASSEMBLY_FILE_NAMES = {
    'FASTA': 'genome.fasta',
    'FLATFILE': 'genome.embl',
    'CHROMOSOME_LIST': 'chromosomes.txt',
    'UNLOCALISED_LIST': 'unlocalised.txt',
}
SEQUENCE_ANNOTATION_FILE_NAMES = ['features.gff', 'features.tab', 'features.fasta', 'features.bed']


def offered_files(form, field, ecs_files):
    '''The files a form's dropdown offers, out of all those uploaded.'''
    return [value for value, _ in form.fields[field].choices if value in ecs_files]


# ── Assemblies: gzip only ───────────────────────────────────────────────

@pytest.mark.parametrize('file_type, file_name', ASSEMBLY_FILE_NAMES.items())
def test_gzipped_assembly_file_is_accepted(file_type, file_name):
    assert assembly_utils.get_assembly_file_error(file_type, f'{file_name}.gz') == ''


@pytest.mark.parametrize('file_type, file_name', ASSEMBLY_FILE_NAMES.items())
def test_uncompressed_assembly_file_is_rejected(file_type, file_name):
    error = assembly_utils.get_assembly_file_error(file_type, file_name)

    assert error.startswith(f'Invalid file <strong>{file_name}</strong>')


@pytest.mark.parametrize('file_type, file_name', ASSEMBLY_FILE_NAMES.items())
def test_bzip2_assembly_file_is_rejected(file_type, file_name):
    # Used by both the assembly form and the reads manifest's `assembly_file` worksheet
    error = assembly_utils.get_assembly_file_error(file_type, f'{file_name}.bz2')

    assert error.startswith(f'Invalid file <strong>{file_name}.bz2</strong>')
    assert '.bz2' not in error.split('</summary>')[-1]  # .bz2 is not listed as a valid extension


@pytest.mark.parametrize('field', assembly_utils.ASSEMBLY_FILE_FIELDS)
def test_every_assembly_extension_is_gzip(field):
    assert all(ext.endswith('.gz') for ext in assembly_utils.ASSEMBLY_FIELD_EXT_MAP[field])


def test_assembly_form_offers_only_gzipped_files():
    ecs_files = ['genome.fasta.gz', 'genome.fasta.bz2', 'genome.fasta',
                 'chromosomes.txt.gz', 'chromosomes.txt.bz2', 'chromosomes.txt']

    form = AssemblyForm(ecs_files=ecs_files)

    assert offered_files(form, 'fasta', ecs_files) == ['genome.fasta.gz']
    # Chromosome lists only need to be gzipped, so any .gz file is offered
    assert offered_files(form, 'chromosome_list', ecs_files) == ['genome.fasta.gz', 'chromosomes.txt.gz']


# ── Sequence annotation: uncompressed or gzip ───────────────────────────

SEQUENCE_ANNOTATION_EXTENSIONS = SEQUENCE_ANNOTATION_TYPES_MAP['sequence_annotation']['valid_file_extensions']


@pytest.mark.parametrize('file_name', SEQUENCE_ANNOTATION_FILE_NAMES)
def test_uncompressed_sequence_annotation_file_is_accepted(file_name):
    assert file_name.endswith(tuple(SEQUENCE_ANNOTATION_EXTENSIONS))


@pytest.mark.parametrize('file_name', SEQUENCE_ANNOTATION_FILE_NAMES)
def test_gzipped_sequence_annotation_file_is_accepted(file_name):
    assert f'{file_name}.gz'.endswith(tuple(SEQUENCE_ANNOTATION_EXTENSIONS))


@pytest.mark.parametrize('file_name', SEQUENCE_ANNOTATION_FILE_NAMES)
def test_bzip2_sequence_annotation_file_is_rejected(file_name):
    assert not f'{file_name}.bz2'.endswith(tuple(SEQUENCE_ANNOTATION_EXTENSIONS))


def test_sequence_annotation_form_offers_uncompressed_and_gzipped_files_only():
    ecs_files = ['features.tab', 'features.tab.gz', 'features.tab.bz2']

    form = AnnotationFilesForm(ecs_files=ecs_files, annotation_type='sequence_annotation')

    assert offered_files(form, 'file', ecs_files) == ['features.tab', 'features.tab.gz']


# ── Decoupled GFF3 annotation: gzip only ────────────────────────────────

@pytest.mark.parametrize('file_name, offered', [
    ('genes.gff3.gz', True),
    ('genes.gff3', False),  # Webin-CLI rejects uncompressed GFF3
    ('genes.gff3.bz2', False),
])
def test_decoupled_annotation_form_offers_only_gzipped_gff3(file_name, offered):
    form = AnnotationFilesForm(ecs_files=[file_name], annotation_type='decoupled_annotation')

    assert (offered_files(form, 'file', [file_name]) == [file_name]) is offered


# ── Reads: gzip or bzip2 ────────────────────────────────────────────────

def validate_read_files(file_names):
    data = pd.DataFrame({'file_name': [file_names]})
    validator = FileSuffixValidator(profile_id='profile1', fields=[], data=data, errors=[], warnings=[], flag=True)
    errors, warnings, flag, _ = validator.validate()
    return errors, flag


@pytest.mark.parametrize('file_names', [
    'reads_1.fastq.gz,reads_2.fastq.gz',
    'reads_1.fastq.bz2,reads_2.fastq.bz2',  # ENA documents bzip2 for FASTQ
    'reads.fastq.bz2',
])
def test_gzipped_or_bzip2_fastq_files_are_accepted(file_names):
    assert validate_read_files(file_names) == ([], True)


def test_uncompressed_fastq_file_is_rejected():
    errors, flag = validate_read_files('reads.fastq')

    assert flag is False
    assert len(errors) == 1
