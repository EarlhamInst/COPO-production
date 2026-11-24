# Unit tests for the ENA run.xml filetype chosen for each read file

import pytest

from common.ena_utils.ena_helper import evaluate_read_file_type


@pytest.mark.parametrize(
    'file_name, declared_type, expected',
    [
        # the manifest's declared type wins
        ('fast5.tar.gz', 'OxfordNanopore_native', 'OxfordNanopore_native'),
        ('reads.fastq.gz', 'fastq', 'fastq'),
        ('reads.bam', 'bam', 'bam'),
        ('reads.cram', 'cram', 'cram'),
        # ENA's spelling is restored regardless of the case submitted
        ('fast5.tar.gz', 'oxfordnanopore_native', 'OxfordNanopore_native'),
        ('fast5.tar.gz', 'OXFORDNANOPORE_NATIVE', 'OxfordNanopore_native'),
        ('reads.fastq.gz', 'FASTQ', 'fastq'),
        # no declared type: fall back to the extension, as before
        ('reads.bam', '', 'bam'),
        ('reads.cram', '', 'cram'),
        ('reads.fastq.gz', '', 'fastq'),
        # a declared type ENA does not accept is ignored rather than sent on
        ('reads.bam', 'not-a-filetype', 'bam'),
        ('reads.fastq.gz', 'not-a-filetype', 'fastq'),
    ],
)
def test_filetype_resolution(file_name, declared_type, expected):
    assert evaluate_read_file_type(file_name, declared_type) == expected


@pytest.mark.parametrize('declared_type', [None, float('nan'), 0, ''])
def test_missing_declared_type_falls_back_to_extension(declared_type):
    assert evaluate_read_file_type('reads.bam', declared_type) == 'bam'


def test_unknown_extension_without_declared_type_keeps_legacy_fastq():
    assert evaluate_read_file_type('fast5.tar.gz', '') == 'fastq'
