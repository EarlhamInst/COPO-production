"""Unit tests for the ENA run.xml filetype chosen for each read file.

COPO used to declare every file that was not .bam/.cram as filetype "fastq",
discarding the File Type the submitter picked in the manifest. ENA rejected a
616GB Oxford Nanopore archive with:

    In filename: "<submission>/reads/fast5.tar.gz", filetype: "fastq".
    Invalid file suffix for file "fast5.tar.gz". File archival is not allowed
    for file type "fastq".

The manifest had declared OxfordNanopore_native, which is a valid ENA filetype.

Imports common.ena_utils.run_filetype rather than ena_helper: the latter reads
WEBIN_USER at module level and cannot be imported without a populated
environment.
"""

import pytest

from common.ena_utils.run_filetype import ena_run_filetype


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
    assert ena_run_filetype(file_name, declared_type) == expected


@pytest.mark.parametrize('declared_type', [None, float('nan'), 0, ''])
def test_missing_declared_type_falls_back_to_extension(declared_type):
    """Rows come from a dataframe, so the value may be NaN, None or empty."""
    assert ena_run_filetype('reads.bam', declared_type) == 'bam'


def test_unknown_extension_without_declared_type_keeps_legacy_fastq():
    """Previous behaviour for anything unrecognised. ENA will reject a .tar.gz
    declared this way, which is the bug that prompted the change, but silently
    guessing OxfordNanopore_native for every archive would be worse."""
    assert ena_run_filetype('fast5.tar.gz', '') == 'fastq'
