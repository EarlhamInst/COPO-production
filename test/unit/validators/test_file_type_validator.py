'''Unit tests for checking that each manifest file matches the file type provided.

Some manifest worksheets have a file type column next to the file names. Each
file must end in an extension of the type chosen on its row:

- Reads manifest, `read_file`: `read_file_type` for `read_1_file` and `read_2_file`.
  ENA (https://ena-docs.readthedocs.io/en/latest/submit/fileprep/reads.html):
  FASTQ compressed with gzip or bzip2, .bam and .cram suffixes, and Oxford
  Nanopore fast5 files as a single tar.gz archive.
  - assembly_file: `assembly_file_type` for `assembly_file`
  - sequencing_annotation_file: `sequencing_annotation_file_type` for `sequencing_annotation_file`
  
- Single cell manifest, `expression_data_file`: `expression_data_file_format`
  for `expression_data_file`. These go to Zenodo, so the extensions are COPO's.

To execute these tests, run: $ pytest test/unit/validators/test_file_type_validator.py
'''

import numpy as np
import pandas as pd
import pytest

from common.ena_utils.ena_helper import READ_FILE_TYPE_EXTS
from src.apps.copo_single_cell_submission.utils.validator.SingleCellSchemaValidators import (
    EXPRESSION_DATA_FILE_FORMAT_EXTS, FileTypeValidator, get_file_type_error)


def run_validator(component, rows):
    data = pd.DataFrame(rows)
    errors, _, flag, _ = FileTypeValidator(
        profile_id='profile1', fields=None, data=data, errors=[], warnings=[], flag=True,
        component=component, isupdate=False).validate()
    return errors, flag


# ── Reads: read_file_type ───────────────────────────────────────────────

@pytest.mark.parametrize('file_type, file_name', [
    ('fastq', 'reads_1.fastq.gz'),
    ('fastq', 'reads_1.fq.gz'),
    ('fastq', 'reads_1.fastq.bz2'),  # ENA accepts bzip2 for FASTQ
    ('bam', 'reads.bam'),
    ('cram', 'reads.cram'),
    ('OxfordNanopore_native', 'fast5.tar.gz'),
    ('oxfordnanopore_native', 'fast5.tar.gz'),  # the type is not case-sensitive
])
def test_read_file_matching_its_type_has_no_error(file_type, file_name):
    assert get_file_type_error(file_name, file_type, READ_FILE_TYPE_EXTS) == ''


@pytest.mark.parametrize('file_type, file_name', [
    ('fastq', 'reads_1.fastq'),  # FASTQ must be compressed
    ('fastq', 'reads.bam'),
    ('bam', 'reads_1.fastq.gz'),
    ('cram', 'reads.bam'),
    ('OxfordNanopore_native', 'reads_1.fastq.gz'),
    ('OxfordNanopore_native', 'fast5.zip'),
])
def test_read_file_not_matching_its_type_is_rejected(file_type, file_name):
    error = get_file_type_error(file_name, file_type, READ_FILE_TYPE_EXTS)

    assert error.startswith(f'File <strong>{file_name}</strong> does not match its file type')


def test_read_files_are_checked_in_both_columns_with_their_row():
    errors, flag = run_validator('read_file', [
        {'read_file_type': 'fastq', 'read_1_file': 'r1.fastq.gz', 'read_2_file': 'r2.fastq.gz'},
        {'read_file_type': 'bam', 'read_1_file': 'reads.bam', 'read_2_file': 'mate.fastq.gz'},
    ])

    assert flag is False
    assert errors == [
        'Sheet <strong>read_file</strong>, row <strong>3</strong>, column <strong>read_2_file</strong>: '
        + get_file_type_error('mate.fastq.gz', 'bam', READ_FILE_TYPE_EXTS)]


@pytest.mark.parametrize('read_2_file', ['', np.nan, None])
def test_single_end_reads_without_a_second_file_are_valid(read_2_file):
    errors, flag = run_validator('read_file', [
        {'read_file_type': 'fastq', 'read_1_file': 'reads.fastq.gz', 'read_2_file': read_2_file}])

    assert (errors, flag) == ([], True)


def test_unknown_read_file_type_is_left_to_another_validator():
    # Values outside the dropdown are reported by IncorrectValueValidator
    assert run_validator('read_file', [{'read_file_type': 'sff', 'read_1_file': 'reads.sff'}]) == ([], True)


def test_missing_read_file_type_column_is_left_to_another_validator():
    # Missing columns are reported by MandatoryValuesValidator
    assert run_validator('read_file', [{'read_1_file': 'reads.txt'}]) == ([], True)


# ── Single cell: expression_data_file_format ────────────────────────────

@pytest.mark.parametrize('file_format, file_name', [
    ('h5ad', 'expression.h5ad'),
    ('rds', 'expression.rds'),
    ('loom', 'expression.loom'),
    ('mtx', 'matrix.mtx'),
    ('mtx', 'matrix.mtx.gz'),
    ('csv', 'expression.csv'),
    ('csv', 'expression.csv.gz'),
])
def test_expression_file_matching_its_format_has_no_error(file_format, file_name):
    assert get_file_type_error(file_name, file_format, EXPRESSION_DATA_FILE_FORMAT_EXTS) == ''


def test_expression_file_not_matching_its_format_is_reported_with_its_row():
    errors, flag = run_validator('expression_data_file', [
        {'expression_data_file_format': 'csv', 'expression_data_file': 'exp_file.csv'},
        {'expression_data_file_format': 'h5ad', 'expression_data_file': 'exp_file.csv'},
    ])

    assert flag is False
    assert errors == [
        'Sheet <strong>expression_data_file</strong>, row <strong>3</strong>, column <strong>expression_data_file</strong>: '
        + get_file_type_error('exp_file.csv', 'h5ad', EXPRESSION_DATA_FILE_FORMAT_EXTS)]
    assert '<li>.h5ad</li>' in errors[0]  # the format's extensions are listed


# ── Other worksheets ────────────────────────────────────────────────────

def test_worksheets_without_a_file_type_check_are_ignored():
    assert run_validator('assembly_file', [{'read_file_type': 'bam', 'read_1_file': 'reads.fastq.gz'}]) == ([], True)
