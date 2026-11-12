'''Unit tests for the data file checks shared by assembly and sequence annotation submissions.

- `matches_expected_file_signature`: a file named .gz or .bz2 must start with
  the gzip or bzip2 signature, so a renamed text file is caught.
- `get_gff3_errors` or `get_gff3_content_errors`: a compressed GFF3 file must
  follow ENA's GFF3 rules. Only the first 64 KB are read from S3 and
  decompressed; Webin-CLI checks the whole file at submission.

ENA's GFF3 rules checked here:

1. ASCII only, with the nine feature columns separated by tabs.
2. The first line is the '##gff-version 3' directive.
3. A '##sequence-region seqid start end' line for every sequence used by
   the features, before those features, with the same seqid.
4. One-based coordinates, with start less than or equal to end.
5. A '##species' line with a taxonomy URL ending in the taxon ID, not an
   organism name.
   
Ref: https://ena-docs.readthedocs.io/en/latest/submit/gff3.html

To execute these tests, run: $ pytest test/unit/utils/test_file_checks.py
'''

import bz2
import gzip
import io
import os
from unittest import mock

import pytest

from common.utils import helpers

GFF3_FILE = 'genes.gff3.gz'
VERSION_LINE = b'##gff-version 3\n'
SEQUENCE_REGION_LINE = b'##sequence-region chr1 1 15000\n'
SPECIES_LINE = b'##species https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/68737\n'
FEATURE_LINE = b'chr1\t.\tgene\t100\t900\t.\t+\t.\tID=gene1;locus_tag=CANAUR_000001\n'
VALID_GFF3 = VERSION_LINE + SEQUENCE_REGION_LINE + SPECIES_LINE + FEATURE_LINE


@pytest.fixture(autouse=True)
def silent_helpers_logger():
    # Logger writes logs/<date>.log into the repo
    with mock.patch.object(helpers, 'l', mock.MagicMock()):
        yield


def make_s3obj(content):
    '''Fake S3 connection holding one file, served by ranged GET like real S3.'''
    s3obj = mock.Mock()

    def get_object(Bucket, Key, Range):
        end = int(Range.split('-')[1])
        return {'Body': io.BytesIO(content[:end + 1])}

    s3obj.s3_client.get_object.side_effect = get_object
    return s3obj


def feature(seqid=b'chr1', start=b'100', end=b'900', separator=b'\t'):
    columns = [seqid, b'.', b'gene', start, end, b'.', b'+', b'.', b'ID=gene1']
    return separator.join(columns) + b'\n'


def content_errors(*lines):
    return helpers.get_gff3_content_errors(b''.join(lines))


# ── Compression signatures ──────────────────────────────────────────────

@pytest.mark.parametrize('file_name, content, matches', [
    ('genes.gff3.gz', gzip.compress(FEATURE_LINE), True),
    ('genome.fasta.bz2', bz2.compress(FEATURE_LINE), True),
    ('genes.gff3.gz', FEATURE_LINE, False),  # plain text renamed to .gz
    ('genome.fasta.bz2', gzip.compress(FEATURE_LINE), False),  # gzip renamed to .bz2
    ('genes.gff3.gz', b'', False),  # empty or unreadable file
    ('genes.gff3', FEATURE_LINE, True),  # no compression extension, so not checked
])
def test_file_signature_must_match_its_compression_extension(file_name, content, matches):
    assert helpers.matches_expected_file_signature(file_name, content[:3]) is matches


# ── Valid files ─────────────────────────────────────────────────────────

def test_valid_gff3_has_no_errors():
    assert content_errors(VALID_GFF3) == []


def test_valid_gff3_from_s3_has_no_errors():
    assert helpers.get_gff3_errors(make_s3obj(gzip.compress(VALID_GFF3)), 'profile1', GFF3_FILE) == []


def test_comments_blank_lines_and_windows_line_endings_are_allowed():
    content = (VALID_GFF3 + b'# a comment\n\n###\n' + feature(start=b'950', end=b'990')).replace(b'\n', b'\r\n')

    assert helpers.get_gff3_content_errors(content) == []


def test_peptide_fasta_section_is_not_checked_as_features():
    content = VALID_GFF3 + b'##FASTA\n>gene1\nMKV\n'

    assert content_errors(content) == []


# ── Rule 1: ASCII and nine tab-separated columns ────────────────────────

def test_non_ascii_characters_are_reported():
    errors = content_errors(VALID_GFF3, 'chr1\t.\tgene\t1\t9\t.\t+\t.\tNote=café\n'.encode('utf-8'))

    assert errors == ['Line 5: only ASCII characters are allowed']


def test_columns_separated_by_spaces_are_reported():
    errors = content_errors(VALID_GFF3, feature(separator=b' '))

    assert errors == ['Line 5: expected 9 tab-separated columns, found 1']


def test_missing_column_is_reported():
    errors = content_errors(VALID_GFF3, b'chr1\t.\tgene\t1\t9\t.\t+\t.\n')  # no attributes column

    assert errors == ['Line 5: expected 9 tab-separated columns, found 8']


# ── Rule 2: the version line ────────────────────────────────────────────

@pytest.mark.parametrize('first_line', [b'##gff-version 3\n', b'##gff-version 3.1.26\n', b'##gff-version   3 \n'])
def test_gff_version_3_is_accepted(first_line):
    assert content_errors(first_line, SEQUENCE_REGION_LINE, SPECIES_LINE, FEATURE_LINE) == []


@pytest.mark.parametrize('first_line', [b'##gff-version 2\n', b'#gff-version 3\n', SEQUENCE_REGION_LINE])
def test_first_line_must_be_gff_version_3(first_line):
    errors = content_errors(first_line, SEQUENCE_REGION_LINE, SPECIES_LINE, FEATURE_LINE)

    assert errors == ["The first line must be '##gff-version 3'"]


# ── Rule 3: '##sequence-region' for every sequence, before its features ─

def test_feature_on_a_sequence_without_a_region_is_reported():
    errors = content_errors(VALID_GFF3, feature(seqid=b'chr2'))

    assert errors == ["Line 5: sequence 'chr2' has no '##sequence-region' line before its features"]


def test_region_after_its_features_is_reported():
    errors = content_errors(VERSION_LINE, SPECIES_LINE, FEATURE_LINE, SEQUENCE_REGION_LINE)

    assert errors == ["Line 3: sequence 'chr1' has no '##sequence-region' line before its features"]


def test_seqid_must_match_its_region_exactly():
    errors = content_errors(VALID_GFF3, feature(seqid=b'Chr1'))

    assert errors == ["Line 5: sequence 'Chr1' has no '##sequence-region' line before its features"]


def test_undeclared_sequence_is_reported_once():
    errors = content_errors(VALID_GFF3, feature(seqid=b'chr2'), feature(seqid=b'chr2'))

    assert len(errors) == 1


def test_one_region_per_sequence_is_accepted():
    errors = content_errors(VALID_GFF3, b'##sequence-region chr2 1 5000\n', feature(seqid=b'chr2'))

    assert errors == []


@pytest.mark.parametrize('region_line', [b'##sequence-region chr1\n', b'##sequence-region chr1 one 15000\n'])
def test_malformed_region_line_is_reported(region_line):
    errors = content_errors(VERSION_LINE, region_line, SPECIES_LINE)

    assert errors[0] == "Line 2: '##sequence-region' must be '##sequence-region seqid start end'"


def test_file_without_regions_or_features_is_reported():
    errors = content_errors(VERSION_LINE, SPECIES_LINE)

    assert errors == ["At least one '##sequence-region' line is required"]


# ── Rule 4: 1-based coordinates, start <= end ───────────────────────────

@pytest.mark.parametrize('start, end', [(b'0', b'900'), (b'-5', b'900'), (b'100', b'9e2'), (b'.', b'900')])
def test_coordinates_must_be_whole_numbers_of_1_or_more(start, end):
    errors = content_errors(VALID_GFF3, feature(start=start, end=end))

    assert len(errors) == 1
    assert errors[0].startswith('Line 5: start and end must be whole numbers of 1 or more')


def test_start_after_end_is_reported():
    errors = content_errors(VALID_GFF3, feature(start=b'900', end=b'100'))

    assert errors == ['Line 5: start (900) must not be greater than end (100)']


def test_feature_crossing_the_origin_of_a_circular_sequence_is_accepted():
    # ENA's example: end = biological end + sequence length, past the region's end
    errors = content_errors(VALID_GFF3, feature(start=b'14700', end=b'15600'))

    assert errors == []


# ── Rule 5: '##species' taxonomy URL ────────────────────────────────────

@pytest.mark.parametrize('species_line', [
    b'##species https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/6239\n',
    b'##species https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=6239\n',
])
def test_taxonomy_url_with_a_taxon_id_is_accepted(species_line):
    assert content_errors(VERSION_LINE, SEQUENCE_REGION_LINE, species_line, FEATURE_LINE) == []


@pytest.mark.parametrize('species_line', [
    b'##species Canis aureus\n',  # an organism name
    b'##species tax-id\n',
    b'##species https://www.ebi.ac.uk/ena/taxonomy/\n',  # no taxon ID
])
def test_species_line_without_a_taxonomy_url_is_reported(species_line):
    errors = content_errors(VERSION_LINE, SEQUENCE_REGION_LINE, species_line, FEATURE_LINE)

    assert len(errors) == 1
    assert errors[0].startswith("Line 3: '##species' must be a taxonomy URL ending in the taxon ID")


def test_missing_species_line_is_reported():
    errors = content_errors(VERSION_LINE, SEQUENCE_REGION_LINE, FEATURE_LINE)

    assert len(errors) == 1
    assert errors[0].startswith("A '##species' line with a taxonomy URL")


# ── Reading the start of the file only ──────────────────────────────────

def test_only_the_start_of_a_large_gff3_file_is_read():
    # Random IDs stop gzip compressing the lines away, so the file is well over 64 KB
    lines = [feature(start=b'100', end=b'900')[:-len(b'ID=gene1\n')] + b'ID=' + os.urandom(32).hex().encode() + b'\n'
             for _ in range(5000)]
    content = gzip.compress(VALID_GFF3 + b''.join(lines))
    assert len(content) > helpers.GFF3_CHECK_BYTES
    s3obj = make_s3obj(content)

    # The last line read is cut off part-way, and must not be reported
    assert helpers.get_gff3_errors(s3obj, 'profile1', GFF3_FILE) == []
    assert s3obj.s3_client.get_object.call_args.kwargs['Range'] == f'bytes=0-{helpers.GFF3_CHECK_BYTES - 1}'


def test_partial_last_line_is_only_ignored_for_partial_content():
    content = VALID_GFF3 + b'chr1\t.\tgene\t1'  # cut off part-way

    assert helpers.get_gff3_content_errors(content, is_partial=True) == []
    assert helpers.get_gff3_content_errors(content, is_partial=False) == [
        'Line 5: expected 9 tab-separated columns, found 4']


def test_damaged_gzip_is_reported():
    content = gzip.compress(VALID_GFF3)[:10] + b'not gzip data' * 10

    assert helpers.get_gff3_errors(make_s3obj(content), 'profile1', GFF3_FILE) == [
        'The file could not be read as gzip; it may be damaged']


def test_many_problems_are_capped():
    errors = content_errors(VALID_GFF3, *[feature(start=b'900', end=b'100')] * 25)

    assert len(errors) == helpers.GFF3_MAX_ERRORS + 1
    assert errors[-1] == '... and 15 more problems'
