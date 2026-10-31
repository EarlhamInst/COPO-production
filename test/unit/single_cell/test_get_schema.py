'''Unit tests for filtering a single-cell schema down to one checklist.

A field is included in a checklist when its checklist column is filled in
(e.g. 'M' or 'O'). Filtering must leave the schemas passed in unchanged so
that the same schemas can be filtered for more than one checklist, such as
when a field is split into one row per checklist, each with its own regex.
'''

import copy

from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas

# Checklist names for PacBio Pre-made and Illumina Pre-made manifests
PACBIO = 'version_pacbio_premade'
ILLUMINA = 'version_illumina_premade'


def field(term_name, term_regex='', pacbio=None, illumina=None):
    return {
        'term_name': term_name,
        'term_label': term_name.replace('_', ' ').title(),
        'term_regex': term_regex,
        PACBIO: pacbio,
        ILLUMINA: illumina,
    }


SCHEMAS = {
    'index': [
        field('sample_id', pacbio='M', illumina='M'),
        field('index_tag', r'^([ACGT]+-[ACGT]+)?$', pacbio='O'),
        field('index_tag', r'^([ACGT]+(?:-[ACGT]+)?)?$', illumina='O'),
    ],
    'study': [
        field('study_id', pacbio='M'),
    ],
}


def term_regexes(schema, component='index'):
    return [(f['term_name'], f['term_regex']) for f in schema.get(component, [])]


def test_includes_only_the_fields_in_the_checklist():
    schema = SinglecellSchemas().get_schema(schemas=copy.deepcopy(SCHEMAS), target_id=ILLUMINA)

    assert term_regexes(schema) == [
        ('sample_id', ''),
        ('index_tag', r'^([ACGT]+(?:-[ACGT]+)?)?$'),
    ]
    # Components with no fields in the checklist are left out
    assert 'study' not in schema


def test_leaves_the_schemas_passed_in_unchanged():
    schemas = copy.deepcopy(SCHEMAS)

    SinglecellSchemas().get_schema(schemas=schemas, target_id=PACBIO)

    assert schemas == SCHEMAS


def test_same_schemas_can_be_filtered_for_each_checklist():
    schemas = copy.deepcopy(SCHEMAS)
    da = SinglecellSchemas()

    pacbio = da.get_schema(schemas=schemas, target_id=PACBIO)
    illumina = da.get_schema(schemas=schemas, target_id=ILLUMINA)

    assert term_regexes(pacbio) == [
        ('sample_id', ''),
        ('index_tag', r'^([ACGT]+-[ACGT]+)?$'),
    ]
    assert term_regexes(illumina) == [
        ('sample_id', ''),
        ('index_tag', r'^([ACGT]+(?:-[ACGT]+)?)?$'),
    ]


def test_get_schemas_filters_each_checklist_from_the_full_schemas():
    result = SinglecellSchemas().get_schemas(
        schema_name='', schemas=copy.deepcopy(SCHEMAS), target_ids=[PACBIO, ILLUMINA]
    )

    assert term_regexes(result[PACBIO]) == [
        ('sample_id', ''),
        ('index_tag', r'^([ACGT]+-[ACGT]+)?$'),
    ]
    assert term_regexes(result[ILLUMINA]) == [
        ('sample_id', ''),
        ('index_tag', r'^([ACGT]+(?:-[ACGT]+)?)?$'),
    ]


def test_returns_the_schemas_unchanged_without_a_checklist():
    schemas = copy.deepcopy(SCHEMAS)

    assert SinglecellSchemas().get_schema(schemas=schemas) is schemas
