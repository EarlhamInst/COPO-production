'''Unit tests for the schema quality checks.

The `SingleCellSchemasHandler` class logs a warning when issues like 
unknown repositories, stray whitespace, misspellings, examples that 
fail their rules and malformed term names are found in a schema 
spreadsheet when it is loaded.
'''

from unittest import mock

import pandas as pd
import pytest

from src.apps.copo_single_cell_submission.utils import schema_checks
from src.apps.copo_single_cell_submission.utils.SingleCellSchemasHandler import (
    SingleCellSchemasHandler, l as handler_logger)


def data_sheet(*terms):
    '''A `data` worksheet. Each term is a dict; missing columns are blank.'''
    columns = ['component_name', 'term_name', 'term_label', 'term_description', 'term_example',
               'term_regex', 'term_error_message', 'term_type', 'term_repository']
    return pd.DataFrame([{column: term.get(column) for column in columns} for term in terms])


def term(name, **fields):
    return {'component_name': 'file', 'term_name': name, 'term_label': name.title(),
            'term_description': f'The {name}.', 'term_example': 'x', 'term_type': 'string', **fields}


REPOSITORIES = pd.DataFrame({'key': ['ena', 'zenodo'], 'name': ['ENA', 'Zenodo']})


def check(sheets):
    return schema_checks.check_schema_sheets(sheets)


def test_clean_schema_has_no_warnings():
    sheets = {
        'data': data_sheet(
            term('read_1_file', term_type='file', term_repository='ena', term_example='r1.fastq.gz'),
            term('primeness', term_type='enum', term_example="3'"),
            term('cell_count', term_regex=r'^\d+$', term_example='10000'),
        ),
        'allowed_values': pd.DataFrame({'primeness': ["3'", "5'"]}),
        'repositories': REPOSITORIES,
    }

    assert check(sheets) == []


# term_repository

def test_term_repository_not_on_the_repositories_worksheet_is_reported():
    sheets = {'data': data_sheet(term('white_list_barcode_file', term_type='file', term_repository='zenodoo')),
              'repositories': REPOSITORIES}

    [warning] = check(sheets)
    assert warning.startswith('data!I2:')
    assert "'zenodoo'" in warning and "'white_list_barcode_file'" in warning


@pytest.mark.parametrize('value', ['ena', ' Zenodo ', 'ena, zenodo', None, ''])
def test_known_or_blank_term_repository_is_accepted(value):
    sheets = {'data': data_sheet(term('a_file', term_type='file', term_repository=value)),
              'repositories': REPOSITORIES}

    assert schema_checks.check_term_repositories(sheets) == []


def test_each_repository_in_a_list_is_checked():
    sheets = {'data': data_sheet(term('a_file', term_type='file', term_repository='zenodo, figshare')),
              'repositories': REPOSITORIES}

    [warning] = schema_checks.check_term_repositories(sheets)
    assert "'figshare'" in warning


def test_term_repository_is_reported_when_there_is_no_repositories_worksheet():
    sheets = {'data': data_sheet(term('a_file', term_type='file', term_repository='ena'))}

    assert len(schema_checks.check_term_repositories(sheets)) == 1


# whitespace and characters

@pytest.mark.parametrize('value, problem', [
    ('   ', 'only whitespace'),
    ('Matrix Type ', 'leading or trailing whitespace'),
    ('Clustering  Resolution', 'double space'),
    ('known cell barcodes in the\ndataset', 'line break'),
    ('CL:0000000 ', 'non-breaking or zero-width space'),
    ('the organism’s name', 'curly quote'),
])
def test_whitespace_and_character_problems_are_reported(value, problem):
    sheets = {'checklists': pd.DataFrame({'description': [value]})}

    [warning] = schema_checks.check_whitespace(sheets)
    assert warning.startswith('checklists!A2:')
    assert problem in warning


def test_regexes_and_formulas_may_contain_spacing():
    sheets = {
        'data': data_sheet(term('samp_size', term_regex=r'^\d+ *(gram|milligram)$', term_example='5 gram')),
        'regex_to_formula': pd.DataFrame({'spreadsheet_formula': ['AND(\n   LEN(A1)  > 0)']}),
    }

    assert schema_checks.check_whitespace(sheets) == []


# spelling

def test_known_misspelling_is_reported_with_its_correction():
    sheets = {'data': data_sheet(term('n_genes', term_description='The number of genese in the data'))}

    [warning] = schema_checks.check_spelling(sheets)
    assert warning.startswith('data!D2:')
    assert "'genese'" in warning and "'genes'" in warning


def test_misspelt_allowed_value_is_reported():
    sheets = {'allowed_values': pd.DataFrame({'clustering_distance_metric': ['euclidean', 'manhatten']})}

    [warning] = schema_checks.check_spelling(sheets)
    assert warning.startswith('allowed_values!A3:')
    assert "'manhattan'" in warning


def test_spelling_ignores_columns_that_are_not_prose():
    # Regexes and term names are not words
    sheets = {'data': data_sheet(term('scientic_name', term_label='Scientific Name',
                                      term_regex='^genese$', term_example='genes'))}

    assert schema_checks.check_spelling(sheets) == []


def test_codespell_dictionary_is_used_when_installed(tmp_path):
    dictionary = tmp_path / 'codespell_lib' / 'data' / 'dictionary.txt'
    dictionary.parent.mkdir(parents=True)
    dictionary.write_text('abberation->aberration\nteh->the, ten\n', encoding='utf-8')

    schema_checks.misspellings.cache_clear()
    with mock.patch('importlib.resources.files', return_value=tmp_path / 'codespell_lib'):
        words = schema_checks.misspellings()
    schema_checks.misspellings.cache_clear()

    assert words['abberation'] == 'aberration'
    assert words['teh'] == 'the'
    assert words['genese'] == 'genes'  # the built-in list is still included


def test_built_in_misspellings_are_used_without_codespell():
    schema_checks.misspellings.cache_clear()
    with mock.patch('importlib.resources.files', side_effect=ModuleNotFoundError('codespell_lib')):
        words = schema_checks.misspellings()
    schema_checks.misspellings.cache_clear()

    assert words == schema_checks.BUILTIN_MISSPELLINGS


# examples

def test_example_that_fails_its_regex_is_reported():
    sheets = {'data': data_sheet(term('lib_reads_seq', term_regex=r'^\d+$', term_example='high;99%'))}

    [warning] = schema_checks.check_examples(sheets)
    assert warning.startswith('data!E2:')
    assert 'term_regex' in warning


def test_example_that_only_matches_the_start_of_an_unanchored_regex_is_reported():
    # The validator uses re.match, so a regex without $ accepts trailing junk
    sheets = {'data': data_sheet(term('clustering_resolution', term_regex=r'^([0-9]*[.])?[0-9]+',
                                      term_example='0.8 or so'))}

    assert len(schema_checks.check_examples(sheets)) == 1


def test_enum_example_not_in_allowed_values_is_reported():
    sheets = {'data': data_sheet(term('primeness', term_type='enum', term_example='5')),
              'allowed_values': pd.DataFrame({'primeness': ["3'", "5'"]})}

    [warning] = schema_checks.check_examples(sheets)
    assert 'allowed values' in warning


def test_missing_example_is_reported():
    sheets = {'data': data_sheet(term('notes', term_example=None))}

    [warning] = schema_checks.check_examples(sheets)
    assert 'no example' in warning


# term names

@pytest.mark.parametrize('name', ['cell-type annotation', 'cell type', 'cell-type'])
def test_term_name_that_is_not_one_word_is_reported(name):
    sheets = {'data': data_sheet(term(name))}

    [warning] = schema_checks.check_term_names(sheets)
    assert warning.startswith('data!B2:')


@pytest.mark.parametrize('name', ['cell_type_annotation', 'bibliographicCitation', 'loading_volume_µl'])
def test_valid_term_names_are_accepted(name):
    assert schema_checks.check_term_names({'data': data_sheet(term(name))}) == []


# SingleCellSchemasHandler: warnings are logged, and loading continues

def test_schema_warnings_are_logged_without_stopping_the_load():
    sheets = {'data': data_sheet(term('a_file', term_type='file', term_repository='zenodoo')),
              'repositories': REPOSITORIES}
    xls = mock.MagicMock(sheet_names=list(sheets))

    with mock.patch.object(pd, 'read_excel', side_effect=lambda _xls, sheet, **kwargs: sheets[sheet]), \
            mock.patch.object(handler_logger, 'log') as log:
        warnings = SingleCellSchemasHandler()._log_schema_warnings('COPO_SINGLE_CELL', xls)

    assert len(warnings) == 1
    logged = [call.args[0] for call in log.call_args_list]
    assert logged[0] == f'Schema COPO_SINGLE_CELL: {warnings[0]}'
    assert 'quality warning' in logged[-1]
    assert all(call.kwargs['level'].name == 'WARNING' for call in log.call_args_list)


def test_clean_schema_logs_nothing():
    sheets = {'data': data_sheet(term('cell_count')), 'repositories': REPOSITORIES}
    xls = mock.MagicMock(sheet_names=list(sheets))

    with mock.patch.object(pd, 'read_excel', side_effect=lambda _xls, sheet, **kwargs: sheets[sheet]), \
            mock.patch.object(handler_logger, 'log') as log:
        SingleCellSchemasHandler()._log_schema_warnings('COPO_SINGLE_CELL', xls)

    log.assert_not_called()


# UK spelling

@pytest.fixture
def no_codespell():
    '''Use only the built-in spelling lists, as when codespell is not installed.'''
    schema_checks.us_to_uk_spellings.cache_clear()
    schema_checks.misspellings.cache_clear()
    with mock.patch('importlib.resources.files', side_effect=ModuleNotFoundError('codespell_lib')):
        yield
    schema_checks.us_to_uk_spellings.cache_clear()
    schema_checks.misspellings.cache_clear()


@pytest.mark.parametrize('us, uk', [('organization', 'organisation'), ('Tumor', 'tumour'),
                                    ('microliter', 'microlitre'), ('normalized', 'normalised')])
def test_us_spelling_is_reported_with_the_uk_spelling(no_codespell, us, uk):
    sheets = {'data': data_sheet(term('funder', term_description=f'The {us} that funded the study.'))}

    [warning] = schema_checks.check_uk_spelling(sheets)
    assert warning.startswith('data!D2:')
    assert f'{us!r}' in warning and f'{uk!r}' in warning


@pytest.mark.parametrize('text', [
    'The licence under which the data can be used.',
    'Users may license the data.',                    # license is the UK verb
    'Good laboratory practice was followed.',          # practice is the UK noun
    'Studies focused on biodiversity.',                # focused is the usual UK form
    'Expected a unit (milliliter, gram or square centimeter).',  # ENA unit names
])
def test_uk_and_allowed_us_spellings_are_not_reported(no_codespell, text):
    sheets = {'data': data_sheet(term('a_term', term_description=text, term_error_message=text))}

    assert schema_checks.check_uk_spelling(sheets) == []


def test_external_vocabulary_allowed_values_keep_their_spelling(no_codespell):
    # ENA's instrument model names cannot be changed; other allowed values can
    sheets = {'allowed_values': pd.DataFrame({
        'sequencing_instrument_model': ['Illumina Genome Analyzer'],
        'library_source': ['TUMOR'],
        'cell_enrichment': ['Tumor cell sorting'],
    })}
    with mock.patch.dict(schema_checks.BUILTIN_US_TO_UK, {'analyzer': 'analyser'}):
        schema_checks.us_to_uk_spellings.cache_clear()
        warnings = schema_checks.check_uk_spelling(sheets)

    assert [warning.split(':')[0] for warning in warnings] == ['allowed_values!C2']


def test_spelling_skips_urls_and_email_addresses(no_codespell):
    text = ('Code is at https://github.com/user/umi-normalization and https://genese.org; '
            'contact tumor.lab@example.com')
    sheets = {'data': data_sheet(term('source_code', term_example=text))}

    assert schema_checks.check_uk_spelling(sheets) == []
    assert schema_checks.check_spelling(sheets) == []


def test_codespell_uk_to_us_dictionary_is_reversed(tmp_path):
    dictionary = tmp_path / 'codespell_lib' / 'data' / 'dictionary_en-GB_to_en-US.txt'
    dictionary.parent.mkdir(parents=True)
    dictionary.write_text('colourise->colorize\nlicence->license\nanalyse->analyze, analyse\n',
                          encoding='utf-8')

    schema_checks.us_to_uk_spellings.cache_clear()
    with mock.patch('importlib.resources.files', return_value=tmp_path / 'codespell_lib'):
        spellings = schema_checks.us_to_uk_spellings()
    schema_checks.us_to_uk_spellings.cache_clear()

    assert spellings['colorize'] == 'colourise'
    assert spellings['analyze'] == 'analyse'
    assert 'license' not in spellings  # allowed: the UK verb
    assert spellings['tumor'] == 'tumour'  # the built-in list is still included


def test_uk_spelling_check_runs_with_the_other_checks(no_codespell):
    sheets = {'data': data_sheet(term('n_genes', term_description='The color of the genes.'))}

    assert any("US spelling 'color'" in warning for warning in check(sheets))
