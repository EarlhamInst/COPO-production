'''Quality checks for a single-cell schema spreadsheet.

These catch problems that the hard validation in
SingleCellSchemasHandler._parseSchemas (empty required cells, duplicate terms,
broken foreign keys, invalid regexes) lets through, but that still cause wrong
behaviour or a confusing manifest:

- a term_repository value that is not a key on the `repositories` worksheet
  (that file would be sent to no repository)
- stray whitespace, line breaks, non-breaking spaces and curly quotes
- known misspellings, and US spellings (the schema uses UK spelling)
- a term_example that fails its own term_regex or allowed values, or is missing
- a term_name that is not a single word (e.g. contains a space or hyphen)

They are reported as warnings, not errors, so a schema still loads.

check_schema_sheets() takes the worksheets as DataFrames and returns the
warnings; it has no Django or database dependencies.
'''

import re
from functools import lru_cache

import pandas as pd
from openpyxl.utils.cell import get_column_letter

# Columns where spacing is part of the value: regexes, and the multi-line
# spreadsheet formulas on the regex_to_formula worksheet
WHITESPACE_EXEMPT_COLUMNS = {'term_regex', 'spreadsheet_formula'}

# Columns whose text users read: checked for spelling
SPELLING_COLUMNS = {
    'data': ['term_label', 'term_description', 'term_example', 'term_error_message'],
    'checklists': ['name', 'description'],
    'components': ['name'],
    'repositories': ['name', 'description'],
    'standards': ['name'],
    'technologies': ['name'],
    'regex_to_formula': ['purpose'],
}

CURLY_QUOTES = re.compile('[‘’“”]')
NON_BREAKING_SPACES = re.compile('[   ​]')
TERM_NAME = re.compile(r'^\w+$')
# A token is a run of word characters; only plain words are spell-checked, so
# identifiers such as term names (scientific_name) and IDs (GRCh38) are skipped
TOKEN = re.compile(r"\w+(?:'\w+)?")
PLAIN_WORD = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")


URL_OR_EMAIL = re.compile(r'\S+://\S+|\S+@\S+')


def words(text):
    '''The plain words in text, leaving out URLs, email addresses and identifiers.'''
    text = URL_OR_EMAIL.sub(' ', text)
    return [token for token in TOKEN.findall(text) if PLAIN_WORD.fullmatch(token)]

# Used when codespell is not installed. Includes misspellings found in earlier
# versions of the single-cell schema.
BUILTIN_MISSPELLINGS = {
    'accomodate': 'accommodate',
    'adress': 'address',
    'checkshum': 'checksum',
    'comparision': 'comparison',
    'definately': 'definitely',
    'enviroment': 'environment',
    'genese': 'genes',
    'identifioer': 'identifier',
    'independant': 'independent',
    'manhatten': 'manhattan',
    'mehalanobis': 'mahalanobis',
    'metic': 'metric',
    'neccessary': 'necessary',
    'nomalised': 'normalised',
    'occured': 'occurred',
    'occurence': 'occurrence',
    'paranent': 'parent',
    'recieve': 'receive',
    'refence': 'reference',
    'scientic': 'scientific',
    'seperate': 'separate',
    'seperated': 'separated',
    'spectometry': 'spectrometry',
    'sucessful': 'successful',
    'untill': 'until',
}


# US spellings mapped to UK. Medical and lab spellings that codespell's
# en-GB_to_en-US dictionary does not cover; used with or without codespell.
BUILTIN_US_TO_UK = {
    'anemia': 'anaemia',
    'anesthesia': 'anaesthesia',
    'center': 'centre',
    'color': 'colour',
    'edema': 'oedema',
    'esophagus': 'oesophagus',
    'estrogen': 'oestrogen',
    'gynecology': 'gynaecology',
    'hematology': 'haematology',
    'hematopoietic': 'haematopoietic',
    'hemoglobin': 'haemoglobin',
    'ischemia': 'ischaemia',
    'leukemia': 'leukaemia',
    'microliter': 'microlitre',
    'microliters': 'microlitres',
    'neighbor': 'neighbour',
    'neighbors': 'neighbours',
    'normalization': 'normalisation',
    'normalized': 'normalised',
    'organization': 'organisation',
    'pediatric': 'paediatric',
    'tumor': 'tumour',
    'tumors': 'tumours',
}

# US forms that are not flagged: they are also correct UK spellings (license is
# the UK verb, practice the UK noun, story a tale; focused and biased are the
# usual UK forms), or ENA requires them (unit names such as square centimeter)
US_SPELLINGS_ALLOWED = {
    'biased', 'focused', 'focuses', 'focusing', 'refocused', 'refocuses', 'refocusing',
    'judgment', 'judgments', 'lambaste', 'lambastes', 'writable',
    'license', 'licensed', 'licenses', 'licensing', 'practice', 'story', 'stories',
    'liter', 'liters', 'meter', 'meters', 'centimeter', 'centimeters',
    'millimeter', 'millimeters', 'kilometer', 'kilometers',
}


# allowed_values columns copied from an external controlled vocabulary (ENA,
# SPDX licences): their values must match the source exactly, whatever the spelling
EXTERNAL_VOCABULARY_COLUMNS = {
    'sequencing_instrument_model',
    'library_strategy',
    'library_selection',
    'library_source',
    'licence',
    'licence_fullname',
}


def codespell_dictionary(name):
    '''Lines of one of codespell's dictionaries, or [] if codespell is not installed.'''
    try:
        from importlib.resources import files
        return files('codespell_lib').joinpath('data', name).read_text(encoding='utf-8').splitlines()
    except (ImportError, ModuleNotFoundError, FileNotFoundError, OSError):
        return []


@lru_cache(maxsize=1)
def us_to_uk_spellings():
    '''US spellings mapped to their UK spelling.

    codespell's en-GB_to_en-US dictionary maps UK spellings to US; this reverses
    it, adds BUILTIN_US_TO_UK, and leaves out US_SPELLINGS_ALLOWED.
    '''
    spellings = dict(BUILTIN_US_TO_UK)
    for line in codespell_dictionary('dictionary_en-GB_to_en-US.txt'):
        uk, _, us_forms = line.partition('->')
        for us in us_forms.split(','):
            us = us.strip().lower()
            if us and uk.strip():
                spellings.setdefault(us, uk.strip())
    return {us: uk for us, uk in spellings.items() if us not in US_SPELLINGS_ALLOWED}


@lru_cache(maxsize=1)
def misspellings():
    '''Known misspellings mapped to their correction.

    Uses codespell's dictionary when codespell is installed, as it covers tens
    of thousands of common misspellings; otherwise BUILTIN_MISSPELLINGS.
    '''
    known = dict(BUILTIN_MISSPELLINGS)
    for line in codespell_dictionary('dictionary.txt'):
        word, _, fixes = line.partition('->')
        if word and fixes:
            known.setdefault(word.strip().lower(), fixes.split(',')[0].strip())
    return known


def cell_ref(sheet, df, row_index, column):
    '''Excel-style reference, e.g. data!I26 (header is row 1).'''
    column_letter = get_column_letter(df.columns.get_loc(column) + 1)
    return f'{sheet}!{column_letter}{row_index + 2}'


def is_blank(value):
    return value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA


def check_whitespace(sheets):
    warnings = []
    for sheet, df in sheets.items():
        for column in df.columns:
            if column in WHITESPACE_EXEMPT_COLUMNS:
                continue
            for row_index, value in df[column].items():
                if is_blank(value) or not isinstance(value, str):
                    continue
                ref = cell_ref(sheet, df, row_index, column)
                if not value.strip():
                    warnings.append(f'{ref}: cell contains only whitespace')
                    continue
                problems = []
                if value != value.strip():
                    problems.append('leading or trailing whitespace')
                if '  ' in value:
                    problems.append('double space')
                if '\n' in value or '\r' in value:
                    problems.append('line break')
                if NON_BREAKING_SPACES.search(value):
                    problems.append('non-breaking or zero-width space')
                if CURLY_QUOTES.search(value):
                    problems.append('curly quote')
                if problems:
                    warnings.append(f'{ref}: {", ".join(problems)} in {value[:60]!r}')
    return warnings


def prose_cells(sheets):
    '''(cell reference, text) for every non-blank cell that users read as text:
    the SPELLING_COLUMNS, and every allowed value.'''
    columns_by_sheet = dict(SPELLING_COLUMNS)
    if 'allowed_values' in sheets:
        columns_by_sheet['allowed_values'] = list(sheets['allowed_values'].columns)

    for sheet, columns in columns_by_sheet.items():
        df = sheets.get(sheet)
        if df is None:
            continue
        for column in columns:
            if column not in df.columns:
                continue
            for row_index, value in df[column].items():
                if not is_blank(value):
                    yield cell_ref(sheet, df, row_index, column), str(value)


def check_spelling(sheets):
    known = misspellings()
    return [f'{ref}: possible misspelling {word!r} (did you mean {known[word.lower()]!r}?)'
            for ref, text in prose_cells(sheets)
            for word in words(text)
            if word.lower() in known]


def check_uk_spelling(sheets):
    '''US spellings in the schema text; the schema uses UK spelling.

    Allowed values taken from an external vocabulary keep their spelling, e.g.
    ENA's instrument model "Illumina Genome Analyzer".
    '''
    us_to_uk = us_to_uk_spellings()
    exempt_refs = {ref for column in EXTERNAL_VOCABULARY_COLUMNS
                   for ref in column_refs(sheets, 'allowed_values', column)}
    return [f'{ref}: US spelling {word!r}; the UK spelling is {us_to_uk[word.lower()]!r}'
            for ref, text in prose_cells(sheets)
            if ref not in exempt_refs
            for word in words(text)
            if word.lower() in us_to_uk]


def column_refs(sheets, sheet, column):
    '''Cell references of every row in one column of a worksheet.'''
    df = sheets.get(sheet)
    if df is None or column not in df.columns:
        return []
    return [cell_ref(sheet, df, row_index, column) for row_index in df.index]


def check_term_repositories(sheets):
    data = sheets.get('data')
    if data is None or 'term_repository' not in data.columns:
        return []

    repositories = sheets.get('repositories')
    known = set()
    if repositories is not None and 'key' in repositories.columns:
        known = {str(key).strip().lower() for key in repositories['key'] if not is_blank(key)}

    warnings = []
    for row_index, value in data['term_repository'].items():
        if is_blank(value):
            continue
        for repository in str(value).split(','):
            repository = repository.strip().lower()
            if repository and repository not in known:
                warnings.append(
                    f'{cell_ref("data", data, row_index, "term_repository")}: term_repository '
                    f'{repository!r} for {data.at[row_index, "term_name"]!r} is not a key on the '
                    f'repositories worksheet, so the file would not be sent to any repository')
    return warnings


def check_examples(sheets):
    data = sheets.get('data')
    if data is None or 'term_example' not in data.columns:
        return []

    allowed_values = sheets.get('allowed_values', pd.DataFrame())
    warnings = []
    for row_index, row in data.iterrows():
        term_name = row.get('term_name')
        example = row.get('term_example')
        ref = cell_ref('data', data, row_index, 'term_example')

        if is_blank(example):
            warnings.append(f'{ref}: no example for {term_name!r}')
            continue
        example = str(example)

        regex = row.get('term_regex')
        if not is_blank(regex):
            try:
                if not re.fullmatch(str(regex).strip(), example):
                    warnings.append(f'{ref}: example {example!r} for {term_name!r} does not match its term_regex')
            except re.error:
                pass  # invalid regexes are rejected by _parseSchemas

        if row.get('term_type') == 'enum' and term_name in allowed_values.columns:
            values = {str(v).strip() for v in allowed_values[term_name] if not is_blank(v)}
            if example.strip() not in values:
                warnings.append(f'{ref}: example {example!r} for {term_name!r} is not one of its allowed values')
    return warnings


def check_term_names(sheets):
    data = sheets.get('data')
    if data is None or 'term_name' not in data.columns:
        return []

    return [f'{cell_ref("data", data, row_index, "term_name")}: term_name {value!r} should be a single word '
            f'of letters, digits and underscores'
            for row_index, value in data['term_name'].items()
            if not is_blank(value) and not TERM_NAME.match(str(value))]


CHECKS = [
    check_term_repositories,
    check_examples,
    check_term_names,
    check_whitespace,
    check_spelling,
    check_uk_spelling,
]


def check_schema_sheets(sheets):
    '''Run every quality check on a schema's worksheets.

    :param sheets: {worksheet name: DataFrame}, read with dtype=str
    :return: a list of warning messages, each starting with the cell reference
    '''
    warnings = []
    for check in CHECKS:
        warnings.extend(check(sheets))
    return warnings
