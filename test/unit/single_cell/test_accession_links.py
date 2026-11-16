'''Unit tests for showing repository accessions as links in single-cell tables.

A repository with a 'link_column' (see REPOSITORY_ACCESSION_LINKS in the
settings) has its accession column hidden, and the 'link_column' column keeps
showing its own value as a link to the accession instead. e.g. a LIMS
manifest's Project ID links to its Sapio project. Repository columns are
titled with the repository's label e.g. 'Status for CKAN'.
'''

import pytest
from django.test import override_settings

from src.apps.copo_single_cell_submission.utils.copo_single_cell import (
    _apply_accession_links,
    get_column_title,
)

LINKS = {
    'ena': {'field_suffix': 'accession_ena', 'url_template': 'https://ena/{accession}'},
    'sapio': {
        'field_suffix': 'accession_sapio',
        'url_template': 'https://sapio/{accession}',
        'link_column': 'study_id',
    },
}


def columns(*names):
    return [{'data': 'record_id', 'visible': False}] + [{'data': name, 'title': name} for name in names]


@override_settings(REPOSITORY_ACCESSION_LINKS=LINKS)
def test_accession_is_hidden_and_shown_as_a_link_on_its_link_column():
    result = _apply_accession_links(columns('study_id', 'status_sapio', 'accession_sapio'))

    by_name = {column['data']: column for column in result}
    assert by_name['accession_sapio']['visible'] is False
    assert by_name['study_id']['render'] == "renderRepoAccessionLink('sapio', 'accession_sapio')"
    # The link column still shows its own value under its own title
    assert by_name['study_id']['title'] == 'study_id'
    assert 'visible' not in by_name['status_sapio']


@override_settings(REPOSITORY_ACCESSION_LINKS=LINKS)
def test_repositories_without_a_link_column_keep_their_accession_column():
    result = _apply_accession_links(columns('study_id', 'accession_ena'))

    by_name = {column['data']: column for column in result}
    assert 'visible' not in by_name['accession_ena']
    assert 'render' not in by_name['study_id']


@override_settings(REPOSITORY_ACCESSION_LINKS=LINKS)
def test_accession_column_is_kept_when_its_link_column_is_missing():
    # e.g. a table without a Project ID column still shows the accession
    result = _apply_accession_links(columns('accession_sapio'))

    assert 'visible' not in result[1]


# --- Column titles ---

LABELS = {'ckan': 'CKAN', 'ena': 'ENA', 'sapio': 'Sapio', 'zenodo': 'Zenodo'}


@override_settings(REPOSITORY_LABELS=LABELS)
@pytest.mark.parametrize('column_name, title', [
    ('status_ckan', 'Status for CKAN'),
    ('accession_ena', 'Accession for ENA'),
    ('error_sapio', 'Error for Sapio'),
    ('status_zenodo', 'Status for Zenodo'),
    ('study_accession_ena', 'Study Accession for ENA'),
    ('study_id', 'Study ID'),
    ('sample_id_ena', 'Sample ID for ENA'),
    ('idea', 'Idea'),
    ('ena', 'Ena'),
])
def test_column_titles_name_repositories_by_their_label(column_name, title):
    assert get_column_title(column_name) == title
