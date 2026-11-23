'''Unit tests for showing repository accessions as links in single-cell tables.

A column whose name ends with a repository's 'field_suffix' (see
REPOSITORY_ACCESSION_LINKS in the settings) is rendered as a link to that
repository e.g. 'accession_zenodo' links to the Zenodo record. Repository
columns are titled with the repository's label e.g. 'Status for ENA'.
'''

import pytest
from django.test import override_settings

from src.apps.copo_single_cell_submission.utils.copo_single_cell import (
    _render_repo_accession_function,
    get_column_title,
)

LINKS = {
    'ena': {'field_suffix': 'accession_ena', 'url_template': 'https://ena/{accession}'},
    'zenodo': {'field_suffix': 'accession_zenodo', 'url_template': 'https://zenodo/{accession}'},
}


# --- Accession links ---

@override_settings(REPOSITORY_ACCESSION_LINKS=LINKS)
@pytest.mark.parametrize('column_name, render', [
    ('accession_ena', "render_repo_accession('ena')"),
    ('study_accession_ena', "render_repo_accession('ena')"),
    ('accession_zenodo', "render_repo_accession('zenodo')"),
    ('ACCESSION_ZENODO', "render_repo_accession('zenodo')"),
    # The BioSample accession has no repository suffix but is always ENA's
    ('biosampleAccession', "render_repo_accession('ena')"),
    ('status_ena', ''),
    ('accession_unknown', ''),
    ('study_id', ''),
])
def test_accession_columns_link_to_their_repository(column_name, render):
    assert _render_repo_accession_function(column_name) == render


# --- Column titles ---

LABELS = {'ena': 'ENA', 'zenodo': 'Zenodo'}


@override_settings(REPOSITORY_LABELS=LABELS)
@pytest.mark.parametrize('column_name, title', [
    ('accession_ena', 'Accession for ENA'),
    ('status_zenodo', 'Status for Zenodo'),
    ('error_zenodo', 'Error for Zenodo'),
    ('study_accession_ena', 'Study Accession for ENA'),
    ('study_id', 'Study ID'),
    ('sample_id_ena', 'Sample ID for ENA'),
    ('idea', 'Idea'),
    ('ena', 'Ena'),
])
def test_column_titles_name_repositories_by_their_label(column_name, title):
    assert get_column_title(column_name) == title
