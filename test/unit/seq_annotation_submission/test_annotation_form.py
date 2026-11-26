'''Unit tests for the sequence annotation form.

The form has two annotation types:

- 'Sequence annotation' (the existing route) needs a sample, and may have
  runs and experiments.
- 'Decoupled annotation' submits one compressed GFF3 file against an assembly
  that ENA has already given a GCA accession. The GCA accession (PRIMARY ID) is
  chosen from a dropdown of the profile's assemblies, so users cannot type one.

Fields that do not apply to the chosen type are hidden in the browser and
cleared when the form is cleaned, so they are never saved.

To execute these tests, run: $ pytest test/unit/seq_annotation_submission/test_annotation_form.py
'''

import pytest

from src.apps.copo_seq_annotation_submission.forms import AnnotationFilesForm, AnnotationForm
from src.apps.copo_seq_annotation_submission.utils.seq_annotation_utils import get_assembly_accession_choices

GCA_ACCESSION = 'GCA_977927465.1'
ASSEMBLY_ACCESSION_CHOICES = [(GCA_ACCESSION, f'mCanAur2 ({GCA_ACCESSION})')]


def make_form(assembly_accession_choices=ASSEMBLY_ACCESSION_CHOICES, pending_assembly_names=None, **data):
    form_data = {
        'study': 'PRJEB101635',
        'title': 'Golden jackal genes',
        'annotation_type': 'decoupled_annotation',
        'description': 'Gene models for mCanAur2',
        'primary_id': GCA_ACCESSION,
    }
    form_data.update(data)
    return AnnotationForm(
        data=form_data,
        sample_accession=['SAMEA120419578'],
        run_accession=['ERR1'],
        experiment_accession=['ERX1'],
        assembly_accession_choices=assembly_accession_choices,
        pending_assembly_names=pending_assembly_names,
    )


# ── GCA accession choices ───────────────────────────────────────────────

def test_assemblies_with_a_gca_accession_become_choices():
    assemblies = [{'assemblyname': 'mCanAur2', 'accession': 'ERZ1', 'gca_accession': GCA_ACCESSION}]

    assert get_assembly_accession_choices(assemblies) == (ASSEMBLY_ACCESSION_CHOICES, [])


def test_submitted_assemblies_without_a_gca_accession_are_pending():
    assemblies = [{'assemblyname': 'mCanAur3', 'accession': 'ERZ2', 'gca_accession': ''},
                  {'assemblyname': 'mCanAur4', 'accession': 'ERZ3'}]

    assert get_assembly_accession_choices(assemblies) == ([], ['mCanAur3', 'mCanAur4'])


def test_assemblies_not_yet_submitted_to_ena_are_left_out():
    assemblies = [{'assemblyname': 'draft', 'accession': ''}, {'assemblyname': 'draft2'}]

    assert get_assembly_accession_choices(assemblies) == ([], [])


# ── PRIMARY ID dropdown ─────────────────────────────────────────────────

def test_primary_id_offers_only_assemblies_with_a_gca_accession():
    form = make_form()

    assert form.fields['primary_id'].choices == [('', 'Select an assembly')] + ASSEMBLY_ACCESSION_CHOICES


@pytest.mark.parametrize('primary_id', [
    'GCA_000000001.1',  # a real-looking accession that is not one of the profile's assemblies
    'not an accession',
])
def test_typed_primary_id_is_rejected(primary_id):
    form = make_form(primary_id=primary_id)

    assert not form.is_valid()
    assert 'Select a valid choice' in form.errors['primary_id'][0]


def test_pending_assemblies_are_listed_in_the_help_text():
    form = make_form(pending_assembly_names=['mCanAur3', 'mCanAur4'])

    assert 'mCanAur3, mCanAur4' in form.fields['primary_id'].help_text


def test_help_text_explains_when_no_assembly_has_a_gca_accession():
    form = make_form(assembly_accession_choices=[], primary_id='')

    assert 'No assemblies with a GCA accession were found' in form.fields['primary_id'].help_text


# ── Required and cleared fields per annotation type ─────────────────────

def test_valid_decoupled_annotation():
    form = make_form()

    assert form.is_valid(), form.errors
    assert form.cleaned_data['primary_id'] == GCA_ACCESSION


@pytest.mark.parametrize('field', ['primary_id', 'description'])
def test_decoupled_annotation_requires_field(field):
    form = make_form(**{field: ''})

    assert not form.is_valid()
    assert field in form.errors


def test_decoupled_annotation_clears_sample_run_and_experiment():
    # Hidden in the browser, but still posted if they had values
    form = make_form(sample='SAMEA120419578', run=['ERR1'], experiment=['ERX1'])

    assert form.is_valid(), form.errors
    assert form.cleaned_data['sample'] == ''
    assert form.cleaned_data['run'] == []
    assert form.cleaned_data['experiment'] == []


def test_sequence_annotation_requires_a_sample():
    form = make_form(annotation_type='sequence_annotation', primary_id='')

    assert not form.is_valid()
    assert 'sample' in form.errors


def test_sequence_annotation_clears_primary_id():
    form = make_form(annotation_type='sequence_annotation', sample='SAMEA120419578')

    assert form.is_valid(), form.errors
    assert form.cleaned_data['primary_id'] == ''


# ── File rows ───────────────────────────────────────────────────────────

ECS_FILES = ['genes.gff3.gz', 'genes.gff3', 'genes.gff.gz', 'features.tab.gz', 'reads.fastq.gz']


def file_choices(form, field):
    return [value for value, _ in form.fields[field].choices if value]


def test_decoupled_file_rows_offer_only_compressed_gff3():
    form = AnnotationFilesForm(ecs_files=ECS_FILES, annotation_type='decoupled_annotation')

    # ENA's Webin-CLI rejects uncompressed GFF3
    assert file_choices(form, 'file') == ['genes.gff3.gz']
    assert file_choices(form, 'type') == ['gff3']


def test_sequence_annotation_file_rows_offer_its_file_types():
    form = AnnotationFilesForm(ecs_files=ECS_FILES, annotation_type='sequence_annotation')

    assert file_choices(form, 'file') == ['genes.gff.gz', 'features.tab.gz']
    assert file_choices(form, 'type') == ['gff', 'tab', 'fasta', 'bed']


def make_file_row(file_name, file_type, annotation_type='sequence_annotation'):
    return AnnotationFilesForm(
        data={'file': file_name, 'type': file_type}, ecs_files=ECS_FILES, annotation_type=annotation_type)


@pytest.mark.parametrize('file_name, file_type, annotation_type', [
    ('genes.gff.gz', 'gff', 'sequence_annotation'),
    ('features.tab.gz', 'tab', 'sequence_annotation'),
    ('genes.gff3.gz', 'gff3', 'decoupled_annotation'),
])
def test_file_row_matching_its_type_is_valid(file_name, file_type, annotation_type):
    form = make_file_row(file_name, file_type, annotation_type)

    assert form.is_valid(), form.errors


@pytest.mark.parametrize('file_name, file_type, expected_extensions', [
    ('genes.gff.gz', 'tab', '.tab, .tab.gz'),
    ('features.tab.gz', 'bed', '.bed, .bed.gz'),
    ('genes.gff.gz', 'fasta', '.fasta, .fasta.gz'),
])
def test_file_row_not_matching_its_type_is_rejected(file_name, file_type, expected_extensions):
    # The file dropdown lists files of every type, so they can be picked mismatched
    form = make_file_row(file_name, file_type)

    assert not form.is_valid()
    assert form.errors['file'] == [
        f"'{file_name}' does not match its file type '{file_type}'. Expected a file ending in {expected_extensions}"]
