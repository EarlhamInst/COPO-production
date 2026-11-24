'''Unit tests for routing Single-cell data files to ENA or Zenodo.

Each data file field in the single-cell schema has a `term_repository` value that
says where the file is deposited. The type of the field is specified by the `term_type` 
column so, for fields that expect data files to be uploaded, term type would be 'file'.

Current repositories supported:
- 'ena': read FASTQs, which are listed in the ENA run XML
- 'zenodo' or blank: everything else (index FASTQs, barcode whitelists,
  expression matrices, analysis files), which go into the Zenodo deposition

These tests check that each repository only receives its own files, and that
non-ENA files never leak into the ENA XML, e.g. as experiment attributes.
'''

from unittest import mock

import pandas as pd
import pytest
from lxml import etree

from common.ena_utils import ena_helper
from src.apps.copo_single_cell_submission.utils import ena_submission
from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas

# File fields of the `file` component, by repository
ENA_FILES = ['read_1_file', 'read_2_file']
ZENODO_FILES = ['index_1_file', 'index_2_file', 'white_list_barcode_file']
# File field of the `analysis_derived_data` component
ANALYSIS_ZENODO_FILES = ['file_derived_from']


def term(name, term_type='string', repository=None):
    return {'term_name': name, 'term_type': term_type, 'term_repository': repository, 'copo_name': None}


# Mirrors the `file` component of singlecell_schema_main_v0.5.xlsx
FILE_SCHEMA = [
    term('study_id'),
    term('file_id'),
    term('library_prep_id'),
    term('sequencing_id'),
    term('read_1_file', 'file', 'ena'),
    term('read_2_file', 'file', 'ena'),
    term('read_1_file_checksum'),
    term('read_2_file_checksum'),
    term('index_1_file', 'file', 'zenodo'),
    term('index_2_file', 'file', 'zenodo'),
    term('white_list_barcode_file', 'file', 'zenodo'),
]

EXPRESSION_DATA_FILE_SCHEMA = [
    term('study_id'),
    term('expression_data_file_id'),
    term('expression_data_file', 'file', 'zenodo'),
    term('expression_data_file_checksum'),
]

# Mirrors the `analysis_derived_data` component of singlecell_schema_main_v0.5.xlsx
ANALYSIS_DERIVED_DATA_SCHEMA = [
    term('study_id'),
    term('file_derived_from', 'file', 'zenodo'),
    term('file_derived_from_checksum'),
]

SCHEMAS = {
    'file': FILE_SCHEMA,
    'expression_data_file': EXPRESSION_DATA_FILE_SCHEMA,
    'analysis_derived_data': ANALYSIS_DERIVED_DATA_SCHEMA,
}

FILE_ROW = {
    'study_id': 'STUDY001',
    'file_id': 'FILE001',
    'library_prep_id': 'LIBPREP001',
    'sequencing_id': 'SEQ001',
    'read_1_file': 'sampleA_R1.fastq.gz',
    'read_2_file': 'sampleA_R2.fastq.gz',
    'read_1_file_checksum': '1' * 32,
    'read_2_file_checksum': '2' * 32,
    'index_1_file': 'sampleA_I1.fastq.gz',
    'index_2_file': 'sampleA_I2.fastq.gz',
    'white_list_barcode_file': 'barcodes.tsv',
}

SINGLECELL = {
    '_id': 'singlecell1',
    'study_id': 'STUDY001',
    'schema_name': 'COPO_SINGLE_CELL',
    'checklist_id': 'version_mixs_sc_rnaseq',
    'components': {
        'study': [{'study_id': 'STUDY001', 'accession_ena': 'PRJEB000001'}],
        'sample': [{'study_id': 'STUDY001', 'sample_id': 'SAMPLE001', 'biosampleAccession': 'SAMEA000001'}],
        'file': [FILE_ROW],
        'expression_data_file': [{
            'study_id': 'STUDY001',
            'expression_data_file_id': 'EXPFILE001',
            'expression_data_file': 'matrix.h5ad',
            'expression_data_file_checksum': '3' * 32,
        }],
        'analysis_derived_data': [{'study_id': 'STUDY001', 'file_derived_from': 'analysis.json'}],
    },
}

ENA_FILE_NAMES = ['sampleA_R1.fastq.gz', 'sampleA_R2.fastq.gz']
ZENODO_FILE_NAMES = ['sampleA_I1.fastq.gz', 'sampleA_I2.fastq.gz', 'barcodes.tsv', 'matrix.h5ad', 'analysis.json']


# SinglecellSchemas.get_file_terms

def test_ena_file_terms_are_the_fields_marked_ena_in_schema_order():
    assert SinglecellSchemas.get_file_terms(FILE_SCHEMA, repository='ena') == ENA_FILES


def test_zenodo_file_terms_are_the_fields_marked_zenodo():
    assert SinglecellSchemas.get_file_terms(FILE_SCHEMA, repository='zenodo') == ZENODO_FILES


def test_analysis_derived_data_file_goes_to_zenodo_not_ena():
    assert SinglecellSchemas.get_file_terms(ANALYSIS_DERIVED_DATA_SCHEMA, repository='zenodo') == ANALYSIS_ZENODO_FILES
    assert SinglecellSchemas.get_file_terms(ANALYSIS_DERIVED_DATA_SCHEMA, repository='ena') == []


@pytest.mark.parametrize('blank', [None, '', float('nan')])
def test_file_field_without_a_repository_goes_to_zenodo_not_ena(blank):
    schema = [term('analysis_file', 'file', blank)]

    assert SinglecellSchemas.get_file_terms(schema, repository='zenodo') == ['analysis_file']
    assert SinglecellSchemas.get_file_terms(schema, repository='ena') == []


def test_file_field_can_go_to_both_repositories():
    schema = [term('shared_file', 'file', 'ena, zenodo')]

    assert SinglecellSchemas.get_file_terms(schema, repository='ena') == ['shared_file']
    assert SinglecellSchemas.get_file_terms(schema, repository='zenodo') == ['shared_file']


def test_repository_match_ignores_case_and_surrounding_spaces():
    schema = [term('read_1_file', 'file', ' ENA ')]

    assert SinglecellSchemas.get_file_terms(schema, repository='ena') == ['read_1_file']


def test_only_file_fields_are_routed():
    # term_repository on a non-file field does not make it a file
    schema = [term('read_1_file_checksum', 'string', 'ena'), term('read_1_file', 'file', 'ena')]

    assert SinglecellSchemas.get_file_terms(schema, repository='ena') == ['read_1_file']


def test_all_file_terms_are_returned_when_no_repository_is_given():
    assert SinglecellSchemas.get_file_terms(FILE_SCHEMA) == ENA_FILES + ZENODO_FILES


def test_schema_without_a_term_repository_column_sends_every_file_to_zenodo():
    schema = [{k: v for k, v in field.items() if k != 'term_repository'} for field in FILE_SCHEMA]

    assert SinglecellSchemas.get_file_terms(schema, repository='ena') == []
    assert SinglecellSchemas.get_file_terms(schema, repository='zenodo') == ENA_FILES + ZENODO_FILES


def test_empty_schema_has_no_file_terms():
    assert SinglecellSchemas.get_file_terms([], repository='ena') == []


# SinglecellSchemas.get_all_files

@pytest.fixture
def schemas_da():
    # Skip DAComponent.__init__, which connects to MongoDB
    return SinglecellSchemas.__new__(SinglecellSchemas)


def test_only_ena_files_are_collected_for_ena(schemas_da):
    files = schemas_da.get_all_files(singlecell=SINGLECELL, schemas=SCHEMAS, repository='ena')

    assert sorted(files) == sorted(ENA_FILE_NAMES)


def test_only_zenodo_files_are_collected_for_zenodo(schemas_da):
    files = schemas_da.get_all_files(singlecell=SINGLECELL, schemas=SCHEMAS, repository='zenodo')

    assert sorted(files) == sorted(ZENODO_FILE_NAMES)


def test_ena_and_zenodo_files_do_not_overlap_and_cover_every_file(schemas_da):
    ena_files = schemas_da.get_all_files(singlecell=SINGLECELL, schemas=SCHEMAS, repository='ena')
    zenodo_files = schemas_da.get_all_files(singlecell=SINGLECELL, schemas=SCHEMAS, repository='zenodo')
    all_files = schemas_da.get_all_files(singlecell=SINGLECELL, schemas=SCHEMAS)

    assert not set(ena_files) & set(zenodo_files)
    assert sorted(ena_files + zenodo_files) == sorted(all_files)


def test_read_1_is_collected_when_a_row_has_no_read_2(schemas_da):
    # A single-end row must not lose its read 1 because read 2 is empty
    singlecell = {'components': {'file': [{**FILE_ROW, 'read_2_file': None}]}}

    files = schemas_da.get_all_files(singlecell=singlecell, schemas=SCHEMAS, repository='ena')

    assert files == ['sampleA_R1.fastq.gz']


def test_components_without_a_schema_are_ignored(schemas_da):
    singlecell = {'components': {'unknown': [{'some_file': 'x.fastq.gz'}], 'file': [FILE_ROW]}}

    files = schemas_da.get_all_files(singlecell=singlecell, schemas=SCHEMAS, repository='ena')

    assert sorted(files) == sorted(ENA_FILE_NAMES)


# ena_submission._prepare_file_submission

@pytest.fixture
def mock_schema_loading():
    identifier_map = {'study': 'study_id', 'sample': 'sample_id', 'file': 'file_id'}
    parent_map = {'file': {'study': 'study_id'}}

    with mock.patch.object(SinglecellSchemas, '__init__', return_value=None), \
            mock.patch.object(SinglecellSchemas, 'get_schema', return_value=SCHEMAS) as get_schema, \
            mock.patch.object(SinglecellSchemas, 'get_term_mapping', return_value={}), \
            mock.patch.object(SinglecellSchemas, 'get_key_map', return_value=(identifier_map, {})), \
            mock.patch.object(SinglecellSchemas, 'get_parent_map', return_value=parent_map):
        yield get_schema


def test_prepare_file_submission_returns_the_ena_file_columns(mock_schema_loading):
    _, _, ena_file_columns = ena_submission._prepare_file_submission(SINGLECELL, file_component_name='file')

    assert ena_file_columns == ENA_FILES


def test_prepare_file_submission_drops_zenodo_files_and_their_checksums(mock_schema_loading):
    singlecell = {**SINGLECELL, 'components': {
        **SINGLECELL['components'],
        'file': [{**FILE_ROW, 'index_1_file_checksum': '4' * 32}],
    }}
    schemas = {**SCHEMAS, 'file': FILE_SCHEMA + [term('index_1_file_checksum')]}
    mock_schema_loading.return_value = schemas

    component_df, _, _ = ena_submission._prepare_file_submission(singlecell, file_component_name='file')

    for column in ZENODO_FILES + ['index_1_file_checksum']:
        assert column not in component_df.columns
    for column in ENA_FILES + ['read_1_file_checksum', 'read_2_file_checksum']:
        assert column in component_df.columns


def test_prepare_file_submission_falls_back_to_read_files_without_ena_fields(mock_schema_loading):
    # e.g. a schema loaded before term_repository existed: ENA must still get the reads
    schema = [{k: v for k, v in field.items() if k != 'term_repository'} for field in FILE_SCHEMA]
    mock_schema_loading.return_value = {**SCHEMAS, 'file': schema}

    with mock.patch.object(ena_submission, 'Logger'):
        component_df, _, ena_file_columns = ena_submission._prepare_file_submission(
            SINGLECELL, file_component_name='file')

    assert ena_file_columns == ENA_FILES
    assert set(ENA_FILES) <= set(component_df.columns)
    assert not set(ZENODO_FILES) & set(component_df.columns)


def test_prepare_file_submission_without_file_rows_returns_nothing_to_submit(mock_schema_loading):
    singlecell = {**SINGLECELL, 'components': {**SINGLECELL['components'], 'file': []}}

    component_df, _, ena_file_columns = ena_submission._prepare_file_submission(
        singlecell, file_component_name='file')

    assert component_df.empty
    assert ena_file_columns == []


# EnaSubmissionHelper.register_files: the XML sent to ENA

def failed_receipt():
    return b'<RECEIPT success="false"><MESSAGES><ERROR>test stop</ERROR></MESSAGES></RECEIPT>'


@pytest.fixture
def submitted_xml(tmp_path):
    '''Run register_files for FILE_ROW and capture the experiment and run XML.

    ENA is never contacted: curl returns a failed receipt, which register_files
    records as a rejection after the XML has been written.
    '''
    written = {}

    def write_xml_file(output_location=str(), xml_object=None, file_name=str()):
        written[file_name] = etree.fromstring(etree.tostring(xml_object))
        return {'status': True, 'value': str(tmp_path / file_name)}

    helper = ena_helper.EnaSubmissionHelper.__new__(ena_helper.EnaSubmissionHelper)
    helper.submission_id = 'submission1'
    helper.profile_id = 'profile1'
    helper.sra_settings = {'sra_center': 'TEST'}
    helper.user_token = 'Webin-0'
    helper.pass_word = 'password'
    helper.write_xml_file = write_xml_file
    helper.logging_info = mock.MagicMock()
    helper.logging_error = mock.MagicMock()
    helper.logging_debug = mock.MagicMock()

    ena_transfers = [{'local_path': f'/uploads/{name}', 'remote_path': 'copo/'}
                     for name in ENA_FILE_NAMES + ZENODO_FILE_NAMES]
    row = {**FILE_ROW, 'sample_id': 'SAMPLE001', 'accession_ena': '',
           'library_strategy': 'RNA-Seq', 'library_source': 'TRANSCRIPTOMIC SINGLE CELL',
           'library_selection': 'cDNA', 'library_layout': 'paired',
           'instrument_model': 'Illumina NovaSeq 6000'}

    def run(component_df, file_columns):
        with mock.patch.object(ena_helper.helpers, 'get_db_data_sources',
                               return_value=[{'value': 'Illumina NovaSeq 6000', 'platform': 'ILLUMINA'}]), \
                mock.patch.object(ena_helper, 'EnaFileTransfer') as ena_file_transfer, \
                mock.patch.object(ena_helper, 'Singlecell'), \
                mock.patch.object(ena_helper.tempfile, 'gettempdir', return_value=str(tmp_path)), \
                mock.patch.object(ena_helper.subprocess, 'check_output', return_value=failed_receipt()):
            ena_file_transfer.return_value.get_all_records_columns.return_value = ena_transfers
            helper.register_files(
                submission_xml_path='submission.xml', modify_submission_xml_path='modify.xml',
                component_df=component_df, identifier_map={'sample': 'sample_id', 'file': 'file_id'},
                singlecell=SINGLECELL, file_component_name='file', file_columns=file_columns)
        return written

    return lambda component_df=None, file_columns=ENA_FILES: run(
        pd.DataFrame([row]) if component_df is None else component_df, file_columns)


def run_file_names(run_xml):
    return [path.split('/')[-1] for path in run_xml.xpath('//RUN/DATA_BLOCK/FILES/FILE/@filename')]


def experiment_attribute_tags(experiment_xml):
    return experiment_xml.xpath('//EXPERIMENT_ATTRIBUTE/TAG/text()')


@pytest.fixture
def submitted_ena_xml(mock_schema_loading, submitted_xml):
    '''The XML sent to ENA for SINGLECELL, prepared the way a submission is:
    _prepare_file_submission routes the files, then register_files builds the XML.'''
    component_df, _, ena_file_columns = ena_submission._prepare_file_submission(
        SINGLECELL, file_component_name='file')
    # Normally merged in from the sample, library and sequencing components
    component_df = component_df.assign(
        sample_id='SAMPLE001', accession_ena='', library_strategy='RNA-Seq',
        library_source='TRANSCRIPTOMIC SINGLE CELL', library_selection='cDNA',
        library_layout='paired', instrument_model='Illumina NovaSeq 6000')

    return submitted_xml(component_df=component_df, file_columns=ena_file_columns)


def test_run_xml_lists_only_the_ena_files(submitted_ena_xml):
    assert run_file_names(submitted_ena_xml['run.xml']) == ENA_FILE_NAMES


def test_run_xml_carries_each_ena_file_checksum(submitted_ena_xml):
    checksums = submitted_ena_xml['run.xml'].xpath('//RUN/DATA_BLOCK/FILES/FILE/@checksum')

    assert checksums == ['1' * 32, '2' * 32]


def test_no_file_names_or_checksums_become_experiment_attributes(submitted_ena_xml):
    tags = experiment_attribute_tags(submitted_ena_xml['experiment.xml'])

    for column in ENA_FILES + ZENODO_FILES + ['read_1_file_checksum', 'read_2_file_checksum']:
        assert column not in tags


def test_no_zenodo_file_name_appears_anywhere_in_the_ena_xml(submitted_ena_xml):
    xml_text = b''.join(etree.tostring(xml) for xml in submitted_ena_xml.values()).decode()

    for name in ZENODO_FILE_NAMES:
        assert name not in xml_text


def test_run_xml_follows_the_file_columns_it_is_given(submitted_xml):
    # The file list comes from the caller (the schema), not a hardcoded read 1/read 2 pair
    written = submitted_xml(file_columns=['read_1_file'])

    assert run_file_names(written['run.xml']) == ['sampleA_R1.fastq.gz']


@pytest.fixture
def pending_check():
    from src.apps.copo_single_cell_submission.utils import copo_single_cell

    def run(transfers, repository='ena', singlecell_record=SINGLECELL):
        submission = mock.MagicMock()
        submission.get_submission_downloading.return_value = [
            {'_id': 'sub1', 'profile_id': 'profile1', 'study': ['STUDY001'], 'repository': repository}]
        singlecell = mock.MagicMock()
        singlecell.get_collection_handle.return_value.find_one.return_value = singlecell_record
        schemas = SinglecellSchemas.__new__(SinglecellSchemas)
        schemas.get_schema = mock.MagicMock(return_value=SCHEMAS)
        file_transfer = mock.MagicMock()
        file_transfer.get_all_records_columns.return_value = transfers

        with mock.patch.object(copo_single_cell, 'Submission', return_value=submission), \
                mock.patch.object(copo_single_cell, 'Singlecell', return_value=singlecell), \
                mock.patch.object(copo_single_cell, 'SinglecellSchemas', return_value=schemas), \
                mock.patch.object(copo_single_cell, 'EnaFileTransfer', return_value=file_transfer):
            copo_single_cell.update_submission_pending()
        return submission

    return run


def test_ena_submission_is_pending_once_its_own_files_are_transferred(pending_check):
    # The ENA files are the only ones that are queued for transfer and as a result, 
    # the Zenodo files never get a transfer record. As such, the submission 
    # should not wait for them.
    transfers = [{'local_path': f'/tmp/profile1/{name}', 'status': 'complete'} for name in ENA_FILE_NAMES]

    submission = pending_check(transfers)

    submission.update_submission_pending.assert_called_once_with(['sub1'], component='study')


def test_ena_submission_waits_while_one_of_its_files_is_still_transferring(pending_check):
    transfers = [{'local_path': '/tmp/profile1/sampleA_R1.fastq.gz', 'status': 'complete'},
                 {'local_path': '/tmp/profile1/sampleA_R2.fastq.gz', 'status': 'processing'}]

    submission = pending_check(transfers)

    submission.update_submission_pending.assert_not_called()


def test_study_deleted_while_downloading_is_removed_from_the_submission(pending_check):
    # A study whose single-cell record was deleted mid-submission is dropped from
    # the submission instead of crashing the beat task for every submission.
    submission = pending_check([], singlecell_record=None)

    submission.remove_component_from_submission.assert_called_once_with(
        sub_id='sub1', component='study', component_ids=['STUDY001'])
