from common.validators.validator import Validator
from django.conf import settings
from common.dal.sample_da import Sample
import re
import pandas as pd
from common.validators.helpers import checkOntologyTerm, checkNCBITaxonTerm, clean_str, OntologyLookupUnavailable
import requests
from common.utils.helpers import get_env
import xml.etree.ElementTree as ET

from src.apps.ei_edp.utils.constants import EDP_PROJECT_STUDY_FIELDS
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES as EDP_PROJECT_MESSAGES
from .validation_messages import MESSAGES as GENERAL_MESSAGES

from src.apps.copo_assembly_submission.utils.assembly_utils import get_assembly_file_error
from src.apps.copo_seq_annotation_submission.utils.seq_annotation_utils import (
    SEQUENCE_ANNOTATION_FILE_TYPE_EXTS, get_sequence_annotation_file_error)
from common.s3.s3Connection import S3Connection
from common.utils.helpers import get_file_header, get_gff3_errors, matches_expected_file_signature
from src.apps.copo_assembly_submission.utils.assembly_utils import has_valid_extension, to_html_list
from common.ena_utils.ena_helper import READ_FILE_TYPE_EXTS
from common.validators.ena_validators.validation_messages import MESSAGES as ena_msg

ena_browser_service = get_env("ENA_BROWSER_SERVICE")
session = requests.Session()
lg = settings.LOGGER

# Merge validation messages
MESSAGES = GENERAL_MESSAGES | EDP_PROJECT_MESSAGES

# The file extensions accepted for each `expression_data_file_format` in the single cell
# manifest. These data files will go to Zenodo only
EXPRESSION_DATA_FILE_FORMAT_EXTS = {
    'h5ad': ['.h5ad'],
    'rds': ['.rds'],
    'loom': ['.loom'],
    'mtx': ['.mtx', '.mtx.gz'],
    'csv': ['.csv', '.csv.gz'],
}

# Worksheets with a file type column and the file columns that must match it:
# {component: (file type column, [file columns], {file type: [extensions]})}
# The che
FILE_TYPE_CHECKS = {
    # Reads manifest (under Biodata profiles)
    #  - The checks for assembly (`assembly_file`) and 
    #    sequencing annotation (`sequencing_annotation_file`) files are covered in the
    #    `AssemblyFileValidator` and `SequencingAnnotationFileValidator` respectively.
    'read_file': ('read_file_type', ['read_1_file', 'read_2_file'], READ_FILE_TYPE_EXTS),

    # Single-cell manifest
    'expression_data_file': ('expression_data_file_format', ['expression_data_file'], EXPRESSION_DATA_FILE_FORMAT_EXTS),
}

class MandatoryValuesValidator(Validator):
    def validate(self):
        schema = self.kwargs.get("schema", {})
        component = self.kwargs.get("component", "")
        missing_mandatory_column_count = self.kwargs.get("missing_mandatory_column_count")

        for key, field in schema.items():
            if field.get("mandatory","") == "M":               
                if key not in self.data.columns:
                    missing_mandatory_column_count += 1
                    error_msg = MESSAGES["missing_column"].format(
                        component=component,
                        field_name=key
                    )
                    self.errors.append(error_msg)
                    self.flag = False
                else:
                    null_rows=[]
                    #null_rows.extend(self.data[self.data[key].isnull()].index.tolist())
                    null_rows.extend(self.data[self.data[key] == ""].index.tolist())
                    null_rows.extend(self.data[self.data[key].isna()].index.tolist())
                    for row in null_rows:
                        error_msg = ''

                        if component == 'study' and key in EDP_PROJECT_STUDY_FIELDS:
                            '''
                            EDP profile-specific missing value handling

                            This is needed because there is no 'study' worksheet in downloaded LIMS manifests.
                            Logic regarding the 'study' component is handled behind-the-scenes so this alternative
                            approach prevents the generic missing value message and uses a specific one for the key.

                            Original response: "Sheet 'study': Missing data in column 'Sample Return' at row '1'."
                            Expected response: "Sheet 'sample': Missing response to 'Sample return requirement'."
                            '''

                            error_msg = MESSAGES[f'missing_value_{key}']
                        else:
                            error_msg = MESSAGES["missing_value"].format(
                                component=component,
                                column_name=field["term_label"],
                                line_no=row + self.first_data_line_no,
                            )
                        self.errors.append(error_msg)
                        self.flag = False
        return self.errors, self.warnings, self.flag, missing_mandatory_column_count


class IncorrectValueValidator(Validator):
    def validate(self):
        schema_map = self.kwargs.get("schema", {})
        component = self.kwargs.get("component", "")
        missing_mandatory_column_count = self.kwargs.get("missing_mandatory_column_count")

        biosampleAccessions = Sample(profile_id=self.profile_id).get_all_records_columns(filter_by= {"biosampleAccession": {"$exists":True, "$ne": ""}})
        biosampleAccessionsMap = {}
        if biosampleAccessions:
            biosampleAccessionsMap = {row["biosampleAccession"]: row for row in biosampleAccessions} 

        #get all BIOSAMPLEACCESSION_EXT_FIELD from the checklist fields
        biosampleAccession_ext_field_map = {key: field for key, field in schema_map.items() if field.get("term_type") == "BIOSAMPLEACCESSION_EXT_FIELD"}

        ols_unavailable = False
        for column in self.data.columns:
            if column in schema_map.keys():
                field = schema_map[column]
                type = field.get("term_type","")
                is_identifier = field.get("identifier", False)

                for i, row in self.data[column].items():                     
                    if row:
                        row = str(row).strip()
                        if type == "enum":
                            if row not in field.get("choice", []):
                                valid_values = field.get('choice', [])
                                error_msg = MESSAGES["invalid_column_value_with_list"].format(
                                    component=component,
                                    invalid_value=row,
                                    column_name=field["term_label"],
                                    line_no=i + self.first_data_line_no,
                                    valid_values="<ul>" + "".join(f"<li>{v}</li>" for v in valid_values) + "</ul>"
                                )
                                self.errors.append(error_msg)
                                self.flag = False
                        elif type == "string":
                            # A regex cell that is empty or only has whitespace means that
                            # there is no regex. Otherwise, it would be an empty pattern
                            # that no value can match
                            regex = field.get("term_regex","")
                            regex = regex.strip() if isinstance(regex, str) else ""
                            if regex:
                                # Use `re.fullmatch()` instead of `re.match()`
                                # to ensure that the entire cell content matches the regex pattern
                                if not re.fullmatch(regex, str(row)):
                                    error_msg = MESSAGES["invalid_column_value_regex"].format(
                                        component=component,
                                        invalid_value=row,
                                        column_name=field["term_label"],
                                        line_no=i + self.first_data_line_no,
                                        field_description=field.get("term_error_message", "") or field.get("term_description", f"Expected a value matching the pattern: {regex}")
                                    )
                                    self.errors.append(error_msg)
                                    self.flag = False
                        elif type == "ontology":
                            reference = field.get("term_reference", "")
                            if reference:
                                # Once OLS can't be reached, the remaining ontology
                                # values aren't checked as each lookup would also
                                # wait for OLS to time out
                                if ols_unavailable:
                                    continue
                                try:
                                    if reference == "NCBITaxon":
                                        if not checkNCBITaxonTerm(row):
                                            error_msg = MESSAGES["invalid_column_value_ontology"].format(
                                                component=component,
                                                invalid_value=row,
                                                column_name=field["term_label"],
                                                line_no=i + self.first_data_line_no,
                                                ontology_name="NCBITaxon"
                                            )
                                            self.errors.append(error_msg)
                                            self.flag = False
                                    else:
                                        #it should be "ontology_id:ancestor, i.e. EFO:0004466"
                                        ontology_id = reference.split(":")[0]
                                        ancestor = clean_str(reference.split(":")[1])
                                        if not checkOntologyTerm(ontology_id, ancestor, row):
                                            error_msg = MESSAGES["invalid_column_value_ontology"].format(
                                                component=component,
                                                invalid_value=row,
                                                column_name=field["term_label"],
                                                line_no=i + self.first_data_line_no,
                                                ontology_name=str(reference)
                                            )
                                            self.errors.append(error_msg)
                                            self.flag = False
                                except OntologyLookupUnavailable:
                                    ols_unavailable = True
                                    self.errors.append(MESSAGES["ontology_lookup_unavailable"].format(
                                        component=component,
                                        column_name=field["term_label"],
                                        ontology_name=str(reference),
                                    ))
                                    self.flag = False
                            else:
                                error_msg = MESSAGES["missing_ontology_term"].format(
                                    component=component,
                                    column_name=field["term_label"]
                                )
                                self.errors.append(error_msg)
                                self.flag = False

                        elif type == "BIOSAMPLEACCESSION_FIELD":
                            if row not in biosampleAccessionsMap.keys():
                                #check biosample from ena                             
                                try:                                    
                                    response = session.get(f"{ena_browser_service}/xml/{row}")
                                    if response.status_code == requests.codes.ok:
                                        root = ET.fromstring(response.text)
                                        sample_name = root.find(".//SAMPLE_NAME")
                                        taxon_id = sample_name.find('TAXON_ID').text
                                        scientific_name = sample_name.find('SCIENTIFIC_NAME').text
                                        if taxon_id:
                                            user_taxon_id = self.data.loc[i].get("taxon_id", "")
                                            if isinstance(user_taxon_id, str):
                                                user_taxon_id = user_taxon_id.strip()
                                            else:
                                                user_taxon_id = str(int(user_taxon_id))
                                            if taxon_id != user_taxon_id:
                                                error_msg = MESSAGES["mismatched_value"].format(
                                                    component=component,
                                                    invalid_value=user_taxon_id,
                                                    column_name="TAXON_ID",
                                                    line_no=i + self.first_data_line_no,
                                                    biosampleAccession=row
                                                )
                                                self.errors.append(error_msg)
                                                self.flag = False
                                        if scientific_name :
                                            user_scientific_name = self.data.loc[i].get("scientific_name","")
                                            if scientific_name != user_scientific_name:
                                                error_msg = MESSAGES["mismatched_value"].format(
                                                    component=component,
                                                    invalid_value=user_scientific_name,
                                                    column_name="SCIENTIFIC_NAME",
                                                    line_no=i + self.first_data_line_no,
                                                    biosampleAccession=row
                                                )
                                                self.errors.append(error_msg) 
                                                self.flag = False
                                            #else:
                                            #    self.data[f"{Validator.PREFIX_4_NEW_FIELD}sraAccession"] = sample_accession
                                    else:
                                        error_msg = MESSAGES["invalid_column_value_generic"].format(
                                            component=component,
                                            invalid_value=row,
                                            column_name=field["term_label"],
                                            line_no=i + self.first_data_line_no,
                                            expected_value="a valid Biosample Accession"
                                        )
                                        self.errors.append(error_msg)
                                        self.flag = False
                                except Exception as e:
                                    lg.exception(e)
                                    error_msg = MESSAGES["biosampleAccession_validation_exception"].format(
                                        component=component,
                                        biosampleAccession=row,
                                        column_name=field["term_label"],
                                        line_no=i + self.first_data_line_no
                                    )
                                    self.errors.append(error_msg)
                                    self.flag = False

                            else:
                                for key, field in biosampleAccession_ext_field_map.items():
                                    value = self.data.loc[i].get(key,"")
                                    sample = biosampleAccessionsMap.get(row)
                                    if key in sample and sample[key] and sample[key] != value:
                                        error_msg = MESSAGES["mismatched_value"].format(
                                            component=component,
                                            invalid_value=value,
                                            column_name=field["term_label"],
                                            line_no=i + self.first_data_line_no,
                                            biosampleAccession=row
                                        )
                                        self.errors.append(error_msg)
                                        self.flag = False
                                                                    
                if is_identifier:
                    df = self.data[column].groupby(self.data[column]).filter(lambda x: len(x) >1).value_counts()
                    for index, row in df.items():
                        error_msg = MESSAGES["identifier_column_not_unique"].format(
                            component=component,
                            column_name=field["term_label"],
                            invalid_value=index
                        )
                        self.errors.append(error_msg)
                        self.flag = False
            else:
                error_msg = MESSAGES["invalid_column"].format(
                    component=component,
                    column_name=column
                )
                self.warnings.append(error_msg)
        return self.errors, self.warnings, self.flag, missing_mandatory_column_count

class StudyComponentValidator(Validator):
    def validate(self):
        component = self.kwargs.get("component", "")
        missing_mandatory_column_count = self.kwargs.get("missing_mandatory_column_count")

        if component == "study":
            if len(self.data) == 0:
                self.errors.append("Study component is missing")
                self.flag = False
            #only one study component is allowed
            elif len(self.data) > 1:
                self.errors.append("Only one study is allowed")
                self.flag = False
        return self.errors, self.warnings, self.flag, missing_mandatory_column_count


class AssemblyFileValidator(Validator):
    # Check the `assembly_file` worksheet against the file types and extensions that ENA accepts

    def validate(self):
        component = self.kwargs.get('component', '')
        if component != 'assembly_file':
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        if not {'assembly_file_type', 'assembly_file'}.issubset(self.data.columns):
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        schema = self.kwargs.get('schema', {})
        file_type_choices = schema.get('assembly_file_type', {}).get('choice') or []

        for i, row in self.data.iterrows():
            file_type = str(row['assembly_file_type'] or '').strip()
            file_name = str(row['assembly_file'] or '').strip()

            # Empty values are reported by `MandatoryValuesValidator` and values
            # outside the dropdown by `IncorrectValueValidator`
            if not file_type or not file_name:
                continue
            if file_type_choices and file_type not in file_type_choices:
                continue

            error = get_assembly_file_error(file_type, file_name)
            if error:
                self.errors.append(
                    f'Sheet <strong>{component}</strong>, row <strong>{i + 2}</strong>: {error}')
                self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)


class SequencingAnnotationFileValidator(Validator):
    # Check whether each file in the `sequencing_annotation_file` worksheet of a 
    # single-cell manifest matches its file type,
    # e.g. a 'gff' file must end in .gff or .gff.gz

    def validate(self):
        component = self.kwargs.get('component', '')
        if component != 'sequencing_annotation_file':
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        if 'sequencing_annotation_file' not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        has_file_type = 'sequencing_annotation_file_type' in self.data.columns
        schema = self.kwargs.get('schema', {})
        file_type_choices = schema.get('sequencing_annotation_file_type', {}).get('choice') or []

        for i, row in self.data.iterrows():
            file_name = str(row['sequencing_annotation_file'] or '').strip()
            if not file_name:
                continue  # Empty values are reported by `MandatoryValuesValidator`

            file_type = str(row['sequencing_annotation_file_type'] or '').strip() if has_file_type else ''
            if file_type and file_type_choices and file_type not in file_type_choices:
                continue  # Values outside the dropdown are reported by `IncorrectValueValidator`

            error = get_sequence_annotation_file_error(file_name, file_type)
            if error:
                self.errors.append(
                    f'Sheet <strong>{component}</strong>, row <strong>{i + 2}</strong>: {error}')
                self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        if 'sequencing_annotation_file' not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        for i, row in self.data.iterrows():
            file_name = str(row['sequencing_annotation_file'] or '').strip()
            if not file_name:
                continue  # Empty values are reported by `MandatoryValuesValidator`

            error = get_sequence_annotation_file_error(file_name)
            if error:
                self.errors.append(
                    f'Sheet <strong>{component}</strong>, row <strong>{i + 2}</strong>: {error}')
                self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)


class SequencingAnnotationGff3FileValidator(Validator):
    # Check each GFF3 file provided in the `sequencing_annotation_gff3_file` worksheet of a reads
    # manifest under Biodata profiles
    #  - the data file name must end in `.gff3.gz`
    #  - if the file is already in COPO then, it must be gzipped and start with the
    #    GFF3 header (i.e. '##gff-version 3', '##sequence-region' and '##species').
    component_name = 'sequencing_annotation_gff3_file'
    file_column = 'sequencing_annotation_gff3_file'

    def validate(self):
        component = self.kwargs.get('component', '')
        if component != self.component_name or self.file_column not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        valid_exts = SEQUENCE_ANNOTATION_FILE_TYPE_EXTS['gff3']
        uploaded_files = None  # listed from S3 only if a file name is valid

        for i, row in self.data.iterrows():
            file_name = str(row[self.file_column] or '').strip()
            if not file_name:
                continue  # Empty values are reported by `MandatoryValuesValidator`

            if not has_valid_extension(file_name, valid_exts):
                error = ena_msg['invalid_file_extension'].format(
                    file_name=file_name, valid_exts=to_html_list(valid_exts))
            else:
                if uploaded_files is None:
                    uploaded_files = {obj['Key'] for obj in (S3Connection().list_objects(self.profile_id) or [])}
                error = self._get_uploaded_file_error(file_name) if file_name in uploaded_files else ''

            if error:
                self.errors.append(f'Sheet <strong>{component}</strong>, row <strong>{i + 2}</strong>: {error}')
                self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

    def _get_uploaded_file_error(self, file_name):
        # Return an error for a GFF3 file in S3 that is not gzipped or not valid GFF3 or ''
        s3obj = S3Connection()
        if not matches_expected_file_signature(file_name, get_file_header(s3obj, self.profile_id, file_name)):
            return ena_msg['invalid_compressed_file'].format(file_name=file_name, file_extension='.gz')

        gff3_errors = get_gff3_errors(s3obj, self.profile_id, file_name)
        if gff3_errors:
            return f'<strong>{file_name}</strong> is not a valid GFF3 file:<br>' + '<br>'.join(gff3_errors)
        return ''


def get_file_type_error(file_name, file_type, extensions_by_type):
    # Return an HTML error if file_name does not end in an extension of file_type or ''.
    # Values outside the dropdown are reported by `IncorrectValueValidator`.
    types_by_lower = {t.lower(): t for t in extensions_by_type}
    canonical_type = types_by_lower.get(str(file_type or '').strip().lower())
    if not canonical_type:
        return ''

    valid_exts = extensions_by_type[canonical_type]
    if has_valid_extension(file_name, valid_exts):
        return ''
    return ena_msg['file_type_extension_mismatch'].format(
        file_name=file_name, file_type=canonical_type, valid_exts=to_html_list(valid_exts))


class FileTypeValidator(Validator):
    # Check each file matches the file type chosen for it on the same row, e.g. a read
    # file of type 'bam' must end in .bam. The worksheets are listed in FILE_TYPE_CHECKS

    def validate(self):
        component = self.kwargs.get('component', '')
        if component not in FILE_TYPE_CHECKS:
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        type_column, file_columns, extensions_by_type = FILE_TYPE_CHECKS[component]
        if type_column not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)

        for i, row in self.data.iterrows():
            file_type = row[type_column]
            for file_column in file_columns:
                if file_column not in self.data.columns:
                    continue
                value = row[file_column]
                file_name = '' if pd.isna(value) else str(value).strip()  # empty cells can be NaN
                if not file_name:
                    continue  # e.g. no read_2_file for single-end reads

                error = get_file_type_error(file_name, file_type, extensions_by_type)
                if error:
                    # Row 1 is the header, so data rows start at row 2
                    self.errors.append(f'Sheet <strong>{component}</strong>, row <strong>{i + 2}</strong>, '
                                       f'column <strong>{file_column}</strong>: {error}')
                    self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('missing_mandatory_column_count', 0)
