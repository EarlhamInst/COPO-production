from common.validators.validator import Validator
from django.conf import settings
from common.dal.sample_da import Sample
import re
import pandas as pd
from common.validators.helpers import checkOntologyTerm, checkNCBITaxonTerm, clean_str
import requests
from common.utils.helpers import get_env
import xml.etree.ElementTree as ET
from .validation_messages import MESSAGES as msg
from src.apps.copo_assembly_submission.utils.assembly_utils import get_assembly_file_error
from src.apps.copo_seq_annotation_submission.utils.seq_annotation_utils import get_sequence_annotation_file_error
from src.apps.copo_assembly_submission.utils.assembly_utils import has_valid_extension, to_html_list
from common.ena_utils.ena_helper import READ_FILE_TYPE_EXTS
from common.validators.ena_validators.validation_messages import MESSAGES as ena_msg

ena_sample_service = get_env("ENA_V1_SAMPLE_SERVICE")
session = requests.Session()
lg = settings.LOGGER


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

        for key, field in schema.items():
            if field.get("mandatory","") == "M":               
                if key not in self.data.columns:
                    self.errors.append("Sheet " + component + ": Mandatory column <strong>" + key + "</strong> is missing")
                    self.flag = False
                else:
                    null_rows=[]
                    null_rows.extend(self.data[self.data[key].isnull()].index.tolist())
                    null_rows.extend(self.data[self.data[key] == ""].index.tolist())
                    null_rows.extend(self.data[self.data[key].isna()].index.tolist())
                    for row in null_rows:
                        self.errors.append("Sheet <strong>%s</strong>: Missing data detected in column <strong>%s</strong> at row <strong>%s</strong>." % (
                            component, field["term_label"], str(row + 1)))
                        self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class IncorrectValueValidator(Validator):
    def validate(self):
        schema_map = self.kwargs.get("schema", {})
        component = self.kwargs.get("component", "")

        biosampleAccessions = Sample(profile_id=self.profile_id).get_all_records_columns(filter_by= {"biosampleAccession": {"$exists":True, "$ne": ""}})
        biosampleAccessionsMap = {}
        if biosampleAccessions:
            biosampleAccessionsMap = {row["biosampleAccession"]: row for row in biosampleAccessions} 

        #get all BIOSAMPLEACCESSION_EXT_FIELD from the checklist fields
        biosampleAccession_ext_field_map = {key: field for key, field in schema_map.items() if field.get("term_type") == "BIOSAMPLEACCESSION_EXT_FIELD"}

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
                                valid_values = field.get("choice")
                                content = (
                                    "<ul>"
                                    + "".join(f"<li>{v}</li>" for v in valid_values)
                                    + "</ul>"
                                )
                                self.errors.append(
                                    msg["validation_msg_invalid_enum"].format(
                                        component=component,
                                        value=row,
                                        column=field['term_label'],
                                        row=i + 2,
                                        num_values=len(valid_values),
                                        content=content,
                                    )
                                )
                                self.flag = False
                        elif type == "string":
                            regex = field.get("term_regex","")
                            if regex and pd.notna(regex):

                                if not re.match(regex.strip(), str(row)):
                                    self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + row + "</strong> in column <strong>" + field["term_label"] + "</strong> at row <strong>" + str(i + 2) + "</strong>.<br>" + field.get("term_error_message", "Expected a value matching the pattern: " + str(regex)))
                                    self.flag = False
                        elif type == "ontology":   
                            reference = field.get("term_reference", "")
                            if reference:
                                if reference == "NCBITaxon":
                                    if not checkNCBITaxonTerm(row):
                                        self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + row + "</strong> in column <strong>" + field["term_label"] + "</strong> at row <strong>" + str(i+2) + "</strong>.<br>Expected a valid NCBI taxon term")
                                        self.flag = False
                                else:
                                    #it should be "ontology_id:ancestor, i.e. EFO:0004466"
                                    ontology_id = reference.split(":")[0]
                                    ancestor = clean_str(reference.split(":")[1])
                                    if not checkOntologyTerm(ontology_id, ancestor, row):
                                        self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + row + "</strong> in column <strong>" + field["term_label"] + "</strong> at row <strong>" + str(i + 2) + "</strong>.<br>" + f"Expected an ontology term listed for <a target='_blank' href='https://purl.obolibrary.org/obo/{str(reference).strip().replace(':','_')}'>{str(reference)}</a>.")
                                        self.flag = False
                            else:
                                self.errors.append("Sheet <strong>" + component + "</strong>: Ontology term reference is missing for column '" + field["term_label"] + "'")
                                self.flag = False

                        elif type == "BIOSAMPLEACCESSION_FIELD":
                            if row not in biosampleAccessionsMap.keys():
                                #check biosample from ena
                                try:
                                    response = session.get(f"{ena_sample_service}/{row}", data={}, timeout=15)
                                    if response.status_code == requests.codes.ok:
                                        root = ET.fromstring(response.text)
                                        sample_name = root.find(".//SAMPLE_NAME")
                                        taxon_id = sample_name.find('TAXON_ID').text
                                        sample_accession = root.find(".//SAMPLE").attrib['accession']
                                        if taxon_id:
                                            read_taxon_id = self.data.iloc[i-2].get("taxon_id","")
                                            if taxon_id != read_taxon_id:
                                                self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + read_taxon_id + "</strong> not match with <strong>" + taxon_id + "</strong> in column <strong>TAXON_ID</strong> at row <strong>" + str(i+2) + "</strong>")
                                                self.flag = False
                                            else:
                                                self.data[f"{Validator.PREFIX_4_NEW_FIELD}sraAccession"] = sample_accession
                                    else:
                                        self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + row + "</strong> in column <strong>" + field["term_label"] + "</strong> at row <strong>" + str(i+2) + "</strong>. Invalid ENA sample")
                                        self.flag = False
                                except Exception as e:
                                    lg.exception(e)
                                    self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + row + "</strong> in column <strong>" + field["term_label"] + "</strong> at row <strong>" + str(i+2) + "</strong>. Invalid ENA sample")
                                    self.flag = False

                            else:
                                for key, field in biosampleAccession_ext_field_map.items():
                                    value = self.data.loc[i].get(key,"")
                                    sample = biosampleAccessionsMap.get(row)
                                    if key in sample and sample[key] and sample[key] != value:
                                        self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + value + "</strong> not match with <strong>" + sample.get(key,"")+ "</strong> in column <strong>" + key + "</strong> at row <strong>" + str(i+2) + "</strong>")
                                        self.flag = False
                                                                    
                if is_identifier:
                    df = self.data[column].groupby(self.data[column]).filter(lambda x: len(x) >1).value_counts()
                    for index, row in df.items():
                        self.errors.append("Sheet <strong>" + component + "</strong>: Invalid value <strong>" + index + "</strong> in column <strong>" + column + "</strong>: duplicated " + ( "twice" if row == 2 else  str(row) ) +" times." )
                        self.flag = False                                         
            else:
                self.errors.append("Sheet <strong>" + component + "</strong>: Invalid column <strong>" + column + "</strong>")
                self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")

class StudyComponentValidator(Validator):
    def validate(self):
        component = self.kwargs.get("component", "")
        if component == "study":
            if len(self.data) == 0:
                self.errors.append("Study component is missing")
                self.flag = False
            #only one study component is allowed
            elif len(self.data) > 1:
                self.errors.append("Only one study is allowed")
                self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class AssemblyFileValidator(Validator):
    # Check the `assembly_file` worksheet against the file types and extensions that ENA accepts

    def validate(self):
        component = self.kwargs.get('component', '')
        if component != 'assembly_file':
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

        if not {'assembly_file_type', 'assembly_file'}.issubset(self.data.columns):
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

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

        return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')


class SequencingAnnotationFileValidator(Validator):
    # Check whether each file in the `sequencing_annotation_file` worksheet of a 
    # single-cell manifest matches its file type,
    # e.g. a 'gff' file must end in .gff or .gff.gz

    def validate(self):
        component = self.kwargs.get('component', '')
        if component != 'sequencing_annotation_file':
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

        if 'sequencing_annotation_file' not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

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

        return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

        if 'sequencing_annotation_file' not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

        for i, row in self.data.iterrows():
            file_name = str(row['sequencing_annotation_file'] or '').strip()
            if not file_name:
                continue  # Empty values are reported by `MandatoryValuesValidator`

            error = get_sequence_annotation_file_error(file_name)
            if error:
                self.errors.append(
                    f'Sheet <strong>{component}</strong>, row <strong>{i + 2}</strong>: {error}')
                self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')


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
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

        type_column, file_columns, extensions_by_type = FILE_TYPE_CHECKS[component]
        if type_column not in self.data.columns:
            # Missing columns are reported by `MandatoryValuesValidator`
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

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

        return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')
