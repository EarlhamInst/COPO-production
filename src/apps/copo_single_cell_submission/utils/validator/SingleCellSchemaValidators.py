from common.validators.validator import Validator
from django.conf import settings
from common.dal.sample_da import Sample
import re
import pandas as pd
from common.validators.helpers import checkOntologyTerm, checkNCBITaxonTerm, clean_str
import requests
from common.utils.helpers import get_env
import xml.etree.ElementTree as ET

from src.apps.ei_edp.utils.edp_utils import EDP_PROJECT_STUDY_FIELDS
from src.apps.ei_edp.utils.edp_validation_messages import MESSAGES as EDP_PROJECT_MESSAGES
from .validation_messages import MESSAGES as GENERAL_MESSAGES


ena_browser_service = get_env("ENA_BROWSER_SERVICE")
session = requests.Session()
lg = settings.LOGGER

# Merge validation messages
MESSAGES = GENERAL_MESSAGES | EDP_PROJECT_MESSAGES

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
                            regex = field.get("term_regex","")
                            if regex and pd.notna(regex):

                                if not re.match(regex.strip(), str(row)):
                                    error_msg = MESSAGES["invalid_column_value_regex"].format(
                                        component=component,
                                        invalid_value=row,
                                        column_name=field["term_label"],
                                        line_no=i + self.first_data_line_no,
                                        field_description=field.get("term_error_message", "") or field.get("term_description", f"Expected a value matching the pattern: {regex.strip()}")
                                    )
                                    self.errors.append(error_msg)
                                    self.flag = False
                        elif type == "ontology":   
                            reference = field.get("term_reference", "")
                            if reference:
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
