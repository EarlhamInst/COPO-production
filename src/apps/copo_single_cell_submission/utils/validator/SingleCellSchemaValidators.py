from common.validators.validator import Validator
from django.conf import settings
from common.dal.sample_da import Sample
import re
import pandas as pd
from common.validators.helpers import checkOntologyTerm, checkNCBITaxonTerm
import requests
from common.utils.helpers import get_env
import xml.etree.ElementTree as ET

ena_sample_service = get_env("ENA_V1_SAMPLE_SERVICE")
session = requests.Session()
lg = settings.LOGGER


class ManifestChecklistValidator(Validator):
    '''
    Checks whether the uploaded manifest's column set
    matches the expected checklist's columns.
    '''

    def validate(self):
        checklist = self.kwargs.get('checklist', {})
        expected_columns = set(checklist['fields'].keys())
        uploaded_columns = set(self.data.columns)

        # Determine the number of expected columns that are missing
        missing = expected_columns - uploaded_columns
        # Determine the number of many unexpected columns that exist in the uploaded manifest
        extras = uploaded_columns - expected_columns

        # Calculate the match ratio
        total_expected = len(expected_columns)
        match_ratio = (total_expected - len(missing)) / max(total_expected, 1)

        # If almost nothing matches, fail with a single message
        if match_ratio < 0.5:  # Threshold: less than 50% columns match
            self.errors.append(
                'The uploaded manifest does not appear to match the expected checklist. '
                'Please ensure that you are uploading the correct manifest for the selected manifest '
                f'(expected {len(expected_columns)} columns, found {len(uploaded_columns)}).'
            )
            self.flag = False
        else:
            # Otherwise, proceed with the other validators
            if missing:
                self.warnings.append(
                    f'Some expected columns are missing: {sorted(missing)}'
                )
            if extras:
                self.warnings.append(
                    f'Some unexpected columns were found: {sorted(extras)}'
                )

        return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')


class MandatoryValuesValidator(Validator):
    def validate(self):
        schema = self.kwargs.get("schema", {})
        component = self.kwargs.get("component", "")

        for key, field in schema.items():
            if field.get("mandatory","") == "M":               
                if key not in self.data.columns:
                    self.errors.append("Sheet " + component + " : Mandatory column: '" + key + "' is missing")
                    self.flag = False
                else:
                    null_rows=[]
                    null_rows.extend(self.data[self.data[key].isnull()].index.tolist())
                    null_rows.extend(self.data[self.data[key] == ""].index.tolist())
                    null_rows.extend(self.data[self.data[key].isna()].index.tolist())
                    for row in null_rows:
                        self.errors.append("Sheet %s : Missing data detected in column <strong>%s</strong> part at row <strong>%s</strong>." % (
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
                                self.errors.append( "Sheet <B>" + component + "</B> : Invalid value <B>" + row + "</B> in column : <B>" + field["term_label"] + "</B> at row " + str(i+2) + ". Valid values are: " + str(field.get("choice")))
                                self.flag = False
                        elif type == "string":
                            regex = field.get("term_regex","")
                            if regex and pd.notna(regex):
                                
                                if not re.match(regex.strip(), str(row)):
                                    self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + row + "</B> in column : <B>" + field["term_label"] + "</B> at row " + str(i+2) + ". " + field.get("term_error_message", "Valid value should match: " + str(regex)))
                                    self.flag = False
                        elif type == "ontology":   
                            reference = field.get("term_reference", "")
                            if reference:
                                if reference == "NCBITaxon":
                                    if not checkNCBITaxonTerm(row):
                                        self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + row + "</B> in column : <B>" + field["term_label"] + "</B> at row " + str(i+2) + ". invalid NCBITaxon term")
                                        self.flag = False
                                else:
                                    #it should be "ontology_id:ancestor, i.e. EFO:0004466"
                                    ontology_id = reference.split(":")[0]
                                    ancestor = reference.split(":")[1]
                                    if not checkOntologyTerm(ontology_id, ancestor, row):
                                        self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + row + "</B> in column : <B>" + field["term_label"] + "</B> at row " + str(i+2) + ". invalid ontology term for : " + str(reference))
                                        self.flag = False
                            else:
                                self.errors.append("Sheet <B>" + component + "</B> : Ontology term reference is missing for column : '" + field["term_label"] + "'")
                                self.flag = False

                        elif type == "BIOSAMPLEACCESSION_FIELD":
                            if row not in biosampleAccessionsMap.keys():
                                #check biosample from ena
                                try:
                                    response = session.get(f"{ena_sample_service}/{row}", data={})
                                    if response.status_code == requests.codes.ok:
                                        root = ET.fromstring(response.text)
                                        sample_name = root.find(".//SAMPLE_NAME")
                                        taxon_id = sample_name.find('TAXON_ID').text
                                        sample_accession = root.find(".//SAMPLE").attrib['accession']
                                        if taxon_id :
                                            read_taxon_id = self.data.iloc[i-2].get("taxon_id","")
                                            if taxon_id != read_taxon_id:
                                                self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + read_taxon_id + "</B> not match with <B>" + taxon_id + "</B> in column: <B>TAXON_ID</B> at row " + str(i+2)) 
                                                self.flag = False
                                            else:
                                                self.data[f"{Validator.PREFIX_4_NEW_FIELD}sraAccession"] = sample_accession
                                    else:
                                        self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + row + "</B> in column: <B>" + field["term_label"] + "</B> at row " + str(i+2)) 
                                        self.flag = False
                                except Exception as e:
                                    lg.exception(e)
                                    self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + row + "</B> in column: <B>" + field["term_label"] + "</B> at row " + str(i+2))
                                    self.flag = False

                            else:
                                for key, field in biosampleAccession_ext_field_map.items():
                                    value = self.data.loc[i].get(key,"")
                                    sample = biosampleAccessionsMap.get(row)
                                    if key in sample and sample[key] and sample[key] != value:
                                        self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + value + "</B> not match with <B>" + sample.get(key,"")+ "</B> in column: <B>" + key + "</B> at row " + str(i+2)) 
                                        self.flag = False
                                                                    
                if is_identifier:
                    df = self.data[column].groupby(self.data[column]).filter(lambda x: len(x) >1).value_counts()
                    for index, row in df.items():
                        self.errors.append("Sheet <B>" + component + "</B> : Invalid value <B>" + index + "</B> in column : <B>" + column + "</B> : duplicated " + ( "twice" if row == 2 else  str(row) ) +" times." )
                        self.flag = False                                         
            else:
                self.errors.append("Sheet <B>" +component + "</B> : Invalid column : '" + column +"'")
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
