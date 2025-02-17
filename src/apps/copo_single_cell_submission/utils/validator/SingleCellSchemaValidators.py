from common.validators.validator import Validator
from django.conf import settings
import re
import pandas as pd
from ..copo_single_cell  import checkTopologyTerm, checkNCBITaxonTerm
lg = settings.LOGGER

class MandatoryValuesValidator(Validator):
    def validate(self):
        schema = self.kwargs.get("schema", {})
        component = self.kwargs.get("component", "")

        for key, field in schema.items():
            if field.get("mandatory","") == "M":               
                if key not in self.data.columns:
                    self.errors.append(component + " : Mandatory column: '" + key + "' is missing")
                    self.flag = False
                else:
                    null_rows=[]
                    null_rows.extend(self.data[self.data[key].isnull()].index.tolist())
                    null_rows.extend(self.data[self.data[key] == ""].index.tolist())
                    null_rows.extend(self.data[self.data[key].isna()].index.tolist())
                    for row in null_rows:
                        self.errors.append("%s : Missing data detected in column <strong>%s</strong> part at row <strong>%s</strong>." % (
                            component, field["term_label"], str(row + 1)))
                        self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class IncorrectValueValidator(Validator):
    def validate(self):
        schema_map = self.kwargs.get("schema", {})
        component = self.kwargs.get("component", "")

        for column in self.data.columns:
            if column in schema_map.keys():
                field = schema_map[column]
                type = field.get("term_type","")
                is_identifier = field.get("identifier", False)
                i = 1
                for row in self.data[column]:
                    i += 1
                    if row:
                        row = str(row).strip()
                        if type == "enum":
                            if row not in field.get("choice", []):
                                self.errors.append( component + " : Invalid value '" + row + "' in column : '" + field["term_label"] + "' at row " + str(i) + ". Valid values are: " + str(field.get("choice")))
                                self.flag = False
                        elif type == "string":
                            regex = field.get("term_regex","")
                            if regex and pd.notna(regex):
                                
                                if not re.match(regex.strip(), str(row)):
                                    self.errors.append(component + " : Invalid value '" + row + "' in column : '" + field["term_label"] + "' at row " + str(i) + ". Valid value should match: " + str(regex))
                                    self.flag = False
                        elif type == "ontology":   
                            reference = field.get("term_reference", "")
                            if reference:
                                if reference == "NCBITaxon":
                                    if not checkNCBITaxonTerm(row):
                                        self.errors.append(component + " : Invalid value '" + row + "' in column : '" + field["term_label"] + "' at row " + str(i) + ". invalid NCBITaxon term")
                                        self.flag = False
                                else:
                                    #it should be "ontology_id:ancestor, i.e. EFO:0004466"
                                    ontology_id = reference.split(":")[0]
                                    ancestor = reference.split(":")[1]
                                    if not checkTopologyTerm(ontology_id, ancestor, row):
                                        self.errors.append(component + " : Invalid value '" + row + "' in column : '" + field["term_label"] + "' at row " + str(i) + ". invalid ontology term for : " + str(reference))
                                        self.flag = False
                            else:
                                self.errors.append(component + " : Ontology term reference is missing for column : '" + field["term_label"] + "'")
                                self.flag = False
                if is_identifier:
                    df = self.data[column].groupby(self.data[column]).filter(lambda x: len(x) >1).value_counts()
                    for index, row in df.items():
                        self.errors.append(component + " : Invalid value '" + index + "' in column : '" + column + "' : duplicated " + ( "twice" if row == 2 else  str(row) ) +" times." )
                        self.flag = False                                         
            else:
                self.errors.append(component + " : Invalid column : '" + column +"'")
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
