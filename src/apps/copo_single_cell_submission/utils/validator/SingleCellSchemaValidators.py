from common.validators.validator import Validator
from django.conf import settings
import re

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
                            component, key, str(row + 1)))
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
                        if type == "enum":
                            if row not in field.get("choice", []):
                                self.errors.append( component + " : Invalid value '" + row + "' in column : '" + field["term_label"] + "' at row " + str(i) + ". Valid values are: " + str(field.get("choice")))
                                self.flag = False
                        elif type == "string":
                            regex = field.get("term_regex","")
                            if regex:
                                if not re.match(regex, str(row)):
                                    self.errors.append(component + " : Invalid value '" + row + "' in column : '" + field["term_label"] + "' at row " + str(i) + ". Valid value should match: " + str(regex))
                                    self.flag = False
                        #elif type == "TAXON_FIELD":   

                if is_identifier:
                    df = self.data[column].groupby(self.data[column]).filter(lambda x: len(x) >1).value_counts()
                    for index, row in df.items():
                        self.errors.append(component + " : Invalid value '" + index + "' in column : '" + column + "' is duplicated " + row +"times." )
                        self.flag = False                                         
            else:
                self.errors.append(component + " : Invalid column : '" + column +"'")
                self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")
