from common.validators.validator import Validator
from common.dal.sample_da import Sample
from django.conf import settings
import re
import requests
from common.utils.helpers import get_env
import xml.etree.ElementTree as ET
from Bio import Entrez
from common.utils.helpers import notify_frontend
from common.validators.helpers import check_taxon_ena_submittable

#check mandatory fields are present in spreadsheet
#check mandatory fields are not empty
#check values are valid: enum, regex
ena_sample_service = get_env("ENA_V1_SAMPLE_SERVICE")
pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]
session = requests.Session()
session.auth = (user_token, pass_word) 

lg = settings.LOGGER



class MandatoryValuesValidator(Validator):
    def validate(self):
        checklist = self.kwargs.get("checklist", {})
        for key, field in checklist["fields"].items():
            if field.get("mandatory","") == "mandatory":               
                if key not in self.data.columns:
                    self.errors.append("Mandatory column: '" + field["name"] + "' is missing")
                    self.flag = False
                else:
                    null_rows=[]
                    null_rows.extend(self.data[self.data[key].isnull()].index.tolist())
                    null_rows.extend(self.data[self.data[key] == ""].index.tolist())
                    null_rows.extend(self.data[self.data[key].isna()].index.tolist())
                    for row in null_rows:
                        self.errors.append("Missing data detected in column <strong>%s</strong> part at row <strong>%s</strong>." % (
                            field["name"], str(row + 1)))
                        self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class IncorrectValueValidator(Validator):
    def validate(self):
        checklist = self.kwargs.get("checklist", {})

        biosampleAccessions = Sample(profile_id=self.profile_id).get_all_records_columns(filter_by= {"biosampleAccession": {"$exists":True, "$ne": ""}}, projection={"biosampleAccession":1, "SPECIMEN_ID":1, "TAXON_ID":1})
        biosampleAccessionsMap = {}
        if biosampleAccessions:
            biosampleAccessionsMap = {row["biosampleAccession"]: row for row in biosampleAccessions} 

        #get all BIOSAMPLEACCESSION_EXT_FIELD from the checklist fields
        biosampleAccession_ext_field_map = {key: field for key, field in checklist["fields"].items() if field.get("type") == "BIOSAMPLEACCESSION_EXT_FIELD"}

        for column in self.data.columns:
            if column in checklist["fields"].keys():
                field = checklist["fields"][column]
                type = field.get("type","")
                i = 1
                for row in self.data[column]:
                    i += 1
                    if row:
                        if type == "TEXT_CHOICE_FIELD":
                            if row not in field.get("choice"):
                                self.errors.append("Invalid value '" + row + "' in column : '" + field["name"] + "' at row " + str(i) + ". Valid values are: " + str(field.get("choice")))
                                self.flag = False
                        elif type == "TEXT_FIELD":
                            regex = field.get("regex","")
                            if regex:
                                if not re.match(regex, row):
                                    '''
                                    if column == 'collection date':
                                        # Remove the time part from the date string if it is present
                                        try:
                                            result = bool(datetime.strptime(row, "%Y-%m-%d %H:%M:%S"))
                                        except ValueError:
                                               result = False

                                        if result:
                                            row = row.split(' ')[0]
                                            self.data.at[i-2, column] = row
                                        else:
                                            self.errors.append("Invalid value '" + row + "' in column : '" + field["name"] + "' at row " + str(i))
                                            self.flag = False        
                                    else:
                                    '''
                                    self.errors.append("Invalid value '" + row + "' in column : '" + field["name"] + "' at row " + str(i) + ". Valid value should match: " + str(regex))
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
                                            read_taxon_id = self.data.iloc[i-2].get("TAXON_ID","")
                                            if taxon_id != read_taxon_id:
                                                self.errors.append("Invalid value " + read_taxon_id + " not match with " + taxon_id + " in column: 'TAXON_ID' at row " + str(i)) 
                                                self.flag = False
                                            else:
                                                self.data[f"{Validator.PREFIX_4_NEW_FIELD}sraAccession"] = sample_accession
                                    else:
                                        self.errors.append("Invalid value " + row + " in column:'" + field["name"] + "'")
                                        self.flag = False
                                except Exception as e:
                                    lg.exception(e)
                                    self.errors.append("Invalid value " + row + " in column:'" + field["name"] + "'")
                                    self.flag = False

                            else:
                                for key, field in biosampleAccession_ext_field_map.items():
                                    value = self.data.iloc[i-2].get(key,"")
                                    #specimen_id = self.data.iloc[i-2].get("SPECIMEN_ID","")
                                    #taxon_id = self.data.iloc[i-2].get("TAXON_ID","")
                                    sample = biosampleAccessionsMap.get(row)
                                    if key in sample and sample[key] != value:
                                        self.errors.append("Invalid value " + value + " not match with " + sample.get(key,"")+ " in column: '" + key + "' at row " + str(i)) 
                                        self.flag = False
                                    
            else:
                self.errors.append("Invalid column : '" + column +"'")
                self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")
    

class TaxonValidator(Validator):
    def validate(self):
        if "organism" in self.data.columns:
            Entrez.api_key = get_env("NIH_API_KEY")
            # build dictioanry of species in this manifest  max 200 IDs per query
            taxon_id_set = set([x for x in self.data['organism'].tolist() if x])
            notify_frontend(data={"profile_id": self.profile_id},
                            msg="Querying NCBI for TAXON_IDs in manifest",
                            action="info",
                            html_id="sample_info")
            taxon_id_list = list(taxon_id_set)
            if any(x for x in taxon_id_list):
                for taxon in taxon_id_list:
                    # check if taxon is submittable
                    ena_taxon_errors = check_taxon_ena_submittable(taxon, by="binomial")
                    if ena_taxon_errors:
                        self.errors += ena_taxon_errors
                        self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")