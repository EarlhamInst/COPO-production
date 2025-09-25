import os
import pandas as pd
from bson import ObjectId
from datetime import datetime
from common.dal.mongo_util import cursor_to_list
from collections import defaultdict
from . import generic_helper as ghlper
from common.dal.profile_da import Profile
from common.utils import helpers
from lxml import etree
from common.schemas.utils.data_utils import simple_utc
from common.utils.helpers import notify_submission_status, get_datetime, get_env, json_to_pytype
from common.lookup.lookup import SRA_SAMPLE_TEMPLATE,SRA_PROJECT_TEMPLATE,SRA_SETTINGS, SRA_RUN_TEMPLATE,SRA_EXPERIMENT_TEMPLATE, SRA_SUBMISSION_MODIFY_TEMPLATE, SRA_SUBMISSION_TEMPLATE
import subprocess
import tempfile
from common.dal.submission_da import Submission
from common.dal.copo_da import EnaChecklist, EnaFileTransfer, DataFile
from common.dal.sample_da import Sample
from src.apps.copo_single_cell_submission.utils.da import Singlecell 
from common.utils.copo_lookup_service import COPOLookup
import requests
from common.utils.logger import Logger
lg = Logger()
from src.apps.copo_assembly_submission.utils.da import Assembly
import re
import glob
import xml.etree.ElementTree as ET
import json


ena_service = get_env('ENA_SERVICE')
pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]
webin_user = get_env('WEBIN_USER')
webin_domain = get_env('WEBIN_USER').split("@")[1]
ena_v2_service_async = get_env("ENA_V2_SERVICE_ASYNC")

class EnaSubmissionHelper:
    def __init__(self, submission_id=str(), profile_id=str()):
        self.submission_id = submission_id
        self.profile_id = profile_id
        self.profile = Profile().get_record(self.profile_id)
        self.sra_settings = json_to_pytype(SRA_SETTINGS).get("properties", dict())

    def logging_debug(self, message=str()):
        ghlper.logging_debug(message, self.submission_id)



    def logging_info(self, message=str()):
        """
        function logs info messages
        :param message:
        :return:
        """
        ghlper.logging_info(message, self.submission_id)
        notify_submission_status(data={"profile_id": self.profile_id}, msg=message, action="info", html_id="submission_info")

    def logging_error(self, message=str()):
        """
        function logs error messages
        :param message:
        :return:
        """
        ghlper.logging_error(message, self.submission_id)
        notify_submission_status(data={"profile_id": self.profile_id},
                            msg=message, action="error", html_id="submission_info")

    def get_submission_xml(self, output_location=str()):
        """
        function creates and return submission xml path
        :return:
        """

        # create submission xml
        self.logging_info("Creating submission xml..." )

        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(SRA_SUBMISSION_TEMPLATE, parser).getroot()

        # set submission attributes
        root.set("broker_name", self.sra_settings["sra_broker"])
        root.set("center_name", self.sra_settings["sra_center"])
        root.set("submission_date", datetime.utcnow().replace(tzinfo=simple_utc()).isoformat())

        # set SRA contacts
        contacts = root.find('CONTACTS')

        # set copo sra contacts
        copo_contact = etree.SubElement(contacts, 'CONTACT')
        copo_contact.set("name", self.sra_settings["sra_broker_contact_name"])
        copo_contact.set("inform_on_error", self.sra_settings["sra_broker_inform_on_error"])
        copo_contact.set("inform_on_status", self.sra_settings["sra_broker_inform_on_status"])

        # set user contacts
        sra_map = {"inform_on_error": "SRA Inform On Error", "inform_on_status": "SRA Inform On Status"}
        user_contacts = self.get_sra_contacts()
        for k, v in user_contacts.items():
            user_sra_roles = [x for x in sra_map.keys() if sra_map[x].lower() in v]
            if user_sra_roles:
                user_contact = etree.SubElement(contacts, 'CONTACT')
                user_contact.set("name", ' '.join(k[1:]))
                for role in user_sra_roles:
                    user_contact.set(role, k[0])

        # todo: add study publications

        # set release action

        return self.write_xml_file(output_location, xml_object=root, file_name="submission.xml")

    def get_edit_submission_xml(self, output_location, submission_xml_path=str()):
        """
        function creates and return submission xml path
        :return:
        """
        # create submission xml
        self.logging_info("Creating submission xml for edit....")

        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(submission_xml_path, parser).getroot()
        actions = root.find('ACTIONS')
        action = actions.find('ACTION')
        add = action.find("ADD")
        if add != None:
            action.remove(add)
        modify = etree.SubElement(action, 'MODIFY')

        return self.write_xml_file(output_location, xml_object=root, file_name="submission_edit.xml")

    def write_xml_file(self, output_location=str(), xml_object=None, file_name=str()):
            """
            function writes xml to the specified location or to a default one
            :param location:
            :param xml_object:
            :param file_name:
            :return:
            """
            output_location_submission_id = os.path.join(output_location, self.submission_id)
            if not os.path.exists(output_location_submission_id):
                os.makedirs(output_location_submission_id)

            result = dict(status=True, value='')


            xml_file_path = os.path.join(output_location_submission_id, file_name)
            tree = etree.ElementTree(xml_object)

            try:
                tree.write(xml_file_path, encoding="utf8", xml_declaration=True, pretty_print=True)
            except Exception as e:
                ghlper.logging_exception(e)
                message = 'Error writing xml file ' + file_name + ": " + str(e)
                self.logging_error(message)
                result['message'] = message
                result['status'] = False
                raise e

            message = file_name + ' successfully written to  ' + xml_file_path
            self.logging_info(message)

            result['value'] = xml_file_path

            return result

    def get_sra_contacts(self):
        """
        function returns users with any SRA roles
        :return:
        """

        sra_contacts = defaultdict(list)
        expected_roles = [x.lower() for x in ['SRA Inform On Status', 'SRA Inform On Error']]

        records = ghlper.get_person_handle().find({"profile_id": self.profile_id})

        for rec in records:
            roles = [role.get("annotationValue", str()).lower() for role in rec.get('roles', []) if
                        role.get("annotationValue", str()).lower() in expected_roles]
            if roles:
                email = rec.get('email', str())
                firstName = rec.get('firstName', str())
                lastName = rec.get('lastName', str())
                sra_contacts[(email, firstName, lastName)].extend(roles)

        return sra_contacts

    def process_sample(self, output_location, root, submission_xml_path, sra_df, is_new=True):
        dt = get_datetime()

        result = self.write_xml_file(output_location=output_location, xml_object=root, file_name="sample.xml")
        if result['status'] is False:
            return result

        sample_xml_path = result['value']

        result = dict(status=True, value='')

        # register samples to the ENA service
        curl_cmd = 'curl -u "' + user_token + ':' + pass_word \
                    + '" -F "SUBMISSION=@' \
                    + submission_xml_path \
                    + '" -F "SAMPLE=@' \
                    + sample_xml_path \
                    + '" "' + ena_service \
                    + '"'
        self.logging_debug(
            "CURL command to submit samples xml to ENA: " + curl_cmd.replace(pass_word, "xxxxxx"))

        try:
            receipt = subprocess.check_output(curl_cmd, shell=True)
        except Exception as e:
            ghlper.logging_exception(e)
            message = 'API call error ' + str(e).replace(pass_word, "xxxxxx"),
            self.logging_debug(message, self.submission_id)
            result['message'] = message
            result['status'] = False
            raise e

        root = etree.fromstring(receipt)

        if root.get('success') == 'false':
            result['status'] = False
            result['message'] = "Couldn't register SAMPLES due to the following errors: "
            errors = root.findall('.//ERROR')
            if errors:
                error_text = str()
                for e in errors:
                    error_text = error_text + " \n" + e.text

                result['message'] = result['message'] + error_text

            # log error
            self.logging_debug("Error in submitting samples to ENA via CURL: " + str(result['message']))
            return result

        # save sample accession for new sample only
        self.write_xml_file(output_location=output_location, xml_object=root, file_name="samples_receipt.xml")
        #ghlper.logging_info("Saving samples accessions to the database", submission_id)
        sample_accessions = list()
        sample_ids = list()
        for accession in root.findall('SAMPLE'):
            biosample = accession.find('EXT_ID')
            sample_alias = accession.get('alias', default=str())
            sample_id = sra_df.loc[sample_alias]['sample_id']
            sample_ids.append(sample_id)
            sample_accessions.append(
                dict(
                    sample_accession=accession.get('accession', default=str()),
                    sample_alias=sample_alias,
                    biosample_accession=biosample.get('accession', default=str()),
                    sample_id=sample_id
                )
            )
            
        if is_new:
            submission_handler = Submission().get_collection_handle()
            submission_record = submission_handler.find_one({"_id": ObjectId(self.submission_id)}, {"accessions": 1})

            if submission_record:
                accessions = submission_record.get("accessions", dict())
                previous = accessions.get('sample', list())
                previous.extend(sample_accessions)
                accessions['sample'] = previous
                submission_record['accessions'] = accessions
                submission_record['date_modified'] = dt

                submission_handler.update_one(
                    {"_id": ObjectId(str(submission_record.pop('_id')))},
                    {'$set': submission_record})

                # update submission status
                status_message = "Samples successfully registered, accessions saved."
                self.logging_info(status_message)
    
            # update sample status
            Sample(profile_id=self.profile_id).update_accession(sample_accessions)
        else:
            status_message = "Samples successfully updated to ENA."
            self.logging_info(status_message)
            # update sample status
            Sample(profile_id=self.profile_id).update_field(field="status", value="accepted", oids=sample_ids)

        return dict(status=True, value='')

    def register_samples(self, submission_xml_path=str(), modify_submission_xml_path=str(), samples=list()):    
        """
        function creates and submits sample xml
        :return:
        """
        output_location = tempfile.gettempdir() 

        result = dict(status=True, value='')
        dt = get_datetime()

        # create sample xml
        message = "Registering samples..."
        self.logging_info(message)
        #ghlper.logging_info(message, str(submission_id))
        #notify_submission_status(data={"profile_id": profile_id}, msg=message, action="info", html_id="sample_info")


        parser = etree.XMLParser(remove_blank_text=True)

        # root element is  SAMPLE_SET
        root = None
        root_add = etree.parse(SRA_SAMPLE_TEMPLATE, parser).getroot()
        root_modify = etree.parse(SRA_SAMPLE_TEMPLATE, parser).getroot()

        if not samples:
            return dict(status=True, value='')
        
        # modify samples

        is_modifed_sample = False
        is_new_sample = False

        # add samples
        sra_samples = list()
        for sample in samples:
            sample_alias = str(self.submission_id) + ":sample:" + sample["name"]
            root = root_add
            if sample.get('biosampleAccession',''):
                is_modifed_sample = True
                root = root_modify
            else:
                is_new_sample = True
            sra_samples.append(dict(sample_id=str(sample['_id']), sample_alias=sample_alias))
            sample_node = etree.SubElement(root, 'SAMPLE')
            sample_node.set("alias", sample_alias)
            sample_node.set("center_name", self.sra_settings["sra_center"])
            sample_node.set("broker_name", self.sra_settings["sra_broker"])

            etree.SubElement(sample_node, 'TITLE').text = sample_alias
            sample_name_node = etree.SubElement(sample_node, 'SAMPLE_NAME')
            etree.SubElement(sample_name_node, 'TAXON_ID').text = sample.get("taxon_id", str())
            etree.SubElement(sample_name_node, 'SCIENTIFIC_NAME').text = sample.get("scientific_name", str())

            # add sample attributes
            sample_attributes_node = etree.SubElement(sample_node, 'SAMPLE_ATTRIBUTES')

            checklist_id = sample.get("checklist_id",str())
            if checklist_id is not None:
                sample_attribute_node = etree.SubElement(sample_attributes_node, 'SAMPLE_ATTRIBUTE')
                checklist = EnaChecklist().get_collection_handle().find_one({"primary_id": checklist_id})
                if checklist:
                    etree.SubElement(sample_attribute_node, 'TAG').text = "ENA-CHECKLIST"
                    etree.SubElement(sample_attribute_node, 'VALUE').text = checklist.get("ena_checklist_id", checklist_id)
                    fields = checklist["fields"]
                    key_mapping = { key :  value["name"] for key, value in fields.items() }
                    unit_mapping = { key :  value["unit"] if "unit" in value.keys() else "" for key, value in fields.items() }
                    for key in sample.keys():
                        if key in key_mapping and sample[key]:
                            sample_attribute_node = etree.SubElement(sample_attributes_node, 'SAMPLE_ATTRIBUTE')
                            etree.SubElement(sample_attribute_node, 'TAG').text = key_mapping[key]
                            etree.SubElement(sample_attribute_node, 'VALUE').text = sample[key]
                            if unit_mapping.get(key, str()):
                                etree.SubElement(sample_attribute_node, 'UNITS').text = unit_mapping[key]

            # add sample collection date & collection location TODO


        if not sra_samples:  # no samples to submit
            log_message = "No new samples to register!"
            self.logging_info(log_message)
            #ghlper.logging_info(log_message, submission_id)
            #notify_read_status(data={"profile_id": profile_id},
            #                    msg=log_message, action="info", html_id="sample_info")
            return dict(status=True, value='')

        sra_df = pd.DataFrame(sra_samples)
        sra_df.index = sra_df['sample_alias']

        # do it for modify
        if is_modifed_sample:
            result = self.process_sample(output_location, root_modify, modify_submission_xml_path, sra_df, is_new=False)

        if result['status']:
            # do it for add
            if is_new_sample:
                result = self.process_sample(output_location, root_add, submission_xml_path, sra_df, is_new=True)

        Submission().remove_component_from_submission(sub_id=self.submission_id, component="sample", component_ids=[str(sample["_id"]) for sample in samples] )

        if result['status'] is False:
            message = "Samples not registered."

            Sample(profile_id=self.profile_id).update_field(oids=[sample["_id"] for sample in samples], field_values={"error": result['message'], "status": "rejected"})
            
            self.logging_error(message)
        else:        
            message = "Samples registered successfully."
            
            self.logging_info(message)
        return result

    def register_project(self, submission_xml_path=str(), modify_submission_xml_path=str(), study=None, singlecell_id=None):
        """
        function creates and submits project (study) xml
        :return:
        """

        # create project xml
        log_message = "Registering project..."
        #ghlper.logging_info(log_message, self.submission_id)
        #ghlper.update_submission_status(status='info', message=log_message, submission_id=self.submission_id)
        self.logging_info(log_message)



        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(SRA_PROJECT_TEMPLATE, parser).getroot()

        # set SRA contacts
        project = root.find('PROJECT')

        # set project descriptors
        #study = singlecell.get("components", {}).get("study", [{}])[0]
        project.set("alias", self.submission_id+":"+ study["study_id"])
        project.set("center_name", self.sra_settings["sra_center"])

        project_title = study.get("title", str())
        project_description = study.get("description", str())

        if project_title:
            etree.SubElement(project, 'NAME').text = project_title 
            etree.SubElement(project, 'TITLE').text = project_title
        if project_description:
            etree.SubElement(project, 'DESCRIPTION').text = project_description

        # set project type - sequencing project
        submission_project = etree.SubElement(project, 'SUBMISSION_PROJECT')
        sequencing_project = etree.SubElement(submission_project, 'SEQUENCING_PROJECT')

        locus_tags = study.get("ena_locus_tags","")  #TBC add locus tags to study
        if locus_tags:
            for tag in locus_tags.split(","):
                etree.SubElement(sequencing_project, 'LOCUS_TAG_PREFIX').text = tag.strip()

        output_location = tempfile.gettempdir()

        # do it for modify
        if study.get("accession_ena"):
            result = self.process_project(output_location, root, modify_submission_xml_path, study, singlecell_id)
        else:
            result = self.process_project(output_location, root, submission_xml_path, study, singlecell_id)

        #Submission().remove_component_from_submission(sub_id=str(self.submission_id), component="study", component_ids=[singlecell["study_id"]])

        if result['status'] is False:
            message = result.get('message', "Study not registered.")
            self.logging_error(message)
        else:        
            message = f"Study {study['study_id']} has been registered successfully to ENA."
            self.logging_info(message)

        #Submission().add_component_submission_accession(sub_id=str(self.submission_id), component="study", accessions=new_accessions)

        return result

    def process_project(self, output_location, root, submission_xml_path, study, singlecell_id):
        """
        function processes study xml
        :param output_location:
        :param root:
        :param submission_xml_path:
        :param sra_df:
        :param is_new:
        :return:
        """
        dt = get_datetime()

        result = self.write_xml_file(output_location=output_location, xml_object=root, file_name="study.xml")
        if result['status'] is False:
            return result

        study_xml_path = result['value']

        result = dict(status=True, value='')

        # register study to the ENA service
        curl_cmd = 'curl -u "' + user_token + ':' + pass_word \
                    + '" -F "SUBMISSION=@' \
                    + submission_xml_path \
                    + '" -F "PROJECT=@' \
                    + study_xml_path \
                    + '" "' + ena_service \
                    + '"'
        self.logging_debug(
            "CURL command to submit study xml to ENA: " + curl_cmd.replace(pass_word, "xxxxxx"))

        try:
            receipt = subprocess.check_output(curl_cmd, shell=True)
        except Exception as e:
            ghlper.logging_exception(e)
            message = 'API call error ' + str(e).replace(pass_word, "xxxxxx")
            self.logging_debug(message)
            self.logging_error('API call error, please try it later')
            raise e

        root = etree.fromstring(receipt)

        if root.get('success') == 'false':
            result['status'] = False
            result['message'] = "Couldn't register STUDY due to the following errors: "
            errors = root.findall('.//ERROR')
            if errors:
                error_text = str()
                for e in errors:
                    error_text = error_text + " \n" + e.text

                result['message'] = result['message'] + error_text

            # log error
            Singlecell().update_component_status(id=singlecell_id, component="study", identifier="study_id", identifier_value=study["study_id"], repository="ena", status_column_value={"status": "rejected",  "error": result['message'] })  
            self.logging_debug("Error in submitting study to ENA via CURL: " + str(result['message']))
            return result


         # save project accession
        self.write_xml_file(output_location=output_location, xml_object=root, file_name="project_receipt.xml")
        self.logging_info("Saving project accessions to the database")
        project_accessions = dict()
        for accession_sec in root.findall('PROJECT'):
            accession=accession_sec.get('accession', default=str())
            alias=accession_sec.get('alias', default=str())
            status=accession_sec.get('status', default=str())
            release_date=accession_sec.get('holdUntilDate', default=str())
            study_id=accession_sec.get('alias', default=str()).split(":")[-1]  # assuming study_id is the last part of the alias
            project_accessions[study_id]= dict(
                    accession=accession,
                    alias=alias,
                    status=status,
                    release_date=release_date,
                    study_id=study_id  # assuming study_id is the last part of the alias
                )
            
            Singlecell().update_component_status(id=singlecell_id, component="study", identifier="study_id", identifier_value=study["study_id"], repository="ena", status_column_value={"status": "accepted", "state": status,  "accession": accession, "release_date": release_date, "error": ""})  

        submission_record = Submission().get_collection_handle().find_one({"_id": ObjectId(self.submission_id)}, {"accessions.project": 1})

        if submission_record:
            updated_project_accessions = submission_record.get("accessions",{}).get('project',[])
            study_found = False
            for accession in updated_project_accessions:
                if accession.get('study_id',"") == study["study_id"]:
                    accession.update(project_accessions[study["study_id"]])
                    study_found = True
                    break
            if not study_found:
                updated_project_accessions.append(project_accessions[study["study_id"]])
            Submission().get_collection_handle().update_one(
                {"_id": ObjectId(str(submission_record.pop('_id')))},
                {'$set': {"accessions.project": updated_project_accessions, "date_modified": dt}})

            # update submission status
            status_message = "Project successfully registered, and accessions saved."
            self.logging_info (status_message)
        return dict(status=True, value='')


    def register_files(self, submission_xml_path=str(), modify_submission_xml_path=str(), component_df=pd.DataFrame(), identifier_map={}, singlecell=None, file_component_name="file"):   
        """
        function creates and submits datafile xml
        :return:
        """
        instruments = COPOLookup(data_source='sequencing_instrument').broker_data_source()
        output_location = tempfile.gettempdir()
        non_attribute_names =[]

        study = singlecell.get("components", {}).get("study", [{}])[0]
        project_accession = study.get("accession_ena", str())

        samples = singlecell.get("components", {}).get("sample", [])
        sample_accession_map = {sample[identifier_map["sample"]]: sample['biosampleAccession'] for sample in samples  }
        errors = []
        file_identifier = identifier_map[file_component_name]

        enafiles = EnaFileTransfer().get_all_records_columns(filter_by={"profile_id":self.profile_id}, projection={"local_path":1,  "remote_path":1})
        enafile_map = {enafile["local_path"].split("/")[-1] : enafile.get("remote_path") for enafile in enafiles if enafile.get("remote_path","")}

        for index, row in component_df.iterrows():

            # create datafile xml
            log_message = f"Registering {row[file_identifier]} ..."
            sample_id = row.get("sample_id", str())
            if not sample_id or sample_id not in sample_accession_map:
                log_message = f"Skipping {row[file_identifier]} as no sample associated."
                self.logging_info(log_message)
                continue

            self.logging_info(log_message)
            #ghlper.logging_info(log_message, self.submission_id)
            #notify_submission_status(data={"profile_id": self.profile_id}, msg=log_message, action="info", html_id="datafile_info")

            parser = etree.XMLParser(remove_blank_text=True)
            experiment_root = etree.parse(SRA_EXPERIMENT_TEMPLATE, parser).getroot()
            run_root = etree.parse(SRA_RUN_TEMPLATE, parser).getroot()

            column_name = "study_id"
            non_attribute_names.append(column_name)
            non_attribute_names.append(file_identifier)
            submission_name = str(self.submission_id) + "_" + row[column_name] +  "_"+ row[file_identifier]
            # add experiment node to experiment set
            experiment_node = etree.SubElement(experiment_root, 'EXPERIMENT')
            experiment_alias = "copo-reads-" + submission_name
            experiment_node.set("alias", experiment_alias)
            experiment_node.set("center_name", self.sra_settings["sra_center"])

            etree.SubElement(experiment_node, 'TITLE').text = submission_name
            etree.SubElement(experiment_node, 'STUDY_REF').set("accession", project_accession)

            # design
            experiment_design_node = etree.SubElement(experiment_node, 'DESIGN')
            column_name = "design_description"
            non_attribute_names.append(column_name)
            etree.SubElement(experiment_design_node, 'DESIGN_DESCRIPTION').text = row.get(column_name, str())
            etree.SubElement(experiment_design_node, 'SAMPLE_DESCRIPTOR').set("accession", sample_accession_map[row["sample_id"]])

            # descriptor
            experiment_library_descriptor_node = etree.SubElement(experiment_design_node, 'LIBRARY_DESCRIPTOR')

            for name in ["library_name", "library_strategy", "library_source", "library_selection"]:
                non_attribute_names.append(name)
                etree.SubElement(experiment_library_descriptor_node, name.upper()).text = row.get(name, str())

            column_name = "library_layout"
            non_attribute_names.append(column_name)
            experiment_library_layout_node = etree.SubElement(experiment_library_descriptor_node, column_name.upper()) 
            etree.SubElement(experiment_library_layout_node, row.get("library_layout", str()).upper())

            # platform
            column_name = "instrument_model"
            non_attribute_names.append(column_name)
            sequencing_instrument = row.get(column_name, str())
            inst_plat = [inst['platform'] for inst in instruments if inst['value'] == sequencing_instrument]

            if len(inst_plat):
                experiment_platform_node = etree.SubElement(experiment_node, 'PLATFORM')
                experiment_platform_type_node = etree.SubElement(experiment_platform_node, inst_plat[0])
                etree.SubElement(experiment_platform_type_node, column_name.upper()).text = sequencing_instrument

            experiment_attributes = etree.SubElement(experiment_node, 'EXPERIMENT_ATTRIBUTES')

            for key, value in row.items():
                if key not in non_attribute_names and value:
                    experiment_attribute = etree.SubElement(experiment_attributes, 'EXPERIMENT_ATTRIBUTE')
                    etree.SubElement(experiment_attribute, 'TAG').text = key
                    etree.SubElement(experiment_attribute, 'VALUE').text = str(value)

            submission_location = os.path.join(output_location, self.profile_id,row["study_id"],row[file_identifier])
            os.makedirs(submission_location, exist_ok=True)

            # write experiement xml
            result = self.write_xml_file(output_location=submission_location, xml_object=experiment_root,
                                            file_name="experiment.xml")
            

            if result['status'] is False:
                errors.append(result['message'])
                continue

            experiement_xml_path = result['value']

            # add run node to run set
            run_node = etree.SubElement(run_root, 'RUN')
            run_node.set("alias", experiment_alias)
            run_node.set("center_name", self.sra_settings["sra_center"])
            etree.SubElement(run_node, 'TITLE').text = submission_name
            etree.SubElement(run_node, 'EXPERIMENT_REF').set("refname", experiment_alias)

            run_data_block_node = etree.SubElement(run_node, 'DATA_BLOCK')
            run_files_node = etree.SubElement(run_data_block_node, 'FILES')

            for name in ["read_1_file", "read_2_file"]:
                if row[name] is None or row[name] == "":
                    continue

                run_file_node = etree.SubElement(run_files_node, 'FILE')
                run_file_node.set("filename", os.path.join(enafile_map[row[name]], row[name])) #TBC for remote_location
                
                _, file_extension = os.path.splitext(row[name])
                if file_extension in [".cram", ".bam"]:
                    run_file_node.set("filetype", file_extension[1:]) 
                else :
                    run_file_node.set("filetype", "fastq")  # todo: what about BAM, CRAM files?
                run_file_node.set("checksum", row[name+"_checksum"])  # todo: is this correct as submission time?
                run_file_node.set("checksum_method", "MD5")

            # write run xml
            result = self.write_xml_file(output_location=submission_location, xml_object=run_root,
                                         file_name="run.xml")

            if result['status'] is False:
                errors.append(result['message'])
                continue

            run_xml_path = result['value']

            is_new = True
            if row["accession_ena"]:
                is_new = False
                final_submission_xml_path = modify_submission_xml_path
            else:
                final_submission_xml_path = submission_xml_path

            curl_cmd = 'curl -u "' + user_token + ':' + pass_word \
                       + '" -F "SUBMISSION=@' \
                       + final_submission_xml_path \
                       + '" -F "EXPERIMENT=@' \
                       + experiement_xml_path \
                       + '" -F "RUN=@' \
                       + run_xml_path \
                       + '" "' + ena_service \
                       + '"'

            self.logging_debug(
                "Submitting EXPERIMENT and RUN XMLs for " +  
                    row[file_identifier] + " using CURL. CURL command is: " + curl_cmd.replace(pass_word, "xxxxx"))

            try:
                receipt = subprocess.check_output(curl_cmd, shell=True)
            except Exception as e:
                ghlper.logging_exception(e)
                message = 'API call error ' + str(e).replace(self.pass_word, "xxxxxx"),
                self.logging_error(message, self.submission_id)
                errors.append(message)
                raise e

            receipt_root = etree.fromstring(receipt)

            if receipt_root.get('success') == 'false':
                result['status'] = False
                result['message'] = "Submission error for datafiles: " + row[file_identifier] + " due to the following errors: "
                receipt_errors = receipt_root.findall('.//ERROR')
                if receipt_errors:
                    error_text = str()
                    for e in receipt_errors:
                        error_text = error_text + " \n" + e.text

                    result['message'] = result['message'] + error_text

                # log error

                errors.append(result['message'])
                Singlecell().update_component_status(id=singlecell["_id"], component=file_component_name, identifier=file_identifier, identifier_value=row[file_identifier], repository="ena", status_column_value={"status": "rejected", "error": ", ".join(errors)})
                continue

            # retrieve and save accessions
            self.write_xml_file(output_location=submission_location, xml_object=receipt_root, file_name="receipt.xml")
            self.logging_info("Saving EXPERIMENT and RUN accessions to the database")
            run_dict = dict(
                accession=receipt_root.find('RUN').get('accession', default=str()),
                alias=receipt_root.find('RUN').get('alias', default=str()),
                project_accession=project_accession,
                sample_accession=sample_accession_map[row["sample_id"]],
            )

            experiment_dict = dict(
                accession=receipt_root.find('EXPERIMENT').get('accession', default=str()),
                alias=receipt_root.find('EXPERIMENT').get('alias', default=str()),
                project_accession=project_accession,
                sample_accession = sample_accession_map[row["sample_id"]],
            )

            if is_new:
                submission_record = Submission().get_collection_handle().update_one({"_id": ObjectId(self.submission_id)},
                                                            {"$addToSet": {"accessions.run": run_dict, "accessions.experiment": experiment_dict}})
                
            Singlecell().update_component_status(id=singlecell["_id"], component=file_component_name, identifier=file_identifier, identifier_value=row[file_identifier], repository="ena", 
                                                 status_column_value={"status": "accepted", "accession": run_dict["accession"], 'run_accession':run_dict["accession"],
                                                                      'experiment_accession': experiment_dict["accession"], "error": ""})

        if errors:
            message = "Datafiles not registered due to the following errors: " + ", ".join(errors)
            self.logging_error(message)
            result['status'] = False
            result['message'] = message
        else:
            message = "Datafiles registered successfully."
            self.logging_info(message)
            result['status'] = True
            result['message'] = message

        return result


    def release_study(self,  singlecell=None):
        result = dict(status="error", message='')
        study = singlecell.get("components", {}).get("study", [{}])[0]
        study_accession = study.get('accession_ena', str())
        if not study_accession:
        # get study accession
            message = f'Study accession not found for study: {study["study_id"]}!'
            self.logging_error(message)
            result['message'] = message
            return result
        
 
        # get study status from API
        project_status = ghlper.get_study_status(user_token=user_token, pass_word=pass_word,
                                        project_accession=study_accession)

        if not project_status:
            message = f'Cannot determine project release status for study: {study["study_id"]}!'
            result['message'] = message
            #self.logging_error(message)
            return result

        release_status = project_status[0].get(
            'report', dict()).get('releaseStatus', str())

        if release_status.upper() == 'PUBLIC':
            # study already released, update the information in the db

            first_public = project_status[0].get(
                'report', dict()).get('firstPublic', str())

            try:
                first_public = datetime.strptime(first_public, "%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                first_public = get_datetime()

            accession = {"state": "PUBLIC", "release_date": first_public}
            Singlecell().update_component_status(
                id=singlecell["_id"], component="study", identifier="study_id", identifier_value=study["study_id"],
                repository="ena", status_column_value=accession)

            message = f'Study {study["study_id"]} is already released. No action taken.'
            self.logging_info(message)
            result['message'] = message
            return result

        # release study
        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(SRA_SUBMISSION_MODIFY_TEMPLATE, parser).getroot()
        actions = root.find('ACTIONS')
        action = etree.SubElement(actions, 'ACTION')
        self.logging_info('Releasing project with study: ' + study["study_id"])

        action_type = etree.SubElement(action, 'RELEASE')
        action_type.set("target", study_accession)

        xml_str = etree.tostring(root, encoding='utf8', method='xml')

        files = {'SUBMISSION': xml_str}
        receipt = None

        message = None
        with requests.Session() as session:
            session.auth = (user_token, pass_word)
            try:
                response = session.post(ena_service, data={}, files=files)
                receipt = response.text
                lg.log("ENA RECEIPT " + receipt)
            except etree.ParseError as e:
                lg.log("Unrecognised response from ENA " + str(e))
                message = " Unrecognised response from ENA - " + str(
                    receipt) + " Please try again later, if it persists contact admins"
                

            except Exception as e:
                lg.exception(e)
                message = 'API call error ' + \
                    "Submitting project xml to ENA via CURL. href is: " + ena_service

        if message:
            self.logging_info(message)
            result['message'] = message
            return result

        if receipt:
            root = etree.fromstring(bytes(receipt, 'utf-8'))

            if root.get('success') == 'false':
                message = "Couldn't release project due to the following errors: "
                errors = root.findall('.//ERROR')
                if errors:
                    error_text = str()
                    for e in errors:
                        error_text = error_text + " \n" + e.text
                    message = message + error_text
                    self.logging_info(message)
                    result['message'] = message
                    return result


            accession = {"state": "PUBLIC", "release_date": get_datetime()}
            Singlecell().update_component_status(
                id=singlecell["_id"], component="study", identifier="study_id", identifier_value=study["study_id"],
                repository="ena", status_column_value=accession)
            result["status"] = "success"
            result["message"] = f'Study {study["study_id"]} has been released successfully to ENA.'
            return result


    def register_sequencing_annotation(self, identifier=str(), analysis_component_data_df=pd.DataFrame(), 
                          analysis_run_ref_data_df=pd.DataFrame(),analysis_file_data_df=pd.DataFrame(), parent_map=dict(), singlecell_id=str(), analysis_component_name="sequencing_annotation"):
        if analysis_component_data_df.empty:
            message = "No sequencing annotation data to submit"
            self.logging_info(message)
            return dict(status=False, message=message)

        result = dict(status=True, value="")

        self.logging_info(f"Submitting sequencing annotation records...")
        study_id = analysis_component_data_df["study_id"].values[0]

        #sub_ids.append(sub["_id"])
        analysis_set_dom_new = ET.Element("ANALYSIS_SET")
        analysis_set_dom_edit = ET.Element("ANALYSIS_SET")
        seq_annotation_id_new = []
        seq_annotation_id_edit = []


        ena_files = EnaFileTransfer().get_all_records_columns(filter_by={"profile_id":self.profile_id}, projection={"local_path":1,  "remote_path":1})
        data_files = DataFile().get_all_records_columns(filter_by={"profile_id":self.profile_id}, projection={"file_name":1,  "file_hash":1 })
        
        analysis_file_data_df["file_hash"] = analysis_file_data_df["sequencing_annotation_file_name"].map( 
                            {data_file["file_name"] : data_file.get("file_hash", "") 
                             for data_file in data_files if data_file.get("file_hash","") } )
        analysis_file_data_df["remote_path"] = analysis_file_data_df["sequencing_annotation_file_name"].map( 
                            {ena_file["local_path"].split("/")[-1] : ena_file.get("remote_path", "") 
                             for ena_file in ena_files if ena_file.get("remote_path","") } )


        for index, seq_annotation in analysis_component_data_df.iterrows():
            
            analysis_run_ref_df = pd.DataFrame()

            if not analysis_run_ref_data_df.empty:
                analysis_run_ref_df = analysis_run_ref_data_df.loc[analysis_run_ref_data_df[parent_map["sequencing_annotation_run_ref"][analysis_component_name]]==seq_annotation[identifier]]

            run_accessions = []
            if  not analysis_run_ref_df.empty and "run_accession_ena" in analysis_run_ref_df.columns:
                run_accessions= analysis_run_ref_df["run_accession_ena"].values.tolist()
            experiment_accessions = []
            if not analysis_run_ref_df.empty and "experiment_accession_ena" in analysis_run_ref_df.columns:
                experiment_accessions= analysis_run_ref_df["experiment_accession_ena"].values.tolist()

            analysis_file_df = pd.DataFrame()
            if not analysis_file_data_df.empty:
                analysis_file_df = analysis_file_data_df.loc[analysis_file_data_df[parent_map["sequencing_annotation_file"][analysis_component_name]]==seq_annotation[identifier]] 
 
            analysis_dom = self._build_analysis_dom(seq_annotation, run_accessions, experiment_accessions, analysis_file_df)
            if seq_annotation.get("accession_ena",""):
                analysis_set_dom_edit.append(analysis_dom)
                seq_annotation_id_edit.append(seq_annotation["sequencing_annotation_id"])
            else:
                analysis_set_dom_new.append(analysis_dom)
                seq_annotation_id_new.append(seq_annotation["sequencing_annotation_id"])

        if len(seq_annotation_id_new) > 0:
            submission_dom = self._build_submission_dom(is_new=True)
            self._submit_ena_v2(submission_dom=submission_dom,  analysis_dom=analysis_set_dom_new, study_id=study_id, analysis_ids=seq_annotation_id_new, analysis_component_name=analysis_component_name)
        if len(seq_annotation_id_edit) > 0:
            submission_dom = self._build_submission_dom(is_new=False)
            self._submit_ena_v2(submission_dom=submission_dom,  analysis_dom=analysis_set_dom_edit, study_id=study_id, analysis_ids=seq_annotation_id_edit, analysis_component_name=analysis_component_name)


    def _build_analysis_dom(self, seq_annotation, run_accessions, experiment_accessions, analysis_file_df):
        """
        <ANALYSIS_SET>
        <ANALYSIS alias="YF3059">
            <TITLE>Y chromosome sequence STR analysis using lobSTR</TITLE>
            <DESCRIPTION>Y chromosome sequence STR analysis using lobSTR</DESCRIPTION>
            <STUDY_REF accession="ERP011288"/>
            <SAMPLE_REF accession="ERS1023190"/>
            <RUN_REF accession="ERR1198112"/>
            <ANALYSIS_TYPE>
                <SEQUENCE_ANNOTATION/>
            </ANALYSIS_TYPE>
            <FILES>
                <FILE filename="STR_for_YF03059_20151228.tab.gz" filetype="tab" checksum_method="MD5"
                    checksum="9f2976d079c10b111669b32590d1eb3e"/>
            </FILES>
        </ANALYSIS>
        </ANALYSIS_SET>
        """
        #analysis_set = ET.Element("ANALYSIS_SET")
        analysis = ET.Element("ANALYSIS", alias=str(seq_annotation["study_id"] + ":" + seq_annotation["sequencing_annotation_id"]))
        ET.SubElement(analysis, "TITLE").text = seq_annotation["sequencing_annotation_title"]
        ET.SubElement(analysis, "DESCRIPTION").text = seq_annotation["sequencing_annotation_description"]
        ET.SubElement(analysis, "STUDY_REF").set("accession", seq_annotation["study_accession"])
        ET.SubElement(analysis, "SAMPLE_REF").set("accession", seq_annotation["biosampleAccession"])

        for run in run_accessions:
            ET.SubElement(analysis, "RUN_REF").set("accession", run)
        """
        for experiment in experiment_accessions:
            ET.SubElement(analysis, "EXPERIMENT_REF").set("accession", experiment)  
        """          
        analysis_type = ET.SubElement(analysis, "ANALYSIS_TYPE")
        ET.SubElement(analysis_type, "SEQUENCE_ANNOTATION")
        files = ET.SubElement(analysis, "FILES")

        for _, file in analysis_file_df.iterrows():
            file_elm = ET.SubElement(files, "FILE")
            file_elm.set("filename", f'{file["remote_path"]}{file["sequencing_annotation_file_name"]}')
            file_elm.set("filetype", file["sequencing_annotation_file_type"])
            file_elm.set("checksum_method","MD5")
            file_elm.set("checksum", file["file_hash"])

        return  analysis



    def register_assembly(self, identifier=str(), assembly_component_data_df=pd.DataFrame(), 
                        assembly_run_ref_data_df=pd.DataFrame(),assembly_file_data_df=pd.DataFrame(), parent_map=dict(), singlecell_id=str()):
        
        if assembly_component_data_df.empty:
            message = "No assembly data to submit"
            self.logging_info(message)
            return dict(status=False, message=message)

        result = dict(status=True, value="")
        ena_fields = []
        for x in Assembly().get_schema().get("schema_dict"):
            if x.get("ena_assembly_submisison", False):
                ena_fields.append(x["id"].split(".")[-1])

        enafiles = EnaFileTransfer().get_all_records_columns(filter_by={"profile_id":self.profile_id}, projection={"local_path":1 })
        enafile_map = {enafile["local_path"].split("/")[-1] : enafile.get("local_path") for enafile in enafiles if enafile.get("local_path","")}

        submission_folder = os.path.join

        errors = []
        for index, row in assembly_component_data_df.iterrows():
            if row.get("assembly_id", str()):
                self.logging_info(f"Registering assembly {row[identifier]}...")

            manifest_content = ""
            manifest_content += "STUDY" + "\t" + row.get("study_accession", "") + "\n"
            manifest_content += "SAMPLE" + "\t" + row.get("biosampleAccession", "") + "\n"

            for field in ena_fields:
                value = row.get(field, None)
                if value:
                    if field.upper() == "ASSEMBLYNAME":
                        value = f'{row["study_id"]}_{row["assembly_id"]}_{value}'
                    manifest_content += field.upper() + "\t" + str(value) + "\n"

            assembly_file_df = assembly_file_data_df.loc[assembly_file_data_df[parent_map["assembly_file"]["assembly"]]==row[identifier]] 
            if assembly_file_df.empty:
                    self.logging_error("no assembly file found")

            for _, file_row in assembly_file_df.iterrows():
                manifest_content += file_row["assembly_file_type"].upper() + "\t" + enafile_map.get(file_row["assembly_file_name"], "") + "\n"

            if not assembly_run_ref_data_df.empty:
                assembly_run_ref_df = assembly_run_ref_data_df.loc[assembly_run_ref_data_df[parent_map["assembly_run_ref"]["assembly"]]==row[identifier]]
    
                run_accessions = []
                if "run_accession_ena" in assembly_run_ref_df.columns:
                    run_accessions= assembly_run_ref_df["run_accession_ena"].values.tolist()

                if run_accessions:
                    manifest_content += "RUN_REF\t" + ",".join(run_accessions) + "\n"
                else:
                    message = f"no run accession found for assembly {row[identifier]}"
                    self.logging_error(message)
                    result['status'] = False
                    result['message'] = message
                    return result

            submission_folder = os.path.join(tempfile.gettempdir(), self.profile_id, row["study_id"], row[identifier])
            os.makedirs(submission_folder, exist_ok=True)
            manifest_path = os.path.join(submission_folder,"manifest.txt")

            with open(manifest_path, "w") as destination:
                destination.write(manifest_content)

            #verify submission
            self.logging_info("validating assembly")

            if not row.get("coverage", ""):
                submission_type = "transcriptome"
            else:
                submission_type = "genome"
                
            return_code, output = self.validate_assembly(file_path=manifest_path, submission_type=submission_type)
            """
        
            test = ""
            if "dev" in ena_service:
                test = " -test "
            #cli_path = "tools/reposit/ena_cli/webin-cli.jar"
            submission_type = row.get("submission_type", "transcriptome")
            webin_cmd = f"java -Xmx6144m -jar webin-cli.jar -username {user_token} -password '{pass_word}' {test} -context {submission_type} -manifest {str(manifest_path)} -validate -ascp"
            
            #qself.logging_debug(webin_cmd)
            try:
                self.logging_info('validating assembly submission')
                output = subprocess.check_output(webin_cmd, stderr=subprocess.STDOUT, shell=True)
                self.logging_debug(output)
                output = output.decode("ascii")
                return_code = 0
            except subprocess.CalledProcessError as cpe:
                return_code = cpe.returncode
                output = cpe.stdout
                output = output.decode("ascii") + " ERROR return code " + str(return_code)

            self.logging_debug(output)
            #report is being stored in webin-cli.report and manifest.txt.report so we can get errors there
            """
            error = ""
            if return_code == 0:
                self.logging_info("submitting assembly")
                return_code, output = self.submit_assembly(file_path=str(manifest_path), submission_type=submission_type)
                if return_code != 0 :
                    #handle possibility submission is not successfull
                    #this may happen for instance if the same assembly has already been submitted, which would not get caught
                    #by the validation step
                    self.logging_error("assembly submission failed: " + output)
                    return {"status": False, "message": output}
                accession = re.search( "ERZ\d*\w" , output).group(0).strip()
                assembly_accession = dict(
                    accession = accession,
                    alias = f'webin-{submission_type}-{row["study_id"]}_{row["assembly_id"]}_{row["assemblyname"]}',
                    project_accession=row["study_accession"],
                    sample_accession=row["biosampleAccession"],
                )
                update_result = Submission().get_collection_handle().update_one({"_id": ObjectId(self.submission_id), "accessions.assembly.alias":assembly_accession["alias"]},
                                                                {"$set": {"accessions.assembly.$": assembly_accession}})
                    
                if update_result.matched_count == 0:
                    Submission().get_collection_handle().update_one({"_id": ObjectId(self.submission_id)},
                                                                {"$addToSet": {"accessions.assembly": assembly_accession}})
                Singlecell().update_component_status(id=singlecell_id, component="assembly", 
                                                     identifier=identifier, 
                                                     identifier_value=row[identifier], 
                                                     repository="ena", 
                                                     status_column_value={"status": "accepted", "accession": accession, "error": ""},
                                                     with_child_components=True)
                self.logging_info(f"Registering assembly {row[identifier]}...successfully submitted to ENA with accession {accession}")
            else:
                error = output
                if return_code == 2:
                    with open(os.path.join(submission_folder,"manifest.txt.report")) as report_file:
                        error = output + " " + report_file.read()
                elif return_code == 3:
                    directories = sorted(glob.glob(f"{submission_folder}/{submission_type}/*"),key=os.path.getmtime)
                    with open(f"{directories[-1]}/validate/webin-cli.report") as report_file:
                        error = output + " " + report_file.read()
                Singlecell().update_component_status(id=singlecell_id, component="assembly", identifier=identifier, identifier_value=row[identifier], repository="ena", status_column_value={"status": "rejected", "error":error})

            if error:
                errors.append(error) 

        if errors:
            message = "Assembly not registered due to the following errors: " + ",".join(errors)
            self.logging_error(message)
            result['status'] = False
            result['message'] = message
        elif not assembly_component_data_df.empty:
            message = "Assembly registered successfully."
            self.logging_info(message)
            result['status'] = True
            result['message'] = message
        else:
            message = "No assembly data to register."
            self.logging_debug(message)
            result['status'] = True
            result['message'] = message
        return result



    def validate_assembly(self, file_path, submission_type):
        test = ""
        if "dev" in ena_service:
            test = " -test "
        webin_cmd = f"java -Xmx6144m -jar webin-cli.jar -username {user_token} -password '{pass_word}' {test} -context {submission_type} -manifest {str(file_path)} -validate -ascp"
        self.logging_debug(webin_cmd)
        try:
            self.logging_info("validating assembly")
            output = subprocess.check_output(webin_cmd, stderr=subprocess.STDOUT, shell=True)
            output = output.decode("ascii")
            return_code = 0
        except subprocess.CalledProcessError as cpe:
            return_code = cpe.returncode
            output = cpe.stdout
            output = output.decode("ascii") + " ERROR return code " + str(cpe.returncode)
        self.logging_debug(output)
        return return_code, output


    def submit_assembly(self, file_path, submission_type):
        test = ""
        if "dev" in ena_service:
            test = " -test "
        webin_cmd = f"java -Xmx6144m -jar webin-cli.jar -username {user_token}  -password '{pass_word}' {test} -context {submission_type} -manifest {str(file_path)} -submit -ascp"
        Logger().debug(msg=webin_cmd)
        # print(webin_cmd)
        # try/except as it turns out this can fail even if validate is successfull
        try:
            #self.logging_info("submitting assembly")
            output = subprocess.check_output(webin_cmd,stderr=subprocess.STDOUT, shell=True)
            output = output.decode("ascii")
            return_code = 0
        except subprocess.CalledProcessError as cpe:
            return_code = cpe.returncode
            output = cpe.stdout
            output = output.decode("ascii") + " ERROR return code " + str(cpe.returncode)
        self.logging_debug(output)

        #todo delete files after successfull submission
        #todo decide if keeping manifest.txt and store accession in assembly objec too
        return return_code, output


    def _build_submission_dom(self, is_new=True):
        """
        <SUBMISSION_SET>
        <SUBMISSION>
    <ACTIONS>
        <ACTION>
            <ADD/>
        </ACTION>
        </ACTIONS>
        </SUBMISSION>
        </SUBMISSION_SET>
        """
        action_str = "ADD"
        if not is_new:
            action_str = "MODIFY"
        submission_set = ET.Element("SUBMISSION_SET")
        submission = ET.SubElement(submission_set, "SUBMISSION")
        actions = ET.SubElement(submission, "ACTIONS")
        action = ET.SubElement(actions, "ACTION")
        add = ET.SubElement(action, action_str)
        return submission_set



    def _submit_ena_v2(self, submission_dom, analysis_dom, study_id, analysis_ids, analysis_component_name):
        webin = ET.Element("WEBIN")
        webin.append(submission_dom)
        webin.append(analysis_dom)
        xml_str = ET.tostring(webin, encoding='utf8', method='xml')
        self.logging_debug(str(xml_str))
        files = {'file': xml_str}

        with requests.Session() as session:    
            session.auth = (user_token, pass_word)    
            try:
                response = session.post(ena_v2_service_async, data={},files = files)
                receipt = response.text
                ghlper.logging_info("ENA RECEIPT " + receipt)
                if response.status_code == requests.codes.ok:
                    #receipt = subprocess.check_output(curl_cmd, shell=True)
                    return self._handle_async_receipt(receipt, study_id, analysis_ids, analysis_component_name )
                else:
                    ghlper.logging_info(response.status_codes, response.text)
                    message = 'API call error ' + "Submitting project xml to ENA via CURL. CURL command is: " + ena_v2_service_async
                    self.logging_error(message)
                    #reset_seq_annotation_submission_status(sub["_id"])
            except ET.ParseError as e:
                ghlper.logging_exception(e)
                message = " Unrecognised response from ENA - " + str(
                    receipt) + " Please try again later, if it persists contact admins"
                self.logging_error(message)
                #reset_seq_annotation_submission_status(sub["_id"])
                return False
            except Exception as e:
                ghlper.logging_exception(e)
                message = 'API call error ' + "Submitting project xml to ENA via CURL. href is: " + ena_v2_service_async
                self.logging_error(message)
                #reset_seq_annotation_submission_status(sub["_id"])
                return False

    def _handle_async_receipt(self, receipt, study_id, analysis_ids, analysis_component_name):
        result = json.loads(receipt)
        submission_id = result["submissionId"]
        href = result["_links"]["poll"]["href"]
        analysis_submission = {'id': submission_id, "component": analysis_component_name,
                      'analysis_ids': analysis_ids, 'href': href, "study_id":study_id}
        return Submission().update_analysis_submission_async(self.submission_id, analysis_submission)



class SubmissionHelper:
    def __init__(self, submission_id=str()):
        self.submission_id = submission_id
        self.profile_id = str()
        self.__converter_errors = list()

        self.collection_handle = ghlper.get_submission_handle()
        doc = self.collection_handle.find_one(
            {"_id": ObjectId(self.submission_id)},
            {
                "profile_id": 1,
                "description_token": 1,
                "bundle": 1,
                "project_release_date": 1,
            },
        )

        if doc:
            self.profile_id = doc.get("profile_id", str())
            self.profile = Profile().get_record(self.profile_id)
            # self.description_token = doc.get("description_token", str())

            # self.description = ghlper.get_description_handle().find_one({"_id": ObjectId(self.description_token)})
            # self.bundle_samples = doc.get("bundle_samples", [])
            self.project_release_date = doc.get("project_release_date", str())
            self.bundle = doc.get("bundle", [])

    def get_converter_errors(self):
        return self.__converter_errors

    def flush_converter_errors(self):
        self.__converter_errors = list()

    def get_pairing_info(self):
        """
        function returns information about datafiles pairing
        :return:
        """
        datafiles_pairing = [
            {"_id": x.split(",")[0], "_id2": x.split(",")[1]}
            for x in self.bundle
            if "," in x
        ]
        return datafiles_pairing
    

    
    def get_study_release(self):
        """
        function returns the release date for a study
        :return:
        """

        release_date = dict()
        release_date = self.project_release_date

        if release_date:
            try:
                release_date = datetime.strptime(release_date, '%d/%m/%Y').strftime(
                    '%Y-%m-%d'
                )
            except:
                pass
            present = datetime.now()
            past = datetime.strptime(release_date, "%Y-%m-%d")

            return dict(
                release_date=release_date, in_the_past=past.date() <= present.date()
            )

        return release_date

    def get_study_descriptors(self):
        """
        function returns descriptors for a study e.g., name, title, description
        :return:
        """

        study_attributes = dict()

        # if not self.description:
        #    return study_attributes

        # profile = Profile().get_record(self.profile_id)
        study_attributes["name"] = self.profile.get("title", str())
        study_attributes["title"] = self.profile.get("title", str())
        study_attributes["description"] = self.profile.get("description", str())
        study_attributes["ena_locus_tags"] = self.profile.get("ena_locus_tags", str())

        # attributes = self.description.get("attributes", dict())
        # study_attributes["name"] = attributes.get("project_details", dict()).get("project_name", str())
        # study_attributes["title"] = attributes.get("project_details", dict()).get("project_title", str())
        # study_attributes["description"] = attributes.get("project_details", dict()).get("project_description", str())
        # if not study_attributes.get("name", str()):
        #    profile = Profile().get_record(self.profile_id)
        #    study_attributes["name"] = profile.get("title", str())
        # if not study_attributes.get("title", str()):
        #    profile = Profile().get_record(self.profile_id)
        #    study_attributes["title"] = profile.get("title", str())
        # if not study_attributes.get("description", str()):
        #    profile = Profile().get_record(self.profile_id)
        #    study_attributes["description"] = profile.get("description", str())
        return study_attributes

    def get_sra_samples(self, submission_location=str()):
        """
        function retrieves study samples and presents them in a format for building an sra sample set
        :param submission_location:
        :return:
        """

        sra_samples = list()

        # get datafiles attached to submission
        # submission_record = self.collection_handle.find_one({"_id": ObjectId(self.submission_id)}, {"bundle": 1})
        '''
        bundle_samples = submission_record.get("bundle_samples", list())
        samples = Sample(profile_id=self.profile_id).get_records(bundle_samples)

        # get datafiles attached to submission
        object_ids = []
        for sample in samples:
            datafile_ids = (sample.get("description", dict()).get("file_id", list()))
            object_ids.extend([ObjectId(x.get("file_id", str())) for x in datafile_ids])
        '''
        object_ids = [
            ObjectId(file_id) for id in self.bundle for file_id in id.split(",")
        ]

        datafiles = cursor_to_list(
            ghlper.get_datafiles_handle().find(
                {"_id": {"$in": object_ids}},
                {
                    '_id': 1,
                    'file_location': 1,
                    "description.attributes": 1,
                    "name": 1,
                    "file_hash": 1,
                },
            )
        )

        samples_id = list()
        df_attributes = []  # datafiles attributes

        for datafile in datafiles:
            # datafile_attributes = [v for k, v in datafile.get("description", dict()).get("attributes", dict()).items()]
            new_dict = dict()
            # for d in datafile_attributes:
            #    new_dict.update(d)
            new_dict.update(
                {
                    k: v
                    for (k, v) in datafile.get("description", dict())
                    .get("attributes", dict())
                    .items()
                    if type(v) is not dict
                }
            )

            new_dict['datafile_id'] = str(datafile['_id'])
            new_dict['datafile_name'] = datafile.get('name', str())
            new_dict['datafile_hash'] = datafile.get('file_hash', str())
            new_dict['datafile_location'] = datafile.get('file_location', str())

            df_attributes.append(new_dict)

        # process datafiles attributes
        df_attributes_df = pd.DataFrame(df_attributes)
        df_columns = df_attributes_df.columns

        # replace null values
        for k in df_columns:
            df_attributes_df[k].fillna('', inplace=True)

        if 'study_samples' in df_columns:
            df_attributes_df['study_samples'] = df_attributes_df['study_samples'].apply(
                lambda x: x[0] if isinstance(x, list) else x.split(",")[-1]
            )
            samples_id = list(df_attributes_df['study_samples'].unique())
            samples_id = [x for x in samples_id if x]

        if not samples_id:
            self.__converter_errors.append("No samples associated with datafiles!")
            return sra_samples

        file_path = os.path.join(submission_location, "datafiles.csv")
        df_attributes_df.to_csv(path_or_buf=file_path, index=False)

        if self.profile["type"] != "genomics":
            return []

        samples_id_object_list = [ObjectId(sample_id) for sample_id in samples_id]

        sample_records = ghlper.get_samples_handle().find(
            {"_id": {"$in": samples_id_object_list}}
        )

        # get sources
        sources = ghlper.get_sources_handle().find(
            {"profile_id": self.profile_id, 'deleted': helpers.get_not_deleted_flag()}
        )

        sra_sources = dict()

        for source in sources:
            sra_source = dict()
            sra_sources[str(source["_id"])] = sra_source

            sra_source["name"] = source.get("name", "")
            sra_source["taxon_id"] = source.get("organism", dict()).get(
                'termAccession', str()
            )
            if 'NCBITaxon_' in sra_source["taxon_id"]:
                sra_source["taxon_id"] = sra_source["taxon_id"].split('NCBITaxon_')[-1]

            sra_source["scientific_name"] = source.get("organism", dict()).get(
                'annotationValue', str()
            )
            sra_source['attributes'] = self.get_attributes(
                source.get("characteristics", list())
            )
            sra_source['attributes'] = sra_source['attributes'] + self.get_attributes(
                source.get("factorValues", list())
            )

        for sample in sample_records:
            sra_sample = sample.copy()
            # find first checklist, TBC: find the submitted checklist
            read = sample.get("read", [])
            if read:
                sra_sample["checklist_id"] = sample["checklist_id"]
            sra_sample['sample_id'] = str(sample['_id'])
            sra_sample['name'] = sample['name']

            sra_sample['attributes'] = self.get_attributes(
                sample.get("characteristics", list())
            )
            sra_sample['attributes'] = sra_sample['attributes'] + self.get_attributes(
                sample.get("factorValues", list())
            )

            # retrieve sample source
            source_id = sample.get("derivesFrom", str())
            # source_id = source_id[0] if source_id else ''
            if not source_id:
                continue
            sample_source = sra_sources.get(source_id, dict())

            if sample_source:
                sra_sample['attributes'].append(
                    dict(tag="Source Name", value=sample_source.get("name", str()))
                )
            else:
                self.__converter_errors.append(
                    "Sample: " + sample['name'] + " has no source information"
                )

            if sample_source.get("taxon_id", str()):
                sra_sample['taxon_id'] = sample_source.get("taxon_id", str())
            else:
                self.__converter_errors.append(
                    "Sample: "
                    + sample['name']
                    + " has no TAXON_ID. Please make sure an organism has "
                    "been set for the source of this sample from the NCBITAXON ontology."
                )

            if sample_source.get("scientific_name", str()):
                sra_sample['scientific_name'] = sample_source.get(
                    "scientific_name", str()
                )
            else:
                self.__converter_errors.append(
                    "Sample: "
                    + sample['name']
                    + " has no SCIENTIFIC_NAME. Please make sure an organism has "
                    "been set for the source of this sample from an ontology."
                )

            if sample_source.get("attributes", list()):
                sra_sample['attributes'] = sra_sample['attributes'] + sample_source.get(
                    "attributes", list()
                )

            sra_samples.append(sra_sample)

        return sra_samples

    def get_attributes(self, attributes):
        """
        function sorts attributes to tag/value and/or unit pair
        :param attributes:
        :return:
        """

        resolved_attributes = list()

        if not attributes:
            return resolved_attributes

        for atrib in attributes:
            tag = atrib.get("category", dict()).get("annotationValue", str()).strip()
            value = atrib.get("value", dict()).get("annotationValue", str()).strip()
            unit = atrib.get("unit", dict()).get("annotationValue", str()).strip()

            if not any(x for x in [tag, value, unit]):
                continue

            valid = True
            feedback = list()

            attribute = dict(tag=tag, value=value, unit=unit)

            if not tag:
                valid = False
                feedback.append('Attribute category not defined')

            if not value:
                valid = False
                feedback.append('Attribute value not defined')

            is_numeric = False

            try:
                float(value)
            except ValueError:
                pass
            else:
                is_numeric = True
                if not unit:
                    valid = False
                    feedback.append('Numeric attribute requires a unit')

            if is_numeric is False:
                del attribute["unit"]

            # store attribute if valid, error otherwise
            if valid is False:
                self.__converter_errors.append((attribute, feedback))
            else:
                resolved_attributes.append(attribute)

        return resolved_attributes

    def get_study_accessions(self):
        """
        function returns study accessions
        :param accessions:
        :return:
        """

        doc = self.collection_handle.find_one(
            {"_id": ObjectId(self.submission_id)}, {"accessions.project": 1}
        )

        if not doc:
            return list()

        return doc.get('accessions', dict()).get('project', list())

    def get_sample_accessions(self):
        """
        function returns sample accessions
        :return:
        """
        result = []
        doc = self.collection_handle.find_one(
            {"_id": ObjectId(self.submission_id)}, {"accessions.sample": 1}
        )
        if not doc:
            return list()
        return doc.get('accessions', dict()).get('sample', list())

    def get_run_accessions(self):
        """
        function returns datafiles (runs) accessions
        :return:
        """

        doc = self.collection_handle.find_one(
            {"_id": ObjectId(self.submission_id)}, {"accessions.run": 1}
        )

        if not doc:
            return list()

        return doc.get('accessions', dict()).get('run', list())
