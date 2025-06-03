__author__ = 'etuka'
__date__ = '02 April 2019'

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
from common.lookup.lookup import SRA_SUBMISSION_TEMPLATE
from common.schemas.utils.data_utils import simple_utc
from common.utils.helpers import notify_submission_status, get_datetime, get_env, json_to_pytype
from common.lookup.lookup import SRA_SAMPLE_TEMPLATE,SRA_PROJECT_TEMPLATE,SRA_SETTINGS
import pandas as pd
import subprocess
import os
import tempfile
from common.dal.submission_da import Submission
from common.dal.copo_da import EnaChecklist
from common.dal.sample_da import Sample
from src.apps.copo_single_cell_submission.utils.da import Singlecell 

__author__ = 'etuka'
__date__ = '02 April 2019'

ena_service = get_env('ENA_SERVICE')
pass_word = get_env('WEBIN_USER_PASSWORD')
user_token = get_env('WEBIN_USER').split("@")[0]
webin_user = get_env('WEBIN_USER')
webin_domain = get_env('WEBIN_USER').split("@")[1]


class EnaSubmissionHelper:
    def __init__(self, submission_id=str(), profile_id=str()):
        self.submission_id = submission_id
        self.profile_id = profile_id
        self.profile = Profile().get_record(self.profile_id)

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
        sra_settings = helpers.json_to_pytype(SRA_SETTINGS).get("properties", dict())

        # create submission xml
        self.logging_info("Creating submission xml..." )

        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(SRA_SUBMISSION_TEMPLATE, parser).getroot()

        # set submission attributes
        root.set("broker_name", sra_settings["sra_broker"])
        root.set("center_name", sra_settings["sra_center"])
        root.set("submission_date", datetime.utcnow().replace(tzinfo=simple_utc()).isoformat())

        # set SRA contacts
        contacts = root.find('CONTACTS')

        # set copo sra contacts
        copo_contact = etree.SubElement(contacts, 'CONTACT')
        copo_contact.set("name", sra_settings["sra_broker_contact_name"])
        copo_contact.set("inform_on_error", sra_settings["sra_broker_inform_on_error"])
        copo_contact.set("inform_on_status", sra_settings["sra_broker_inform_on_status"])

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

        sra_settings = json_to_pytype(SRA_SETTINGS).get("properties", dict())
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
            sample_node.set("center_name", sra_settings["sra_center"])
            sample_node.set("broker_name", sra_settings["sra_broker"])

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
                    key_mapping = { key :  value["synonym"] if "synonym" in value.keys() else value["name"] for key, value in fields.items() }
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

    def register_project(self, submission_xml_path=str(), modify_submission_xml_path=str(), singlecell=None):
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
        sra_settings = helpers.json_to_pytype(SRA_SETTINGS).get("properties", dict())

        # set project descriptors
        study = singlecell.get("components", {}).get("study", [{}])[0]
        project.set("alias", self.submission_id+":"+ study["study_id"])
        project.set("center_name", sra_settings["sra_center"])

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

        locus_tags = study.get("ena_locus_tags","")
        if locus_tags:
            for tag in locus_tags.split(","):
                etree.SubElement(sequencing_project, 'LOCUS_TAG_PREFIX').text = tag.strip()

        output_location = tempfile.gettempdir()

        # do it for modify
        if study.get("accession_ena"):
            result = self.process_project(output_location, root, modify_submission_xml_path, singlecell )
        else:
            result = self.process_project(output_location, root, submission_xml_path, singlecell)

        Submission().remove_component_from_submission(sub_id=str(self.submission_id), component="study", component_ids=[singlecell["study_id"]])

        if result['status'] is False:
            message = "Study not registered."
            self.logging_error(message)
        else:        
            message = f"Study {study["study_id"]} has been registered successfully to ENA."
            self.logging_info(message)

        #Submission().add_component_submission_accession(sub_id=str(self.submission_id), component="study", accessions=new_accessions)

        return result

    def process_project(self, output_location, root, submission_xml_path, singlecell):
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
            Singlecell().update_component_status(id=singlecell["_id"], component="study", identifier="study_id", identifier_value=singlecell["study_id"], repository="ena", status_column_value={"status": "rejected",  "error": result['message'] })  
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
            
            Singlecell().update_component_status(id=singlecell["_id"], component="study", identifier="study_id", identifier_value=singlecell["study_id"], repository="ena", status_column_value={"status": "accepted", "state": status,  "accession": accession, "release_date": release_date, "error": ""})  

        submission_record = Submission().get_collection_handle().find_one({"_id": ObjectId(self.submission_id)}, {"accessions.project": 1})

        if submission_record:
            updated_project_accessions = submission_record.get("accessions",{}).get('project',[])
            study_found = False
            for accession in updated_project_accessions:
                if accession.get('study_id',"") == singlecell["study_id"]:
                    accession.update(project_accessions[singlecell["study_id"]])
                    study_found = True
                    break
            if not study_found:
                updated_project_accessions.append(project_accessions[singlecell["study_id"]])
            Submission().get_collection_handle().update_one(
                {"_id": ObjectId(str(submission_record.pop('_id')))},
                {'$set': {"accessions.project": updated_project_accessions, "date_modified": dt}})

            # update submission status
            status_message = "Project successfully registered, and accessions saved."
            self.logging_info (status_message)
        return dict(status=True, value='')


class SubmissionHelper:
    def __init__(self, submission_id=str()):
        self.submission_id = submission_id
        self.profile_id = str()
        self.__converter_errors = list()

        self.collection_handle = ghlper.get_submission_handle()
        doc = self.collection_handle.find_one({"_id": ObjectId(self.submission_id)},
                                              {"profile_id": 1, "description_token": 1, "bundle": 1, "project_release_date": 1})

        if doc:
            self.profile_id = doc.get("profile_id", str())
            self.profile = Profile().get_record(self.profile_id)
            #self.description_token = doc.get("description_token", str())

            #self.description = ghlper.get_description_handle().find_one({"_id": ObjectId(self.description_token)})
            #self.bundle_samples = doc.get("bundle_samples", [])
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
        datafiles_pairing = [ {"_id": x.split(",")[0], "_id2": x.split(",")[1] } for x in self.bundle if "," in x ]
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
                release_date = datetime.strptime(release_date, '%d/%m/%Y').strftime('%Y-%m-%d')
            except:
                pass
            present = datetime.now()
            past = datetime.strptime(release_date, "%Y-%m-%d")

            return dict(release_date=release_date, in_the_past=past.date() <= present.date())

        return release_date
    
    def get_study_descriptors(self):
        """
        function returns descriptors for a study e.g., name, title, description
        :return:
        """

        study_attributes = dict()

        #if not self.description:
        #    return study_attributes

        #profile = Profile().get_record(self.profile_id)
        study_attributes["name"] = self.profile.get("title", str())
        study_attributes["title"] = self.profile.get("title", str())
        study_attributes["description"] = self.profile.get("description", str())
        study_attributes["ena_locus_tags"] = self.profile.get("ena_locus_tags", str())
        
        #attributes = self.description.get("attributes", dict())
        #study_attributes["name"] = attributes.get("project_details", dict()).get("project_name", str())
        #study_attributes["title"] = attributes.get("project_details", dict()).get("project_title", str())
        #study_attributes["description"] = attributes.get("project_details", dict()).get("project_description", str())
        #if not study_attributes.get("name", str()):
        #    profile = Profile().get_record(self.profile_id)
        #    study_attributes["name"] = profile.get("title", str())
        #if not study_attributes.get("title", str()):
        #    profile = Profile().get_record(self.profile_id)
        #    study_attributes["title"] = profile.get("title", str())
        #if not study_attributes.get("description", str()):
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
        #submission_record = self.collection_handle.find_one({"_id": ObjectId(self.submission_id)}, {"bundle": 1})
        '''
        bundle_samples = submission_record.get("bundle_samples", list())
        samples = Sample(profile_id=self.profile_id).get_records(bundle_samples)

        # get datafiles attached to submission
        object_ids = []
        for sample in samples:
            datafile_ids = (sample.get("description", dict()).get("file_id", list()))
            object_ids.extend([ObjectId(x.get("file_id", str())) for x in datafile_ids])
        '''
        object_ids = [ ObjectId(file_id) for id in self.bundle for file_id in id.split(",") ]


        datafiles = cursor_to_list(ghlper.get_datafiles_handle().find({"_id": {"$in": object_ids}},
                                                                      {'_id': 1, 'file_location': 1, "description.attributes": 1, "name": 1,
                                                                       "file_hash": 1}))

        samples_id = list()
        df_attributes = []  # datafiles attributes

        for datafile in datafiles:
            #datafile_attributes = [v for k, v in datafile.get("description", dict()).get("attributes", dict()).items()]
            new_dict = dict()
            #for d in datafile_attributes:
            #    new_dict.update(d)
            new_dict.update({k : v for (k, v) in  datafile.get("description", dict()).get("attributes", dict()).items() if type(v) is not dict } )

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
                lambda x: x[0] if isinstance(x, list) else x.split(",")[-1])
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

        sample_records = ghlper.get_samples_handle().find({"_id": {"$in": samples_id_object_list}})

        # get sources
        sources = ghlper.get_sources_handle().find(
            {"profile_id": self.profile_id, 'deleted': helpers.get_not_deleted_flag()})

        sra_sources = dict()

        for source in sources:
            sra_source = dict()
            sra_sources[str(source["_id"])] = sra_source

            sra_source["name"] = source.get("name", "")
            sra_source["taxon_id"] = source.get("organism", dict()).get('termAccession', str())
            if 'NCBITaxon_' in sra_source["taxon_id"]:
                sra_source["taxon_id"] = sra_source["taxon_id"].split('NCBITaxon_')[-1]

            sra_source["scientific_name"] = source.get("organism", dict()).get('annotationValue', str())
            sra_source['attributes'] = self.get_attributes(source.get("characteristics", list()))
            sra_source['attributes'] = sra_source['attributes'] + self.get_attributes(
                source.get("factorValues", list()))

        for sample in sample_records:
            sra_sample = sample.copy()
            #find first checklist, TBC: find the submitted checklist
            read = sample.get("read",[])
            if read:
                sra_sample["checklist_id"] = sample["checklist_id"]
            sra_sample['sample_id'] = str(sample['_id'])
            sra_sample['name'] = sample['name']
        
            sra_sample['attributes'] = self.get_attributes(sample.get("characteristics", list()))
            sra_sample['attributes'] = sra_sample['attributes'] + self.get_attributes(
                sample.get("factorValues", list()))

            # retrieve sample source
            source_id = sample.get("derivesFrom", str())
            #source_id = source_id[0] if source_id else ''
            if not source_id:
                continue
            sample_source = sra_sources.get(source_id, dict())

            if sample_source:
                sra_sample['attributes'].append(dict(tag="Source Name", value=sample_source.get("name", str())))
            else:
                self.__converter_errors.append("Sample: " + sample['name'] + " has no source information")

            if sample_source.get("taxon_id", str()):
                sra_sample['taxon_id'] = sample_source.get("taxon_id", str())
            else:
                self.__converter_errors.append("Sample: " + sample[
                    'name'] + " has no TAXON_ID. Please make sure an organism has "
                              "been set for the source of this sample from the NCBITAXON ontology.")

            if sample_source.get("scientific_name", str()):
                sra_sample['scientific_name'] = sample_source.get("scientific_name", str())
            else:
                self.__converter_errors.append("Sample: " + sample[
                    'name'] + " has no SCIENTIFIC_NAME. Please make sure an organism has "
                              "been set for the source of this sample from an ontology.")

            if sample_source.get("attributes", list()):
                sra_sample['attributes'] = sra_sample['attributes'] + sample_source.get("attributes", list())

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

        doc = self.collection_handle.find_one({"_id": ObjectId(self.submission_id)}, {"accessions.project": 1})

        if not doc:
            return list()

        return doc.get('accessions', dict()).get('project', list())

    def get_sample_accessions(self):
        """
        function returns sample accessions
        :return:
        """
        result = []
        doc = self.collection_handle.find_one({"_id": ObjectId(self.submission_id)}, {"accessions.sample": 1})
        if not doc:
            return list()
        return doc.get('accessions', dict()).get('sample', list())
 

    def get_run_accessions(self):
        """
        function returns datafiles (runs) accessions
        :return:
        """

        doc = self.collection_handle.find_one({"_id": ObjectId(self.submission_id)}, {"accessions.run": 1})

        if not doc:
            return list()

        return doc.get('accessions', dict()).get('run', list())
    