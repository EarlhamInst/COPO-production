from common.validators.validator import Validator
from common.dal.profile_da import Profile
from common.dal.sample_da import Sample
from common.schema_versions.lookup import dtol_lookups as lookup
from common.utils.helpers import notify_frontend
from common.validators.helpers import check_taxon_ena_submittable
from .validation_messages import MESSAGES as msg
from Bio import Entrez
from django_tools.middlewares import ThreadLocal
import os

class SinglePairedValuesValidator(Validator):
    def validate(self):
        row_count = 1
        for row in self.data.iterrows():
            row_count = row_count + 1
            layout = row[1]["library_layout"]
            files = row[1]["file_name"]
            file_checksums = row[1]["file_md5"]
            if layout == "PAIRED":
                if len(files.split(",")) != 2 or len(file_checksums.split(",")) != 2:
                    self.errors.append(msg["validation_msg_paired_file_error"] % (str(row_count)))
                    self.flag = False
            elif layout == "SINGLE":
                if len(files.split(",")) != 1 or len(file_checksums.split(",")) != 1:
                    self.errors.append(msg["validation_msg_single_file_error"] % (str(row_count)))
                    self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


"""
class TaxonValidator(Validator):
    def validate(self):
        if "organism" not in self.data.columns:
            return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")
        
        Entrez.api_key = lookup.NIH_API_KEY
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

class GzipValidator(Validator):

    def validate(self):
        for row in self.data.iterrows():
            file_names = row[1]["file_name"]
            if file_names.strip() == "":
                continue
            for f in file_names.split(","):
                if not f.strip().endswith(".gz"):
                    error_str = f + ": File not gzipped. All files must be gzipped and end in '.gz'"
                    self.errors.append(error_str)
                    self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")
"""

class FileSuffixValidator(Validator):
    def validate(self):
        for row in self.data.iterrows():
            file_names = row[1]["file_name"]
            if file_names.strip() == "":
                continue
            no_of_file = 0
            for f in file_names.split(","):
                if os.path.dirname(f.strip()) != "":
                    error_str = f + ": There should be no folder name for the file."
                    self.errors.append(error_str)
                    self.flag = False
                no_of_file += 1 
                # unpacking the tuple
                file_name, file_extension = os.path.splitext(f.strip())
                if file_extension not in [".gz", ".bz2", ".bam", ".cram"]:
                    error_str = f + ": File must be a gz / bz2 file for fastq or bam / cram file."
                    self.errors.append(error_str)
                    self.flag = False
                if file_extension in [".bam", ".cram"] and no_of_file > 1:
                    error_str = f + ": File cannot be in pair for bam / cram type."
                    self.errors.append(error_str)
                    self.flag = False
                if file_extension in [".gz", ".bz2"] and not file_name.endswith(".fastq"):
                    error_str = f + ": for gz / bz2 file, please make sure it is fastq type"
                    self.warnings.append(error_str)
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class ReadNotInSubmissionQueueValidator(Validator):
    def validate(self):
        sampleMap = {}
        if "sample" not in self.data.columns:
            biosamlpeAccessions = list(self.data["biosampleAccession"])
            samples = Sample(profile_id=self.profile_id).get_all_records_columns(projection=dict(read=1,name=1, biosampleAccession=1), filter_by=dict(profile_id=self.profile_id, biosampleAccession={"$in": biosamlpeAccessions}, read={"$exists": True}))
            sampleMap = {sample["biosampleAccession"]: sample.get("read",[]) for sample in samples if "biosamlpeAccession" in sample}
        else:
            sample_names = list(self.data["sample"])
            samples = Sample(profile_id=self.profile_id).get_all_records_columns(projection=dict(read=1,name=1), filter_by=dict(profile_id=self.profile_id, name={"$in": sample_names}, read={"$exists": True}))
            sampleMap = {sample["name"]: sample.get("read",[]) for sample in samples}
             
        for index, row in self.data.iterrows():
            file_names = row["file_name"]
            sample_name = row["sample"] if "sample" in self.data.columns else row["biosampleAccession"]
            reads = sampleMap.get(sample_name, None)
            if reads:
                for read in reads:
                    if set(read.get("file_name", str()).split(",")) == set(file_names.split(",")) and read.get("status","pending") == "processing":
                        self.errors.append("File " + file_names + " already in submission queue for sample " + sample_name)
                        self.flag = False 
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")

'''    
class DuplicatedSample(Validator):
    def validate(self):
        if "biosampleAccession" in self.data.columns:
            samples = list(self.data["biosampleAccession"])
        elif "sample" in self.data.columns:
            samples = list(self.data["sample"])
        if samples:
            sample = [ x for x in samples if samples.count(x) > 1]
            for s in set(sample):
                self.errors.append("Sample " + s + " is duplicated in manifest")
                self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")        
'''
    
class DuplicatedDataFile(Validator):
    def _extract_ref_parts(self, ref_string):
        '''
            Purpose: This function extracts parts of a sample reference string and returns them as a list.
            Example: ref_string = "6576e64d8ec944db43757896|ERC000046|sample1"
            Output: ['6576e64d8ec944db43757896', 'ERC000046', 'sample1']
            
            Explanation:
                - '6576e64d8ec944db43757896' is the profile ID. This is replaced with the profile title.
                - 'ERC000046' is the checklist ID
                - 'sample1' is the sample name
        '''

        parts = ref_string.split('|') # Split the string by '|'
        
        # Add empty strings if there are fewer than expected parts
        while len(parts) < 3:
            parts.append('')

        # If profile ID is present, replace the value with the profile title
        if len(parts) > 0:
            profile_id = parts[0]
            profile_title = Profile().get_name(profile_id)
            if profile_title:
                parts[0] = profile_title
        return parts
        
    def validate(self):
        checklist = self.kwargs.get("checklist", {})
        checklist_id = checklist.get('primary_id',"")  
        user = ThreadLocal.get_current_user()
        file_names = list(self.data["file_name"])
        samples = Sample(profile_id=self.profile_id).get_collection_handle().find({"profile_id":self.profile_id, "read":{"$exists": True}} ,{"checklist_id":1, "read":1,"name":1, "biosampleAccession":1, "profile_id":1})
        fileMap = {}
        for sample in samples:
            for read in sample.get("read", []):
                files = read.get("file_name", str()).split(",")
                for f in files:
                    fileMap[f] = sample["profile_id"]+   "|" + read.get("checklist_id", sample.get("checklist_id","")) +  "|"+ (sample["name"] if "name" in sample else sample["biosampleAccession"])

        file_name_list = [ file_name  for paried_names in file_names if paried_names.strip() != "" for file_name in paried_names.split(",")]
        file = [ x for x in file_name_list if file_name_list.count(x) > 1]

        for f in set(file):
            self.errors.append("File " + f + " is duplicated in manifest")
            self.flag = False               

        for index, row in self.data.iterrows():
            file_names = row["file_name"]
            sample_name = self.profile_id + "|" + checklist_id +  "|" + (row["sample"] if "sample" in self.data.columns else row["biosampleAccession"])
            files = file_names.split(",")
            for f in files:
                sample = fileMap.get(f, None)
                if sample and sample != sample_name:
                    existing_sample_parts = self._extract_ref_parts(sample)
                    # uploaded_sample_parts = self._extract_ref_parts(sample_name)

                    self.errors.append(msg["validation_msg_sample_duplication_error"].format(
                        filename=f,
                        existing_sample_name=existing_sample_parts[2],
                        existing_sample_profile_title=existing_sample_parts[0], 
                        existing_sample_checklist=existing_sample_parts[1],
                    ))
                    self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")