
import pymongo
from bson.errors import InvalidId
from common.dal.mongo_util import cursor_to_list, cursor_to_list_str
from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES
from common.utils import helpers
from bson.objectid import ObjectId
from .copo_base_da import DAComponent, handle_dict, DataSchemas
from src.apps.copo_core.models import ProfileType
import re

class ProfileInfo:
    def __init__(self, profile_id=None):
        self.profile_id = profile_id

    def get_counts(self):
        status = dict()
        """
        Method to return current numbers of Publication, Person, Data,
        Sample, Accessions and Submission objects in the given profile
        :return: Dictionary containing the data
        """
        """
        num_dict = dict(num_pub="publication",
                        num_person="person",
                        num_data="datafile",
                        num_sample="sample",
                        num_accessions="accessions",
                        num_submission="submission",
                        num_annotation="annotation",
                        num_temp="metadata_template",
                        num_seqannotation="seqannotation",
                        num_assembly="assembly",
                        num_read="sample"
                        )


        profile_type = Profile().get_type(self.profile_id)
        for k, v in num_dict.items():

            if v in handle_dict:
                if v == "accessions":
                    if "Stand-alone" in profile_type:
                        # Stand-alone projects
                        status[k] = handle_dict["submission"].count_documents({"$and": [
                            {"profile_id": self.profile_id, "repository": "ena",
                             "accessions": {"$exists": True, "$ne": {}}}]})

                    if any(x.upper() in profile_type for x in TOL_PROFILE_TYPES):
                        # Other projects
                        status[k] = handle_dict.get(v).count_documents({"$and": [
                            {'profile_id': self.profile_id, "biosampleAccession": {"$exists": True, "$ne": ""}}]})
                else:
                    status[k] = handle_dict.get(v).count_documents(
                        {'profile_id': self.profile_id})
        """
        return status

    def source_count(self):
        return handle_dict["source"].count_documents(
            {'profile_id': self.profile_id, 'deleted': helpers.get_not_deleted_flag()})
      

class Profile(DAComponent):
    def __init__(self, profile_id=None):
        super(Profile, self).__init__(None, "profile")

    def get_num(self):
        return self.get_collection_handle().count_documents({})

    def get_all_profiles(self, user=None, id_only=False):
        mine = list(self.get_for_user(user, id_only))
        shared = list(self.get_shared_for_user(user, id_only))
        return shared + mine

    def get_type(self, profile_id):
        p = self.get_collection_handle().find_one(
            {"_id": ObjectId(profile_id)})

        if p:
            return p.get("type", "").upper()
        else:
            return False
        
    """
    def get_associated_type(self, profile_id, value=True, label=True):
        p = self.get_collection_handle().find_one(
            {"_id": ObjectId(profile_id)})
        if p:
            if p.get("associated_type", ""):
                if value and not label:
                    return [i.get("value", "") for i in p.get("associated_type", "") if i.get("value", "")]
                elif label and not value:
                    return [i.get("label", "") for i in p.get("associated_type", "") if i.get("label", "")]
                else:
                    return p.get("associated_type", "")
            else:
                return []
        else:
            return False
    """
    def get_for_user(self, user=None, id_only=False):
        if not user:
            user = helpers.get_current_user().id

        if id_only:
            docs = self.get_collection_handle().find(
                {"user_id": user, "deleted": helpers.get_not_deleted_flag()}, {"_id": 1})
        else:
            docs = self.get_collection_handle().find({"user_id": user, "deleted": helpers.get_not_deleted_flag()}).sort(
                'date_modified', pymongo.DESCENDING)

        if docs:
            return docs
        else:
            return None

    def get_shared_for_user(self, user=None, id_only=False):
        # get profiles shared with user
        if not user:
            user = helpers.get_current_user().id
        groups = handle_dict["group"].find({'member_ids': str(user)})

        p_list = list()
        for g in groups:
            gp = dict(g)
            p_list.extend(gp['shared_profile_ids'])

        # remove duplicates
        # p_list = list(set(p_list))

        if id_only:
            docs = self.get_collection_handle().find({
                "_id": {"$in": p_list},
                "deleted": helpers.get_not_deleted_flag()
            }, {"_id": 1, "type": 1})
        else:
            docs = self.get_collection_handle().find(
                {
                    "_id": {"$in": p_list},
                    "deleted": helpers.get_not_deleted_flag()
                }
            ).sort("date_modified", pymongo.DESCENDING)

        out = list(docs)
        for d in out:
            d['shared'] = True

        return out

    def save_record(self, auto_fields=dict(), **kwargs):
        from .copo_da import Person
        from .sample_da import Sample
        new_record = False
        profile_type = auto_fields.get("copo.profile.type", "")

        if not kwargs.get("target_id", str()):
            new_record = True
            for k, v in dict(
                    copo_id=helpers.get_copo_id(),
                    user_id=helpers.get_user_id()
            ).items():
                auto_fields[self.get_qualified_field(k)] = v

        schema = self.get_component_schema(profile_type = profile_type)
        rec = super(Profile, self).save_record(auto_fields, schema=schema, **kwargs)

        # Trigger if record has been updated
        if not new_record:
            if ProfileType.objects.get(type=profile_type).is_dtol_profile:
                associated_type_lst = auto_fields.get("copo.profile.associated_type", [])

                # Get associated type(s) as string separated by '|' symbol
                associated_type = " | ".join(associated_type_lst)

                # Update the 'associated_tol_project' field for unaccepted sample record(s) (if any exist)
                is_associated_tol_project_update_required =  Sample().is_associated_tol_project_update_required(profile_id=kwargs["target_id"], new_associated_tol_project=associated_type)
                
                if is_associated_tol_project_update_required:
                    Sample().update_associated_tol_project(profile_id=kwargs["target_id"], associated_tol_project=associated_type)

        # trigger after save actions
        if not kwargs.get("target_id", str()):
            Person(profile_id=str(rec["_id"])).create_sra_person()
        return rec

    def add_dataverse_details(self, profile_id, dataverse):
        handle_dict['profile'].update_one({'_id': ObjectId(profile_id)}, {
                                          '$set': {'dataverse': dataverse}})

    def check_for_dataverse_details(self, profile_id):
        p = self.get_record(profile_id)
        if 'dataverse' in p:
            return p['dataverse']

    def add_dataverse_dataset_details(self, profile_id, dataset):
        handle_dict['profile'].update_one(
            {'_id': profile_id}, {'$push': {'dataverse.datasets': dataset}})
        return [dataset]

    def check_for_dataset_details(self, profile_id):
        p = self.get_record(profile_id)
        if 'dataverse' in p:
            if 'datasets' in p['dataverse']:
                return p['dataverse']['datasets']

    def get_profiles(self, filter="all_profiles", group_filter=None, search_filter=None, sort_by="date_created", dir=-1):
        from .sample_da import Sample
        profile_ids_from_sample = Sample().get_collection_handle().distinct("profile_id", {"$text": {"$search": search_filter} })
        profile_oids_from_sample = [ObjectId(id) for id in profile_ids_from_sample ]
        profile_condition = {"$or" : [{"_id": {"$in" : profile_oids_from_sample }}, {"title": { "$regex" : search_filter, "$options": "i"}}]}
        profile_condition["type"] = group_filter

        if filter == "my_profiles": 
            associated_profile_types = helpers.get_users_associated_profile_checkers()
            profile_condition["associated_type"] = {"$in": [str(x.name) for x in associated_profile_types]}

        p = self.get_collection_handle().find(profile_condition).sort(sort_by, pymongo.DESCENDING if dir == -1 else pymongo.ASCENDING)
        return cursor_to_list(p)


    def get_profile_records(self, data, currentUser=True):
        #from common.schemas.utils import data_utils
        p = None
        owner_id = helpers.get_user_id()

        if type(data) == str:
            if currentUser:
                p = self.get_collection_handle().find(
                    {"user_id": owner_id, "type": {"$regex": data, "$options": "i"}}).sort(
                    "date_created", pymongo.DESCENDING)
            else:

                p = self.get_collection_handle().find(
                    {"type": {"$regex": data, "$options": "i"}}).sort("date_created", pymongo.DESCENDING)
        return cursor_to_list(p)

    def get_profiles_based_on_sample_data(self, projects, manifest_ids):
        profile_ids = handle_dict["sample"].get_profileID_by_project_and_manifest_id(projects, manifest_ids)
        # Convert string profileID to ObjectId
        profile_ids = [ObjectId(x.get("profile_id", "")) for x in profile_ids]

        p = self.get_collection_handle().find(
            {"_id": {"$in": profile_ids}}).sort("date_created", pymongo.DESCENDING)

        return cursor_to_list_str(p)

    def get_name(self, profile_id):
        p = self.get_record(profile_id)
        if type(p) != InvalidId:
            return p.get("title", "")
        else:
            return "profile not exists"

    def get_by_title(self, title):
        p = self.get_collection_handle().find({"title": title}, {"_id":1})
        return cursor_to_list(p)
    
    def get_profile_by_sequencing_centre(self, sequencing_centre, getProfileIDOnly=False):
        projection = {"_id": 1} if getProfileIDOnly else dict()
        p = self.get_collection_handle().find({'sequencing_centre': {'$in': [sequencing_centre]}}, projection)
        return cursor_to_list(p)
    
    def validate_and_delete(self, profile_id):
        # check if any submission object reference this profile, if so do not delete
        condition = {"profile_id": profile_id}
        for da in ["submission", "sample", "datafile"]:
            if handle_dict[da].count_documents(condition) > 0:
                return False
        self.get_collection_handle().delete_one({"_id": ObjectId(profile_id)})
        return True


    def validate_record(self, auto_fields=dict(), validation_result=dict(), **kwargs):
        """
        validates record. useful before CRUD actions
        :param auto_fields:
        :param validation_result:
        :param kwargs:
        :return:
        """
        if validation_result.get("status", "success") == "error":  # no need continuing with validation, propagate error
            return super(Profile, self).validate_record(auto_fields, result=validation_result, **kwargs)
        local_result = dict(status="success", message="")

        profile_id = kwargs.get("target_id","")
        is_error_found = False
        profile_type = auto_fields.get("copo.profile.type", "")

        if profile_id:
            profile = self.get_record(profile_id)
            if not profile:
                local_result["message"] = "Record not found"
                is_error_found = True
            elif profile and profile.get("type", "") != profile_type:
                local_result["message"] = "It is not possible to modify the profile type"
                is_error_found = True
        if is_error_found:
            local_result["status"] = "error"
            return super(Profile, self).validate_record(auto_fields, validation_result=local_result, **kwargs)

        schema = self.get_component_schema(profile_type=profile_type)
        for f in schema:
            field = f.get("id", "").split(".")[-1]
            label = f.get("label", field)
            field_value = auto_fields.get(f.get("id", ""), "")

            if f.get("required", "false") == "true":
                if not field_value:
                    local_result["message"] = "Missing required field: " + label + " " + field
                    is_error_found = True
                    break
            if f.get("is_unique", False):
                result = self.get_collection_handle().find({field: field_value},{"_id":1})
                if result:
                    profile_with_same_titles = cursor_to_list(result)
                    if profile_with_same_titles:
                        if not profile_id or  ObjectId(profile_id) != profile_with_same_titles[0].get("_id",""):
                            #status = "error"
                            local_result["message"] = f"Record already exist with {field}: {field_value}"

                            is_error_found = True
                            break
            if f.get("type", "string") == "string":
                regex = f.get("regex", str())
                if regex and field_value and not re.match(regex, field_value):
                    local_result["message"] = f"{label} : Invalid value. It should be in the format {regex}"   
                    is_error_found = True
                    break
         
        '''
        locus_tags = auto_fields["copo.profile.ena_locus_tags"]
        if not is_error_found and locus_tags:
            regex = "^[A-Z][A-Z0-9]{2,11}(,[A-Z][A-Z0-9]{2,11})*$"
            if not re.match(regex, locus_tags):
                local_result["message"] = "ENA locus tag format error : It should start with a captial letter and follow by 2 to 11 captial letters or digits"
                is_error_found = True
            elif not profile_id:
                local_result["status"] = "warning"
                local_result["message"] = "ENA locus tag will be submitted to ENA with the READ submission"
        '''
        if is_error_found:
            local_result["status"] = "error"
        return super(Profile, self).validate_record(auto_fields, validation_result=local_result, **kwargs)


    def get_component_schema(self,  **kwargs):
            result = list()
            profile_type = kwargs.get("profile_type", "")
            schema = super(Profile, self).get_component_schema()
            for f in schema:
                # Filter schema based on manfest type and manifest version
                f_specifications = f.get("specifications", "")

                if f.get("id", "") == "copo.profile.type":
                    f["default_value"] = profile_type
                if not f_specifications:
                    result.append(f)
                elif profile_type in f_specifications:
                    result.append(f)
            return result
