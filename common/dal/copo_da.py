import os
from datetime import datetime, date

import copy
import re
import pandas as pd
import pymongo
from bson import ObjectId, regex
from django.conf import settings
from django.contrib.auth.models import User
from common.dal.mongo_util import cursor_to_list, cursor_to_list_str
from common.lookup.lookup import DB_TEMPLATES
from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES
from common.utils import helpers
from common.schemas.utils.cg_core.cg_schema_generator import CgCoreSchemas
from .copo_base_da import DAComponent, handle_dict

lg = settings.LOGGER


class Audit(DAComponent):
    def __init__(self, profile_id=None):
        super(Audit, self).__init__(profile_id, 'audit')
        self.filter = {'action': 'update', 'collection_name': 'SampleCollection'}
        self.projection = {
            'update_log.updated_by': 0
        }  # Exclude the sensitive field, 'updated_by', field from the projection
        self.doc_included_field_lst = [
            'copo_id',
            'manifest_id',
            'sample_type',
            'RACK_OR_PLATE_ID',
            'TUBE_OR_WELL_ID',
        ]
        self.doc_excluded_field_lst = [
            'biosampleAccession',
            'public_name',
            'SPECIMEN_ID',
            'sraAccession',
        ]

        self.update_log_addtl_fields = [
            *self.doc_included_field_lst,
            *self.doc_excluded_field_lst,
        ]
        self.update_log_addtl_fields.sort()

    def get_sample_info_based_on_audit(self, sample_id_list, documents):
        from .sample_da import Sample
        from .profile_da import Profile

        # Get samples by their IDs
        samples = Sample().get_by_field('_id', sample_id_list)

        if not samples:
            return dict()

        # Map sample ID i.e. '_id' to documents for lookup
        document_map = {doc.get('_id'): doc for doc in documents}

        # Store profile titles per manifest_id to avoid redundant queries
        profile_title_map = {}
        out = {}

        for sample in samples:
            sample_info = {
                field: sample.get(field, str()) for field in self.doc_excluded_field_lst
            }

            # Find the corresponding audit document for this sample
            document = document_map.get(sample.get('_id', ObjectId()), {})

            # Fields in the document but outside the 'update_log' dictionary
            audit_info = {
                field: document.get(field, str())
                for field in self.doc_included_field_lst
                if field in document
            }

            # Merge sample and audit info
            merged_info = sample_info | audit_info

            # Retrieve `manifest_id`, `profile_id` and `copo_id` from the sample
            manifest_id = sample.get('manifest_id', str())
            profile_id = sample.get('profile_id', str())
            copo_id = str(sample.get('_id', ''))  # Ensure copo_id is stored as a string

            # Retrieve or fetch profile title
            if manifest_id:
                if manifest_id not in profile_title_map:
                    if profile_id:
                        profile = Profile().get_record(profile_id)
                        profile_title_map[manifest_id] = profile.get('title', '')

            # Get the profile title
            merged_info['copo_profile_title'] = profile_title_map.get(manifest_id, '')

            # Sort the dictionary by key
            sorted_info = {key: merged_info[key] for key in sorted(merged_info)}

            # Use `copo_id` as the key for the output dictionary
            out[copo_id] = sorted_info
        return out

    def get_merged_audit_and_sample_info(self, audits):
        audits_df = pd.DataFrame(
            audits
        )  # Convert the list of dictionaries to a DataFrame

        # Fetch additional audit information based on the sample ID i.e. 'copo_id'
        # and ensure that 'copo_id' has no 'NaN' values before converting to list
        sample_id_list = audits_df['copo_id'].dropna().tolist()

        # Fetch additional audit information based on the sample ID
        audit_addtl_info = self.get_sample_info_based_on_audit(sample_id_list, audits)
        audit_addtl_df = pd.DataFrame.from_dict(audit_addtl_info, orient='index')

        # Merge audits_df with audit_addtl_df on 'copo_id'
        merged_df = audits_df.merge(
            audit_addtl_df,
            left_on='copo_id',  # Use 'copo_id' for merging
            right_index=True,
            how='left',
            suffixes=('_audit', '_sample'),
        )

        return merged_df, audit_addtl_info

    def get_sample_update_audits(self, sample_id_list, updatable_field, project):
        out = []

        if project:
            self.filter['sample_type'] = project

        if sample_id_list:
            self.filter |= {'copo_id': {'$in': sample_id_list}}

        pipeline = [
            {'$match': self.filter},
            {
                '$addFields': {
                    'update_log': (
                        {
                            '$filter': {
                                'input': '$update_log',
                                'as': 'log',
                                'cond': {'$eq': ['$$log.field', updatable_field]},
                            }
                        }
                        if updatable_field
                        else '$update_log'
                    )
                }
            },
            {'$project': self.projection},
        ]

        audits = cursor_to_list(self.get_collection_handle().aggregate(pipeline))

        if not audits:
            return out

        merged_df, audit_addtl_info = self.get_merged_audit_and_sample_info(audits)

        # Iterate over each row in the dataframe that has existing samples
        # and merge the 'update_log' with 'audit_addtl_info'
        out = []
        for _, row in merged_df.iterrows():
            logs = row.get('update_log') or []
            sample_type = row.get('sample_type_audit', '')

            # Fetch the corresponding audit_addtl_info for the row's copo_id
            copo_id = str(row.get('copo_id_audit', ''))
            audit_info = audit_addtl_info.get(copo_id, {})

            # Merge each log entry in the 'update_log_sample' with the corresponding 'audit_info'
            update_log = [
                (
                    log | audit_info
                    if updatable_field and log.get('field', '') == updatable_field
                    else log | audit_info
                )
                for log in logs
            ]

            # If merged data exists, append to the output list
            if update_log:
                out.append({'sample_type': sample_type, 'update_log': update_log})

        # Return the merged output as a list of dictionaries
        return out

    def get_sample_update_audits_field_value_lst(self, value_lst, key):
        out = []

        if value_lst:
            self.filter |= {key: {'$in': value_lst}}

        audits = cursor_to_list(
            self.get_collection_handle().find(self.filter, self.projection)
        )

        if not audits:
            return out

        merged_df, audit_addtl_info = self.get_merged_audit_and_sample_info(audits)

        # Iterate over each row in the dataframe that has existing samples
        # and merge the 'update_log' with 'audit_addtl_info'
        out = []
        for _, row in merged_df.iterrows():
            logs = row.get('update_log') or []
            sample_type = row.get('sample_type_audit', '')

            # Fetch the corresponding audit_addtl_info for the row's copo_id
            copo_id = str(row.get('copo_id_audit', ''))
            audit_info = audit_addtl_info.get(copo_id, {})

            # Merge each log entry in the 'update_log_sample' with the corresponding 'audit_info'
            update_log = [log | audit_info for log in logs]

            if update_log:
                out.append({'sample_type': sample_type, 'update_log': update_log})
        return out

    def get_sample_update_audits_by_field_and_value(self, field, value):
        from .sample_da import Sample

        out = []

        # Fields in the document but outside the 'update_log' dictionary
        if field in self.doc_included_field_lst:
            # self.projection |=  {field: 1} # Merge the projection dictionary with the field
            self.filter[field] = value

        elif field in self.doc_excluded_field_lst:
            # Fields excluded from the document and 'update_log' dictionary
            sample = Sample().get_by_field(field, [value])

            if sample is None or len(sample) == 0:
                return []

            copo_id = sample[0]['_id']
            self.filter['copo_id'] = copo_id
        else:
            # Fields in the 'update_log' dictionary
            self.filter['update_log'] = {'$elemMatch': {field: value}}

        audits = cursor_to_list(
            self.get_collection_handle().find(self.filter, self.projection)
        )

        if not audits:
            return out

        merged_df, audit_addtl_info = self.get_merged_audit_and_sample_info(audits)

        # Iterate over each row in the dataframe that has existing samples
        # and merge the 'update_log' with 'audit_addtl_info'
        out = []
        for _, row in merged_df.iterrows():
            sample_type = row.get('sample_type_audit', '')

            # Fetch the corresponding audit_addtl_info for the row's copo_id
            copo_id = str(row.get('copo_id_audit', ''))
            audit_info = audit_addtl_info.get(copo_id, {})

            # Merge each log entry in the 'update_log_sample' with the corresponding 'audit_info'
            update_log = []
            logs = row.get('update_log') or []
            for x in logs:
                x |= audit_info

                if (
                    field not in self.doc_included_field_lst
                    and field not in self.doc_excluded_field_lst
                ):
                    if x[field] == value:
                        update_log.append(x)
                else:
                    update_log.append(x)

            if update_log:
                out.append({'sample_type': sample_type, 'update_log': update_log})
        return out

    def get_sample_update_audits_by_update_type(self, sample_type_list, update_type):
        out = []

        self.filter['update_log'] = {'$elemMatch': {'update_type': update_type}}

        if sample_type_list:
            self.filter['sample_type'] = {'$in': sample_type_list}

        audits = cursor_to_list(
            self.get_collection_handle().find(self.filter, self.projection)
        )

        if not audits:
            return out

        merged_df, audit_addtl_info = self.get_merged_audit_and_sample_info(audits)

        # Iterate over each row in the dataframe that has existing samples
        # and merge the 'update_log' with 'audit_addtl_info'
        out = []
        for _, row in merged_df.iterrows():
            logs = row.get('update_log') or []
            sample_type = row.get('sample_type_audit', '')

            # Fetch the corresponding audit_addtl_info for the row's copo_id
            copo_id = str(row.get('copo_id_audit', ''))
            audit_info = audit_addtl_info.get(copo_id, {})

            update_log = [
                log | audit_info
                for log in logs
                if log.get('update_type', '') == update_type
            ]

            # If merged data exists, append to the output list
            if update_log:
                out.append({'sample_type': sample_type, 'update_log': update_log})
        return out

    def get_sample_update_audits_by_date(self, d_from, d_to):
        out = []

        self.filter['sample_type'] = {'$in': TOL_PROFILE_TYPES}
        self.filter['update_log'] = {
            '$elemMatch': {'time_updated': {'$gte': d_from, '$lt': d_to}}
        }

        audits = cursor_to_list(
            self.get_collection_handle()
            .find(self.filter, self.projection)
            .sort([['update_log.time_updated', -1]])
        )

        if not audits:
            return out

        merged_df, audit_addtl_info = self.get_merged_audit_and_sample_info(audits)

        # Iterate over each row in the dataframe that has existing samples
        # and merge the 'update_log' with 'audit_addtl_info'
        out = []
        for _, row in merged_df.iterrows():
            logs = row.get('update_log') or []
            sample_type = row.get('sample_type_audit', '')

            # Fetch the corresponding audit_addtl_info for the row's copo_id
            copo_id = str(row.get('copo_id_audit', ''))
            audit_info = audit_addtl_info.get(copo_id, {})

            # Merge each log entry in the 'update_log_sample' with the corresponding 'audit_info'
            update_log = [log | audit_info for log in logs]

            if update_log:
                out.append({'sample_type': sample_type, 'update_log': update_log})
        return out


class TestObjectType(DAComponent):
    def __init__(self, profile_id=None):
        super(TestObjectType, self).__init__(profile_id=profile_id, component="test")


class Publication(DAComponent):
    def __init__(self, profile_id=None):
        super(Publication, self).__init__(profile_id, "publication")


class TextAnnotation(DAComponent):
    def __init__(self, profile_id=None):
        super(TextAnnotation, self).__init__(profile_id, "textannotation")

    def add_term(self, data):
        data["file_id"] = ObjectId(data["file_id"])
        id = self.get_collection_handle().insert_one(data)
        return id.inserted_id

    def get_all_for_file_id(self, file_id):
        records = self.get_collection_handle().find({"file_id": ObjectId(file_id)})
        return cursor_to_list_str(records, use_underscore_in_id=False)

    def remove_text_annotation(self, id):
        done = self.get_collection_handle().delete_one({"_id": ObjectId(id)})
        return done

    def update_text_annotation(self, id, data):
        data["file_id"] = ObjectId(data["file_id"])
        done = self.get_collection_handle().update_one(
            {"_id": ObjectId(id)}, {"$set": data}
        )
        return done

    def get_file_level_metadata_for_pdf(self, file_id):
        docs = self.get_collection_handle().find({"file_id": ObjectId(file_id)})
        if docs:
            return cursor_to_list_str(docs)


class MetadataTemplate(DAComponent):
    def __init__(self, profile_id=None):
        super(MetadataTemplate, self).__init__(profile_id, "metadata_template")

    def update_name(self, template_name, template_id):
        record = self.get_collection_handle().update_one(
            {"_id": ObjectId(template_id)}, {"$set": {"template_name": template_name}}
        )
        record = self.get_by_id(template_id)
        return record

    def get_by_id(self, id):
        record = self.get_collection_handle().find_one({"_id": ObjectId(id)})
        return record

    def update_template(self, template_id, data):
        record = self.get_collection_handle().update_one(
            {"_id": ObjectId(template_id)}, {"$set": {"terms": data}}
        )
        return record

    def get_terms_by_template_id(self, template_id):
        terms = self.get_collection_handle().find_one(
            {"_id": ObjectId(template_id)}, {"terms": 1, "_id": 0}
        )
        return terms


class Annotation(DAComponent):
    def __init__(self, profile_id=None):
        super(Annotation, self).__init__(profile_id, "annotation")

    def add_or_increment_term(self, data):
        # check if annotation is already present
        a = self.get_collection_handle().find_one(
            {"uid": data["uid"], "iri": data["iri"], "label": data["label"]}
        )
        if a:
            # increment
            return self.get_collection_handle().update_one(
                {"_id": a["_id"]}, {"$inc": {"count": 1}}
            )
        else:
            data["count"] = 1
            return self.get_collection_handle().insert_one(data).inserted_id

    def decrement_or_delete_annotation(self, uid, iri):
        a = self.get_collection_handle().find_one({"uid": uid, "iri": iri})
        if a:
            if a["count"] > 1:
                # decrement
                return self.get_collection_handle().update_one(
                    {"_id": a["_id"]}, {"$inc": {"count": -1}}
                )
            else:
                return self.get_collection_handle().delete_one({"_id": a["_id"]})
        else:
            return False

    def get_terms_for_user_alphabetical(self, uid):
        a = (
            self.get_collection_handle()
            .find({"uid": uid})
            .sort("label", pymongo.ASCENDING)
        )
        return cursor_to_list(a)

    def get_terms_for_user_ranked(self, uid):
        a = (
            self.get_collection_handle()
            .find({"uid": uid})
            .sort("count", pymongo.DESCENDING)
        )
        return cursor_to_list(a)

    def get_terms_for_user_by_dataset(self, uid):
        docs = self.get_collection_handle().aggregate(
            [
                {"$match": {"uid": uid}},
                {"$group": {"_id": "$file_id", "annotations": {"$push": "$$ROOT"}}},
            ]
        )
        data = cursor_to_list(docs)
        return data


class Person(DAComponent):
    def __init__(self, profile_id=None):
        super(Person, self).__init__(profile_id, "person")

    def get_people_for_profile(self):
        docs = self.get_collection_handle().find({'profile_id': self.profile_id})
        if docs:
            return docs
        else:
            return False

    def create_sra_person(self):
        """
        creates an (SRA) person record and attach to profile
        Returns:
        """

        people = self.get_all_records()
        sra_roles = list()
        for record in people:
            for role in record.get("roles", list()):
                sra_roles.append(role.get("annotationValue", str()))

        # has sra roles?
        has_sra_roles = all(
            x in sra_roles for x in ['SRA Inform On Status', 'SRA Inform On Error']
        )

        if not has_sra_roles:
            try:
                user = helpers.get_current_user()

                auto_fields = {
                    'copo.person.roles.annotationValue': 'SRA Inform On Status',
                    'copo.person.lastName': user.last_name,
                    'copo.person.firstName': user.first_name,
                    'copo.person.roles.annotationValue___0___1': 'SRA Inform On Error',
                    'copo.person.email': user.email,
                }
            except Exception as e:
                lg.exception(e)
                pass
            else:
                kwargs = dict()
                self.save_record(auto_fields, **kwargs)
        return


class CGCore(DAComponent):
    def __init__(self, profile_id=None):
        super(CGCore, self).__init__(profile_id, "cgcore")

    def get_component_schema(self, **kwargs):
        """
        function returns sub schema for a composite attribute
        :param kwargs:
        :return:
        """
        schema_fields = super(CGCore, self).get_component_schema()

        if not schema_fields:
            return list()

        referenced_field = kwargs.get("referenced_field", str())
        referenced_type = kwargs.get("referenced_type", str())

        if referenced_field:  # resolve dependencies
            schema_fields = [
                x
                for x in schema_fields
                if 'dependency' in x and x['dependency'] == referenced_field
            ]

            if not schema_fields:
                return list()

            # add an attribute to capture the referenced field - mark this as hidden for UI purposes
            dependent_record_label = 'dependency_id'
            new_attribute = copy.deepcopy(schema_fields[-1])
            new_attribute["id"] = new_attribute["id"].split(".")
            new_attribute["id"][-1] = dependent_record_label
            new_attribute["id"] = ".".join(new_attribute["id"])
            new_attribute["control"] = 'text'
            new_attribute["hidden"] = 'true'
            new_attribute["required"] = True
            new_attribute["help_tip"] = ''
            new_attribute["label"] = ''
            new_attribute["default_value"] = referenced_field
            new_attribute["show_in_form"] = True
            new_attribute["show_in_table"] = False
            new_attribute["versions"] = [dependent_record_label]
            schema_fields = [new_attribute] + schema_fields

        if referenced_type:  # set field constraints
            schema_df = CgCoreSchemas().resolve_field_constraint(
                schema=schema_fields, type_name=referenced_type
            )
            columns = list(schema_df.columns)

            for col in columns:
                schema_df[col].fillna('n/a', inplace=True)

            schema_fields = schema_df.sort_values(by=['field_constraint_rank']).to_dict(
                'records'
            )

            # delete non-relevant attributes
            for item in schema_fields:
                for k in columns:
                    if item[k] == 'n/a':
                        del item[k]

        for item in schema_fields:
            # set array types to string - child array types are accounted for by the parent
            item["type"] = "string"

        if schema_fields:
            # add a mandatory label field - for lookups and uniquely identifying a sub-record
            dependent_record_label = 'copo_name'
            new_attribute = copy.deepcopy(schema_fields[-1])
            new_attribute["id"] = new_attribute["id"].split(".")
            new_attribute["id"][-1] = dependent_record_label
            new_attribute["id"] = ".".join(new_attribute["id"])
            new_attribute["control"] = 'text'
            new_attribute["hidden"] = 'false'
            new_attribute["field_constraint"] = 'required'
            new_attribute["required"] = True
            new_attribute["unique"] = True
            new_attribute["help_tip"] = (
                'Please provide a unique label for this dependent record.'
            )
            new_attribute["label"] = 'Label'
            new_attribute["show_in_form"] = True
            new_attribute["show_in_table"] = True
            new_attribute["versions"] = [dependent_record_label]
            new_attribute["field_constraint_rank"] = 1
            schema_fields = [new_attribute] + schema_fields

        return schema_fields

    def get_all_records(self, sort_by='_id', sort_direction=-1, **kwargs):
        doc = dict(deleted=helpers.get_not_deleted_flag())
        if self.profile_id:
            doc["profile_id"] = self.profile_id

        referenced_field = kwargs.get("referenced_field", str())

        if referenced_field:
            doc["dependency_id"] = referenced_field

        return cursor_to_list(
            self.get_collection_handle().find(doc).sort([[sort_by, sort_direction]])
        )

    def save_record(self, auto_fields=dict(), **kwargs):
        all_keys = [x.lower() for x in auto_fields.keys() if x]
        schema_fields = self.get_component_schema()
        schema_fields = [x for x in schema_fields if x["id"].lower() in all_keys]

        schema_fields.append(dict(id="dependency_id", type="string", control="text"))
        schema_fields.append(dict(id="date_modified", type="string", control="text"))
        schema_fields.append(dict(id="deleted", type="string", control="text"))
        schema_fields.append(dict(id="date_created", type="string", control="text"))
        schema_fields.append(dict(id="profile_id", type="string", control="text"))

        # get dependency id
        dependency_id = [
            v for k, v in auto_fields.items() if k.split(".")[-1] == "dependency_id"
        ]
        kwargs["dependency_id"] = dependency_id[0] if dependency_id else ''
        kwargs["schema"] = schema_fields

        return super(CGCore, self).save_record(auto_fields, **kwargs)


'''
class Repository(DAComponent):
    def __init__(self, profile=None):
        super(Repository, self).__init__(None, "repository")

    def get_by_uid(self, uid):
        doc = self.get_collection_handle().find({"uid": uid}, {"name": 1, "type": 1, "url": 1})
        return doc

    def get_from_list(self, repo_list):
        oids = list(map(lambda x: ObjectId(x), repo_list))
        docs = self.get_collection_handle().find({"_id": {"$in": oids}, "personal": True}, {"apikey": 0})
        return cursor_to_list_str(docs, use_underscore_in_id=False)

    def get_by_ids(self, uids):
        doc = list()
        if (uids):
            oids = list(map(lambda x: ObjectId(x), uids))
            doc = self.get_collection_handle().find({"_id": {"$in": oids}})
        return cursor_to_list(doc)

    def get_by_username(self, username):
        doc = self.get_collection_handle().find({"username": username})
        return doc

    def get_users(self, repo_id):
        doc = self.get_collection_handle().find_one({"_id": ObjectId(repo_id)})
        return doc['users']

    def push_user(self, repo_id, uid, first_name, last_name, username, email):
        args = {'uid': uid, "first_name": first_name, "last_name": last_name, "username": username, "email": email}
        return self.get_collection_handle().update(
            {'_id': ObjectId(repo_id)},
            {'$push': {'users': args}}
        )

    def pull_user(self, repo_id, user_id):
        doc = self.get_collection_handle().update({'_id': ObjectId(repo_id)},
                                                  {'$pull': {'users': {'uid': user_id}}})

        return doc

    def add_personal_dataverse(self, url, name, apikey, type, username, password):
        u = ThreadLocal.get_current_user()
        doc = self.get_collection_handle().insert(
            {"isCG": False, "url": url, "name": name, "apikey": apikey, "personal": True, "uid": u.id, "type": type,
             "username": username, "password": password})
        udetails = u.userdetails
        udetails.repo_submitter.append(str(doc))
        udetails.save()
        return doc

    def validate_record(self, auto_fields=dict(), validation_result=dict(), **kwargs):
        """
        validates record. useful before CRUD actions
        :param auto_fields:
        :param validation_result:
        :param kwargs:
        :return:
        """

        if validation_result.get("status", True) is False:  # no need continuing with validation, propagate error
            return super(Repository, self).validate_record(auto_fields, result=validation_result, **kwargs)

        local_result = dict(status=True, message="")
        kwargs["validate_only"] = True  # causes the subsequent call to save_record to do everything else but save

        new_record = super(Repository, self).save_record(auto_fields, **kwargs)
        new_record_id = kwargs.get("target_id", str())

        existing_records = cursor_to_list(
            self.get_collection_handle().find({}, {"name": 1, "type": 1, "visibility": 1}))

        # check for uniqueness of name - repository names must be unique!
        same_name_records = [str(x["_id"]) for x in existing_records if
                             x.get("name", str()).strip().lower() == new_record.get("name", str()).strip().lower()]

        uniqueness_error = "Action error: duplicate repository name is not allowed."
        if len(same_name_records) > 1:
            # multiple duplicate names, shouldn't be
            local_result["status"] = False
            local_result["message"] = uniqueness_error

            return super(Repository, self).validate_record(auto_fields, validation_result=local_result, **kwargs)
        elif len(same_name_records) == 1 and new_record_id != same_name_records[0]:
            local_result["status"] = False
            local_result["message"] = uniqueness_error

            return super(Repository, self).validate_record(auto_fields, validation_result=local_result, **kwargs)

        # check repo visibility constraint - i.e. one public repository per repository type
        if new_record.get("visibility", str()).lower() == 'public':
            same_visibility_records = [str(x["_id"]) for x in existing_records if
                                       x.get("type", str()).strip().lower() == new_record.get("type",
                                                                                              str()).strip().lower()
                                       and x.get("visibility", str()).lower() == 'public']

            visibility_error = "Action error: multiple public instances of the same repository type is not allowed."
            if len(same_visibility_records) > 1:
                local_result["status"] = False
                local_result[
                    "message"] = visibility_error
                return super(Repository, self).validate_record(auto_fields, validation_result=local_result, **kwargs)
            elif len(same_visibility_records) == 1 and new_record_id != same_visibility_records[0]:
                local_result["status"] = False
                local_result[
                    "message"] = visibility_error
                return super(Repository, self).validate_record(auto_fields, validation_result=local_result, **kwargs)

        return super(Repository, self).validate_record(auto_fields, validation_result=local_result, **kwargs)

    def delete(self, repo_id):
        # have to delete repo id from UserDetails model as well as remove mongo record
        uds = UserDetails.objects.filter(repo_manager__contains=[repo_id])
        for ud in uds:
            ud.repo_manager.remove(repo_id)
            ud.save()
        uds = UserDetails.objects.filter(repo_submitter__contains=[repo_id])
        for ud in uds:
            ud.repo_submitter.remove(repo_id)
            ud.save()
        doc = self.get_collection_handle().remove({"_id": ObjectId(repo_id)})
        return doc

    def validate_and_delete(self, target_id=str(), target_ids=list()):
        """
        function deletes repository only if there are no dependent records
        :param target_id:
        :return:
        """

        repository_id = target_id

        result = dict(status='success', message="")

        if not repository_id:
            return dict(status='error', message="Repository record identifier not found!")

        # any dependent submission record?

        count_submissions = Submission().get_collection_handle().find(
            {"destination_repo": repository_id, 'deleted': helpers.get_not_deleted_flag()}).count()

        if count_submissions > 0:
            return dict(status='error', message="Action not allowed: dependent records exist!")

        uds = UserDetails.objects.filter(repo_manager__contains=[repository_id])
        for ud in uds:
            ud.repo_manager.remove(repository_id)
            ud.save()

        uds = UserDetails.objects.filter(repo_submitter__contains=[repository_id])
        for ud in uds:
            ud.repo_submitter.remove(repository_id)
            ud.save()
        self.get_collection_handle().remove({"_id": ObjectId(repository_id)})

        return result
'''
'''
class RemoteDataFile:
    def __init__(self, profile_id=None):
        self.RemoteFileCollection = get_collection_ref(RemoteFileCollection)
        self.profile_id = profile_id

    def GET(self, id):
        doc = self.RemoteFileCollection.find_one({"_id": ObjectId(id)})
        if not doc:
            pass
        return doc

    def get_by_sub_id(self, sub_id):
        doc = self.RemoteFileCollection.find_one({"submission_id": sub_id})
        return doc

    def create_transfer(self, submission_id, file_path=None):
        # before creating a new transfer record for this submission, remove all others
        remote_record = self.get_by_sub_id(submission_id)
        if remote_record:
            self.delete_transfer(str(remote_record["_id"]))

        fields = helpers.json_to_pytype(DB_TEMPLATES['REMOTE_FILE_COLLECTION'])
        fields['submission_id'] = submission_id
        fields['profile_id'] = self.profile_id
        fields['file_path'] = file_path
        transfer_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        fields["commenced_on"] = transfer_time
        fields["current_time"] = transfer_time
        fields["transfer_rate"] = ""

        if file_path:
            d = DataFile().GET(submission_id)
            chunked_upload = ChunkedUpload.objects.get(id=int(d['file_id']))
            fields["file_size_bytes"] = u.filesize_toString(chunked_upload.offset)

        doc = self.RemoteFileCollection.insert(fields)

        # return inserted record
        df = self.GET(str(doc))
        return df

    def delete_transfer(self, transfer_id):
        self.RemoteFileCollection.delete_one({'_id': ObjectId(transfer_id)})

    def update_transfer(self, transfer_id, fields, delete="0"):

        fields["deleted"] = delete
        if 'transfer_rate' in fields:
            # speed = fields.pop("transfer_rate")

            self.RemoteFileCollection.update(
                {
                    "_id": ObjectId(transfer_id)
                },
                {
                    # '$push': {"transfer_rate": float(speed)},
                    '$set': fields
                }
            )
        else:
            self.RemoteFileCollection.update(
                {
                    "_id": ObjectId(transfer_id)
                },
                {
                    '$set': fields
                }
            )

    def get_all_records(self):
        doc = {'profile_id': self.profile_id, 'deleted': helpers.get_not_deleted_flag()}
        return cursor_to_list(self.RemoteFileCollection.find(doc))

    def get_by_datafile(self, datafile_id):
        doc = {'datafile_id': ObjectId(datafile_id), 'deleted': helpers.get_not_deleted_flag()}
        return cursor_to_list(self.RemoteFileCollection.find(doc))

    def sanitise_remote_files(self):
        pass

'''


class Stats(DAComponent):
    def __init__(self, profile=None):
        super(Stats, self).__init__(profile, "stats")

    def update_stats(self):
        datafiles = DataFile().get_collection_handle().count_documents({})
        profiles = handle_dict["profile"].count_documents({})
        samples = handle_dict["sample"].count_documents({})
        users = users = len(User.objects.all())
        out = {
            "datafiles": datafiles,
            "profiles": profiles,
            "samples": samples,
            "users": users,
            "date": str(date.today()),
        }
        self.get_collection_handle().insert_one(out)


class Description(DAComponent):
    def __init__(self, profile_id=None):
        super(Description, self).__init__(profile_id, "description")
        self.DescriptionCollection = self.get_collection_handle()

    def GET(self, id):
        doc = self.DescriptionCollection.find_one({"_id": ObjectId(id)})
        if not doc:
            pass
        return doc

    def create_description(
        self,
        stages=list(),
        attributes=dict(),
        profile_id=str(),
        component=str(),
        meta=dict(),
        name=str(),
    ):
        self.component = component

        fields = dict(
            stages=stages,
            attributes=attributes,
            profile_id=profile_id,
            component=component,
            meta=meta,
            name=name,
            created_on=helpers.get_datetime(),
        )

        doc = self.DescriptionCollection.insert_one(fields)

        # return inserted record
        df = self.GET(str(doc.inserted_id))
        return df

    def edit_description(self, description_id, fields):
        self.DescriptionCollection.update_one(
            {"_id": ObjectId(description_id)}, {'$set': fields}
        )

    def delete_description(self, description_ids=list()):
        object_ids = []
        for id in description_ids:
            object_ids.append(ObjectId(id))

        self.DescriptionCollection.delete_many({"_id": {"$in": object_ids}})

    def get_all_descriptions(self):
        return cursor_to_list(self.DescriptionCollection.find())

    def get_all_records_columns(
        self, sort_by='_id', sort_direction=-1, projection=dict(), filter_by=dict()
    ):
        return cursor_to_list(
            self.DescriptionCollection.find(filter_by, projection).sort(
                [[sort_by, sort_direction]]
            )
        )

    def is_valid_token(self, description_token):
        is_valid = False

        if description_token:
            if self.DescriptionCollection.find_one(
                {"_id": ObjectId(description_token)}
            ):
                is_valid = True

        return is_valid

    def get_elapsed_time_dataframe(self):
        pipeline = [
            {
                "$project": {
                    "_id": 1,
                    "diff_days": {
                        "$divide": [
                            {"$subtract": [helpers.get_datetime(), "$created_on"]},
                            1000 * 60 * 60 * 24,
                        ]
                    },
                }
            }
        ]
        description_df = pd.DataFrame(
            cursor_to_list(self.DescriptionCollection.aggregate(pipeline))
        )

        return description_df

    def remove_store_object(self, object_path=str()):
        if os.path.exists(object_path):
            import shutil

            shutil.rmtree(object_path)


class Barcode(DAComponent):
    def __init__(self, profile_id=None):
        super(Barcode, self).__init__(profile_id, "barcode")

    def add_sample_id(self, specimen_id, sample_id):
        self.get_collection_handle().update_many(
            {"specimen_id": specimen_id},
            {"$set": {"sample_id": sample_id, "specimen_id": specimen_id}},
            upsert=True,
        )


class EnaFileTransfer(DAComponent):
    def __init__(self, profile_id=None):
        super(EnaFileTransfer, self).__init__(profile_id, "enaFileTransfer")
        self.profile_id = profile_id
        # self.component = str()

    def get_pending_transfers(self):
        result_list = []
        result = (
            self.get_collection_handle()
            .find({"transfer_status": {"$ne": 2}, "status": "pending"})
            .limit(10)
        )

        if result:
            result_list = list(result)
        # at most download 2 files at the sametime
        count = self.get_collection_handle().count_documents(
            {"transfer_status": 2, "status": "processing"}
        )
        if count <= 1:
            result = (
                self.get_collection_handle()
                .find({"transfer_status": 2, "status": "pending"})
                .limit(2 - count)
            )
            if result:
                result_list.extend(list(result))
        return result_list

    def get_processing_transfers(self):
        return self.get_collection_handle().find(
            {"transfer_status": {"$gt": 0}, "status": "processing"}
        )

    def set_processing(self, tx_ids):
        self.get_collection_handle().update_many(
            {"_id": {"$in": tx_ids}},
            {"$set": {"status": "processing", "last_checked": helpers.get_datetime()}},
        )

    def set_pending(self, tx_id):
        self.get_collection_handle().update_one(
            {"_id": ObjectId(tx_id)},
            {"$set": {"status": "pending", "last_checked": helpers.get_datetime()}},
        )

    def set_complete(self, tx_id):
        self.get_collection_handle().update_one(
            {"_id": ObjectId(tx_id)},
            {"$set": {"status": "complete", "last_checked": helpers.get_datetime()}},
        )

    def get_transfer_status_by_local_path(self, profile_id, local_paths):
        # return self.get_collection_handle().find({"profile_id"})
        result = self.get_collection_handle().find(
            {"local_path": {"$in": local_paths}, "profile_id": profile_id},
            {"transfer_status": 1, "local_path": 1},
        )
        result_map = {x["local_path"]: x["transfer_status"] for x in list(result)}
        return result_map

    def get_transfer_status_by_ecs_path(self, ecs_locations):
        result = self.get_collection_handle().find(
            {"ecs_location": {"$in": ecs_locations}},
            {"transfer_status": 1, "ecs_location": 1, "status": 1},
        )
        result_map = {x["ecs_location"]: x["status"] for x in list(result)}
        return result_map

    def update_transfer_status_by_ecs_path(self, ecs_locations, status):
        self.get_collection_handle().update_many(
            {"ecs_location": {"$in": ecs_locations}, "transfer_status": 0},
            {"$set": {"status": status}},
        )


class APIValidationReport(DAComponent):
    def __init__(self, profile_id=None):
        super(APIValidationReport, self).__init__(profile_id, "apiValidationReport")

    def setComplete(self, report_id):
        self.get_collection_handle().update_one(
            {"_id": ObjectId(report_id)}, {"$set": {"status": "complete"}}
        )

    def setRunning(self, report_id):
        self.get_collection_handle().update_one(
            {"_id": ObjectId(report_id)}, {"$set": {"status": "running"}}
        )

    def setFailed(self, report_id, msg):
        # make tuple list of text replacements for html elements
        replacements = list()
        replacements.append(("<h4>", "\r"))
        replacements.append(("</h4>", ""))
        replacements.append(("<ol>", ""))
        replacements.append(("</ol>", ""))
        replacements.append(("<li>", "\r"))
        replacements.append(("</li>", ""))
        replacements.append(("<strong>", ""))
        replacements.append(("</strong>", ""))
        for el in replacements:
            msg = msg.replace(el[0], el[1])
        self.get_collection_handle().update_one(
            {"_id": ObjectId(report_id)}, {"$set": {"status": "failed", "content": msg}}
        )


'''

class TaggedSequenceChecklist(DAComponent):
    def __init__(self, profile_id=None):
        super(TaggedSequenceChecklist, self).__init__(
            profile_id, "taggedSequenceChecklist")

    def get_checklist(self, checklist_id):
        return self.execute_query({"primary_id": checklist_id})

    def get_checklists(self):
        return self.get_all_records_columns(projection={"primary_id": 1, "name": 1, "description": 1})
'''


class CopoGroup(DAComponent):
    def __init__(self):
        super(CopoGroup, self).__init__(None, "group")
        self.Group = self.get_collection_handle()

    def get_by_owner(self, owner_id):
        doc = self.Group.find({'owner_id': owner_id})
        if not doc:
            return list()
        return doc

    def get_group_names(self, owner_id=None, with_id=False):
        if not owner_id:
            owner_id = helpers.get_user_id()

        projection = {'_id': 1, 'name': 1} if with_id else {'_id': 0, 'name': 1}

        doc = list(self.Group.find({'owner_id': owner_id}, projection))
        if not doc:
            return list()
        return doc

    def create_shared_group(self, name, description, owner_id=None):
        group_names = self.get_group_names()
        group_fields = helpers.json_to_pytype(DB_TEMPLATES['COPO_GROUP'])

        if not owner_id:
            owner_id = helpers.get_user_id()

        if any(x['name'] == name for x in group_names):
            return False  # Group name already exists
        else:
            group_fields['owner_id'] = owner_id
            group_fields['name'] = name
            group_fields['description'] = description
            group_fields['date_created'] = helpers.get_datetime().strftime(
                "%d-%m-%Y %H:%M:%S"
            )
            # Get inserted document ID
            return self.Group.insert_one(group_fields).inserted_id

    def edit_group(self, group_id, name, description):
        update_info = {}
        group_names = self.get_group_names(with_id=True)

        update_info['name'] = name
        update_info['description'] = description
        update_info['date_modified'] = helpers.get_datetime().strftime(
            "%d-%m-%Y %H:%M:%S"
        )

        if any(str(x['_id']) == group_id and x['name'] != name for x in group_names):
            # If edited group name is not equal to the exisiting group name
            # but the group ID is the matchES the current group ID, update the document
            self.Group.find_one_and_update(
                {"_id": ObjectId(group_id)}, {"$set": update_info}
            )
            return True
        elif any(str(x['_id']) != group_id and x['name'] == name for x in group_names):
            # If edited group name is equal to an exisiting group name
            # and the group ID does not match the current group ID, return an error
            return False  # Group name already exists
        else:
            # Update document
            self.Group.find_one_and_update(
                {"_id": ObjectId(group_id)}, {"$set": update_info}
            )
            return True

    def delete_group(self, group_id):
        result = self.Group.delete_one({'_id': ObjectId(group_id)})
        return result.deleted_count > 0

    def view_shared_group(self, group_id):
        group = cursor_to_list(self.Group.find({"_id": ObjectId(group_id)}))

        if group:
            return group[0]
        else:
            return False

    def get_shared_users_info_by_owner_and_profile_id(self, owner_id, profile_id):
        # Get user name and email address of the shared users of a profile
        # NB: 'member_ids' is a list of dictionaries containing user IDs
        member_ids = cursor_to_list(
            self.Group.find(
                {'owner_id': owner_id, 'shared_profile_ids': {'$in': [profile_id]}},
                {'_id': 0, 'member_ids': 1},
            )
        )
        shared_users_info = list()

        if not member_ids:
            return list()

        for u in member_ids:
            user_ids = u.get('member_ids', list())
            for id in user_ids:
                shared_user = User.objects.get(pk=id)
                x = {
                    'email': shared_user.email,
                    'name': f"{shared_user.first_name} {shared_user.last_name}",
                }
                shared_users_info.append(x)
        return shared_users_info

    def add_profile(self, group_id, profile_id):
        return self.Group.update_one(
            {'_id': ObjectId(group_id)},
            {'$push': {'shared_profile_ids': ObjectId(profile_id)}},
        )

    def remove_profile(self, group_id, profile_id):
        return self.Group.update_one(
            {'_id': ObjectId(group_id)},
            {'$pull': {'shared_profile_ids': ObjectId(profile_id)}},
        )

    def get_profiles_for_group_info(self, group_id):
        from .profile_da import Profile

        # If current logged in  user is in the 'data_manager' group i.e.
        # if current user is a member of the  COPO development team, return all profiles
        # If not, return only the profiles for the current logged in user
        member_groups = helpers.get_group_membership_asString()
        profiles = (
            Profile().get_profiles(filter='all_profiles')
            if 'data_managers' in member_groups
            else Profile().get_for_user(helpers.get_user_id())
        )
        p_list = cursor_to_list(profiles)

        # Sort list of profiles by 'title' key
        p_list = sorted(p_list, key=lambda x: x['title'])

        group = CopoGroup().get_record(group_id)

        if p_list and group:
            for p in p_list:
                if p['_id'] in group['shared_profile_ids']:
                    p['selected'] = True
                else:
                    p['selected'] = False
        return p_list

    '''
    def get_repos_for_group_info(self, uid, group_id):
        g = CopoGroup().get_record(group_id)
        docs = cursor_to_list(Repository().Repository.find({'users.uid': uid}))
        for d in docs:
            if d['_id'] in g['repo_ids']:
                d['selected'] = True
            else:
                d['selected'] = False
        return list(docs)
    '''

    def get_users_for_group_info(self, group_id):
        group = CopoGroup().get_record(group_id)
        user_list = list()

        if group:
            member_ids = group['member_ids']

            for u in member_ids:
                usr = User.objects.get(pk=u)
                x = {
                    'id': usr.id,
                    'first_name': usr.first_name,
                    'last_name': usr.last_name,
                    'email': usr.email,
                    'username': usr.username,
                }
                user_list.append(x)
        return user_list

    def add_user_to_group(self, group_id, user_id):
        return self.Group.update_one(
            {'_id': ObjectId(group_id)}, {'$push': {'member_ids': user_id}}
        )

    def remove_user_from_group(self, group_id, user_id):
        return self.Group.update_one(
            {'_id': ObjectId(group_id)}, {'$pull': {'member_ids': user_id}}
        )

    def add_repo(self, group_id, repo_id):
        return self.Group.update_one(
            {'_id': ObjectId(group_id)}, {'$push': {'repo_ids': ObjectId(repo_id)}}
        )

    def remove_repo(self, group_id, repo_id):
        return self.Group.update_one(
            {'_id': ObjectId(group_id)}, {'$pull': {'repo_ids': ObjectId(repo_id)}}
        )


class DataFile(DAComponent):
    def __init__(self, profile_id=None):
        super(DataFile, self).__init__(profile_id=profile_id, component="datafile")

    def get_for_profile(self, profile_id):
        docs = self.get_collection_handle().find({"profile_id": profile_id})
        return docs

    def get_by_file_id(self, file_id=None):
        docs = None
        if file_id:
            docs = self.get_collection_handle().find_one(
                {"file_id": file_id, "deleted": helpers.get_not_deleted_flag()}
            )

        return docs

    def get_by_file_name_id(self, file_id=None):
        docs = None
        if file_id:
            docs = self.get_collection_handle().find_one(
                {"_id": ObjectId(file_id), "deleted": helpers.get_not_deleted_flag()},
                {"name": 1},
            )

        return docs

    def get_image_filenames_by_specimen_id(self, specimen_ids):
        from .sample_da import Sample

        # Get sample by SPECIMEN_ID
        samples = Sample().get_by_field('SPECIMEN_ID', specimen_ids)
        sample_type = samples[0].get('sample_type', '') if samples else ''
        sample_type = "DToL" if sample_type in ["asg", "dtol"] else sample_type.upper()

        # Specimen is also known as the  source
        specimen = (
            samples[0].get(
                'sampleDerivedFrom',
                samples[0].get('sampleSameAs', samples[0].get('sampleSymbiontOf', '')),
            )
            if samples
            else ''
        )

        # Match the SPECIMEN_ID to the beginning of the image file name using regex
        specimen_ids_regex = [regex.Regex(f'^{x}') for x in specimen_ids]
        projection = {'_id': 0, 'file_location': 1}

        filter = dict()
        filter['name'] = {'$in': specimen_ids_regex}
        filter['bucket_name'] = {'$exists': False}
        filter['ecs_location'] = {'$exists': False}
        filter['file_location'] = {'$exists': True, '$ne': ''}

        cursor = self.get_collection_handle().find(filter, projection)

        results = list(cursor)

        # Get values from the list of dictionaries
        image_filenames = [
            x for element in results for x in element.values() if results
        ]

        # Check if the filename path exists in the database
        # If it does not, retrieve the image file path from BioImage Archive (BIA)
        bia_image_file_prefix = f'{settings.BIA_IMAGE_URL_PREFIX}{sample_type}/'
        if image_filenames and specimen:
            for x in image_filenames:
                filename = os.path.basename(x)

                lg.log(f'Checking for BIA image URL for: {filename}')

                bia_image_url = helpers.check_and_save_bia_image_url(
                    f'{bia_image_file_prefix}{filename}'
                )
                if bia_image_url and not os.path.exists(x) and bia_image_url != x:
                    lg.log('BIA image URL found...updating image URL')

                    image_filenames[image_filenames.index(x)] = bia_image_url
                    _, _, file_suffix = filename.rpartition('_')

                    filter['name'] = (
                        f'{specimen_ids[image_filenames.index(x)]}-{file_suffix}'
                    )
                    self.get_collection_handle().update_one(
                        filter,
                        {'$set': {'file_location': bia_image_url}},
                    )

        # Return a list of image file paths ensuring that it beings with '/'
        image_filenames = [
            f'/{x}' if not x.startswith('/') else x for x in image_filenames
        ]
        return image_filenames

    def get_record_property(self, datafile_id=str(), elem=str()):
        """
        eases the access to deeply nested properties
        :param datafile_id: record id
        :param elem: schema property(key)
        :return: requested property or some default value
        """

        datafile = self.get_record(datafile_id)
        description = datafile.get("description", dict())
        description_attributes = description.get("attributes", dict())
        description_stages = description.get("stages", list())

        property_dict = dict(
            target_repository=description_attributes.get(
                "target_repository", dict()
            ).get("deposition_context", str()),
            attach_samples=description_attributes.get("attach_samples", dict()).get(
                "study_samples", str()
            ),
            sequencing_instrument=description_attributes.get(
                "nucleic_acid_sequencing", dict()
            ).get("sequencing_instrument", str()),
            study_type=description_attributes.get("study_type", dict()).get(
                "study_type", str()
            ),
            description_attributes=description_attributes,
            description_stages=description_stages,
        )

        return property_dict.get(elem, str())

    def add_fields_to_datafile_stage(self, target_ids, fields, target_stage_ref):

        for target_id in target_ids:
            # for each file in target_ids retrieve the datafile object
            df = self.get_record(target_id)
            # get the stage using list comprehension and add new fields
            for idx, stage in enumerate(df['description']['stages']):
                if 'ref' in stage and stage['ref'] == target_stage_ref:
                    for field in fields:
                        df['description']['stages'][idx]['items'].append(field)

            # now update datafile record
            self.get_collection_handle().update_one(
                {'_id': ObjectId(target_id)},
                {'$set': {'description.stages': df['description']['stages']}},
            )

    def update_file_level_metadata(self, file_id, data):
        self.get_collection_handle().update_one(
            {"_id": ObjectId(file_id)}, {"$push": {"file_level_annotation": data}}
        )
        return self.get_file_level_metadata_for_sheet(file_id, data["sheet_name"])

    def insert_sample_ids(self, file_name, sample_ids):
        self.get_collection_handle().update_one(
            {"name": file_name},
            {
                "$push": {
                    "description.attributes.attach_samples.study_samples": {
                        "$each": sample_ids
                    }
                }
            },
        )

    def update_bioimage_name(self, file_name, bioimage_name, bioimage_path):
        self.get_collection_handle().update_one(
            {"name": file_name},
            {"$set": {"bioimage_name": bioimage_name, "file_location": bioimage_path}},
        )

    def get_file_level_metadata_for_sheet(self, file_id, sheetname):

        docs = self.get_collection_handle().aggregate(
            [
                {"$match": {"_id": ObjectId(file_id)}},
                {"$unwind": "$file_level_annotation"},
                {"$match": {"file_level_annotation.sheet_name": sheetname}},
                {"$project": {"file_level_annotation": 1, "_id": 0}},
                {"$sort": {"file_level_annotation.column_idx": 1}},
            ]
        )
        return cursor_to_list(docs)

    def delete_annotation(self, col_idx, sheet_name, file_id):
        docs = self.get_collection_handle().update_one(
            {"_id": ObjectId(file_id)},
            {
                "$pull": {
                    "file_level_annotation": {
                        "sheet_name": sheet_name,
                        "column_idx": str(col_idx),
                    }
                }
            },
        )
        return docs

    def get_num_pending_samples(self, sub_id):
        doc = self.get_collection_handle().find_one({"_id", ObjectId(sub_id)})

    def get_records_by_field(self, field, value):
        sub = self.get_collection_handle().find({field: value})
        return cursor_to_list(sub)

    def get_records_by_fields(self, fields):
        sub = self.get_collection_handle().find(fields)
        return cursor_to_list(sub)

    def get_datafile_names_by_name_regx(self, names):
        regex_names = [re.compile(f"^{name}") for name in names]
        sub = self.get_collection_handle().find(
            {
                "name": {"$in": regex_names},
                "bioimage_name": {"$ne": ""},
                "deleted": helpers.get_not_deleted_flag(),
            },
            {"name": 1, "_id": 0},
        )
        datafiles = cursor_to_list(sub)
        result = [i["name"] for i in datafiles if i['name']]
        return set(result)

    def update_file_hash(self, file_oid, file_hash):
        self.get_collection_handle().update_one(
            {"_id": file_oid}, {"$set": {"file_hash": file_hash}}
        )


class EnaChecklist(DAComponent):
    def __init__(self, profile_id=None):
        super(EnaChecklist, self).__init__(
            profile_id=profile_id, component="enaChecklist"
        )

    def get_checklist(self, checklist_id):
        checklists = self.execute_query({"primary_id": checklist_id})
        if checklists:
            if checklist_id == 'read':
                fields = checklists[0].get("fields", {})
                fields = {k: v for k, v in fields.items() if v.get("for_dtol", True)}
                checklist = checklists[0]
                checklist["fields"] = fields
                return checklist

            return checklists[0]

    def get_barcoding_checklists_no_fields(self):
        return self.get_all_records_columns(
            filter_by={"primary_id": {"$in": settings.BARCODING_CHECKLIST}},
            projection={"primary_id": 1, "name": 1, "description": 1},
        )

    def get_sample_checklists_no_fields(self):
        return self.get_all_records_columns(
            filter_by={"primary_id": {"$regex": "^(ERC|read)"}},
            projection={"primary_id": 1, "name": 1, "description": 1},
        )


'''
class EnaObject(DAComponent):
    def __init__(self, profile_id=None):
        super(EnaObject, self).__init__(profile_id, "enaObject")

    def get_schema(self, target_id=str()):
        if not target_id:
            return dict(schema_dict=[],
                        schema=[]
                        )
        taggedSeq = EnaObject(self.profile_id).get_record(target_id)
        fields = []
        if taggedSeq:
            checklist = EnaChecklist().execute_query(
                {"primary_id": taggedSeq["checklist_id"]})
            if checklist:
                for key, field in checklist[0].get("fields", {}).items():
                    if taggedSeq.get(key, ""):
                        field["id"] = key
                        field["show_as_attribute"] = True
                        field["label"] = field["name"]
                        field.pop("name")
                        field["control"] = "text"
                        if field["type"] == "TEXT_AREA_FIELD":
                            field["control"] = "textarea"

                    fields.append(field)

            return dict(schema_dict=fields,
                        schema=fields
                        )

    def validate_and_delete(self, target_id=str(), target_ids=list()):
        if not target_ids:
            target_ids = []
        if target_id:
            target_ids.append(target_id)

        tagged_seq_ids = [ObjectId(id) for id in target_ids]
        result = self.execute_query({"_id": {"$in": tagged_seq_ids},  "$or": [{"accession": {
                                    "$exists": True, "$ne": ""}}, {"status": {"$exists": True, "$ne": "pending"}}]})
        if result:
            return dict(status='error', message="One or more Ena object/s have been accessed or scheduled to submit!")

        self.get_collection_handle().delete_many(
            {"_id": {"$in":   tagged_seq_ids}})
        return dict(status='success', message="Ena object/s have been deleted!")

    def update_ena_object_processing(self, profile_id=str(), tagged_seq_ids=list()):
        tagged_seq_obj_ids = [ObjectId(id) for id in tagged_seq_ids]
        self.get_collection_handle().update_many({"profile_id": profile_id,  "_id": {"$in":   tagged_seq_obj_ids},  "$or": [{"status": {"$exists": False}}, {"status": "pending"}]},
                                                 {"$set": {"status":  "processing"}})
'''

'''
class Read(DAComponent):
    def __init__(self, profile_id=None):
        super(Read, self).__init__(profile_id, "read")

    def get_schema(self, target_id):

        if not target_id:
            return dict(schema_dict=[], schemas=[])

        read = Read(self.profile_id).get_record(target_id)
        fields = []
        if read:
            checklist = EnaChecklist().execute_query(
                {"primary_id": read["checklist_id"]})
            if checklist:
                for key, field in checklist[0].get("fields", {}).items():
                    if read.get(key, ""):
                        field["id"] = key
                        field["show_as_attribute"] = True
                        field["label"] = field["name"]
                        field.pop("name")
                        field["control"] = "text"
                        if field["type"] == "TEXT_AREA_FIELD":
                            field["control"] = "textarea"

                    fields.append(field)

            return dict(schema_dict=fields,
                        schema=fields
                        )
'''
