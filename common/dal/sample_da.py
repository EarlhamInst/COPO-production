
from datetime import datetime, timezone
from pymongo import ReturnDocument
from django.conf import settings
from django_tools.middlewares import ThreadLocal
from common.dal.mongo_util import cursor_to_list, cursor_to_list_str, cursor_to_list_no_ids
from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES, SANGER_TOL_PROFILE_TYPES, PERMIT_FILENAME_COLUMN_NAMES
from pymongo.collection import ReturnDocument
from common.utils import helpers
from bson.objectid import ObjectId

from src.apps.copo_core.models import SequencingCentre
from .copo_base_da import DAComponent, handle_dict
import shortuuid
 

lg = settings.LOGGER

class Source(DAComponent):
    def __init__(self, profile_id=None):
        super(Source, self).__init__(profile_id, "source")

    def get_from_profile_id(self, profile_id):
        return self.get_collection_handle().find({'profile_id': profile_id})

    def get_specimen_biosample(self, value):
        return cursor_to_list(
            self.get_collection_handle().find(
                {"sample_type": {"$in": ["dtol_specimen", "asg_specimen", "erga_specimen"]},
                 "SPECIMEN_ID": value}))

    def add_accession(self, biosample_accession, sra_accession, submission_accession, oid):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$set":
                {
                    'biosampleAccession': biosample_accession,
                    'sraAccession': sra_accession,
                    'submissionAccession': submission_accession,
                    'status': 'accepted'}
             })

    def get_by_specimen(self, value):
        # todo can this be find one
        return cursor_to_list(self.get_collection_handle().find({"SPECIMEN_ID": value}))

    def get_sourcemap_by_specimens(self, value):
        sources = cursor_to_list(self.get_collection_handle().find(
            {"SPECIMEN_ID": {"$in": value}}))
        source_map = {}
        for source in sources:
            source_map[source["SPECIMEN_ID"]] = source
        return source_map

    def get_by_specimen_id_regex(self, value):
        # Get sources from Mongo database similar to SQL's '%' operator or 'LIKE'
        return cursor_to_list(
            self.get_collection_handle().find({"SPECIMEN_ID": {'$regex': value, '$options': 'i'}}))

    def get_by_field(self, field, value):
        return cursor_to_list(self.get_collection_handle().find({field: value}))

    def add_fields(self, fieldsdict, oid):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$set":
             fieldsdict
             }
        )

    def add_rejected_status(self, status, oid):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$set":
             {'error': status["msg"],
              'status': "rejected"}
             }
        )

    def update_field(self, field, value, oid):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$set":
                {field: value, 'date_modified': datetime.now(timezone.utc).replace(microsecond=0), 'time_updated': datetime.now(
                    timezone.utc).replace(microsecond=0), 'update_type': 'system'}
             })

    def update_public_name(self, name):
        self.get_collection_handle().update_many(
            {"SPECIMEN_ID": name['specimen']["specimenId"],
                "TAXON_ID": str(name['species']["taxonomyId"])},
            {"$set": {"public_name": name.get("tolId", "")}})

    def record_manual_update(self, field, old, new, oid):
        if not self.get_collection_handle().find({
            "_id": ObjectId(oid),
            "changelog": {"$exists": True}
        }):
            self.get_collection_handle().update_one({
                "_id": ObjectId(oid)
            }, {"$set": {"changelog": []}})
        return self.get_collection_handle().update_one({
            "_id": ObjectId(oid)
        }, {"$push": {"changelog": {
            "key": field,
            "from": old,
            "to": new,
            "date": datetime.now(timezone.utc).replace(microsecond=0),
            "type": "manual",
            "user": "copo@earlham.ac.uk"
        }}})

    def record_barcoding_update(self, field, old, new, oid):
        if not self.get_collection_handle().find({
            "_id": ObjectId(oid),
            "changelog": {"$exists": True}
        }):
            self.get_collection_handle().update_one({
                "_id": ObjectId(oid)
            }, {"$set": {"changelog": []}})
        return self.get_collection_handle().update_one({
            "_id": ObjectId(oid)
        }, {"$push": {"changelog": {
            "key": field,
            "from": old,
            "to": new,
            "date": datetime.now(timezone.utc).replace(microsecond=0),
            "type": "barcoding",
            "user": "copo@earlham.ac.uk"
        }}})


class Sample(DAComponent):
    def __init__(self, profile_id=None):
        super(Sample, self).__init__(profile_id, "sample")

    def get_sample_by_specimen_id(self, specimen_id):
        return self.get_collection_handle().find({"SPECIMEN_ID": specimen_id})

    def get_sample_by_specimen_id_regex(self, specimen_id):
        # Get samples from Mongo database similar to SQL's '%' operator or 'LIKE'
        return self.get_collection_handle().find({"SPECIMEN_ID": {'$regex': specimen_id, '$options': 'i'}})

    def get_sample_by_id(self, sample_id):
        cursor = self.get_collection_handle().find({"_id": sample_id})

        # get schema
        sc = self.get_component_schema()
        out = list()
        taxon = dict()
        for i in list(cursor):
            if "species_list" in i:
                sp_lst = i["species_list"]
                for sp in sp_lst:
                    # only extract target info...don't extract symnbiont info
                    if sp["SYMBIONT"] == "TARGET":
                        for k, v in sp.items():
                            i[k] = v
                    else:
                        pass
            sam = dict()
            for cell in i:
                for field in sc:

                    if cell == field.get("id", "").split(".")[-1] or cell == "_id":
                        if set(TOL_PROFILE_TYPES).intersection(set(field.get("specifications", ""))):
                            if field.get("show_in_table", ""):
                                sam[cell] = i[cell]
            out.append(sam)

        return out
    
    def get_permit_filenames_by_sample_id(self, sample_ids):
        sample_ids = [ObjectId(x) for x in sample_ids]
        projection = {col:1 for col in PERMIT_FILENAME_COLUMN_NAMES}
        projection["_id"] = 0
        
        # Check if any of the permit file name columns exist
        filter = {col: {'$exists': True} for col in PERMIT_FILENAME_COLUMN_NAMES}
        filter["_id"] = {"$in": sample_ids}

        cursor = self.get_collection_handle().find(filter,  projection)
        
        results = list(cursor)

        # Remove value from the list of dictionaries if it is equal to "NOT_APPLICABLE"
        permit_filenames = [{key: value for key, value in element.items() if value.upper().replace(' ', '_').strip() != "NOT_APPLICABLE"} for element in results]

        # Remove duplicate permit file names from a list of dictionaries
        permit_filenames_unique = [element for index, element in enumerate(permit_filenames) if element not in permit_filenames[:index]]

        # Get values from a list of unique dictionaries
        permit_filenames = [x for element in permit_filenames_unique for x in element.values()]

        return permit_filenames
    
    def get_sample_by_GAL(self, gal, getProfileIDOnly=False):
        projection = {'_id': 0, 'profile_id':1} if getProfileIDOnly else dict()
        samples = self.get_collection_handle().find({'GAL': {'$in': [gal]}}, projection)
        return cursor_to_list(samples)
    
    def count_samples_by_specimen_id_for_barcoding(self, specimen_id):
        # specimens must not have already been submitted to ENA so should have status of pending
        return self.get_collection_handle().count_documents(
            {"SPECIMEN_ID": specimen_id, "status": {"$nin": ["rejected", "accepted", "processing"]}})

    def find_incorrectly_rejected_samples(self):
        # TODO - for some reason, some dtol samples end up rejected even though the have accessions, so find these and
        # flip them to accepted
        self.get_collection_handle().update_many(
            {"biosampleAccession": {"$ne": ""}, "status": "rejected"},
            {"$set": {"status": "accepted"}}
        )

    def get_name(self, column, records):
        return self.get_collection_handle().find({"_id": {"$in": records}}, {"name": 1})

    def get_characteristic(self, column, records):
        return self.get_collection_handle().aggregate([
            {"$match": {"_id": {"$in": records}}},
            {"$unwind": "$characteristics"},
            {"$match": {"characteristics.category.annotationValue": column}},
            {"$project": {"characteristics": 1, "name": 1}}
        ])

    def set_characteristic_or_factor(self, column, records, char_or_fac, element):
        # index value is obtained from the dropdown control which selects the column to be updated
        index = str(element["idx"])
        if "unit" in element["header"].lower():
            # update unit with ontology data
            return self.get_collection_handle().update_many({"_id": {"$in": records}},
                                                            {"$set": {char_or_fac + "." + index +
                                                                      ".unit.annotationValue":
                                                                      element["value"],
                                                                      char_or_fac + "." + index + ".unit.termSource":
                                                                      element["ontology_prefix"],
                                                                      char_or_fac + "." + index + ".unit.termAccession":
                                                                      element["iri"],
                                                                      char_or_fac + "." + index + ".unit.comments": element[
                                                                          "description"]
                                                                      }})
        else:
            if element["value"].isdigit():
                # update value with simple numeric
                return self.get_collection_handle().update_many({"_id": {"$in": records}},
                                                                {"$set": {char_or_fac + "." + index +
                                                                          ".value.annotationValue":
                                                                          element["value"]}})
            else:
                # update value with ontology data
                return self.get_collection_handle().update_many({"_id": {"$in": records}},
                                                                {"$set": {char_or_fac + "." + index +
                                                                          ".value.annotationValue":
                                                                          element["value"],
                                                                          char_or_fac + "." + index + ".value.termSource":
                                                                          element["ontology_prefix"],
                                                                          char_or_fac + "." + index + ".value.termAccession":
                                                                          element["iri"],
                                                                          char_or_fac + "." + index + ".value.comments":
                                                                          element[
                                                                              "description"]
                                                                          }})

    def get_factor(self, column, records):
        return self.get_collection_handle().aggregate([
            {"$match": {"_id": {"$in": records}}},
            {"$unwind": "$factorValues"},
            {"$match": {"factorValues.category.annotationValue": column}},
            {"$project": {"factorValues": 1, "name": 1}}
        ])

    def save_record(self, auto_fields=dict(), **kwargs):
        from .profile_da import Profile
        from common.schemas.utils import data_utils
        fields = dict()
        schema = kwargs.get("schema", list()) or self.get_component_schema()

        # set auto fields
        if auto_fields:
            fields = data_utils.DecoupleFormSubmission(
                auto_fields, schema).get_schema_fields_updated_dict()

        # should have target_id for updates and return empty string for inserts
        target_id = kwargs.pop("target_id", str())

        # set system fields
        system_fields = dict(
            date_modified=helpers.get_datetime(),
            deleted=helpers.get_not_deleted_flag()
        )

        if not target_id:
            system_fields["date_created"] = helpers.get_datetime()
            system_fields["profile_id"] = self.profile_id

        # Get profile type
        manifest_type = Profile().get_type(self.profile_id)
        manifest_type = manifest_type.lower()
        current_schema_version = ""
        profile_type = ""

        # Get manifest version based on profile type
        if "asg" in manifest_type:
            profile_type = "asg"
        elif "dtolenv" in manifest_type:
            profile_type = "dtolenv"
        elif "dtol" in manifest_type:
            profile_type = "dtol"
        elif "erga" in manifest_type:
            profile_type = "erga"

        current_schema_version = settings.MANIFEST_VERSION.get(
            profile_type.upper(), "")

        # extend system fields
        for k, v in kwargs.items():
            system_fields[k] = v

        # add system fields to 'fields' and set default values - insert mode only
        for f in schema:
            # Filter schema based on manfest type and manifest version
            f_specifications = f.get("specifications", "")
            f_manifest_version = f.get("manifest_version", "")

            if f_specifications and profile_type not in f_specifications or f_manifest_version and current_schema_version not in f_manifest_version:
                continue

            f_id = f["id"].split(".")[-1]
            try:
                v_id = f["versions"][0]
            except:
                v_id = ""
            if f_id in system_fields:
                fields[f_id] = system_fields.get(f_id)
            elif v_id in system_fields:
                fields[f_id] = system_fields.get(v_id)

            if not target_id and f_id not in fields:
                fields[f_id] = helpers.default_jsontype(f["type"])

        # if True, then the database action (to save/update) is never performed, but validated 'fields' are returned
        validate_only = kwargs.pop("validate_only", False)
        fields["date_modified"] = datetime.now()
        # check if there is attached profile then update date modified
        if "profile_id" in fields:
            self.update_profile_modified(fields["profile_id"])
        if validate_only is True:
            return fields
        else:
            if target_id:
                self.get_collection_handle().update_one(
                    {"_id": ObjectId(target_id)},
                    {'$set': fields})
            else:
                doc = self.get_collection_handle().insert_one(fields)
                target_id = str(doc.inserted_id)

            # return saved record
            rec = self.get_record(target_id)

            return rec

    def update_public_name(self, name):
        self.get_collection_handle().update_many(
            {"SPECIMEN_ID": name['specimen']["specimenId"]},
            {"$set": {"public_name": name.get("tolId", "")}})

    def delete_sample(self, sample_id):
        sample = self.get_record(sample_id)
        # check if sample has already been accepted
        if sample.get("status", "") in ["accepted", "processing", "sending"] or sample.get("biosmpleAccession", ""):
            return "Sample {} with accession {} cannot be deleted as it has already been submitted to ENA.".format(
                sample.get("SPECIMEN_ID", ""), sample.get("biosampleAccession", "X"))
        else:
            # delete sample from mongo
            self.get_collection_handle().delete_one(
                {"_id": ObjectId(sample_id)})
            message = "Sample {} was deleted".format(
                sample.get("SPECIMEN_ID", ""))
            # check if the parent source to see if it can also be delete
            if self.get_collection_handle().count_documents({"SPECIMEN_ID": sample.get("SPECIMEN_ID", "")}) < 1:
                handle_dict["source"].delete_one(
                    {"SPECIMEN_ID": sample.get("SPECIMEN_ID", "")})
                message = message + \
                    "Specimen with id {} was deleted".format(
                        sample.get("SPECIMEN_ID", ""))
            return message

    def check_dtol_unique(self, rack_tube):
        rt = list(rack_tube)
        return cursor_to_list(self.get_collection_handle().find(
            {"rack_tube": {"$in": rt}},
            {"RACK_OR_PLATE_ID": 1, "TUBE_OR_WELL_ID": 1}
        ))

    def get_samples_by_date(self, d_from, d_to):
        return cursor_to_list(self.get_collection_handle().aggregate(
            [
                {"$match": {"sample_type": {"$in": TOL_PROFILE_TYPES},
                            "time_created": {"$gte": d_from, "$lt": d_to}}},
                {"$sort": {"time_created": -1}},

            ]))

    def get_all_dtol_samples(self):
        return cursor_to_list(self.get_collection_handle().find(
            {"sample_type": "dtol"},
            {"_id": 1}
        ))

    def get_project_samples(self, projects):
        return cursor_to_list(self.get_collection_handle().find(
            {"sample_type": {"$in": projects}},
            {"_id": 1}
        ))

    def get_project_samples_by_associated_project_type(self, values):
        regex_values = ' | '.join(values)
        return cursor_to_list(
            self.get_collection_handle().find({"associated_tol_project": {"$regex": regex_values, "$options": "i"}}))

    def get_gal_names(self, projects):
        return cursor_to_list(self.get_collection_handle().find(
            {"sample_type": {"$in": projects}},
            {"GAL": 1}
        ))

    def get_all_tol_samples(self):
        return self.get_collection_handle().find({"tol_project": {"$in": ["ASG", "DTOL"]}})

    def get_number_of_samples_by_sample_type(self, sample_type, d_from, d_to):
        filter = dict()

        if sample_type:
            filter["sample_type"] = sample_type
        
        if d_from and d_to:
            filter["time_created"] = {"$gte": d_from, "$lt": d_to}
        
        if not sample_type and d_from is None and d_to is None:
            return self.get_number_of_samples()
        elif sample_type and d_from and d_to:
            return self.get_collection_handle().count_documents(filter)
        elif sample_type and d_from is None and d_to is None:
            return self.get_collection_handle().count_documents(filter)
        else:
            return self.get_number_of_samples()

    def get_number_of_samples(self):
        return self.get_collection_handle().count_documents({})

    def get_tol_project_accessions(self, sort_by='_id', sort_direction=-1, projection=dict(), filter_by=dict()):
        filter_by["biosampleAccession"] = {"$exists": True, "$ne": ""}
        filter_by["sample_type"] = {"$in": TOL_PROFILE_TYPES}

        projection = {"biosampleAccession": 1, "sraAccession": 1,
                      "submissionAccession": 1, "SCIENTIFIC_NAME": 1,
                      "SPECIMEN_ID": 1, "TAXON_ID": 1, "tol_project": 1, "manifest_id": 1}

        return cursor_to_list_str(self.get_collection_handle().find(filter_by, projection).sort([[sort_by, sort_direction]]))

    def get_dtol_type(self, id):
        return self.get_collection_handle().find_one(
            {"$or": [{"biosampleAccession": id}, {"sraAccession": id}, {"biosampleAccession": id}]})

    def get_from_profile_id(self, profile_id):
        return self.get_collection_handle().find({'profile_id': profile_id})

    def timestamp_dtol_sample_created(self, sample_id):
        email = ThreadLocal.get_current_user().email
        sample = self.get_collection_handle().update_one({"_id": ObjectId(sample_id)},
                                                         {"$set": {"time_created": datetime.now(timezone.utc).replace(
                                                             microsecond=0), "created_by": email}})

    def timestamp_dtol_sample_updated(self, sample_id=str(), sample_ids=[]):
        if not sample_ids:
            sample_ids = list()
        if sample_id:
            sample_ids.append(sample_id)
        sample_obj_ids = [ObjectId(x) for x in sample_ids]

        try:
            email = ThreadLocal.get_current_user().email
        except:
            email = "copo@earlham.ac.uk"

        # Determine if the update is being done by a user or by the system
        set_update_data = {"time_updated": datetime.now(timezone.utc).replace(
            microsecond=0),  "date_modified": datetime.now(timezone.utc).replace(microsecond=0)}

        if "copo@earlham.ac.uk" in email:
            set_update_data['update_type'] = 'system'
        else:
            set_update_data['updated_by'] = email
            set_update_data['update_type'] = 'user'

        self.get_collection_handle().update_many({"_id": {"$in": sample_obj_ids}},
                                                 {"$set": set_update_data})

    def mark_forced(self, sample_id, reason):
        u = ThreadLocal.get_current_user()
        sample = self.get_collection_handle().update_one(
            {"_id": ObjectId(sample_id)},
            {"$set": {"forced_by": u.email, "reason": reason},
             })

    def add_accession(self, biosample_accession, sra_accession, submission_accession, oid):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$set":
                {
                    'error': "",
                    'biosampleAccession': biosample_accession,
                    'sraAccession': sra_accession,
                    'submissionAccession': submission_accession,
                    'status': 'accepted',
                    'time_updated': datetime.now(timezone.utc).replace(
                        microsecond=0),
                    'date_modified': datetime.now(timezone.utc).replace(
                        microsecond=0),
                    'update_type': 'system'
                }
             })

    def update_field(self, field=None, value=None, oid=None, field_values={}, oids=[]):
        if not oids:
            oids = []
        if oid:
            oids.append(oid)

        if not field_values:
            field_values = {}
        
        if field:
            field_values[field] = value

        # Determine if the update is being done by a user or by the system
        set_update_data = {'date_modified': datetime.now(timezone.utc).replace(microsecond=0), 'time_updated': datetime.now(timezone.utc).replace(
            microsecond=0)}

        try:
            email = ThreadLocal.get_current_user().email
            set_update_data['updated_by'] = email
            set_update_data['update_type'] = 'tempuser_'+str(shortuuid.ShortUUID().random(length=10)) #special handling for audit log
        except:
            set_update_data['updated_by'] = 'system'
            set_update_data['update_type'] = 'system'
      
        set_update_data.update(field_values)

        return self.get_collection_handle().update_many({"_id": {"$in": [ObjectId(oid) for oid in oids]}}, {"$set": set_update_data})

    def remove_field(self, field, oid):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$unset":
                {
                    field: ""
                }}
        )

    def add_rejected_status(self, status, oid):
        return self.update_field(field_values={'error': status["msg"],'status': "rejected"}, oid=oid)
        '''
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(oid)
            },
            {"$set":
             {'error': status["msg"],
              'status': "rejected"}
             }
        )
        '''

    def add_rejected_status_for_tolid(self, specimen_id):
        return self.get_collection_handle().update_many(
            {
                "SPECIMEN_ID": specimen_id
            },
            {"$set":
             {'tolid_error': "public name request has been rejected at Sanger",
              'status': 'rejected'}
             }
        )

    def get_by_profile_and_field(self, profile_id, field, value):
        return cursor_to_list(self.get_collection_handle().find({field: {"$in": value}, "profile_id": profile_id},
                                                                {"_id": 1}))

    def get_by_project_and_field(self, project, field, value):
        return cursor_to_list(self.get_collection_handle().find({field: {"$in": value}, "tol_project": project}))

    def get_dtol_from_profile_id(self, profile_id, filter, draw, start, length, sort_by, dir, search, profile_type):

        sc = self.get_component_schema()
        if sort_by == "0":
            sort_by_column = "_id"
        else:
            i = 0
            sort_by_column = ""
            for field in sc:
                if profile_type in field.get("specifications", []) and field.get(
                        "show_in_table", ""):
                    i = i + 1
                    if i == int(sort_by):
                        sort_by_column = field.get("id", "").split(".")[-1]
                        break
        total_count = 0

        find_condition = dict()
        if search:
            find_condition["$text"] = {"$search": search}
        find_condition["profile_id"] = profile_id
        sort_clause = [[sort_by_column, dir]]
        handler = self.get_collection_handle()

        if filter == "pending":
            # $nin will return where status neq to values in array, or status is absent altogether
            find_condition["status"] = {
                "$nin": ["barcode_only", "rejected", "accepted", "processing", "conflicting", "private", "sending", "bge_pending"]}

        elif filter == "conflicting_barcode":
            find_condition["status"] = "conflicting"

        elif filter == "processing":
            find_condition["status"] = {
                "$in": ["processing", "sending"]}

        else:
            find_condition["status"] = filter

        cursor = handler.find(find_condition).sort(
            sort_clause).skip(int(start)).limit(int(length))
        total_count = handler.count_documents(find_condition)
        samples = list(cursor)

        if filter == "conflicting_barcode":
            id_query = [x["_id"] for x in samples]
            barcodes = handle_dict["barcode"].find(
                {"sample_id": {"$in": id_query}})
            for bc in barcodes:
                for idx, s in enumerate(samples):
                    if bc["sample_id"] == str(s["_id"]):
                        samples[idx]["barcoding"] = bc

        # get schema
        # samples = list(cursor);
        # sc = self.get_component_schema()
        out = list()
        taxon = dict()
        for i in samples:
            if "species_list" in i:
                sp_lst = i["species_list"]
                for sp in sp_lst:
                    # only extract target info...don't extract symnbiont info
                    if sp["SYMBIONT"] == "TARGET":
                        for k, v in sp.items():
                            i[k] = v
                    else:
                        pass
            sam = dict()
            sam["_id"] = str(i["_id"])
            for field in sc:
                if profile_type in field.get("specifications", []) and field.get(
                        "show_in_table", ""):
                    name = field.get("id", "").split(".")[-1]
                    sam[name] = i.get(name, "")

            sam["error"] = i.get("error", "")
            out.append(sam)

        result = dict()
        result["recordsTotal"] = total_count
        result["recordsFiltered"] = total_count
        result["draw"] = draw
        result["data"] = out
        return result

    def get_sample_display_column_names(self, group_filter=None):

        sc = self.get_component_schema()
        columns = []
        columns.append("_id")
        group_set = set(TOL_PROFILE_TYPES)
        if group_filter:
            group_set = {group_filter}
        for field in sc:

            if  group_set.intersection(set(field.get("specifications", ""))) and field.get("show_in_table",
                                                                                                       ""):
                column = field.get("id", "").split(".")[-1]
                if column not in columns:
                    columns.append(column)

        columns.append("error")
        return columns

    def get_dtol_from_profile_id_and_project(self, profile_id, project):
        cursor = self.get_collection_handle().find(
            {'profile_id': profile_id, "tol_project": project})

        # get schema
        sc = self.get_component_schema()
        out = list()
        taxon = dict()
        for i in list(cursor):
            if "species_list" in i:
                sp_lst = i["species_list"]
                for sp in sp_lst:
                    # only extract target info...don't extract symnbiont info
                    if sp["SYMBIONT"] == "TARGET":
                        for k, v in sp.items():
                            i[k] = v
                    else:
                        pass
            sam = dict()
            for cell in i:
                for field in sc:

                    if cell == field.get("id", "").split(".")[-1] or cell == "_id":
                        if set(TOL_PROFILE_TYPES).intersection(set(field.get("specifications", ""))):
                            if field.get("show_in_table", ""):
                                sam[cell] = i[cell]
            out.append(sam)
        return out

    def get_dtol_by_aggregation(self, match_dict):
        cursor = self.get_collection_handle().aggregate(
            [
                {
                    "$match": match_dict
                },
                {"$sort":
                 {"time_created": -1}
                 },
            ]
        )

        records = cursor_to_list_str(cursor)

        # get schema
        sc = self.get_component_schema()
        out = list()
        taxon = dict()
        for i in records:
            if "species_list" in i:
                sp_lst = i["species_list"]
                for sp in sp_lst:
                    # only extract target info...don't extract symnbiont info
                    if sp["SYMBIONT"] == "TARGET":
                        for k, v in sp.items():
                            i[k] = v
                    else:
                        pass
            sam = dict()
            for cell in i:
                for field in sc:

                    if cell == field.get("id", "").split(".")[-1] or cell == "_id":
                        if set(TOL_PROFILE_TYPES).intersection(set(field.get("specifications", ""))):
                            if field.get("show_in_table", ""):
                                sam[cell] = i[cell]
            out.append(sam)
        return out

    def mark_rejected(self, sample_id, reason="Sample rejected by curator."):

        #return self.get_collection_handle().update_one({"_id": ObjectId(sample_id)},
        #                                               {"$set": {"status": "rejected", "error": reason}})
        self.update_field(field_values={"status": "rejected", "error": reason}, oid=sample_id)


    def mark_processing(self, sample_id=str(), sample_ids=[]):
        if not sample_ids:
            sample_ids = list()
        if sample_id:
            sample_ids.append(sample_id)
        return self.update_field(field="status", value="processing", oids=sample_ids)
        #sample_obj_ids = [ObjectId(x) for x in sample_ids]
        #return self.get_collection_handle().update_many({"_id": {"$in": sample_obj_ids}}, {"$set": {"status": "processing"}})

    def mark_pending(self, sample_ids, is_erga=False, is_private=False):
        if is_erga:
            status = "bge_pending"
        elif is_private:
            status = "private"
        else :
            status = "pending"
        return self.update_field(field="status", value=status, oids=sample_ids)    
        #sample_obj_ids = [ObjectId(x) for x in sample_ids]
        #return self.get_collection_handle().update_many({"_id": {"$in": sample_obj_ids}}, {"$set": {"status": status}})

    def get_by_manifest_id(self, manifest_id):
        samples = cursor_to_list(
            self.get_collection_handle().find({"manifest_id": manifest_id}))
        if samples:
            profile = cursor_to_list(handle_dict["profile"].find({"_id": ObjectId(samples[0]["profile_id"])},{"title":1}))
            profile_title = ""
            if profile:
                profile_title = profile[0]["title"]
            for s in samples:
                s["copo_profile_title"] = profile_title
        return samples
    
    def get_by_sequencing_centre(self, sequencing_centre, isQueryByManifestLevel=False):
        from .profile_da import Profile

        # Get the value of the sequencing centre based on the label
        sequencing_centre = SequencingCentre.objects.get(label=sequencing_centre)

        # Get the profile id based on the sequencing centre
        profile_ids_based_on_profile = cursor_to_list_no_ids(Profile().get_profile_by_sequencing_centre(sequencing_centre.name, getProfileIDOnly=True))
        profile_ids_based_on_sample = self.get_sample_by_GAL(sequencing_centre.label, getProfileIDOnly=True)
        
        # Get string only from ObjectId, 'profile_id'
        profile_ids_based_on_profile = [str(x) for x in profile_ids_based_on_profile]
        
        # Get 'profile_id' string only from dictionary object
        profile_ids_based_on_sample = [x.get('profile_id', str()) for x in profile_ids_based_on_sample]

        # Get unique profile ids i.e. remove duplicates
        profile_ids = list(set(profile_ids_based_on_profile + profile_ids_based_on_sample))

        if not profile_ids:
            return list()
        
        # Get the manifest ids based on the profile ids
        filter = dict()
        filter['sample_type'] = {"$in": SANGER_TOL_PROFILE_TYPES}
        filter['profile_id'] = {"$in": profile_ids}

        if isQueryByManifestLevel:        
            cursor = self.get_collection_handle().aggregate(
                [
                    {
                        "$match": filter
                    },
                    {"$sort":
                        {"time_created": -1}
                    },
                    {"$group":
                        {
                            "_id": "$manifest_id",
                            "created": {"$first": "$time_created"}
                        }
                    }
                ])
            out =  cursor_to_list_no_ids(cursor)
        else:
            out = cursor_to_list(self.get_collection_handle().find(filter))

        return out
    
    def get_profileID_by_project_and_manifest_id(self, projects, manifest_ids):
        return cursor_to_list(self.get_collection_handle().aggregate(
            [{"$match": {"tol_project": {"$in": projects}, "manifest_id": {"$in": manifest_ids}}},
             {"$project": {"profile_id": 1}}]))

    def get_statuses_by_manifest_id(self, manifest_id):
        return cursor_to_list(self.get_collection_handle().find({"manifest_id": manifest_id},
                                                                {"status": 1, "copo_id": 1, "manifest_id": 1,
                                                                 "time_created": 1, "time_updated": 1}))

    def get_by_biosample_ids(self, biosample_ids):
        return cursor_to_list(self.get_collection_handle().find({"biosampleAccession": {"$in": biosample_ids}}))

    def get_by_field(self, dtol_field, value):
        return cursor_to_list(self.get_collection_handle().find({dtol_field: {"$in": value}}))

    def get_specimen_biosample(self, value):
        return cursor_to_list(
            self.get_collection_handle().find(
                {"sample_type": {"$in": ["dtol_specimen", "asg_specimen", "erga_specimen"]},
                 "SPECIMEN_ID": value}))

    def get_target_by_specimen_id(self, specimenid):
        return cursor_to_list(self.get_collection_handle().find({"sample_type": {"$in": TOL_PROFILE_TYPES},
                                                                 "species_list.SYMBIONT": {'$in': ["TARGET", "target"]},
                                                                 "SPECIMEN_ID": specimenid}))

    def get_target_by_field(self, field, value):
        return cursor_to_list(self.get_collection_handle().find({"sample_type": {"$in": TOL_PROFILE_TYPES},
                                                                 "species_list": {'$elemMatch': {"SYMBIONT": "TARGET"}},
                                                                 field: value}))

    def get_manifests(self):
        cursor = self.get_collection_handle().aggregate(
            [
                {
                    "$match": {
                        "sample_type": {"$in": SANGER_TOL_PROFILE_TYPES}
                    }
                },
                {"$sort":
                 {"time_created": -1}
                 },
                {"$group":
                    {
                        "_id": "$manifest_id",
                        "created": {"$first": "$time_created"}
                    }
                 }
            ])
        return cursor_to_list_no_ids(cursor)  

    def get_manifests_by_date(self, d_from, d_to):
        ids = self.get_collection_handle().aggregate(
            [
                {"$match": {"sample_type": {"$in": TOL_PROFILE_TYPES},
                            "time_created": {"$gte": d_from, "$lt": d_to}}},
                {"$sort": {"time_created": -1}},
                {"$group":
                    {
                        "_id": "$manifest_id",
                        "created": {"$first": "$time_created"}
                    }
                 }
            ])
        out = cursor_to_list_no_ids(ids)
        return out

    def get_manifests_by_date_and_project(self, project, d_from, d_to):
        projectlist = project.split(",")
        projectlist = list(map(lambda x: x.strip(), projectlist))
        # remove any empty elements in the list (e.g. where 2 or more comas have been typed in error
        projectlist[:] = [x for x in projectlist if x]
        ids = self.get_collection_handle().aggregate(
            [
                {"$match": {"sample_type": {"$in": projectlist},
                            "time_created": {"$gte": d_from, "$lt": d_to}}},
                {"$sort": {"time_created": -1}},
                {"$group":
                    {
                        "_id": "$manifest_id",
                        "created": {"$first": "$time_created"}
                    }
                 }
            ])
        out = cursor_to_list_no_ids(ids)
        return out

    def check_and_add_symbiont(self, s):
        sample = self.get_collection_handle().find_one(
            {"RACK_OR_PLATE_ID": s["RACK_OR_PLATE_ID"], "TUBE_OR_WELL_ID": s["TUBE_OR_WELL_ID"]})
        if sample:
            out = helpers.make_tax_from_sample(s)
            self.add_symbiont(sample, out)
            return True
        return False

    def add_symbiont(self, s, out):
        self.get_collection_handle().update_one(
            {"RACK_OR_PLATE_ID": s["RACK_OR_PLATE_ID"],
                "TUBE_OR_WELL_ID": s["TUBE_OR_WELL_ID"]},
            {"$push": {"species_list": out}}
        )
        return True

    def add_blank_barcode_record(self, specimen_id, barcode_id):
        self.get_collection_handle().update_many({"specimen_id": specimen_id},
                                                 {"$set": {"specimen_id": specimen_id, "barcode_id":
                                                           barcode_id}}, upsert=True)

    def update_tol_by_specimen(self, specimen_id, sample_data):

        return self.get_collection_handle().find_one_and_update({"SPECIMEN_ID": specimen_id}, {"$set": sample_data},
                                                                return_document=ReturnDocument.AFTER)

    def record_user_update(self, field, old, new, oid):
        if not self.get_collection_handle().find({
            "_id": ObjectId(oid),
            "changelog": {"$exists": True}
        }):
            self.get_collection_handle().update_one({
                "_id": ObjectId(oid)
            }, {"$set": {"changelog": []}})
        return self.get_collection_handle().update_one({
            "_id": ObjectId(oid)
        }, {"$push": {"changelog": {
            "key": field,
            "from": old,
            "to": new,
            "date": datetime.now(timezone.utc).replace(microsecond=0),
            "type": "user",
            "user": ThreadLocal.get_current_user().email
        }}})

    def record_manual_update(self, field, old, new, oid):
        if not self.get_collection_handle().find({
            "_id": ObjectId(oid),
            "changelog": {"$exists": True}
        }):
            self.get_collection_handle().update_one({
                "_id": ObjectId(oid)
            }, {"$set": {"changelog": []}})
        return self.get_collection_handle().update_one({
            "_id": ObjectId(oid)
        }, {"$push": {"changelog": {
            "key": field,
            "from": old,
            "to": new,
            "date": datetime.now(timezone.utc).replace(microsecond=0),
            "type": "manual",
            "user": "copo@earlham.ac.uk"
        }}})

    def record_barcoding_update(self, field, old, new, oid):
        if not self.get_collection_handle().find({
            "_id": ObjectId(oid),
            "changelog": {"$exists": True}
        }):
            self.get_collection_handle().update_one({
                "_id": ObjectId(oid)
            }, {"$set": {"changelog": []}})
        return self.get_collection_handle().update_one({
            "_id": ObjectId(oid)
        }, {"$push": {"changelog": {
            "key": field,
            "from": old,
            "to": new,
            "date": datetime.now(timezone.utc).replace(microsecond=0),
            "type": "barcoding",
            "user": "copo@earlham.ac.uk"
        }}})

    def update_read_accession(self, sample_accessions):
        for accession in sample_accessions:
            update_fields = {"biosampleAccession": accession["biosample_accession"],
                                                               "sraAccession": accession["sample_accession"],
                                                               "status": "accepted"}
            self.update_field(field_values=update_fields, oid=accession["sample_id"])
            '''
            self.get_collection_handle().update_many({"_id": ObjectId(accession["sample_id"])},
                                                     {"$set": {"biosampleAccession": accession["biosample_accession"],
                                                               "sraAccession": accession["sample_accession"],
                                                               "status": "accepted"}})
            '''
            
    def update_datafile_status(self, datafile_ids, status):
        dt = helpers.get_datetime()
        for id in datafile_ids:
            self.get_collection_handle().update_one({"profile_id": self.profile_id, "read.file_id": {
                "$regex": id}, "read.$.status": {"$ne": status}}, {"$set": {"read.$.status": status, "modifed_date":  dt}})
