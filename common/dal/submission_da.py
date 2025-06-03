
from common.dal.mongo_util import cursor_to_list, cursor_to_list_str
from common.schema_versions.lookup.dtol_lookups import NON_SAMPLE_ACCESSION_TYPES, TOL_PROFILE_TYPES
from common.utils import helpers
from common.utils.logger import Logger
from .sample_da import Sample
from .copo_da import DataFile, EnaFileTransfer
from .copo_base_da import DAComponent
import pymongo.errors as pymongo_errors
from bson import ObjectId, json_util
from django.conf import settings
from common.schemas.utils.cg_core.cg_schema_generator import CgCoreSchemas
from datetime import datetime, timedelta
from django_tools.middlewares import ThreadLocal


lg =  Logger()

class Submission(DAComponent):
    def __init__(self, profile_id=None, subcomponent=None):
        super(Submission, self).__init__(profile_id, "submission")

    def dtol_sample_processed(self, sub_id, submission_id):
        self.update_dtol_sample(sub_id, [], submission_id, "bioimage_pending")

    def dtol_sample_rejected(self, sub_id, sam_ids, submission_id):
        self.update_dtol_sample(sub_id, sam_ids, submission_id, "complete")

    def update_dtol_sample(self, sub_id, sam_ids, submission_id, next_status):
        # when dtol sample has been processed, pull id from submission and check if there are remaining
        # samples left to go. If not, make submission complete. This will stop celery processing the this submission.
        sub_handle = self.get_collection_handle()
        # for sam_id in sam_ids:
        action = {}
        if submission_id:
            action.update( {"$pull": {"submission": {"id": submission_id}}})
        if sam_ids:
            action.update({"$pull": {"dtol_samples": {"$in": sam_ids}}})

        sub_handle.update_one({"_id": ObjectId(sub_id)}, action )
     
        sub = sub_handle.find_one({"_id": ObjectId(sub_id)}, {
                                  "submission": 1, "dtol_samples": 1, "profile_id":1})
        if len(sub["submission"]) < 1 and len(sub["dtol_samples"]) < 1:
            sub_handle.update_one({"_id": ObjectId(sub_id)},
                                  {"$set": {"dtol_status": next_status, "date_modified": helpers.get_datetime()}})
            
        elif len(sub["dtol_samples"]) > 0:
            dtol_samples_ids = [ObjectId(x) for x in sub["dtol_samples"]]
            processing_sample_count = Sample().get_collection_handle().count_documents({"profile_id":sub["profile_id"], "_id": {"$in": dtol_samples_ids}, "status":"processing"})
            if processing_sample_count > 0:
                sub_handle.update_one({"_id": ObjectId(sub_id)},
                                  {"$set": {"dtol_status":"pending" , "date_modified": helpers.get_datetime() } })


    def update_dtol_specimen_for_bioimage_tosend(self, sub_id, sepcimen_ids):
        sub_handle = self.get_collection_handle()
        sub_handle.update_one({"_id": ObjectId(sub_id)}, {"$push": {"dtol_specimen": {"$each": sepcimen_ids}},
                                                          "$set": {"date_modified": helpers.get_datetime()}})

    def update_submission_async(self, sub_id, href, sample_ids, submission_id):
        sub_handle = self.get_collection_handle()
        submission = {'id': submission_id,
                      'sample_ids': sample_ids, 'href': href}
        sub_handle.update_one({"_id": ObjectId(sub_id)},
                              {"$set": {"date_modified": helpers.get_datetime()}, "$push": {"submission": submission},
                               "$pull": {"dtol_samples": {"$in": sample_ids}}})

    def get_async_submission(self):
        sub_handle = self.get_collection_handle()
        sub = sub_handle.find({"submission": {"$exists": True, "$ne": []}, "repository": "ena", "deleted": helpers.get_not_deleted_flag()  },
                              {"_id": 1, "submission": 1, "profile_id": 1, "dtol_specimen": 1})
        return cursor_to_list(sub)

    def get_dtol_samples_in_biostudy(self, study_ids):
        sub = self.get_collection_handle().find(
            {"accessions.study_accessions.bioProjectAccession": {"$in": study_ids}},
            {"accessions": 1, "_id": 0}
        )
        return cursor_to_list(sub)

    def get_bioimage_pending_submission(self):
        REFRESH_THRESHOLD = 3600  # time in seconds to retry stuck submission
        # called by celery to get samples the supeprvisor has set to be sent to ENA
        # those not yet sent should be in pending state. Occasionally there will be
        # stuck submissions in sending state, so get both types
        sub = self.get_collection_handle().find(
            {"type": {"$in": TOL_PROFILE_TYPES}, "repository":"ena", "dtol_status": {
                "$in": ["bioimage_sending", "bioimage_pending"]}},
            {"dtol_specimen": 1, "dtol_status": 1, "profile_id": 1, "dtol_samples":1,
             "date_modified": 1, "type": 1})
        sub = cursor_to_list(sub)
        out = list()

        for s in sub:
            # calculate whether a submission is an old one
            recorded_time = s.get("date_modified", helpers.get_datetime())
            current_time = helpers.get_datetime()
            time_difference = current_time - recorded_time
            if s.get("dtol_status", "") == "bioimage_sending" and time_difference.total_seconds() > (REFRESH_THRESHOLD):
                # submission retry time has elapsed so re-add to list
                out.append(s)
                self.update_submission_modified_timestamp(s["_id"])
                lg.error("ADDING STALLED BIOIMAGE SUBMISSION " + str(s["_id"]) + "BACK INTO QUEUE - copo_da:1083")

                # no need to change status
            elif s.get("dtol_status", "") == "bioimage_pending":
                out.append(s)
                self.update_submission_modified_timestamp(s["_id"])
                self.get_collection_handle().update_one({"_id": ObjectId(s["_id"])},
                                                        {"$set": {"dtol_status": "bioimage_sending"}})
        return out

    def get_pending_dtol_samples(self):
        REFRESH_THRESHOLD = 3600  # time in seconds to retry stuck submission
        # called by celery to get samples the supeprvisor has set to be sent to ENA
        # those not yet sent should be in pending state. Occasionally there will be
        # stuck submissions in sending state, so get both types
        current_time = helpers.get_datetime()

        sub = self.get_collection_handle().find(
            {"type": {"$in": TOL_PROFILE_TYPES}, "repository": "ena",
                "dtol_status": {"$in": ["pending"]}},
            {"dtol_samples": 1, "dtol_status": 1, "profile_id": 1,
             "date_modified": 1, "type": 1, "dtol_specimen": 1}).limit(1)
        sub = cursor_to_list(sub)
        out = list()

        for s in sub:
            # calculate whether a submission is an old one
            recorded_time = s.get("date_modified", helpers.get_datetime())
            time_difference = current_time - recorded_time
            if s.get("dtol_status", "") == "sending" and time_difference.total_seconds() > (REFRESH_THRESHOLD):
                # submission retry time has elapsed so re-add to list
                out.append(s)
                self.update_submission_modified_timestamp(s["_id"])
                lg.error("ADDING STALLED SUBMISSION " + str(s["_id"]) + "BACK INTO QUEUE - copo_da:1083")

                # no need to change status
            elif s.get("dtol_status", "") == "pending":
                out.append(s)
                self.update_submission_modified_timestamp(s["_id"])
                self.get_collection_handle().update_one({"_id": ObjectId(s["_id"])}, {
                    "$set": {"dtol_status": "sending"}})
        return out

    def get_pending_samples(self, repositiory="ena"):
        current_time = helpers.get_datetime()
        sub = self.get_collection_handle().find(
            {"repository": repositiory,
                "sample_status": {"$in": ["pending"]}},
            {"samples": 1, "sample_status": 1, "profile_id": 1,
             "date_modified": 1}).limit(1)
        sub = cursor_to_list(sub)
        out = list()

        for s in sub:
            out.append(s)
            self.update_submission_modified_timestamp(s["_id"])
            self.get_collection_handle().update_one({"_id": ObjectId(s["_id"])}, {
                "$set": {"sample_status": "sending", "date_modified": current_time}})
        return out        

    def get_records_by_field(self, field, value):
        sub = self.get_collection_handle().find({
            field: value
        })
        return cursor_to_list(sub)

    def get_awaiting_tolids(self):
        sub = self.get_collection_handle().find(
            {"type": {"$in": TOL_PROFILE_TYPES}, "repository": "ena",
                "dtol_status": {"$in": ["awaiting_tolids"]}},
            {"dtol_samples": 1, "dtol_status": 1, "profile_id": 1,
             "date_modified": 1})
        sub = cursor_to_list(sub)
        return sub

    def get_incomplete_submissions_for_user(self, user_id, repo):
        doc = self.get_collection_handle().find(
            {"user_id": user_id,
             "repository": repo,
             "complete": "false"}
        )
        return doc

    def make_dtol_status_pending(self, sub_id):
        doc = self.get_collection_handle().update_one({"_id": ObjectId(sub_id)}, {
            "$set": {"dtol_status": "pending", "date_modified": helpers.get_datetime()}})

    def make_dtol_status_awaiting_tolids(self, sub_id):
        doc = self.get_collection_handle().update_one({"_id": ObjectId(sub_id)}, {
            "$set": {"dtol_status": "awaiting_tolids", "date_modified": helpers.get_datetime()}})

    def save_record(self, auto_fields=dict(), **kwargs):
        if not kwargs.get("target_id", str()):
            repo = kwargs.pop("repository", str())
            for k, v in dict(
                    repository=repo,
                    status=False,
                    complete='false',
                    user_id=helpers.get_current_user().id,
                    date_created=helpers.get_datetime()
            ).items():
                auto_fields[self.get_qualified_field(k)] = v

        return super(Submission, self).save_record(auto_fields, **kwargs)

    def validate_and_delete(self, target_id=str(), target_ids=list()):
        """
        function deletes a submission record, but first checks for dependencies
        :param target_id:
        :return:
        """

        submission_id = str(target_id)

        result = dict(status='success', message="")

        if not submission_id:
            return dict(status='error', message="Submission record identifier not found!")

        # get submission record
        submission_record = self.get_collection_handle().find_one(
            {"_id": ObjectId(submission_id)})

        # check completion status - can't delete a completed submission
        if str(submission_record.get("complete", False)).lower() == 'true':
            return dict(status='error', message="Submission record might be tied to a remote or public record!")

        # check for accession - can't delete record with accession
        if submission_record.get("accessions", dict()):
            return dict(status='error', message="Submission record has associated accessions or object identifiers!")

        # ..and other checks as they come up

        # delete record
        self.get_collection_handle().delete_one(
            {"_id": ObjectId(submission_id)})

        return result

    def get_submission_metadata(self, submission_id=str()):
        """
        function returns the metadata associated with this submission
        :param submission_id:
        :return:
        """

        result = dict(
            status='error', message="Metadata not found or unspecified procedure.", meta=list())

        if not submission_id:
            return dict(status='error', message="Submission record identifier not found!", meta=list())

        try:
            repository_type = self.get_repository_type(
                submission_id=submission_id)
        except Exception as e:
            repository_type = str()
            lg.exception(e)

        if not repository_type:
            return dict(status='error', message="Submission repository unknown!", meta=list())

        if repository_type in ["dataverse", "ckan", "dspace"]:
            query_projection = dict()

            for x in self.get_schema().get("schema_dict"):
                query_projection[x["id"].split(".")[-1]] = 0

            query_projection["bundle"] = {"$slice": 1}

            submission_record = self.get_collection_handle().find_one({"_id": ObjectId(submission_id)},
                                                                      query_projection)

            if len(submission_record["bundle"]):
                items = CgCoreSchemas().extract_repo_fields(
                    str(submission_record["bundle"][0]), repository_type)

                if repository_type == "dataverse":
                    items.append({"dc": "dc.relation", "copo_id": "submission_id", "vals": "copo:" + str(submission_id),
                                  "label": "COPO Id"})

                return dict(status='success', message="", meta=items)
        else:
            pass  # todo: if required for other repo, can use metadata from linked bundle

        return result

    '''
    def lift_embargo(self, submission_id=str()):
        """
        function attempts to lift the embargo on the submission, releasing to the public
        :param submission_id:
        :return:
        """

        result = dict(status='info', message="Release status unknown or unspecified procedure.")

        if not submission_id:
            return dict(status='error', message="Submission record identifier not found!")

        # this process is repository-centric...
        # so every repository type should provide its own implementation if needed

        try:
            repository_type = self.get_repository_type(submission_id=submission_id)
        except Exception as e:
            repository_type = str()
            lg.exception(e)

        if not repository_type:
            return dict(status='error', message="Submission repository unknown!")

        if repository_type == "ena":
            from submission import enareadSubmission
            return enareadSubmission.EnaReads(submission_id=submission_id).process_study_release(force_release=True)

        return result
    '''

    def get_repository_type(self, submission_id=str()):
        """
        function returns the repository type for this submission
        :param submission_id:
        :return:
        """

        # first check if this is a manifest submission
        s = self.get_collection_handle().find_one(
            {"_id": ObjectId(submission_id)})
        if s.get("manifest_submission", 0):
            return s["repository"]

        # specify filtering
        filter_by = dict(_id=ObjectId(str(submission_id)))

        # specify projection
        query_projection = {
            "_id": 1,
            "repository_docs.type": 1,
        }

        doc = self.get_collection_handle().aggregate(
            [
                {"$addFields": {
                    "destination_repo_converted": {
                        "$convert": {
                            "input": "$destination_repo",
                            "to": "objectId",
                            "onError": 0
                        }
                    }
                }
                },
                {
                    "$lookup":
                        {
                            "from": "RepositoryCollection",
                            "localField": "destination_repo_converted",
                            "foreignField": "_id",
                            "as": "repository_docs"
                        }
                },
                {
                    "$project": query_projection
                },
                {
                    "$match": filter_by
                }
            ])

        records = cursor_to_list(doc)

        try:
            repository = records[0]['repository_docs'][0]['type']
        except (IndexError, AttributeError) as error:
            message = "Error retrieving submission repository " + str(error)
            lg.error(message)
            raise

        return repository

    '''
    def get_repository_details(self, submission_id=str()):
        """
        function returns the repository details for this submission
        :param submission_id:
        :return:
        """

        # specify filtering
        filter_by = dict(_id=ObjectId(str(submission_id)))

        # specify projection
        query_projection = {
            "_id": 1,
        }

        for x in Repository().get_schema().get("schema_dict"):
            query_projection["repository_docs." + x["id"].split(".")[-1]] = 1

        doc = self.get_collection_handle().aggregate(
            [
                {"$addFields": {
                    "destination_repo_converted": {
                        "$convert": {
                            "input": "$destination_repo",
                            "to": "objectId",
                            "onError": 0
                        }
                    }
                }
                },
                {
                    "$lookup":
                        {
                            "from": "RepositoryCollection",
                            "localField": "destination_repo_converted",
                            "foreignField": "_id",
                            "as": "repository_docs"
                        }
                },
                {
                    "$project": query_projection
                },
                {
                    "$match": filter_by
                }
            ])

        records = cursor_to_list(doc)

        try:
            repository_details = records[0]['repository_docs'][0]
        except (IndexError, AttributeError) as error:
            message = "Error retrieving submission repository details " + str(error)
            lg.log(message, level=Loglvl.ERROR, type=Logtype.FILE)
            raise

        return repository_details

    def mark_all_token_obtained(self, user_id):

        # mark all submissions for profile with type figshare as token obtained
        return self.get_collection_handle().update_many(
            {
                'user_id': user_id,
                'repository': 'figshare'
            },
            {
                "$set": {
                    "token_obtained": True
                }
            }
        )

    def mark_figshare_article_published(self, article_id):
        return self.get_collection_handle().update_many(
            {
                'accessions': article_id
            },
            {
                "$set": {
                    "status": 'published'
                }
            }
        )
    '''

    def clear_submission_metadata(self, sub_id):
        return self.get_collection_handle().update_one({"_id": ObjectId(sub_id)}, {"$set": {"meta": {}}})

    def isComplete(self, sub_id):
        doc = self.get_collection_handle().find_one({"_id": ObjectId(sub_id)})

        return doc.get("complete", False)

    def is_manifest_submission(self, sub_id):
        docs = Submission().get_collection_handle().find_one(
            {"_id": ObjectId(sub_id)}
        )
        return docs.get("manifest_submission", 0) == 1

    def insert_dspace_accession(self, sub, accessions):
        # check if submission accessions are not a list, if not delete as multiple accessions cannot be added to object
        doc = self.get_collection_handle().find_one(
            {"_id": ObjectId(sub["_id"])})
        if type(doc['accessions']) != type(list()):
            self.get_collection_handle().update_one(
                {"_id": ObjectId(sub["_id"])},
                {"$unset": {"accessions": ""}}
            )

        doc = self.get_collection_handle().update_one(
            {"_id": ObjectId(sub["_id"])},
            {"$push": {"accessions": accessions}}
        )
        return doc

    def insert_ckan_accession(self, sub, accessions):

        try:
            doc = self.get_collection_handle().update_one(
                {"_id": ObjectId(sub)},
                {"$push": {"accessions": accessions}}
            )
        except pymongo_errors.WriteError:
            self.get_collection_handle().update_one(
                {"_id": ObjectId(sub)}, {"$unset": {"accessions": ""}})
            doc = self.get_collection_handle().update_one({"_id": ObjectId(sub)}, {
                "$push": {"accessions": accessions}})
        return doc

    def mark_submission_complete(self, sub_id, article_id=None):
        now = helpers.get_datetime()
        if article_id:
            if not type(article_id) is list:
                article_id = [article_id]
            f = {
                "$set": {
                    "complete": "true",
                    "completed_on": now,
                    "accessions": article_id
                }
            }
        else:
            f = {
                "$set": {
                    "complete": "true",
                    "completed_on": now
                }
            }
        doc = self.get_collection_handle().update_one(
            {
                '_id': ObjectId(sub_id)
            },
            f

        )

    def mark_figshare_article_id(self, sub_id, article_id):
        if not type(article_id) is list:
            article_id = [article_id]
        doc = self.get_collection_handle().update_one(
            {
                '_id': ObjectId(sub_id)
            },
            {
                "$set": {
                    "accessions": article_id,
                }
            }
        )

    def get_file_accession(self, sub_id):
        doc = self.get_collection_handle().find_one(
            {
                '_id': ObjectId(sub_id)
            },
            {
                'accessions': 1,
                'bundle': 1,
                'repository': 1
            }
        )
        if doc['repository'] == 'figshare':
            return {'accessions': doc['accessions'], 'repo': 'figshare'}
        else:
            filenames = list()
            for file_id in doc['bundle']:
                f = DataFile().get_by_file_name_id(file_id=file_id)
                filenames.append(f['name'])
            if isinstance(doc['accessions'], str):
                doc['accessions'] = None
            return {'accessions': doc['accessions'], 'filenames': filenames, 'repo': doc['repository']}

    '''
    def get_file_accession_for_dataverse_entry(self, mongo_file_id):
        return self.get_collection_handle().find_one({'accessions.mongo_file_id': mongo_file_id},
                                                     {'_id': 0, 'accessions.$': 1})
    '''

    def get_complete(self):
        complete_subs = self.get_collection_handle().find({'complete': True})
        return complete_subs

    def get_ena_type(self):
        subs = self.get_collection_handle().find(
            {'repository': {'$in': ['ena-ant', 'ena', 'ena-asm']}})
        return subs

    '''
    def update_destination_repo(self, submission_id, repo_id):
        if repo_id == 'default':
            return self.get_collection_handle().update(
                {'_id': ObjectId(submission_id)}, {'$set': {'destination_repo': 'default'}}
            )
        r = Repository().get_record(repo_id)
        dest = {"url": r.get('url'), 'apikey': r.get('apikey', ""), "isCG": r.get('isCG', ""), "repo_id": repo_id,
                "name": r.get('name', ""),
                "type": r.get('type', ""), "username": r.get('username', ""), "password": r.get('password', "")}
        self.get_collection_handle().update(
            {'_id': ObjectId(submission_id)},
            {'$set': {'destination_repo': dest, 'repository': r['type'], 'date_modified': helpers.get_datetime()}}
        )

        return r
    '''

    def update_meta(self, submission_id, meta):
        return self.get_collection_handle().update_one(
            {'_id': ObjectId(submission_id)}, {
                '$set': {'meta': json_util.loads(meta)}}
        )

    def get_dataverse_details(self, submission_id):
        doc = self.get_collection_handle().find_one(
            {'_id': ObjectId(submission_id)}, {'destination_repo': 1}
        )
        default_dataverse = {'url': settings.DATAVERSE["HARVARD_TEST_API"],
                             'apikey': settings.DATAVERSE["HARVARD_TEST_TOKEN"]}
        if 'destination_repo' in doc:
            if doc['destination_repo'] == 'default':
                return default_dataverse
            else:
                return doc['destination_repo']
        else:
            return default_dataverse

    def mark_as_published(self, submission_id):
        return self.get_collection_handle().update_one(
            {'_id': ObjectId(submission_id)}, {'$set': {'published': True}}
        )

    def get_dtol_submission_for_profile(self, profile_id):
        return self.get_collection_handle().find_one({
            "profile_id": profile_id, "type": {"$in": TOL_PROFILE_TYPES}, "repository": "ena",
        })

    def add_accession(self, biosample_accession, sra_accession, submission_accession, oid, collection_id):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(collection_id)
            },
            {"$addToSet":
                {
                    'accessions.sample': {
                        'biosample_accession': biosample_accession,
                        'sample_accession': sra_accession,
                        'sample_id': str(oid)}
                }})

    def add_study_accession(self, bioproject_accession, sra_study_accession, study_accession, collection_id):
        return self.get_collection_handle().update_one(
            {
                "_id": ObjectId(collection_id)
            },
            {"$set":
                {
                    'accessions.study_accessions': {
                        'bioProjectAccession': bioproject_accession,
                        'sraStudyAccession': sra_study_accession,
                        'submissionAccession': study_accession,
                        'status': 'accepted'}
                }}
        )

    def get_study(self, collection_id):
        # return if study has been already submitted
        return self.get_collection_handle().count_documents(
            {'$and': [{'_id': ObjectId(collection_id)}, {'accessions.study_accessions': {'$exists': 'true'}}]})

    def update_submission_modified_timestamp(self, sub_id):
        return self.get_collection_handle().update_one(
            {"_id": ObjectId(sub_id)}, {
                "$set": {"date_modified": helpers.get_datetime()}}
        )

    def get_submission_from_sample_id(self, s_id):
        query = "accessions.sample_accessions." + s_id
        projection = "accessions.study_accessions"
        return cursor_to_list(self.get_collection_handle().find({query: {"$exists": True}}, {projection: 1}))

    def set_manifest_submission_pending(self, s_id):
        if self.get_collection_handle().update_one({"_id": ObjectId(s_id)},
                                                   {"$set": {"processing_status": "pending", "date_modified":
                                                             helpers.get_datetime()}}):
            return True
        else:
            return False

    def add_assembly_accession(self, s_id, accession, alias, assembly_idstr):
        # todo if it's decided to have multiple assemblies per profile add accessions.assembly.sample to be able to cross
        # reference assembly and sample
        assembly_accession = self.get_collection_handle().find_one(
            {"_id": ObjectId(s_id), "accessions.assembly.accession": accession}, {"_id": 1})
        if not assembly_accession:
            self.get_collection_handle().update_one({"_id": ObjectId(s_id)},
                                                    {"$push": {
                                                        "accessions.assembly": {"accession": accession, "alias": alias,
                                                                                "assembly_id": assembly_idstr}}})
        return

    def add_annotation_accessions(self, s_id, accession):
        # todo if it's decided to have multiple assemblies per profile add accessions.annotation.sample to be able to cross
        # reference annotation and sample
        # self.get_collection_handle().update_one(
        #    {"_id": ObjectId(s_id)}, {"$addToSet": {"accessions.seq_annotation": {"$each": accession}}, "$pull" : {"seq_annotation_submission_error": {"seq_annotation_id": accession[0]["alias"]}}})
        self.get_collection_handle().update_one({"_id": ObjectId(s_id)},
                                                {"$addToSet": {"accessions.seq_annotation": {"$each": accession}}})

    def make_seq_annotation_submission_uploading(self, sub_id, seq_annotation_ids):
        sub_handle = self.get_collection_handle()
        submission = sub_handle.find_one({"_id": ObjectId(sub_id)}, {
                                         "seq_annotation_status": 1})

        if not submission:
            return dict(status='error', message="System Error! Please contact the administrator.")

        if submission.get("seq_annotation_status", str()) in ["pending", "sending"]:
            return dict(status='error', message="Sequence annotation submission is in process, please try again later!")

        sub_handle.update_one({"_id": ObjectId(sub_id)},
                              {"$set": {"seq_annotation_status": "uploading", "date_modified":
                                        helpers.get_datetime()},
                               "$addToSet": {"seq_annotations": {"$each": seq_annotation_ids}}})
        return dict(status='success', message="Sequence annotation submission has been scheduled!")

    def update_seq_annotation_submission(self, sub_id, seq_annotation_id=str(), submission_id=[]):
        # when dtol sample has been processed, pull id from submission and check if there are remaining
        # samples left to go. If not, make submission complete. This will stop celery processing the this submission.
        sub_handle = self.get_collection_handle()
        # for sam_id in sam_ids:
        if submission_id:
            sub_handle.update_one({"_id": ObjectId(sub_id)},
                                  {"$pull": {"seq_annotation_submission": {"id": submission_id}}})
        if seq_annotation_id:
            sub_handle.update_one({"_id": ObjectId(sub_id)}, {
                "$pull": {"seq_annotations": seq_annotation_id}})
        sub = sub_handle.find_one({"_id": ObjectId(sub_id)}, {
                                  "seq_annotation_submission": 1, "seq_annotations": 1})
        if len(sub["seq_annotation_submission"]) < 1 and len(sub["seq_annotations"]) < 1:
            sub_handle.update_one({"_id": ObjectId(sub_id)},
                                  {"$set": {"seq_annotation_status": "complete",
                                            "date_modified": helpers.get_datetime()}})

    def get_seq_annotation_pending_submission(self):
        REFRESH_THRESHOLD = 3600  # time in seconds to retry stuck submission
        # called by celery to get samples the supeprvisor has set to be sent to ENA
        # those not yet sent should be in pending state. Occasionally there will be
        # stuck submissions in sending state, so get both types
        subs = self.get_collection_handle().find(
            {"repository":"ena","seq_annotation_status": {"$in": ["sending", "pending"]}},
            {"seq_annotation_status": 1, "profile_id": 1, "date_modified": 1, "seq_annotations": 1})
        sub = cursor_to_list(subs)
        out = list()
        current_time = helpers.get_datetime()
        for s in sub:
            # calculate whether a submission is an old one
            if s.get("seq_annotation_status", "") == "sending":
                if len(s.get("seq_annotations", [])) == 0:
                    # all samples have been submitted
                    self.get_collection_handle().update_one({"_id": s["_id"]},
                                                            {"$set": {"seq_annotation_status": "complete", "date_modified": current_time}})

                recorded_time = s.get("date_modified", current_time)
                time_difference = current_time - recorded_time
                if time_difference.total_seconds() > (REFRESH_THRESHOLD):
                    # submission retry time has elapsed so re-add to list
                    out.append(s)
                    self.update_submission_modified_timestamp(s["_id"])
                    lg.error("ADDING STALLED SEQ ANNOTATION SUBMISSION " + str(s["_id"]) + "BACK INTO QUEUE - copo_da")
                    # no need to change status
            elif s.get("seq_annotation_status", "") == "pending":
                out.append(s)
                # self.update_submission_modified_timestamp(s["_id"])
                self.get_collection_handle().update_one({"_id": s["_id"]},
                                                        {"$set": {"seq_annotation_status": "sending",
                                                                  "date_modified": current_time}})
        return out

    def get_seq_annotation_file_uploading(self):
        subs = self.get_collection_handle().find(
            {"repository":"ena","seq_annotation_status": "uploading"},
            {"seq_annotations": 1, "profile_id": 1, "date_modified": 1})
        return cursor_to_list(subs)

    def update_seq_annotation_submission_async(self, sub_id, href, seq_annotation_ids, submission_id):
        sub_handle = self.get_collection_handle()
        submission = {'id': submission_id,
                      'seq_annotation_id': seq_annotation_ids, 'href': href}
        sub_handle.update_one({"_id": ObjectId(sub_id)},
                              {"$set": {"date_modified": helpers.get_datetime()},
                               "$push": {"seq_annotation_submission": submission},
                               "$pull": {"seq_annotations": {"$in": seq_annotation_ids}}})

    def get_async_seq_annotation_submission(self):
        sub_handle = self.get_collection_handle()
        subs = sub_handle.find({"repository":"ena","seq_annotation_submission": {"$exists": True, "$ne": []}},
                               {"_id": 1, "seq_annotation_submission": 1, "profile_id": 1})
        return cursor_to_list(subs)

    '''
    def update_seq_annotation_submission_error(self, sub_id, seq_annotation_ids, error):

        sub_handle = self.get_collection_handle()
        sub = sub_handle.find_one({"_id": ObjectId(sub_id), "seq_annotation_submission.id": seq_annotation_submission_id}, {"seq_annotation_submission.seq_annotation_id": 1})
        seq_annotation_ids = sub["seq_annotation_submission"][0]["seq_annotation_id"]
        for id in seq_annotation_ids:    
            count = sub_handle.find({"_id": ObjectId(sub_id), "seq_annotation_submission_error.seq_annotation_id": id}).count()
            if count == 0:
                sub_handle.update_one({"_id": ObjectId(sub_id)},
                            {"$set": {"date_modified": datetime.now()}, "$push": {"seq_annotation_submission_error": {"seq_annotation_id": id, "error": error}}})
            else:
                sub_handle.update_one({"_id": ObjectId(sub_id), "seq_annotation_submission_error.seq_annotation_id": id},
                            {"$set": {"date_modified": datetime.now(), "seq_annotation_submission_error.$.error": error}})
    '''

    def update_seq_annotation_submission_pending(self, sub_ids):
        self.get_collection_handle().update_many({"_id": {"$in": sub_ids}},
                                                 {"$set": {"seq_annotation_status": "pending"}})

    def reset_read_submisison_bundle(self, submission_id):
        submission = self.get_record(submission_id)

        samples = Sample(profile_id=self.profile_id).get_all_records_columns(
            filter_by={"read.file_id": {"$in": submission["bundle"]}}, projection={"read": 1})
        for sample in samples:
            is_update = False
            for read in sample["read"]:
                if read["file_id"] in submission["bundle"]:
                    if read["status"] == "processing":
                        read["status"] = "pending"
                        is_update = True

            if is_update:
                sample["date_modified"] = helpers.get_datetime()
                Sample(profile_id=self.profile_id).get_collection_handle().update_one({"_id": sample["_id"]},
                                                                                      {"$set": sample})
        self.get_collection_handle().update_one(
            {"_id": ObjectId(submission_id)}, {"$set": {"bundle": []}})

    def reset_dtol_submission_status(self, submission_id, samples_ids):
        doc = self.get_collection_handle().find_one(
            {"_id": ObjectId(submission_id)})
        l = len(doc["dtol_samples"])
        if l > 0:
            status = "pending"
        else:
            status = "complete"

        Submission().get_collection_handle().update_one(
            {"_id": ObjectId(submission_id)}, {"$set": {"dtol_status": status}})
        if samples_ids:
            object_samples_ids = [ObjectId(x) for x in samples_ids]
            Sample().get_collection_handle().update_many({"_id": {"$in": object_samples_ids}},
                                                     {"$set": {"status": "processing"}})

    def make_tagged_seq_submission_pending(self, sub_id, target_ids):
        self.get_collection_handle().update_one({"_id": ObjectId(sub_id)},  {"$set": {
            "tagged_seq_status": "pending"}, "$addToSet": {"tagged_seqs": {"$each": target_ids}}})

    def get_tagged_seq_pending_submission(self):
        REFRESH_THRESHOLD = 3600  # time in seconds to retry stuck submission
        # called by celery to get samples the supeprvisor has set to be sent to ENA
        # those not yet sent should be in pending state. Occasionally there will be
        # stuck submissions in sending state, so get both types
        subs = self.get_collection_handle().find(
            {"tagged_seq_status": {"$in": ["sending", "pending"]}},
            {"tagged_seq_status": 1, "profile_id": 1, "date_modified": 1, "tagged_seqs": 1})
        sub = cursor_to_list(subs)
        out = list()
        current_time = helpers.get_datetime()
        for s in sub:
            # calculate whether a submission is an old one
            if s.get("tagged_seq_status", "") == "sending":
                recorded_time = s.get("date_modified", current_time)
                time_difference = current_time - recorded_time
                if time_difference.total_seconds() > (REFRESH_THRESHOLD):
                    # submission retry time has elapsed so re-add to list
                    out.append(s)
                    self.update_submission_modified_timestamp(s["_id"])
                    lg.error("ADDING STALLED TAGGED SEQ SUBMISSION " + str(s["_id"]) + "BACK INTO QUEUE - copo_da")
                    # no need to change status
            elif s.get("tagged_seq_status", "") == "pending":
                out.append(s)
                # self.update_submission_modified_timestamp(s["_id"])
                self.get_collection_handle().update_one({"_id": ObjectId(s["_id"])},
                                                        {"$set": {"tagged_seq_status": "sending", "date_modified": current_time}})
        return out
    
    def get_accession_records_count(self, element_dict):
        # Check if there are any records in the collection in the database
        # based on the 'accessions' field
        showAllCOPOAccessions = element_dict['showAllCOPOAccessions']
        isUserProfileActive = element_dict['isUserProfileActive']
        profile_id = element_dict['profile_id']

        filter = dict()
        filter["accessions"] = {"$exists": True, "$ne": {}}

        if not showAllCOPOAccessions:
            if isUserProfileActive and profile_id:
                filter['profile_id'] = profile_id

        return self.get_collection_handle().count_documents(filter)
    
    def get_non_sample_accessions(self, element_dict):
        from .profile_da import Profile
        from .da_utils import filter_non_sample_accession_dict_lst

        # Get elements from the dictionary
        draw = element_dict['draw']
        start = element_dict['start']
        length = element_dict['length']
        sort_by = element_dict['sort_by']
        dir = element_dict['dir']
        search = element_dict['search']
        showAllCOPOAccessions = element_dict['showAllCOPOAccessions']
        isUserProfileActive = element_dict['isUserProfileActive']
        profile_id = element_dict['profile_id']
        filter_accessions = element_dict['filter_accessions']
        
        filter = dict()

        if not showAllCOPOAccessions:
            if isUserProfileActive and profile_id:
                filter['profile_id'] = profile_id

        filter["accessions"] = {"$exists": True, "$ne": {}}
        projection = {"_id": 1, "accessions": 1, "profile_id": 1}
        
        total_count = 0
        sort_clause = [[sort_by, dir]]
        handler = self.get_collection_handle()

        records = cursor_to_list_str(handler.find(
            filter, projection).sort(sort_clause), use_underscore_in_id=False)

        # Declare labels for 'sample' accession
        sample_accession_labels = ['sample_accession','sample_alias']
        labels = ['accession', 'alias', 'profile_title']

        out = list()
        
        if records:
            for i in records:
                # Get profile title
                try:
                    profile_title = Profile().get_name(i.get("profile_id", ""))
                except AttributeError as e:
                    profile_title = ""
                
                # Reorder the list of accessions types
                reordered_accessions_dict = {k: i.get("accessions", "").get(k, "") for k in
                                            NON_SAMPLE_ACCESSION_TYPES if i.get("accessions", "").get(k, "")}

                for key, value in reordered_accessions_dict.items():
                    for accession in value:
                        row_data = dict()
                        row_data['record_id'] = i.get('id','')
                        row_data['DT_RowId'] = 'row_' + i.get('id','')
                        row_data['profile_id'] = i.get('profile_id','')
                        row_data['accession_type'] = key
                        
                        # Filter value dictionary to get 'accession' and 'alias' key-value pair
                        if key == 'sample':
                            # Account for 'sample' accession which has accession and alias in a different format 
                            value_dict = {k.split('_')[1]: v for k, v in accession.items() if k in sample_accession_labels}
                            
                            # Rename 'sample_accession' to 'accession' and 'sample_alias' to 'alias'
                            value_dict = {k.replace('sample_', ''): v for k, v in value_dict.items()}
                        else:
                            value_dict = {k: v for k, v in accession.items() if k in labels}
                        
                        for k, v in value_dict.items():
                            row_data.update({k: v})
                        
                        # Update list of dictionaries with profile title
                        row_data.update({'profile_title': profile_title})
                        out.append(row_data)
                
        if filter_accessions or search:
            # Filter based on accession type   
            if filter_accessions:
                out = [x for x in out if x.get("accession_type") in filter_accessions]

            # Filter based on search query
            if search:
                out = filter_non_sample_accession_dict_lst(out, search)

        # Slice 'out' which is a list of dictionaries 
        # based on the values for start and length
        total_count = len(out)
        out = out[int(start):int(start) + int(length)]

        result = dict()
        result["recordsTotal"] = total_count
        result["recordsFiltered"] = total_count
        result["draw"] = draw
        result["data"] = out
        return result

    def make_assembly_submission_uploading(self, sub_id, assembly_ids):
        sub_handle = self.get_collection_handle()
        submission = sub_handle.find_one(
            {"_id": ObjectId(sub_id)}, {"assembly_status": 1})

        if not submission:
            return dict(status='error', message="System Error! Please contact the administrator.")

        if submission.get("assembly_status", str()) in ["pending", "sending"]:
            return dict(status='error', message="Assembly submission is in process, please try again later!")

        sub_handle.update_one({"_id": ObjectId(sub_id)},
                              {"$set": {"assembly_status": "uploading", "date_modified":
                                        helpers.get_datetime()},
                               "$addToSet": {"assemblies": {"$each": assembly_ids}}})
        return dict(status='success', message="Assembly submission has been scheduled!")

    def update_assembly_submission(self, sub_id, assembly_id=str()):
        # when dtol sample has been processed, pull id from submission and check if there are remaining
        # samples left to go. If not, make submission complete. This will stop celery processing the this submission.
        sub_handle = self.get_collection_handle()
        # for sam_id in sam_ids:
        if assembly_id:
            sub_handle.update_one({"_id": ObjectId(sub_id)}, {
                "$pull": {"assemblies": assembly_id}})

        sub = sub_handle.find_one({"_id": ObjectId(sub_id)}, {"assemblies": 1})
        if len(sub["assemblies"]) < 1:
            sub_handle.update_one({"_id": ObjectId(sub_id)},
                                  {"$set": {"assembly_status": "complete",
                                            "date_modified": helpers.get_datetime()}})

    def get_assembly_pending_submission(self):
        REFRESH_THRESHOLD = 3600  # time in seconds to retry stuck submission
        # called by celery to get samples the supeprvisor has set to be sent to ENA
        # those not yet sent should be in pending state. Occasionally there will be
        # stuck submissions in sending state, so get both types
        subs = self.get_collection_handle().find(
            {"repository":"ena", "assembly_status": {"$in": ["sending", "pending"]}},
            {"assembly_status": 1, "profile_id": 1, "date_modified": 1, "assemblies": 1})
        sub = cursor_to_list(subs)
        out = list()
        current_time = helpers.get_datetime()
        for s in sub:
            # calculate whether a submission is an old one
            if s.get("assembly_status", "") == "sending":
                recorded_time = s.get("date_modified", current_time)
                time_difference = current_time - recorded_time
                if time_difference.total_seconds() > (REFRESH_THRESHOLD):
                    # submission retry time has elapsed so re-add to list
                    out.append(s)
                    self.update_submission_modified_timestamp(s["_id"])
                    lg.error("ADDING STALLED ASSEMBLY SUBMISSION " + str(s["_id"]) + "BACK INTO QUEUE - copo_da")
                    # no need to change status
            elif s.get("assembly_status", "") == "pending":
                out.append(s)
                # self.update_submission_modified_timestamp(s["_id"])
                self.get_collection_handle().update_one({"_id": ObjectId(s["_id"])},
                                                        {"$set": {"assembly_status": "sending",
                                                                  "date_modified": current_time}})
        return out

    def get_assembly_file_uploading(self):
        subs = self.get_collection_handle().find(
            {"repository":"ena", "assembly_status": "uploading"},
            {"assemblies": 1, "profile_id": 1, "date_modified": 1})
        return cursor_to_list(subs)

    def update_assembly_submission_pending(self, sub_ids):
        self.get_collection_handle().update_many({"_id": {"$in": sub_ids}},
                                                 {"$set": {"assembly_status": "pending"}})


    def process_stale_dtol_submissions(self, refresh_threshold=3600):
        update_data = {"dtol_status": "pending", "repository":"ena", "date_modified": helpers.get_datetime()}
        self.get_collection_handle().update_many(
            {"type": {"$in": TOL_PROFILE_TYPES}, "repository":"ena", "dtol_status": "sending", "date_modified": {"$lt": helpers.get_datetime() - timedelta(seconds=refresh_threshold)}},{"$set": update_data})
        
                
    def make_submission_downloading(self, profile_id, component, component_id, repository="zenodo"):
        sub_handle = self.get_collection_handle()
        submission = sub_handle.find_one(
            {"profile_id": profile_id, "repository":repository}, {f"{component}_status": 1})
        dt = helpers.get_datetime()
        user = ThreadLocal.get_current_user() 

        if not submission:
            submission = {"profile_id": profile_id, "deleted": helpers.get_not_deleted_flag(), "repository": repository, f"{component}_status":"downloading", "created_by": user.id, "date_created": dt, "updated_by": user.id, "date_modified": dt}
            #create a new submission
            result = Submission().get_collection_handle().insert_one(submission)
            submission["_id"] = result.inserted_id

        if submission.get(f"{component}_status", str()) in ["sending"]:
            return dict(status='error', message="Submission is in process, please try again later!")

        sub_handle.update_one({"_id": submission["_id"]},
                              {"$set": {f"{component}_status": "downloading", "date_modified":dt, "updated_by": user.id},
                               "$addToSet": {component: component_id}})
            
        return dict(status='success', message="Submission has been scheduled!")

    def get_submission_downloading(self, component="study"):
        subs = self.get_collection_handle().find(
            {f"{component}_status": "downloading", "deleted": helpers.get_not_deleted_flag()},
            {component: 1, "profile_id": 1, "date_modified": 1})
        return cursor_to_list(subs)

    def remove_component_from_submission(self, sub_id, component="study", component_ids=[]):
            sub_handle = self.get_collection_handle()
            sub = sub_handle.find_one({"_id": ObjectId(sub_id)}, {component: 1})
            if not sub:
                return dict(status='error', message="System Error! Please contact the administrator.")
            
            update_data = {}
            update_data["$set"] = {}

            sub[component] = list(set(sub[component]) - set(component_ids))
            if len(sub[component]) == 0:
                update_data["$set"] = {f"{component}_status": "complete", component: []}
            else:
                update_data["$pull"] = {component: {"$each": component_ids}}
                
            if update_data.get("$set", None) or update_data.get("$pull", None):
                update_data["$set"]["date_modified"] = helpers.get_datetime()
                update_data["$set"]["updated_by"] = "system"
                sub_handle.update_one({"_id": ObjectId(sub_id)}, update_data)

    def update_submission_pending(self, sub_ids, component="study"):
        self.get_collection_handle().update_many({"_id": {"$in": sub_ids}},
                                                 {"$set": {f"{component}_status": "pending"}})
        
 
    def add_component_submission_accession(self, sub_id, accessions=[], component="study"):
        self.get_collection_handle().update_one({"_id": ObjectId(sub_id)},
                                                  {"$addToSet": {f"accessions.{component}": {"$each": accessions}}})

    def get_pending_submission(self, repository="zenodo", component="study"):
        REFRESH_THRESHOLD = 600  # time in seconds to retry stuck submission
        # called by celery to get samples the supeprvisor has set to be sent to Zenodo
        # those not yet sent should be in pending state. Occasionally there will be
        # stuck submissions in sending state, so get both types
        subs = self.get_collection_handle().find(
            {f"{component}_status": {"$in": ["sending", "pending"]}, "repository": repository},
            {f"{component}_status": 1, "profile_id": 1, "date_modified": 1, component: 1, "accessions":1})
        sub = cursor_to_list(subs)
        out = list()
        current_time = helpers.get_datetime()
        for s in sub:
            # calculate whether a submission is an old one
            if s.get(f"{component}_status", "") == "sending":
                recorded_time = s.get("date_modified", current_time)
                time_difference = current_time - recorded_time
                if time_difference.total_seconds() > (REFRESH_THRESHOLD):
                    # submission retry time has elapsed so re-add to list
                    out.append(s)
                    self.update_submission_modified_timestamp(s["_id"])
                    lg.error("ADDING STALLED SUBMISSION " + str(s["_id"]) + "BACK INTO QUEUE - copo_da")
                    # no need to change status
            elif s.get(f"{component}_status", "") == "pending":
                out.append(s)
                # self.update_submission_modified_timestamp(s["_id"])
                self.get_collection_handle().update_one({"_id": ObjectId(s["_id"])},
                                                        {"$set": {f"{component}_status": "sending",
                                                                  "date_modified": current_time}})
        return out