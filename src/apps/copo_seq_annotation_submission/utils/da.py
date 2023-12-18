from common.dal.copo_da import DAComponent
from common.dal.submission_da import Submission
from common.dal.mongo_util import cursor_to_list
from bson.objectid import ObjectId

class SequenceAnnotation(DAComponent):
    def __init__(self, profile_id=None):
        super(SequenceAnnotation, self).__init__(profile_id, "seqannotation")


    def add_accession(self, id, accession):
        self.get_collection_handle().update_one({"_id": ObjectId(id)},
                                                {"$set": {"accession": accession, "error": []}})

    def update_seq_annotation_error(self, seq_annotation_ids, seq_annotation_sub_id, msg):
        seq_annotation_obj_ids = None

        if seq_annotation_ids:
            seq_annotation_obj_ids = [
                ObjectId(id) for id in seq_annotation_ids]

        elif seq_annotation_sub_id:
            result = Submission().get_collection_handle().find({"seq_annotation_submission.id": seq_annotation_sub_id},
                                                               {"seq_annotation_submission.$": 1})
            if result:
                records = cursor_to_list(result)
                seq_annotation_obj_ids = [ObjectId(id) for id in
                                          records[0]['seq_annotation_submission'][0]['seq_annotation_id']]

        if seq_annotation_obj_ids:
            self.get_collection_handle().update_many({"_id": {"$in": seq_annotation_obj_ids}},
                                                     {"$set": {"error": msg}})

    def validate_and_delete(self, target_id=str(), target_ids=list()):
        if not target_ids:
            target_ids = []
        if target_id:
            target_ids.append(target_id)

        seq_annotation_obj_ids = [ObjectId(id) for id in target_ids]
        result = self.execute_query(
            {"_id": {"$in": seq_annotation_obj_ids},  "accession": {"$exists": True, "$ne": ""}})
        if result:
            return dict(status='error', message="One or more sequence annotation record/s have been accessed!")

        self.get_collection_handle().delete_many(
            {"_id": {"$in": seq_annotation_obj_ids}})
        return dict(status='success', message="Sequence annotation record/s have been deleted!")