from common.dal.copo_da import DAComponent, EnaChecklist
from bson.objectid import ObjectId

class TaggedSequence(DAComponent):
    def __init__(self, profile_id=None, subcomponent=None):
        super(TaggedSequence, self).__init__(profile_id, "taggedseq", subcomponent=subcomponent)

    def get_schema(self, target_id=str()):
        if not target_id:
            return dict(schema_dict=[],
                        schema=[]
                        )
        taggedSeq = TaggedSequence(self.profile_id).get_record(target_id)
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
            return dict(status='error', message="One or more tagged sequence record/s have been accessed or scheduled to submit!")

        self.get_collection_handle().delete_many(
            {"_id": {"$in":   tagged_seq_ids}})
        return dict(status='success', message="Tagged Sequence record/s have been deleted!")

    def update_tagged_seq_processing(self, profile_id=str(), tagged_seq_ids=list()):
        tagged_seq_obj_ids = [ObjectId(id) for id in tagged_seq_ids]
        self.get_collection_handle().update_many({"profile_id": profile_id,  "_id": {"$in":   tagged_seq_obj_ids},  "$or": [{"status": {"$exists": False}}, {"status": "pending"}]},
                                                 {"$set": {"status":  "processing"}})