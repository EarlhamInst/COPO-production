from common.dal.copo_da import DAComponent
from bson.objectid import ObjectId

class Assembly(DAComponent):
    def __init__(self, profile_id=None):
        super(Assembly, self).__init__(profile_id, "assembly")

    def add_accession(self, id, accession):
        self.get_collection_handle().update_one({"_id": ObjectId(id)},
                                                {"$set": {"accession": accession, "error": ""}})

    def update_assembly_error(self, assembly_ids, msg):
        assembly_obj_ids = [ObjectId(id) for id in assembly_ids]
        self.get_collection_handle().update_many({"_id": {"$in": assembly_obj_ids}},
                                                 {"$set": {"error": msg}})

    def validate_and_delete(self, target_id=str(), target_ids=list()):
        if not target_ids:
            target_ids = []
        if target_id:
            target_ids.append(target_id)
        assembly_obj_ids = [ObjectId(id) for id in target_ids]
        result = self.execute_query(
            {"_id": {"$in": assembly_obj_ids},  "accession": {"$exists": True, "$ne": ""}})
        if result:
            return dict(status='error', message="One or more Assembly has been accessed!")

        self.get_collection_handle().delete_many(
            {"_id": {"$in": assembly_obj_ids}})
        return dict(status='success', message="Assembly record/s have been deleted!")
