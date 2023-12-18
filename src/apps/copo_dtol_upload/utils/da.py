from common.dal.copo_da import DAComponent
from bson.objectid import ObjectId

class ValidationQueue(DAComponent):
    def __init__(self, profile_id=None):
        super(ValidationQueue, self).__init__(profile_id, "validationQueue")

    def get_queued_manifests(self):
        m_list = self.get_collection_handle().find(
            {"schema_validation_status": "pending", "taxon_validation_status": "pending"})
        out = list(m_list)
        for el in out:
            self.get_collection_handle().update_one({"_id": el["_id"]}, {
                "$set": {"schema_validation_status": "processing", "taxon_validation_status":
                         "processing"}})
        return out

    def update_manifest_data(self, record_id, manifest_data):
        self.get_collection_handle().update_one({"_id": ObjectId(record_id)},
                                                {"$set": {"manifest_data": manifest_data}})

    def set_update_flag(self, record_id):
        self.get_collection_handle().update_one(
            {"_id": ObjectId(record_id)}, {"$set": {"isupdate": True}})

    def set_taxon_validation_complete(self, record_id):
        self.get_collection_handle().update_one({"_id": ObjectId(record_id)},
                                                {"$set": {"taxon_validation_status": "complete"}})

    def set_taxon_validation_error(self, record_id, err):
        self.get_collection_handle().update_one({"_id": ObjectId(record_id)},
                                                {"$set": {"taxon_validation_status": "error"}, "$push": {"err_msg":
                                                                                                         err}})

    def set_schema_validation_complete(self, record_id):
        self.get_collection_handle().update_one({"_id": ObjectId(record_id)},
                                                {"$set": {"schema_validation_status": "complete"}})

    def set_schema_validation_error(self, record_id, err):
        self.get_collection_handle().update_one({"_id": ObjectId(record_id)},
                                                {"$set": {"schema_validation_status": "error"}, "$push": {"err_msg":
                                                                                                          err}})