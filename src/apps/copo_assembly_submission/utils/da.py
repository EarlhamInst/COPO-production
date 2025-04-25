from common.dal.copo_da import DAComponent
from bson.objectid import ObjectId
from common.dal.submission_da import Submission
from common.dal.copo_da import EnaFileTransfer, DataFile
 
class Assembly(DAComponent):
    def __init__(self, profile_id=None, subcomponent=None):
        super(Assembly, self).__init__(profile_id, "assembly", subcomponent=subcomponent)

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

        submission = Submission().get_collection_handle().find_one({"profile_id": self.profile_id, "assemblies": {"$in": target_ids}},{"_id": 1})
        if submission:
            return dict(status='error', message="One or more assembly record/s have been submitting!")
        
        assembly_obj_ids = [ObjectId(id) for id in target_ids]
        
        #delete the corresponding file records
        file_ids = []
        assemblies = self.get_collection_handle().find({"_id": {"$in": assembly_obj_ids}},{"accession":1, "files": 1})
        for assembly in assemblies:
            if assembly.get("accession",""):
                return dict(status='error', message="One or more assembly record/s have been accessed!")
            file_ids.extend(assembly.get('files', []))
        EnaFileTransfer(profile_id=self.profile_id).get_collection_handle().delete_many({"file_id": {"$in": file_ids}})   
        DataFile(profile_id=self.profile_id).get_collection_handle().delete_many({"_id": {"$in": [ObjectId(id) for id in file_ids]}})


        self.get_collection_handle().delete_many(
            {"_id": {"$in": assembly_obj_ids}})
        return dict(status='success', message="Assembly record/s have been deleted!")
