from zenodoclient import ZenodoClient

import os

class Zenodo_deposition:
    def __init__(self, metadata={}, creators=[]):
        self.metadata = metadata
        self.creators = creators

    def read(self, _id):
        return ZenodoClient().do_get(method=f"/deposit/depositions/{_id}")

    def create(self, deposition):
        return ZenodoClient().do_post(method="/deposit/depositions", params=deposition)

    def update(self, _id, deposition ):
        return ZenodoClient().do_put(method=f"/deposit/depositions/{_id}", params=deposition)
    
    def upload_files(self, _id, files , bytesstring=None): 
        file_data = {}
        for file in files: 
            file_data[os.base.path(file)] =  open(file, 'rb')
        if bytesstring:
            file_data["manifest"] = bytesstring
        return ZenodoClient().do_post(method=f"/deposit/depositions/{_id}/files",  files=file_data )


    def delete(self, _id):
        return ZenodoClient().do_delete(method=f"/deposit/depositions/{_id}")
    
    def publish(self, _id):
        return ZenodoClient().do_post(method=f"/deposit/depositions/{_id}/actions/publish")
    
    def unlock(self, _id):
        return ZenodoClient().do_post(method=f"/deposit/depositions/{_id}/actions/edit")
