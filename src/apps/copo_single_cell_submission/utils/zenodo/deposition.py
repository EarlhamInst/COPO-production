from .zenodoclient import ZenodoClient

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
    
    """
    def upload_files(self, _id, files , bytesstring=None): 
        file_data = {}
        for file in files: 
            file_data[os.path.basename(file)] =  open(file, 'rb')
        if bytesstring:
            file_data["manifest"] = bytesstring
        return ZenodoClient().do_post(method=f"/deposit/depositions/{_id}/files",  files=file_data )
    """
    def upload_files(self, bucket_link, files , bytesstring=None): 
        for file in files: 
            file_data = {}
            file_name = os.path.basename(file)
            file_data[file_name] = open(file, 'rb')
            ZenodoClient().do_put(url=f"{bucket_link}/{file_name}", files=file_data )
        # Upload the manifest file if provided
        if bytesstring:
            file_data = {}
            file_name = "manifest.xlsx"
            file_data[file_name] = bytesstring
            ZenodoClient().do_put(url=f"{bucket_link}/{file_name}", files=file_data )

    def delete_file(self, _id, file_id):
        return ZenodoClient().do_delete(method=f"/deposit/depositions/{_id}/files/{file_id}")


    def delete(self, _id):
        return ZenodoClient().do_delete(method=f"/deposit/depositions/{_id}")
    
    def publish(self, _id):
        return ZenodoClient().do_post(method=f"/deposit/depositions/{_id}/actions/publish")
    
    def unlock(self, _id):
        return ZenodoClient().do_post(method=f"/deposit/depositions/{_id}/actions/edit")
