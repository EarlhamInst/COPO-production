from common.dal.submission_da import Submission
from da import Singlecell, SinglecellSchemas
from common.utils.logger import Logger
from common.dal.copo_da import EnaFileTransfer
from bson import regex
import os
from common.utils.helpers import  notify_singlecell_status, get_not_deleted_flag
from io import BytesIO
from SingleCellSchemasHandler import SingleCellSchemasHandler
from zenodo.deposition import Zenodo_deposition
import datetime



def update_submission_pending():
    subs = Submission().get_submission_downloading(repository="zenodo")
    all_downloaded_sub_ids = []
    for sub in subs:
        all_file_downloaded = True
        for study_id in sub["studies"]:
            singlecell = Singlecell().get_all_records_columns(
                filter_by={"profile_id":sub["profile_id"], "study_id":study_id,"deleted":helpers.get_not_deleted_flag()},
                projection={"schema_name":1,"checklist_id":1, "components":1})
            if not singlecell:
                Submission().update_singlecell_submission(sub_id=str(sub["_id"]), study_id= study_id)
                continue
            schemas = SinglecellSchemas().get_schema(schema_name=singlecell["schema_name"], target_id=singlecell["checklist_id"])
            files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas["components"])

            local_path = [regex.Regex(f'^{x}$') for x in files]
            projection = {'_id':0, 'file_location':1, 'status':1}
            filter = dict()
            filter['local_path'] = {'$in': local_path}
            filter['profile_id'] = sub["profile_id"]
        
            enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)
 
            if len(files) > len(enaFiles):
                all_file_downloaded = False
                # Log the missing files
                missing_files = set(files) - {os.path.basename(enaFile["file_location"]) for enaFile in enaFiles}
                Logger().error(f"Files not uploaded for submission {sub['_id']} : study {study_id} : {missing_files} ") 
                break

            elif not all( enaFile["status" ] == "complete" for enaFile in enaFiles):
                all_file_downloaded = False
                break

        if all_file_downloaded:
            all_downloaded_sub_ids.append(sub["_id"])

    if all_downloaded_sub_ids:
        Submission().update_submission_pending(all_downloaded_sub_ids)


def process_pending_submission():
    submissions = Submission().get_pending_submission(repository="zenodo")
    if not submissions:
        return
    
    for sub in submissions:
        notify_singlecell_status(data={"profile_id": sub["profile_id"]},
                msg=f"Submitting to zenodo...",
                action="info",
                html_id="singlecell_info")
        
        accession_map = {}
        new_accession_map = {}
        study_accessions  = sub.get("accessions",{}).get("study", [])
        for item in study_accessions:
            accession_map[item["study_id"]] = item 

        for study_id in sub["studies"]:
            singlecell = Singlecell().get_all_records_columns(
                filter_by={"profile_id":sub["profile_id"], "study_id":study_id,"deleted": get_not_deleted_flag()},
                projection={"schema_name":1,"checklist_id":1, "components":1})
            #generate the manifest for submission
            schemas = SinglecellSchemas().get_collection_handle().find_one({"name":singlecell["schema_name"]})
            bytesstring = BytesIO()
            SingleCellSchemasHandler().write_manifest(singlecell_schema=schemas, checklist_id=singlecell["checklist_id"], singlecell=singlecell, file_path=bytesstring)
            #bytesstring.getvalue()

            files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas["components"])
            local_path = [regex.Regex(f'^{x}$') for x in files]
            projection = {'_id':0, 'file_location':1, 'status':1}
            filter = dict()
            filter['local_path'] = {'$in': local_path}
            filter['profile_id'] = sub["profile_id"]
        
            enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)
            file_locations = [enaFile["file_location"] for enaFile in enaFiles ]

            creators = [{"name": "COPO", "affiliation": "EI"}]
            for person in singlecell.get("person",[]):
                if person.get("givenName","") and person.get("familyName",""):
                    creators.append( {"name": f"{person['givenName']} {person['familyName']}",
                                       "affiliation": person.get("institution", ""),
                                        "orcid": person.get("orcid", "")
                                       } )
                                       
                else:
                    Logger().error(f"Missing first name or last name for person: {person}")


            zenodo_data = {
                "metadata": {
                    "title":  singlecell.get("study",{}).get("title", study_id),
                    "upload_type": "dataset",
                    #"image_type": "photo",
                    "access_right": "embargoed",
                    "description": singlecell.get("study",{}).get("description", study_id),
                    "license": "CC-BY-4.0",
                    "creators":  creators,
                    "embargo_date":  (datetime.datetime.now() + datetime.timedelta(days=2*365)).strftime("%Y-%m-%d")
                },  "keywords": [study_id, "COPO broker"] 
            }

            accession = accession_map.get(study_id,{})
            deposition_id = ""
            if not accession:
                deposition = Zenodo_deposition().create(deposition=zenodo_data)
                new_accession_map[study_id] =  {"study_accession":deposition["id"], study_id:study_id, "doi": deposition["doi"], "is_published": deposition["submitted"]}
                deposition_id = deposition["id"]
            else:
                deposition_id = accession["study_accession"]
                Zenodo_deposition().unlock(_id=deposition_id)   
                Zenodo_deposition().update(_id=deposition_id, deposition=zenodo_data)

            Zenodo_deposition().upload_files(_id=deposition_id, files= file_locations, bytesstring=bytesstring.getvalue())
    

        Submission().update_zendodo_submission_accession(sub_id=str(sub["_id"]), accessions=new_accession_map)