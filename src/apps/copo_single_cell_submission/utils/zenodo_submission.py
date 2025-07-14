from common.dal.submission_da import Submission
from .da import Singlecell, SinglecellSchemas
from common.utils.logger import Logger
from common.dal.copo_da import EnaFileTransfer, DataFile 
from bson import regex
import os
from common.utils.helpers import  notify_singlecell_status, get_not_deleted_flag
from io import BytesIO
from .SingleCellSchemasHandler import SingleCellSchemasHandler
from .zenodo.deposition import Zenodo_deposition
import datetime

def process_pending_submission_zendo():
    submissions = Submission().get_pending_submission(repository="zenodo", component="study")
    if not submissions:
        return
    
    for sub in submissions:

        accession_map = {}
        new_accessions = []
        study_accessions  = sub.get("accessions",{}).get("study", [])
        for item in study_accessions:
            accession_map[item["study_id"]] = item 

        for study_id in sub["study"]:
            notify_singlecell_status(data={"profile_id": sub["profile_id"]},
                msg=f"Submitting {study_id} to zenodo...",
                action="info",
                html_id="submission_info")
        
            singlecell = Singlecell().get_collection_handle().find_one(
                 {"profile_id":sub["profile_id"], "study_id":study_id,"deleted": get_not_deleted_flag()},
                 {"schema_name":1,"checklist_id":1, "components":1})
            #generate the manifest for submission
            if not singlecell:
                msg = f"Missing singlecell for study: {study_id}"
                Logger().error(msg)
                notify_singlecell_status(data={"profile_id": sub["profile_id"]},
                    msg=msg,
                    action="error",
                    html_id="submission_info")
                Submission().remove_study_from_singlecell_submission(sub_id=str(sub["_id"]), study_id=study_id)
                continue

            singlecell_components = singlecell.get("components",{})

            studies = singlecell_components.get("study",[])
            if not studies:
                msg = f"Missing study for singlecell: {study_id}"
                Logger().error(msg)
                notify_singlecell_status(data={"profile_id": sub["profile_id"]},
                    msg=msg,
                    action="error",
                    html_id="submission_info")
                Submission().remove_study_from_singlecell_submission(sub_id=str(sub["_id"]), study_id=study_id)
                continue
            study = studies[0]

            schemas = SinglecellSchemas().get_collection_handle().find_one({"name":singlecell["schema_name"]})
            bytesstring = BytesIO()
            #bytesstring.getvalue()
            SingleCellSchemasHandler().write_manifest(singlecell_schema=schemas, checklist_id=singlecell["checklist_id"], singlecell=singlecell, file_path=bytesstring)

            schemas = SinglecellSchemas().get_schema(schema_name=singlecell["schema_name"], target_id=singlecell["checklist_id"])
            files = SinglecellSchemas().get_all_files(singlecell=singlecell, schemas=schemas)
            local_path = [regex.Regex(f'{x}$') for x in files]
            projection = {'_id':0, 'local_path':1, 'status':1}
            filter = dict()
            filter['local_path'] = {'$in': local_path}
            filter['profile_id'] = sub["profile_id"]
        
            enaFiles = EnaFileTransfer().get_all_records_columns(filter_by=filter, projection=projection)
            datafiles = DataFile().get_all_records_columns(filter_by={"profile_id":sub["profile_id"], "filename": {"$in":files}}, projection={"_id":0, "file_name":1, "file_hash":1})

            file_locations_map = {os.path.basename(enaFile["local_path"]) : enaFile["local_path"] for enaFile in enaFiles }
            file_hash_map = {datafile["file_name"]: datafile["file_hash"] for datafile in datafiles}


            creators = [{"name": "COPO", "affiliation": "EI"}]
            for person in singlecell_components.get("person",[]):
                if person.get("givenName","") and person.get("familyName",""):
                    creators.append( {"name": f"{person['givenName']} {person['familyName']}",
                                       "affiliation": person.get("institution", ""),
                                        "orcid": person.get("orcid", "")
                                       } )
                                       
                else:
                    Logger().error(f"Missing first name or last name for person: {person}")

 
            zenodo_data = {
                "metadata": {
                    "title":  study.get("title", study_id),
                    "upload_type": "dataset",
                    #"image_type": "photo",
                    "access_right": "open",
                    "description": study.get("description", study_id),
                    "license":  study.get("licence", "CC0-1.0"),
                    "creators":  creators,
                    #"embargo_date":  (datetime.datetime.now() + datetime.timedelta(days=2*365)).strftime("%Y-%m-%d"),
                    "keywords": [study_id, "COPO broker"] 
                },
            }

            accession = accession_map.get(study_id,{})
            deposition_id = ""
            changed_files = []
            try: 
                if not accession:
                    deposition = Zenodo_deposition().create(deposition=zenodo_data)
                    new_accessions.append( {"study_id":study_id, "deposition_id":deposition["id"]})
                    deposition_id = deposition["id"]
                    changed_files = list(file_locations_map.values())

                else:
                    deposition_id = accession["deposition_id"]
                    deposition = Zenodo_deposition().read(_id=deposition_id)
                    if deposition.get("state","") == "done":
                        deposition = Zenodo_deposition().new_version(_id=deposition_id)    
                        deposition_id = deposition.get("id","")
                        Submission().update_component_submission_accession(profile_id=sub["profile_id"], repository="zenodo", component="study", identifier="study_id", accession={"study_id":study_id, "deposition_id":deposition["id"]})
                    else:
                        deposition = Zenodo_deposition().unlock(_id=deposition_id)
                    deposition = Zenodo_deposition().update(_id=deposition_id, deposition=zenodo_data)
                    uploaded_files = deposition.get("files",[]) 
                    changed_files_name  = set(file_locations_map.keys()) - {file["filename"] for file in uploaded_files}

                    for file in uploaded_files:
                        if file.get("filename","") not in files:
                            Zenodo_deposition().delete_file(_id=deposition_id, file_id=file["id"])
                        elif file.get("checksum","") != file_hash_map.get(file["filename"],""):
                            changed_files_name.add(file["filename"])
                    changed_files.extend([file_locations_map[file] for file in changed_files_name])

                Zenodo_deposition().upload_files(bucket_link=deposition.get("links",{}).get("bucket",""), files=changed_files, bytesstring=bytesstring)            
            except Exception as e:
                notify_singlecell_status(data={"profile_id": sub["profile_id"]},
                    msg=f"Problem to zenodo: {str(e)}",
                    action="error",
                    html_id="submission_info")
                continue

            Singlecell().update_component_status(id=singlecell["_id"], component="study", identifier="study_id", identifier_value=study_id, repository="zenodo", status_column_value={"status": "accepted", "state" : deposition["state"],  "accession": str(deposition["id"]), "doi" : deposition.get("doi",""), "error": "", "embargo_date":deposition.get("metadata",{}).get("embargo_date","")})  
            Submission().remove_component_from_submission(sub_id=str(sub["_id"]), component="study", component_ids=[study_id])

            notify_singlecell_status(data={"profile_id": sub["profile_id"]},
                    msg=f"{study_id} has been submitted to Zenodo.",
                    action="info",
                    html_id="submission_info")

        Submission().add_component_submission_accession(sub_id=str(sub["_id"]), component="study", accessions=new_accessions)

def publsh_zendo(profile_id, deposition_id, singlecell):
    """
    Publishes a submission to Zenodo.
    :param submission_id: The ID of the submission.
    :param deposition_id: The deposition_id information.
    :param singlecell: The singlecell data.
    """
    notify_singlecell_status(data={"profile_id": singlecell["profile_id"]},
        msg=f"Publishing {singlecell['study_id']} to zenodo...",
        action="info",
        html_id="submission_info")

    result = {}
    repository = "zenodo"
    try:
        deposition = Zenodo_deposition().read(_id=deposition_id)
        if deposition.get("state","") == "done":
            msg = f"{singlecell['study_id']} has been already published to Zenodo."
            Logger().log(msg)
            notify_singlecell_status(data={"profile_id": profile_id},
                msg=msg,
                action="info",
                html_id="submission_info")
            return {"status": "success", "message": msg, "doi": deposition.get("doi","")}
        deposition = Zenodo_deposition().publish(_id=deposition_id)
        Singlecell().update_component_status(id=singlecell["_id"], component="study", identifier="study_id", identifier_value=singlecell["study_id"], repository=repository, status_column_value={"status":"published", "state" : deposition["state"], "accession": str(deposition["id"]), "doi" : deposition.get("doi",""), "embargo_date":deposition.get("metadata",{}).get("embargo_date","")})
        new_accession = {"study_id": singlecell["study_id"], "deposition_id": deposition["id"], "doi": deposition.get("doi","")}
        Submission().update_component_submission_accession(profile_id=profile_id, repository=repository, component="study", identifier="study_id", accession=new_accession)
        result = {"status": "success", "message": f"{singlecell['study_id']} has been published to Zenodo.", "doi": deposition.get("doi","")}
        """
        notify_singlecell_status(data={"profile_id": singlecell["profile_id"]},
            msg=f"{singlecell['study_id']} has been published to Zenodo.",
            action="info",
            html_id="submission_info")
        """    
    except Exception as e:
        msg = f"Cannot publish to zenodo: {str(e)}"
        Logger().error(msg)
        Logger().exception(e)
        """
        notify_singlecell_status(data={"profile_id": singlecell["profile_id"]},              
            msg=msg,
            action="error",
            html_id="submission_info")
        """    
        result = {"status": "error", "message": msg}
    return result