from common.utils.logger import Logger
from sapiopylib.rest.utils.recordmodel.PyRecordModel import PyRecordModel
from common.dal.profile_da import Profile
from .sapio.sapio_datamanager  import Sapio
from typing import List
import math

l = Logger()

def get_sapio_sample_type_options():
    config = Sapio().picklistManager.get_picklist("Exemplar Sample Types")
    return [{"value": s, "label": s} for s in config.entry_list]
     
def pre_save_edp_profile(auto_fields, **kwargs):
    target_id = kwargs.get("target_id","")
    if not target_id:
        return {"status": "success"}
    profile = Profile().get_record(target_id)
    sapio_project_id = profile.get("sapio_project_id","")

    if sapio_project_id:            
        no_of_samples = auto_fields.get("copo.profile.no_of_samples", [])
        #plate_ids = auto_fields.get("copo.profile.sapio_plate_ids", "")
        project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project", 
                                                data_field_name="C_ProjectIdentifier", 
                                                value_list=[sapio_project_id]).result_list
        if not project_records or len(project_records) ==0:
            return {"status": "error", "message": f"Sapio Project {profile['sapio_project_id']} not found."}                
        project_record = project_records[0]
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)  
        Sapio().relationship_man.load_children([project], 'Sample')
        samples_under_project: List[PyRecordModel] = project.get_children_of_type('Sample')
        if samples_under_project:
            if len(samples_under_project) > int(no_of_samples):
                diff = len(samples_under_project) - int(profile["no_of_samples"])
                for sample in samples_under_project:
                    if not sample.get_field_value("C_CustomerSampleName"):
                        diff -=1

                    if diff <=0:
                        break
                if diff >0:
                    return {"status": "error", "message": f"Sapio Project {profile['sapio_project_id']} has customer samples associated. Cannot decrease the no. of samples."}

            """     
            plates = plate_ids.split(",")            
            assigned_plates = set()
            for sample in samples_under_project:
                assigned_plate = sample.get_field_value("PlateId")
                if assigned_plate:
                    assigned_plates.add(assigned_plate)
                    if assigned_plate not in plates:
                        return {"status": "error", "message": f"Sapio Project {profile['sapio_project_id']} has samples associated with plate {assigned_plate}. Cannot remove this plate from profile."}                    
            """
    """
    plates_str = auto_fields.get("copo.profile.sapio_plate_ids","").strip()
    if plates_str:
        plates = plates_str.split(",")  
        plates_records = Sapio().dataRecordManager.query_data_records(data_type_name="Plate", 
                                            data_field_name="PlateId", 
                                            value_list=plates).result_list
        if len(plates_records) < len(plates):
            existing_plate_ids_sapio = {plate_record.get_field_value("PlateId") for plate_record in plates_records}
            missing_plates = set(plates) - existing_plate_ids_sapio
            return {"status": "error", "message": f"Plates {', '.join(missing_plates)} not found in Sapio. Please create the plate(s) in Sapio first."}
    """
    return {"status": "success"}

def post_save_edp_profile(profile):
    project_record = None
    try:
        #update /create Sapio Project
        if not profile.get("sapio_project_id",""):
            project_records = Sapio().dataRecordManager.add_data_records_with_data(data_type_name="Project", field_map_list=[{"ProjectName": profile.get("jira_ticket_number",""),
                                                                                                    "ProjectDesc": profile.get("description",""),
                                                                                                    "C_BudgetHolder": profile.get("budget_user","")}])            
            
            sapio_project_id = project_records[0].get_field_value('C_ProjectIdentifier')
            profile["sapio_project_id"] = sapio_project_id
            Profile().get_collection_handle().update_one({"_id":profile["_id"]},{"$set":{"sapio_project_id":sapio_project_id}})
            project_record = project_records[0]
            #add project to Directory 1
            directories = Sapio().dataRecordManager.query_data_records(data_type_name="Directory", 
                                                        data_field_name="RecordId", 
                                                        value_list=[1]).result_list
            directory_record = directories[0]
            directory: PyRecordModel = Sapio().inst_man.add_existing_record(directory_record)  
            Sapio().relationship_man.load_children([directory], 'Project')
            project : PyRecordModel = Sapio().inst_man.add_existing_record(project_record)
            directory.add_child(project)

        else:
            project_records = Sapio().dataRecordManager.query_data_records(data_type_name="Project", 
                                                        data_field_name="C_ProjectIdentifier", 
                                                        value_list=[profile["sapio_project_id"]]).result_list
            if not project_records or len(project_records) ==0:
                raise Exception(f"Failed to Find Sapio Project {profile["sapio_project_id"]}")
            project_record = project_records[0]
            project_record.set_field_value("ProjectName", profile.get("jira_ticket_number",""))
            project_record.set_field_value("ProjectDesc", profile.get("description",""))
            project_record.set_field_value("C_BudgetHolder", profile.get("budget_user",""))
            Sapio().dataRecordManager.commit_data_records([project_record])

        #attach samples to Sapio Project
        #get all samples for Sapio Project
        project: PyRecordModel = Sapio().inst_man.add_existing_record(project_record)  
        Sapio().relationship_man.load_children([project], 'Sample')
        samples_under_project: List[PyRecordModel] = project.get_children_of_type('Sample')
        samples_under_project = sorted(samples_under_project, key=lambda x: x.get_field_value("PlateId"))
        Sapio().relationship_man.load_children([project], 'Plate')
        plates_under_project: List[PyRecordModel] = project.get_children_of_type('Plate')

        assigned_plates_map_for_samples_to_delete = {}
        samples_to_remove = []

        existing_plate_ids_under_project = set()
        for plate in plates_under_project:
            existing_plate_ids_under_project.add(plate.get_field_value("PlateId"))

        #create samples if not exists
        if not samples_under_project or len(samples_under_project) < int(profile["no_of_samples"]):    
            existing_no_of_samples = len(samples_under_project) if samples_under_project else 0
            sample_records = Sapio().dataRecordManager.add_data_records_with_data(data_type_name="Sample", 
                                                                                  field_map_list=[{"ExemplarSampleType": profile["sample_type"], 
                                                                                                  "ContainerType": profile["container_type"]}
                                                                                                  for _ in range(existing_no_of_samples, int(profile["no_of_samples"]))])
            samples : List[PyRecordModel] = Sapio().inst_man.add_existing_records(sample_records)
            project.add_children(samples)
            samples_under_project.extend(samples)

        #delete samples if more than required
        diff = len(samples_under_project) - int(profile["no_of_samples"])
        if diff > 0:
            for sample in samples_under_project:
                if not sample.get_field_value("C_CustomerSampleName"):
                    samples_to_remove.append(sample)
                    diff -=1
                    assigned_plate_id = sample.get_field_value("PlateId")
                    if assigned_plate_id:
                        if assigned_plate_id not in assigned_plates_map_for_samples_to_delete:
                            assigned_plates_map_for_samples_to_delete[assigned_plate_id] = []
                        assigned_plates_map_for_samples_to_delete[assigned_plate_id].append(sample)
                if diff <=0:
                    break
            if diff >0:
                raise Exception(f"Sapio Project {profile['sapio_project_id']} has customer samples associated. Cannot decrease the no. of samples.")

            project.remove_children(samples_to_remove)
               
            samples_under_project = [s for s in samples_under_project if s not in samples_to_remove]
        

        #attach plate to Sapio Project, assume it is  96 well plate (8 rows x 12 columns)
        #create plate if necessary        
        
        """
        plates_str = profile.get("sapio_plate_ids","").strip()
        if plates_str:
            plates = plates_str.split(",")
            missing_plates = set(plates) - existing_plate_ids_under_project
            unwanted_plates = existing_plate_ids_under_project - set(plates)

            missing_plate_records = Sapio().dataRecordManager.query_data_records(data_type_name="Plate", 
                                                data_field_name="PlateId", 
                                                value_list=list(missing_plates)).result_list
            existing_plate_ids_sapio = set()
            for missing_plate_record in missing_plate_records:
                existing_plate_ids_sapio.add(missing_plate_record.get_field_value("PlateId"))
            
            existing_plate_record_models: List[PyRecordModel] = Sapio().inst_man.add_existing_records(missing_plate_records)  

            project.add_children(existing_plate_record_models)

            missing_plates_not_in_sapio = missing_plates - existing_plate_ids_sapio

            if missing_plates_not_in_sapio:
                raise Exception(f"Plates {', '.join(missing_plates_not_in_sapio)} not found in Sapio. Please create these plates in Sapio first.")
                            
            if unwanted_plates:
                #check if not attached samples to these plates
                for sample in samples_under_project:
                    assigned_plate = sample.get_field_value("PlateId")
                    if assigned_plate in unwanted_plates:
                        raise Exception(f"Sapio Project {profile['sapio_project_id']} has samples associated with plate {assigned_plate}. Cannot remove this plate from profile.")

                plate_records = Sapio().dataRecordManager.query_data_records(data_type_name="Plate", 
                                                data_field_name="PlateId", 
                                                value_list=list(unwanted_plates)).result_list           
                #remove plate from Sapio project
                plate_record_models: List[PyRecordModel] = Sapio().inst_man.add_existing_records(plate_records)  
                project.remove_children(plate_record_models)
        """

        #Sapio().relationship_man.load_children([project], 'Plate')
        #plates_under_project: List[PyRecordModel] = project.get_children_of_type('Plate')
        plates_under_project_map = {plate.get_field_value("PlateId") : plate for plate in plates_under_project}
        
        #remove samples from plates
        for plate_id, samples in assigned_plates_map_for_samples_to_delete.items():
            plate = plates_under_project_map.get(plate_id,None)
            if plate:
                plate.remove_children(samples)

        samples_without_plate = set()
        if samples_under_project:
            for sample in samples_under_project:
                assigned_plate = sample.get_field_value("PlateId")
                if not assigned_plate:
                    assigned_plate = sample.get_field_value("StorageUnitPath")
                    if not assigned_plate:
                        samples_without_plate.add(sample)

        Sapio().relationship_man.load_children(plates_under_project, 'Sample')        
        if samples_without_plate:
            for plate in plates_under_project:
                sample_for_plate: List[PyRecordModel] = []
                plate_assignments = {( str(column), row): False for column in range(1,13) for row in ["A", "B","C","D","E","F","G","H"]} #12 columns, 8 rows
                samples_under_plate: List[PyRecordModel] = plate.get_children_of_type('Sample')
                for sample in samples_under_plate:
                    plate_assignments_key = (sample.get_field_value("ColPosition"), sample.get_field_value("RowPosition"))
                    plate_assignments[plate_assignments_key] = True

                for _ in range(len(samples_under_plate), 96): #assume 96 well plate, 8X12
                    if not samples_without_plate:
                        break
                    key = next((k for k,v in plate_assignments.items() if not v), None)
                    if not key:
                        l.error("No more positions available in plate when assigning samples!")
                        break
                    sample = samples_without_plate.pop()
                    #sample_model = Sapio().inst_man.add_existing_record(sample)  
                    sample_for_plate.append(sample)
                    sample.set_field_value("PlateId", plate.get_field_value("PlateId"))
                    plate_assignments[key] = True
                    sample.set_field_value("ColPosition", key[0]) 
                    sample.set_field_value("RowPosition", key[1]) 
                if sample_for_plate:
                    plate.add_children(sample_for_plate)

        #create new plates if still samples without plate
        if samples_without_plate:
            no_of_plates_needed = math.ceil(len(samples_without_plate) / 96)
            new_plate_records = Sapio().dataRecordManager.add_data_records_with_data(data_type_name="Plate", 
                                                                                  field_map_list=[{"PlateSampleType": profile["sample_type"], 
                                                                                                  "PlateColumns": 12,"PlateRows": 8}
                                                                                                  for _ in range(no_of_plates_needed)])
            #attach plates to project
            new_plates : List[PyRecordModel] = Sapio().inst_man.add_existing_records(new_plate_records)
            project.add_children(new_plates)
            Sapio().relationship_man.load_children(new_plates, 'Sample')

            #assign samples to plates
            for plate in new_plates:
                sample_for_plate: List[PyRecordModel] = []
                plate_assignments = {( str(column), row): False for column in range(1,13) for row in ["A", "B","C","D","E","F","G","H"]} #12 columns, 8 rows
                for _ in range(96): #assume 96 well plate, 8X12
                    if not samples_without_plate:
                        break
                    key = next((k for k,v in plate_assignments.items() if not v), None)
                    if not key:
                        l.error("No more positions available in plate when assigning samples!")
                        break                    
                    sample = samples_without_plate.pop()
                    #sample_model = Sapio().inst_man.add_existing_record(sample)  
                    sample_for_plate.append(sample)
                    sample.set_field_value("PlateId", plate.get_field_value("PlateId"))                       
                    plate_assignments[key] = True
                    sample.set_field_value("ColPosition", key[0])
                    sample.set_field_value("RowPosition", key[1])         
                if sample_for_plate:
                    plate.add_children(sample_for_plate)

        Sapio().rec_man.store_and_commit()
        Sapio().dataRecordManager.delete_data_record_list([sample.get_data_record() for sample in samples_to_remove], recursive_delete=True)

        if samples_without_plate:
            l.error("Not all samples have been assigned to plates!")
            return  {"status":"warning", "message":"Profile has been saved. However, it is failed to update to Sapio! "}

        #assign plate locations, TBD

    except Exception as e:        
        l.exception(e)
        l.error("Failed to create or update sapio project for profile id: " + str(profile["_id"]) + " Error: " + str(e))
        return  {"status":"warning", "message":"Profile has been saved. However, it is failed to update to Sapio! "}
        
    return {"status": "success"}


def post_delete_edp_profile(profile):
    if profile.get("sapio_project_id",""):
        try:
            record = Sapio().dataRecordManager.query_data_records(data_type_name="Project", 
                                                    data_field_name="C_ProjectIdentifier", 
                                                    value_list=[profile["sapio_project_id"]]).result_list[0]
            Sapio().dataRecordManager.delete_data_record(record=record, recursive_delete=True)
        except Exception as e:
            l.exception(e)
            l.error("Failed to delete sapio profile for profile id: " + str(profile["_id"]) + " Error: " + str(e))
            return  {"status":"warning", "message":"Profile has been deleted. However, it is failed to delete from Sapio! "}
    return {"status": "success"}