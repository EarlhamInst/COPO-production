from common.utils.logger import Logger
from .da import SinglecellSchemas, Singlecell
import pandas as pd
from common.utils.helpers import get_datetime, get_not_deleted_flag

l = Logger()


def generate_singlecell_record(profile_id, checklist_id=str(), study_id=str()):

    data_set = {}
    columns = {}
    column_keys = {}
    studies = []
    new_column_name = {}
    identifier_map = {}
    if checklist_id:
        schemas = SinglecellSchemas().get_schema(target_id=checklist_id)

        for component_name, component_schema in schemas.items():
            columns[component_name] = []
            component_schema_df = pd.DataFrame.from_records(component_schema)
            identifier_df = component_schema_df.loc[component_schema_df['identifier'], 'term_name']                           
            if not identifier_df.empty:
                identifier_map[component_name]= identifier_df.iloc[0]

            detail_dict = dict(className='summary-details-control detail-hover-message', orderable=False, data=None,
                           title='', defaultContent='', width="5%")
            #columns[component_name].insert(0, detail_dict)
            columns[component_name].append(dict(data="record_id", visible=False))
            columns[component_name].append(dict(data="DT_RowId", visible=False))
            columns[component_name].extend([dict(data=item["term_name"], title=item["term_label"], defaultContent='') for item in component_schema])
            column_keys[component_name] = ([item["term_name"] for item in component_schema])
 
         
        studies = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"checklist_id": checklist_id}, projection={"study_id": 1, "components.study": 1})
        if not studies:
            return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))
        
        if not study_id:
            study_id = studies[0]["study_id"]
        
        #retriever all components info for first study
        singlecell = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"checklist_id": checklist_id, "study_id": study_id})
        if not singlecell or not singlecell[0]["components"].get("study",[]):
            return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))
        
        if len(studies) > 1:
            #combine all study component info from studies except the first one
            for study in studies:
                if study["study_id"] != study_id:
                    singlecell[0]["components"]["study"].extend(study["components"].get("study",[]))

        for component_name, component_data in singlecell[0]["components"].items():
            if not component_data:
                continue

            component_data_df = pd.DataFrame.from_records(component_data)
            
            for column in component_data_df.columns:
                if column not in column_keys.get(component_name, []):
                    component_data_df.drop(column, axis=1, inplace=True)

            #set the identifier to DT_RowId
            #set the identifier to record_id
                    
            component_data_df["DT_RowId"] = component_name + ( "_"+ study_id if component_name != 'study' else "")  + "_" + component_data_df.get(identifier_map.get(component_name,""), "")
            component_data_df["record_id"] = component_data_df["DT_RowId"]

            data_set[component_name] = component_data_df.to_dict(orient="records")

    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       components=list(columns.keys()),
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict

def _check_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map):
    
    for child_component_name, foreign_key in child_map.get(component_name, {}).items():
        child_component_data = singlecell_data["components"].get(child_component_name, [])
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        child_identifier_key = identifier_map.get(child_component_name, "")

        if not child_component_data_df.empty:
            children_df = child_component_data_df.loc[child_component_data_df[foreign_key].isin(identifiers)]
            children_df["has_accession"] = children_df["accession"]!=""
            if not children_df.loc[child_component_data_df["accession"]!=""].empty:
                l.error("Record deleted failed! Child record has accession number" + str(children_df[child_component_data_df["accession"]!=""]))
                return False
            _check_child_component_data(singlecell_data, child_component_name, children_df[child_identifier_key].tolist(),  identifier_map, child_map)
    return True

def _delete_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map):

    for child_component_name, foreign_key in child_map.get(component_name, {}).items():

        child_component_data = singlecell_data["components"].get(child_component_name, [])
        child_component_data_df = pd.DataFrame.from_records(child_component_data)
        child_component_identifier_key = identifier_map.get(child_component_name, "")
        
        if not child_component_data_df.empty:
            children_df = child_component_data_df.loc[child_component_data_df[foreign_key].isin(identifiers)]
            if not children_df.empty:        
                _delete_child_component_data(singlecell_data, child_component_name, children_df[child_component_identifier_key].tolist(), identifier_map, child_map)

                child_component_data_df = child_component_data_df.drop(child_component_data_df[child_component_data_df[foreign_key].isin(identifiers)].index)
                if not child_component_data_df.empty:
                    singlecell_data["components"][child_component_name] = child_component_data_df.to_dict(orient="records")
                else:
                    singlecell_data["components"].pop(child_component_name, None)

def delete_singlecell_records(profile_id, checklist_id, target_ids=[],target_id="", study_id=""):
    if target_id:
        target_ids = [target_id]

    if not target_ids:
        return dict(status='error', message="Please select one or more records to delete!")
    
    dt = get_datetime()
    study_ids = []
    if study_id:
        is_single_study = True
        study_ids.append(study_id)
    else:
        is_single_study = False

    schemas = SinglecellSchemas().get_schema(target_id=checklist_id)
    identifier_map, foreignkey_map = SinglecellSchemas().get_key_map(schemas)
    child_map = SinglecellSchemas().get_child_map(foreignkey_map)

    identifiers = []
    component_name = ""
    

    #all target_id should come from same component
    for target_id in target_ids:

        tmp = target_id.split("_")
        if not component_name:
            component_name = tmp[0]
        elif component_name != tmp[0]:
            return dict(status='error', message="Please select records from the same component!")
        
        if (len(tmp) < 2):
            return dict(status='error', message="taget id incorrect: " + target_id)
        
        if study_id != tmp[1] and is_single_study:
            return dict(status='error', message="Please select records from the same study!")
        elif not is_single_study:
            study_ids.append(tmp[1])

        identifier = tmp[2] if len(tmp) > 2 else study_id
        identifiers.append(identifier)

    identifier_key = identifier_map.get(component_name, "")
    if not identifier_key:
        return dict(status='error', message="Identifier not found for component: " + component_name)
    
    for study_id in study_ids:
        singlecell_data =  Singlecell(profile_id=profile_id).get_collection_handle().find_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id }, {"components": 1})
        
        if not singlecell_data:
            return dict(status='error', message="Study not found for study_id: " + study_id)
        
        #delete the record if both of it and its child records have no accession number
        component_data_df = pd.DataFrame.from_records(singlecell_data["components"][component_name])
        if not component_data_df.loc[(component_data_df[identifier_key].isin(identifiers)) & (component_data_df["accession"] !="")].empty:
            return dict(status='error', message="Record deleted failed!")   
     
        if _check_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map):
            #delete the record and the child records
            _delete_child_component_data(singlecell_data, component_name, identifiers, identifier_map, child_map)
            component_data_df = component_data_df.drop(component_data_df[component_data_df[identifier_key].isin(identifiers)].index)
            if not component_data_df.empty:
                singlecell_data["components"][component_name] = component_data_df.to_dict(orient="records")
            else:
                singlecell_data["components"].pop(component_name, None)

            if singlecell_data["components"]:                    
                Singlecell(profile_id=profile_id).get_collection_handle().update_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id}, {"$set": {"components": singlecell_data["components"], "last_modified": dt, "last_update_by": dt}})
            else:
                Singlecell(profile_id=profile_id).get_collection_handle().delete_one({"profile_id": profile_id, "checklist_id": checklist_id, "study_id": study_id})
        else:
            return {"status": "error", "message": "Record deleted failed!"}
        
    return {"status": "success", "message": "Record deleted successfully!"}