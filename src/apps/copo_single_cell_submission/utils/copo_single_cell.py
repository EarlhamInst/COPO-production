from common.utils.logger import Logger
from django.contrib.auth.models import User
from .da import SinglecellSchemas, Singlecell
import pandas as pd

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
        if not study_id:
            study_id = studies[0]["study_id"]
        
        if study_id:
            #retriever all components info for first study
            singlecell = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"checklist_id": checklist_id, "study_id": study_id})
            if not singlecell:
                return dict(dataSet=data_set, columns=columns, components=list(columns.keys()))
            
            if len(studies) > 1:
                #combine all study component info from studies except the first one
                for study in studies:
                    if study["study_id"] != study_id:
                        singlecell[0]["components"]["study"].extend(study["components"]["study"])
 
            for component_name, component_data in singlecell[0]["components"].items():
                component_data_df = pd.DataFrame.from_records(component_data)
                
                for column in component_data_df.columns:
                    if column not in column_keys.get(component_name, []):
                        component_data_df.drop(column, axis=1, inplace=True)

                #set the identifier to DT_RowId
                #set the identifier to record_id
                        
                component_data_df["DT_RowId"] = component_name + ( "_"+ study_id if component_name != 'study' else "")  + "_" + component_data_df.get(identifier_map.get(component_name,""), "")
                component_data_df["record_id"] = study_id

                data_set[component_name] = component_data_df.to_dict(orient="records")

    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       components=list(columns.keys()),
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict