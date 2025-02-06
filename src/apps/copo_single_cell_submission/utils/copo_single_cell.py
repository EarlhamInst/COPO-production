from common.utils.logger import Logger
from django.contrib.auth.models import User
from .da import SinglecellSchemas, Singlecell
import pandas as pd

l = Logger()


def generate_singlecell_record(profile_id, checklist_id=str(), study_id=str()):

    data_set = {}
    columns = {}
    column_keys = {}
    new_column_name = {}
    identifier_map = {}
    if checklist_id and study_id:
        schemas = SinglecellSchemas().get_schema(target_id=checklist_id)

        for component_name, component_schema in schemas.items():
            columns[component_name] = []
            component_schema_df = pd.DataFrame.from_records(component_schema)
            identifier_df = component_schema_df.loc[component_schema_df['identifier'], 'term_name']                           
            if not identifier_df.empty:
                identifier_map[component_name]= identifier_df.iloc[0]

            detail_dict = dict(className='summary-details-control detail-hover-message', orderable=False, data=None,
                           title='', defaultContent='', width="5%")
            columns[component_name].insert(0, detail_dict)
            columns[component_name].append(dict(data="record_id", visible=False))
            columns[component_name].append(dict(data="DT_RowId", visible=False))
            columns[component_name].extend([item["term_label"] for item in component_schema])
            column_keys[component_name] = ([item["term_name"] for item in component_schema])

            new_column_name[component_name] = {field["term_name"] : field["term_label"]  for field in component_schema }

        singlecell = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"checklist_id": checklist_id, "study_id": study_id})
        if singlecell:
            for component_name, component_data in singlecell[0]["components"].items():
                component_data_df = pd.DataFrame(component_data)
                
                for column in component_data_df.columns:
                    if column not in column_keys.get(component_name, []):
                        component_data_df.drop(column, axis=1, inplace=True)

                #set the identifier to DT_RowId
                #set the identifier to record_id
                component_data_df["DT_RowId"] = component_name + "#"+ component_data_df.get(identifier_map.get(component_name,""), "")
                component_data_df["record_id"] = study_id

                component_data_df.rename(columns=new_column_name[component_name], inplace=True)  
                data_set[component_name] = component_data_df.to_dict(orient="records")
    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict