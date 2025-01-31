from common.dal.copo_da import DAComponent
import pandas as pd

class SinglecellSchemas(DAComponent):
    def __init__(self, profile_id=None):
        super(SinglecellSchemas, self).__init__(profile_id, "singlecellSchemas")


    def get_checklists(self, checklist_id=str()):
        
        projection = {"checklists": 1}
        if checklist_id:
            projection = {"checklists."+checklist_id: 1}
        projection["_id"] = 0
        checklists = self.get_collection_handle().find_one({"name":"copo"}, projection)

        if checklists:
            return checklists.get("checklists", {})
        else:
            return {}

    def get_schema(self, target_id=str()) :
        schemas = {}

        if target_id:
            #singlecell = SinglecellSchemas().get_collection_handle().find_one({"name":"copo"},{"schemas":1, "enums":1})
            singlecell = SinglecellSchemas().get_collection_handle().find_one({"name":"copo"},{"schemas":1})
            singlecell_schemas = singlecell["schemas"]

            if singlecell_schemas:
                for component, item in singlecell_schemas.items():
                    component_schema_df = pd.DataFrame.from_records(item)
                    component_schema_df = component_schema_df.drop(component_schema_df[pd.isna(component_schema_df[target_id])].index)
                    #component_schema_df["choice"] = component_schema_df[component_schema_df["term_type"] == "enum"]["term_name"].apply(lambda x:singlecell["enums"].get(x, []))
                    #component_schema_df["mandatory"] = component_schema_df[self.checklist_id]

                    component_schema_df["label"] = component_schema_df["term_name"]
                    component_schema_df["control"] = "text"
                    component_schema_df["show_as_attribute"] = True
                    component_schema_df["id"] = "term_name"

                    schemas[component] = component_schema_df[["id","label","control","show_as_attribute" ,"id"]].to_dict(orient="records")      

        return dict(schema_dict=schemas,
                    schema=schemas
                    )
