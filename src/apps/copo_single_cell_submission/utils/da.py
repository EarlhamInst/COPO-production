from common.dal.copo_da import DAComponent
import pandas as pd
from common.utils.helpers import get_not_deleted_flag

ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE = {"status":"pending", "accession":"", "error":""}

class SinglecellSchemas(DAComponent):
    def __init__(self, profile_id=None, subcomponent=None):
        super(SinglecellSchemas, self).__init__(profile_id, "singlecellSchemas", subcomponent=subcomponent)

    def get_submission_repositiory(self, schema_name, checklist_id=str()):
        if not checklist_id:
            return []
        checklist = self.get_checklists(schema_name, checklist_id)
        if checklist:
            repositories = checklist.get(checklist_id).get("submission_repository", "ena,zenodo")
            return repositories.split(",")

    def get_checklists(self, schema_name, checklist_id=str()):
        
        projection = {"checklists": 1}
        if checklist_id:
            projection = {"checklists."+checklist_id: 1}
        projection["_id"] = 0
        checklists = self.get_collection_handle().find_one({"name":schema_name}, projection)

        if checklists:
            return checklists.get("checklists", {})
        else:
            return {}

    #target_id is the checklist_id
    def get_schema(self, schema_name, target_id=str()) :
        schemas = {}

        if target_id:
            singlecell = SinglecellSchemas().get_collection_handle().find_one({"name":schema_name},{"schemas":1})
            singlecell_schemas = singlecell["schemas"]

            if singlecell_schemas:
                for component, item in singlecell_schemas.items():
                    component_schema_df = pd.DataFrame.from_records(item)
                    component_schema_df = component_schema_df.drop(component_schema_df[pd.isna(component_schema_df[target_id])].index)
                    if component_schema_df.empty:
                        continue    

                    component_schema_df["label"] = component_schema_df["term_label"]
                    component_schema_df["control"] = "text"
                    component_schema_df["show_as_attribute"] = True
                    component_schema_df["id"] = component_schema_df["term_name"]

                    schemas[component] = component_schema_df.to_dict(orient="records")      

        return schemas

    def get_identifier_map(self, schemas={}):
        identifier_map = {}
        for component, schema in schemas.items():
            schema_df = pd.DataFrame.from_records(list(schema))
            identifier_df =  schema_df.loc[schema_df['identifier'], 'term_name']
            if not identifier_df.empty:
                identifier_map[component]= identifier_df.iloc[0]
        return identifier_map
    
    def get_key_map(self, schemas=[]):
        identifier_map = {}
        foreignkey_map = {}
        for component, schema in schemas.items():
            schema_df = pd.DataFrame.from_records(list(schema))

            identifier_df =  schema_df.loc[schema_df['identifier'], 'term_name']
            if not identifier_df.empty:
                identifier_map[component]= identifier_df.iloc[0]

            referenced_components = schema_df["referenced_component"].unique()
            foreignkey_map[component] = []
            for referenced_component in referenced_components:
                if pd.isna(referenced_component):
                    continue
                df = schema_df.loc[schema_df["referenced_component"] == referenced_component, 'term_name']
                if df.empty:
                    continue
                foreign_key = df.iloc[0]
                foreignkey_map[component].append({"referenced_component": referenced_component, "foreign_key": foreign_key})

        return identifier_map, foreignkey_map

    def get_parent_map(self, foreignkey_map):
        parent_map = {}
        for component, foreignkeys in foreignkey_map.items():
            parent_map[component] = {}
            for foreignkey in foreignkeys:
                parent_map[component][foreignkey["referenced_component"]] = foreignkey["foreign_key"]
        return parent_map
    
    def get_child_map(self, foreignkey_map):
        child_map = {}
        for component, foreignkeys in foreignkey_map.items():
            for foreignkey in foreignkeys:
                parent_component = foreignkey["referenced_component"]
                if parent_component not in child_map:
                    child_map[parent_component] = {}
                child_map[parent_component][component] = foreignkey["foreign_key"]
        return child_map

    #get all files from the manifest
    def get_all_files(self, singlecell, schemas=[]):
        filelist = []
        for component, component_data in singlecell["components"].items():
            schema = schemas[component]        
            schema_df = pd.DataFrame.from_records(schema)
            schema_file_df = schema_df.loc[schema_df['term_type'] == 'file', "term_name"]
            df = pd.DataFrame.from_records(component_data)
            if not schema_file_df.empty:
                file_df = df[schema_file_df.tolist()]
                file_df = file_df.dropna()
                fileslist = file_df.values.tolist()
                for files in fileslist:
                    filelist.extend(list(filter(None, files)))
        return filelist

class Singlecell(DAComponent):
    def __init__(self, profile_id=None, subcomponent=None):
        super(Singlecell, self).__init__(profile_id, "singlecell", subcomponent=subcomponent)
    

    def get_table_attributes(self, target_id=str()):
        if not target_id:
            return dict(schema_dict=[],
                        schema=[]
                        )
        
        #format for target_id "study_AB33445"
        tmp = target_id.split("_")

        if len(tmp) >1:
            study_id = target_id.split("_")[1]
        if len(tmp) > 2:
            subcomponent_id = target_id.split("_")[2]

        if self.subcomponent == "study":
            subcomponent_id = study_id
        
        singlecell = self.get_collection_handle().find_one({"profile_id": self.profile_id, "study_id": study_id}, {"schema_name": 1, "checklist_id": 1, "components": 1})
        schemas = SinglecellSchemas().get_schema(schema_name= singlecell["schema_name"], target_id=singlecell["checklist_id"])

        fields = []
        data={}

        """
        schema = schemas.get(self.subcomponent, [])
        for term in schema:
            field = {}
            field["id"] = term["term_name"]
            field["show_as_attribute"] = True
            field["label"] = term["term_label"]
            field["control"] = "text"
            fields.append(field)
        """    
        identifier_map = SinglecellSchemas().get_identifier_map(schemas=schemas)
        subcomponent_identifioer = identifier_map.get(self.subcomponent, "")
        if not subcomponent_identifioer:
            return fields, data

        repositiories = SinglecellSchemas().get_submission_repositiory(schema_name=singlecell["schema_name"], checklist_id=singlecell["checklist_id"])
        component_data = singlecell["components"].get(self.subcomponent, [])

        for row in component_data:
            if row[subcomponent_identifioer] != subcomponent_id:
              continue
            for key, value in row.items():
                index = key.rfind("_")
                if index != -1:
                    if key[index+1:] in repositiories:
                        field = {}
                        field["id"] = key
                        field["show_as_attribute"] = True
                        field["label"] = key[:index] + " for " + key[index+1:]
                        field["control"] = "text"
                        fields.append(field)
                data[key] = value

        return fields, data

    def update_component_status(self, id, component="study", identifier="study_id", identifier_value=str(), repository="ena", additional_columns_value={}):
        self.get_collection_handle().update_one({"_id": id, "deleted": get_not_deleted_flag(), f"components.{component}.{identifier}": identifier_value},
                            {"$set": {f"components.{component}.$.{key}_{repository}": value for key, value in additional_columns_value.items()}})
