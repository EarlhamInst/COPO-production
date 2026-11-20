from common.dal.copo_da import DAComponent
import pandas as pd
from common.utils.helpers import get_datetime, get_not_deleted_flag, notify_frontend
from django_tools.middlewares import threadlocal as ThreadLocal
from bson.objectid import ObjectId

ADDITIONAL_COLUMNS_PREFIX_DEFAULT_VALUE = {"status":"pending", "accession":"", "error":""}

# Default repository that data files will be deposited to if not specified in the schema
DEFAULT_DATA_FILE_REPOSITORY = 'zenodo'

class SinglecellSchemas(DAComponent):
    def __init__(self, profile_id=None, subcomponent=None):
        super(SinglecellSchemas, self).__init__(profile_id, "singlecellSchemas", subcomponent=subcomponent)

    """
    def get_submission_repository(self, schema_name, checklist_id=str()):
        if not checklist_id:
            return []
        checklist = self.get_checklists(schema_name, checklist_id)
        if checklist:
            repositories = checklist.get(checklist_id).get("submission_repository", "ena,zenodo")
            return repositories.split(",")
    """

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

    def get_submission_repository(self,  schema_name=str(),schemas=dict()):
        #get all the components from the schema
        """
        the result is
        index    ena              zenodo
        study    study            study  
        file     read             
        """
        if schemas:
            singlecell_schemas = schemas
        elif schema_name:
            singlecell_schemas = SinglecellSchemas().get_collection_handle().find_one({"name":schema_name},{"components":1})
        if singlecell_schemas:
            components = singlecell_schemas["components"]
            component_df = pd.DataFrame.from_dict(components, orient='index')
            #component_df = component_df.set_index("key", inplace=True)
            not_repository_columns = [column for column in component_df.columns if not column.startswith("repository_")]
            component_df = component_df.drop(not_repository_columns, axis=1)
            component_df = component_df.dropna(how="all")
            component_df = component_df.rename(columns=lambda x: x.replace("repository_", ""))
            component_df = component_df.astype(object).fillna("")
            return component_df

    def get_term_mapping(self, schema_name):
        #get all the components from the schema
        singlecell = SinglecellSchemas().get_collection_handle().find_one({"name":schema_name},{"term_mapping":1})
        if singlecell:
            return singlecell["term_mapping"]
        return {}

    #target_id is the checklist_id
    def get_schema(self, schema_name="", schemas=dict(), target_id=str()):        
        if schemas:
            singlecell_schemas = schemas
         
        elif schema_name:
            singlecell = SinglecellSchemas().get_collection_handle().find_one({"name":schema_name},{"schemas":1})
            singlecell_schemas = singlecell["schemas"]

        if singlecell_schemas and target_id:
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


    def get_schemas(self, schema_name, schemas=dict(), target_ids=[]):
        singlecell_schemas = schemas
        if not singlecell_schemas and schema_name:
            singlecell = SinglecellSchemas().get_collection_handle().find_one({"name":schema_name},{"schemas":1})
            singlecell_schemas = singlecell["schemas"]
        result = {}
        if singlecell_schemas and target_ids:
            for target_id in target_ids:
                schemas = self.get_schema(schemas=schemas, target_id=target_id)
                result[target_id] = schemas
        return result
    

    def get_identifier_map(self, schemas={}):
        identifier_map = {}
        for component, schema in schemas.items():
            schema_df = pd.DataFrame.from_records(list(schema))
            identifier_df =  schema_df.loc[schema_df['identifier'], 'term_name']
            if not identifier_df.empty:
                identifier_map[component]= identifier_df.iloc[0]
        return identifier_map
    
    def get_key_map(self, schemas={}):
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

    @staticmethod
    def get_file_terms(schema, repository=None):
        # Return the names of fields where the `term_type` is equal to 'file'.
        # If a `term_repository` is specified, it should be considered when filtering
        # No value i.e. a blank value for the `term_repository` column means
        # the default repository which is, DEFAULT_DATA_FILE_REPOSITORY.
        if not schema:
            return []

        schema_df = pd.DataFrame.from_records(schema)
        file_terms = schema_df.loc[schema_df['term_type'] == 'file']

        if repository and not file_terms.empty:
            repo_col = (file_terms['term_repository'] if 'term_repository' in file_terms
                        else pd.Series(DEFAULT_DATA_FILE_REPOSITORY, index=file_terms.index))
            repo_col = repo_col.astype(object)
            is_blank = repo_col.isna() | (repo_col.astype(str).str.strip() == '')
            repo_col = repo_col.where(~is_blank, DEFAULT_DATA_FILE_REPOSITORY).astype(str).str.lower()
            file_terms = file_terms[repo_col.str.split(',').apply(lambda r: repository in [x.strip() for x in r])]
            
        return file_terms['term_name'].tolist()

    # Get all files from the manifest
    def get_all_files(self, singlecell, schemas=[], repository=None):
        # Get all data files from single-cell manifests based on the
        # repository that the data files should be deposited to
        filelist = []
        for component, component_data in singlecell["components"].items():
            file_terms = self.get_file_terms(schemas.get(component, []), repository=repository)
            if not file_terms:
                continue

            df = pd.DataFrame.from_records(component_data)
            cols = [term for term in file_terms if term in df.columns]

            for row in df[cols].values.tolist():
                filelist.extend(f for f in row if isinstance(f, str) and f)
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

        if len(tmp) > 2:
            study_id = tmp[-2]
            subcomponent_id = tmp[-1]

        if self.subcomponent == "study":
            subcomponent_id = study_id
        
        singlecell = self.get_collection_handle().find_one({"profile_id": self.profile_id, "study_id": study_id}, {"schema_name": 1, "checklist_id": 1, "components": 1})
        schemas = SinglecellSchemas().get_schema(schema_name= singlecell["schema_name"], schemas=dict(), target_id=singlecell["checklist_id"])

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

        repositories = SinglecellSchemas().get_submission_repository(schema_name=singlecell["schema_name"])
        component_data = singlecell["components"].get(self.subcomponent, [])

        for row in component_data:
            if row[subcomponent_identifioer] != subcomponent_id:
              continue
            for key, value in row.items():
                index = key.rfind("_")
                if index != -1:
                    if key[index+1:] in repositories:
                        field = {}
                        field["id"] = key
                        field["show_as_attribute"] = True
                        field["label"] = key[:index].replace("_"," ") + " for " + key[index+1:]
                        field["control"] = "text"
                        field["sorting_name"] = key[index+1:] + "_" + key[:index]
                        fields.append(field)
                data[key] = value

        fields.sort(key=lambda x: x["sorting_name"], reverse=False)
        return fields, data

    def update_component_status(self, id, component="study", identifier="study_id", identifier_value=str(), repository="ena", status_column_value={}, with_child_components=False):
        username = "system"
        user = ThreadLocal.get_current_user()
        if user:
            username = user.id
        dt = get_datetime()
        
        update_element_values = {f"components.{component}.$.{key}_{repository}": value for key, value in status_column_value.items()}
        update_element_values.update({"updated_by": username, "date_modified": dt})

        self.get_collection_handle().update_one({"_id": ObjectId(id), "deleted": get_not_deleted_flag(), f"components.{component}.{identifier}": identifier_value},
                            {"$set": update_element_values})

        singlecell = self.get_collection_handle().find_one({"_id": ObjectId(id)}, {"schema_name": 1, "checklist_id": 1, "components": 1, "profile_id": 1})

        # Push a refresh to any open single-cell tab for this profile so the row
        # repaints when the async worker finishes. Only for study-level terminal
        # transitions — child updates arrive in bursts and would thrash the UI.
        if component == "study" and status_column_value.get("status") in ("accepted", "rejected"):
            profile_id = singlecell.get("profile_id") if singlecell else ""
            if profile_id:
                notify_frontend(
                    action="refresh_table",
                    msg="",
                    data={"profile_id": profile_id},
                    html_id="submission_info",
                    group_name=f"submission_status_{profile_id}",
                )

        if with_child_components:
            #get all the child components
            schemas = SinglecellSchemas().get_schema(schema_name=singlecell["schema_name"], schemas=dict(), target_id=singlecell["checklist_id"])
            _, foreignkey_map = SinglecellSchemas().get_key_map(schemas=schemas)
            child_map = SinglecellSchemas().get_child_map(foreignkey_map=foreignkey_map)
            if component in child_map:
                for child_component, foreign_key in child_map[component].items():
                    child_component_data = singlecell["components"].get(child_component,[])
                    for row in child_component_data:
                        if row[foreign_key] == identifier_value:
                            self.update_component_status(id=id, component=child_component, identifier=foreign_key, identifier_value=row[foreign_key], repository=repository, status_column_value=status_column_value, with_child_components=True)