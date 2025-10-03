from common.validators.validator import Validator
from django.conf import settings
import pandas as pd
 

lg = settings.LOGGER
class ForeignKeyValidator(Validator):
    def validate(self):
        schemas = self.kwargs.get("schemas", {})
        identifier_map = {}
        foreignkey_map = {}
        for component, schema in schemas.items():
            for key, field in schema.items():
                field["term_name"] = key
            schema_df = pd.DataFrame.from_records(list(schema.values()))
            identifier_df =  schema_df.loc[schema_df['identifier'], 'term_name']
            if not identifier_df.empty:
                identifier_map[component]= identifier_df.iloc[0]

            referenced_components = schema_df["referenced_component"].unique()
            foreignkey_map[component] = []
            for referenced_component in referenced_components:
                if pd.isna(referenced_component):
                    continue
                df = schema_df.loc[schema_df["referenced_component"] == referenced_component, 'term_name']
                #it won't happen
                if df.empty:
                    self.errors.append("Sheet <B>" + component + "</B>: Referenced component: '" + referenced_component + "' is missing")
                    self.flag = False
                foreign_key = df.iloc[0]
                foreignkey_map[component].append({"referenced_component": referenced_component, "foreign_key": foreign_key})

        for component, df in self.data.items():
            for referenced_component_dict in foreignkey_map[component]:
                if referenced_component_dict["referenced_component"] and referenced_component_dict["referenced_component"] not in self.data.keys():
                    self.errors.append("Sheet <B>" + component + "</B>: Referenced component: '" + referenced_component_dict["referenced_component"] + "' is missing")
                    self.flag = False
                else:
                    for index, row in df.iterrows():
                        foreign_key =  referenced_component_dict["foreign_key"]
                        referenced_component = referenced_component_dict["referenced_component"]
                        if referenced_component and row[foreign_key] and row[foreign_key] not in self.data[referenced_component][identifier_map[referenced_component]].values:
                            self.errors.append( "Sheet <B>" + component + "</B>: <B>" + row[foreign_key] + "</B> at row " + str(index + 2) + " is missing in the referenced sheet : <B>" + referenced_component + "</B> and column : <B>" + identifier_map[referenced_component] +"</B>.")
                            self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")