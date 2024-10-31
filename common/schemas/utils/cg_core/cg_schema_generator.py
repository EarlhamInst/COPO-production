__author__ = 'etuka'

import os
import json
import numpy as np
import pandas as pd
from bson import ObjectId
from common.dal.mongo_util import cursor_to_list

import common.lookup.lookup as lkup
from common.lookup.resolver import RESOLVER
from common.utils import helpers

class CgCoreSchemas:
    def __init__(self):
        self.resource_path = RESOLVER['cg_core_schemas']
        self.schemas_utils_paths = RESOLVER["cg_core_utils"]
        self.path_to_json = os.path.join(self.resource_path, 'cg_core.json')
        self.type_field_status_path = os.path.join(self.schemas_utils_paths, 'type_field_STATUS.csv')
        self.map_type_subtype_path = os.path.join(self.schemas_utils_paths, 'cg_types.csv')
        self.copo_schema_spec_path = os.path.join(self.schemas_utils_paths, 'copo_schema.csv')
        self.cg_wizard_stages = os.path.join(self.schemas_utils_paths, 'cg_wizard_stages.csv')

    def retrieve_schema_specs(self, path_to_spec):
        """
        function retrieves csv, returns dataframe
        :param path_to_spec:
        :return:
        """

        df = pd.read_csv(path_to_spec)

        # set index using 'type' column
        df.index = df['harmonized labeling']
        df = df.drop(['harmonized labeling'], axis='columns')

        # drop null index
        df = df[df.index.notnull()]

        return df

    def get_wizard_stages_df(self):
        """
        function returns a dataframe of wizard stages
        :return:
        """
        path_to_spec = self.cg_wizard_stages
        df = pd.read_csv(path_to_spec)

        df['stage_label'] = df['stage_label'].fillna("Description")
        df['stage_id'] = df['stage_id'].fillna("000")
        df.stage_id = df.stage_id.astype(str)
        df['stage_message'] = df['stage_message'].fillna("CG Core stage")

        return df

    def get_type_field_matrix(self):
        """
        function returns cg core fields and corresponding constraints as a dataframe
        :return: dataframe
        """

        df = self.retrieve_schema_specs(self.type_field_status_path)

        # filter valid types and subtypes - no longer relevant
        # df['match_type_subtype_x'] = df['in cgc2 typelist'].astype(str) + df['in cgc2 subtypelist'].astype(str)
        # df = df[(df['match_type_subtype_x'] == '01') | (df['match_type_subtype_x'] == '10')]

        # substitute value
        # df = df.replace('required if applicable', 'required')
        df = df.replace('required if applicable', 'required-if-applicable')

        # drop noisy columns - that do not define any constraint
        constraints = ['required', 'recommended', 'optional', 'not applicable', 'required-if-applicable']

        # filter out noisy columns
        df = df.T
        df = df[df.isin(constraints)]
        df = df.dropna(how='all')

        # filter out noisy rows
        df = df.T
        df = df.dropna()

        return df

    def get_type_constraints(self, type_name):
        """
        given a type (or a subtype) function returns relevant schemas with associated constraints
        :param type_name:
        :return:
        """

        from dal.copo_base_da import DataSchemas
        schema_fields = DataSchemas.get_ui_template_node('COPO', 'cgCore')

        df = self.resolve_field_constraint(schema=schema_fields, type_name=type_name)
        return df

    def resolve_field_constraint(self, schema=list(), type_name=str()):
        """
        given type_name, function sets field constraint for schema items - also filters out non-applicable fields
        :param schema:
        :param type_name:
        :return:
        """

        df = self.get_type_field_matrix()
        df.index = df.index.str.lower()
        df_type_series = df.loc[type_name.lower()]
        df_type_series = df_type_series[df_type_series != 'not applicable']
        df_type_series.index = df_type_series.index.str.lower()

        schemas_df = pd.DataFrame(schema)
        schemas_df.index = schemas_df.ref.str.lower()
        schemas_df = schemas_df[schemas_df.index.isin(df_type_series.index)]

        # set constraints
        schemas_df.loc[schemas_df.index, 'required'] = df_type_series
        schemas_df.loc[schemas_df.index, 'field_constraint'] = df_type_series

        schemas_df["required"] = schemas_df["required"].replace(
            {'required': True, 'recommended': False, 'optional': False, 'required-if-applicable': False})

        # rank fields by constraints
        constraint_to_rank = self.get_constraint_ranking()

        lowered = schemas_df['field_constraint'].str.lower()
        schemas_df['field_constraint_rank'] = lowered.map(constraint_to_rank)

        schemas_df['cg_type_name'] = type_name  # needed for resolving dependency constraints

        return schemas_df

    def get_constraint_ranking(self):
        return {
            'required': 1,
            'required-if-applicable': 2,
            'recommended': 3,
            'optional': 4
        }

    def get_cg_subtypes(self, type_name):
        """
        function returns relevant subtypes for a type
        :param type_name:
        :return:
        """

        df = pd.read_csv(self.map_type_subtype_path)
        df = df[['Type', 'Subtype']]
        df = df[df.Type.str.strip().str.lower() == type_name.lower()]
        df = df[['Subtype']]
        df.columns = ['type']

        all_types_index = self.get_type_field_matrix().index

        df_series = pd.Series(list(df['type'].str.strip().str.lower().dropna().unique()))
        all_types_series = pd.Series(all_types_index.str.strip().str.lower())

        qualified_types = list(df_series[df_series.isin(all_types_series)])

        all_types_series = pd.Series(all_types_index)

        qualified_types = list(all_types_series[all_types_series.str.strip().str.lower().isin(qualified_types)])

        return qualified_types

    def get_cg_types(self):
        """
        function returns all types
        :return:
        """

        df = pd.read_csv(self.map_type_subtype_path)
        df = df[['Information product type', 'Cgcore_field']]
        df = df[df.Cgcore_field.str.strip().str.lower() == "dc.type"]
        df = df[['Information product type']]
        df.columns = ['type']

        all_types_index = self.get_type_field_matrix().index

        df_series = pd.Series(list(df['type'].str.strip().str.lower().dropna().unique()))
        all_types_series = pd.Series(all_types_index.str.strip().str.lower())

        qualified_types = list(df_series[df_series.isin(all_types_series)])

        all_types_series = pd.Series(all_types_index)

        qualified_types = list(all_types_series[all_types_series.str.strip().str.lower().isin(qualified_types)])

        return qualified_types

    def extract_repo_fields(self, datafile_id=str(), repo=str()):
        """
        given a datafile id, and repository type function returns a list of dictionaries of fields matching the repo
        :param datafile_id:
        :param repo:
        :return:
        """

        from dal.copo_da import DataFile, CGCore
        from dal.copo_base_da import DataSchemas

        if not repo:  # no repository to filter by
            return list()

        repo_type_option = lkup.DROP_DOWNS["REPO_TYPE_OPTIONS"]
        repo_type_option = [x for x in repo_type_option if x["value"].lower() == repo.lower()]

        if not repo_type_option:
            return list()

        repo_type_option = repo_type_option[0]

        cg_schema = DataSchemas.get_ui_template_node('COPO','cgCore')

        # filter schema items by repo
        cg_schema = [x for x in cg_schema if
                     x.get("target_repo", str()).strip() != str() and
                     repo_type_option.get("abbreviation", str()) in [y.strip() for y in
                                                                     x.get("target_repo").split(',')]]

        record = DataFile().get_record(datafile_id)
        description = record.get("description", dict())

        attributes = description.get("attributes", dict())
        stages = description.get("stages", list())

        schema_df = pd.DataFrame(cg_schema)
        schema_df.id = schema_df.id.str.lower().str.split(".").str[-1]
        schema_df.index = schema_df.id
        schema_df = schema_df[['ref', 'id', 'prefix', 'label']]
        schema_df = schema_df[~schema_df['ref'].isna()]

        # get all stage items
        all_items = [item for st in stages for item in st.get("items", list())]

        # filter stage items - stage items should conform to specifications of the repo
        schema_ids = list(schema_df.id)
        items = {item.get("id", str()).lower().split(".")[-1]: st.get("ref", "").lower() for st in stages for item in
                 st.get("items", list()) if item.get("id", str()).lower().split(".")[-1] in schema_ids}

        # ...also, account for any filtering performed by client agents (e.g., dependencies in COPO Wizard),
        # within the context of the target repo
        schema_df = schema_df[schema_df.index.isin(items.keys())]

        # obtain attributes for filtered stage items
        target_stages = list(set(items.values()))
        datafile_attributes = [v for k, v in attributes.items() if k in target_stages]

        new_dict = dict()
        for d in datafile_attributes:
            new_dict.update(d)

        new_dict_series = pd.Series(new_dict)
        new_dict_series.index = new_dict_series.index.str.lower()
        schema_df['vals'] = new_dict_series
        schema_df['vals'] = schema_df['vals'].fillna('')

        schema_df = schema_df[['ref', 'id', 'vals', 'prefix', 'label']]

        # get composite attributes
        composite_attrib = [x for x in all_items if x["id"] in list(schema_df.id) and x.get("create_new_item", False)]

        # expand composite attributes
        for cattrib in composite_attrib:
            comp_series = schema_df.loc[cattrib["id"]]
            schema_df = schema_df[~schema_df.id.isin([cattrib["id"]])]
            children_schemas = [x for x in cg_schema if x.get("dependency", str()).lower() == comp_series.ref.lower()]

            accessions = comp_series.vals
            if isinstance(accessions, str):
                accessions = accessions.split(",")

            object_ids = [ObjectId(x) for x in accessions if x.strip()]

            records = list()
            if len(object_ids):
                records = cursor_to_list(CGCore().get_collection_handle().find({"_id": {"$in": object_ids}}))

            attr_list = list()
            for child in children_schemas:
                child_dict = dict(ref=child["ref"], id=child["id"].split(".")[-1], prefix=child["prefix"], vals=[],
                                  label=child["label"])
                attr_list.append(child_dict)
                for rec in records:
                    child_dict["vals"].append(rec.get(child_dict["id"], str()))

            if attr_list:
                attr_df = pd.DataFrame(attr_list)
                attr_df.index = attr_df.id
                schema_df = pd.concat([schema_df, attr_df], sort=False)

        schema_df.rename(index=str, columns={"ref": "dc", "id": "copo_id"}, inplace=True)

        dc_list = schema_df.to_dict('records')

        return dc_list

    def controls_mapping(self):
        """
        function maps to COPO controls
        :return:
        """

        control = 'text'

        return control

    def get_schema_spec(self):
        """
        function returns cg core field specifications e.g., field id, field type, field label
        :return:
        """

        df = self.retrieve_schema_specs(self.copo_schema_spec_path)

        # filter out columns not found in type-field matrix
        df_spec_col_series = pd.Series(df.columns)
        type_field_series = pd.Series(self.get_type_field_matrix().columns)
        spec_qualified_cols = df_spec_col_series[df_spec_col_series.isin(type_field_series)]

        df = df[spec_qualified_cols]

        # filter out columns with no copo id
        cid_series = df.loc['COPO_ID']
        df = df[cid_series[~cid_series.isna()].index]

        # substitute for NANs
        df.loc['LABEL'] = df.loc['LABEL'].fillna('**No label**')
        df.loc['HELP_TIP'] = df.loc['HELP_TIP'].fillna('n/a')
        df.loc['COPO_CONTROL'] = df.loc['COPO_CONTROL'].fillna('text')
        df.loc['TYPE'] = df.loc['TYPE'].fillna('string')
        df.loc['DEPENDENCY'] = df.loc['DEPENDENCY'].fillna('')
        df.loc['COPO_DATA_SOURCE'] = df.loc['COPO_DATA_SOURCE'].fillna('')
        df.loc['REPO'] = df.loc['REPO'].fillna('')
        df.loc['REPO_PREFIX'] = df.loc['REPO_PREFIX'].fillna('')
        df.loc['Wizard_Stage_ID'] = df.loc['Wizard_Stage_ID'].fillna('-1')

        return df

    def process_schema(self):
        """
        function builds schema fragments to file, which is later called to generate the complete schema in db
        :return:
        """

        specs_df = self.get_schema_spec()

        # compose copo schema from cg-core spec
        df = specs_df.T.copy()
        df["ref"] = list(df.index)

        df["id"] = df['COPO_ID'].apply(lambda x: ".".join(("copo", "cgCore", x)))
        df["label"] = df['LABEL']
        df["help_tip"] = df['HELP_TIP']
        df["dependency"] = df['DEPENDENCY']
        df["control"] = df['COPO_CONTROL']
        df["stage_id"] = df['Wizard_Stage_ID']
        df["target_repo"] = df['REPO']
        df["prefix"] = df['REPO_PREFIX']
        df["data_maxItems"] = -1

        # set max item for lookup control
        temp_df_1 = df[(df['control'] == 'copo-lookup2') & (df['TYPE'] == '1')]
        if len(temp_df_1):
            df.loc[temp_df_1.index, 'data_maxItems'] = 1

        # set cardinality
        df["type"] = df['TYPE'].replace({'1': 'string', 'm': 'array'})

        # set data source for relevant controls
        df['data_source'] = np.where(
            df['control'].isin(['copo-lookup2', 'copo-multi-select2', 'copo-button-list', 'copo-single-select']),
            df['COPO_DATA_SOURCE'],
            '')

        # reset 'type' to string for select2 controls
        temp_df_1 = df[df['control'].isin(['copo-lookup2', 'copo-multi-select2', 'copo-single-select', 'copo-select2'])]
        df.loc[temp_df_1.index, 'type'] = 'string'

        filtered_columns = ["ref", "id", "label", "help_tip", "control", "type", "stage_id", "data_source",
                            "data_maxItems", "dependency", "target_repo", "prefix"]

        df = df.loc[:, filtered_columns]

        df["required"] = False  # this will be set later
        df["field_constraint"] = "optional"  # this will be set later

        schema_list = df.to_dict('records')

        # update schema in file
        cg_schema = helpers.json_to_pytype(self.path_to_json)
        cg_schema['properties'] = schema_list

        with open(self.path_to_json, 'w') as fout:
            json.dump(cg_schema, fout)

        return True
