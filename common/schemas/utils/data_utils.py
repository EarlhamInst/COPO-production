__author__ = 'etuka'

import copy
import json
import os
import uuid
import xml.etree.ElementTree as ET
from collections import namedtuple
from datetime import datetime, tzinfo, timedelta
from common.utils.logger import Logger
from bson.json_util import dumps
from bson import ObjectId
import common.lookup.lookup as lookup
from common.lookup.resolver import RESOLVER
from common.utils import helpers
from .copo_isa_ena import ISAHelpers

l = Logger()


class simple_utc(tzinfo):
    def tzname(self, **kwargs):
        return "UTC"

    def utcoffset(self, dt):
        return timedelta(0)


def pretty_print(data, path=None):
    if path is None:
        print(dumps(data, sort_keys=True,
                    indent=4, separators=(',', ': ')))
    else:
        s = dumps(data, sort_keys=True,
                  indent=4, separators=(',', ': '))
        with open(path, 'w+') as file:
            file.write(s)

def convertListToString(lst):
    # Convert list to string
    return ','.join(lst)

def convertStringToBoolean(string):
    # Convert string boolean to boolean
    return str(string).lower() in ("yes", "true", "t", "1")

def convertStringToList(string):
    # Split the string into a list
    lst = string.split(',')
    lst = list(map(lambda x: x.strip().lower(), lst))

    # Remove any empty elements in the list e.g.
    # where 2 or more commas have been typed in error
    lst[:] = [x for x in lst if x]

    return lst
    
def convertStringToTitleCase(str):
    # Convert given a string to title case/sentence case
    return str.title() \
        .replace("_", " ") \
        .replace("Id", "Identifier") \
        .replace("accession", " Accession") \
        .replace("Sra", "SRA") \
        .replace("Seq", "Sequence")

def get_profile_type(profile_type):
    return profile_type.upper()

    
def join_list_with_and_as_last_entry(lst):
    # Join the list of sequencing centre labels 
    # with commas then, have 'and' as the last entry
    if len(lst) > 2:
        return ', '.join(lst[:-1]) + ", and " + str(lst[-1])
    elif len(lst) == 2:
        return ' and '.join(lst)
    elif len(lst) == 1:
        return lst[0]
    
def json_to_object(data_object):
    # converts a dictionary to object
    data = ""
    if isinstance(data_object, dict):
        data = json.loads(json.dumps(data_object), object_hook=lambda d: namedtuple(
            'X', d.keys())(*d.values()))
    return data


def is_valid_uuid(value):
    # Check if string is a valid uuid
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False


def is_valid_ObjectId(value):
    # Check if value is a valid ObjectId
    if ObjectId.is_valid(value):
        return True
    else:
        return False


def get_label(value, list_of_elements, key_name):
    for dict in list_of_elements:
        return dict["label"] if dict[key_name] == value else ''


def lookup_study_type_label(val):
    # get study types
    lbl = str()
    study_types = lookup.DROP_DOWNS['STUDY_TYPES']

    for st in study_types:
        if st["value"].lower() == val.lower():
            lbl = st["label"]
            break

    return lbl


def get_copo_exception(key):
    messages = json_to_pytype(lookup.MESSAGES_LKUPS["exception_messages"])[
        "properties"]

    return messages.get(key, str())


def log_copo_exception(message):
    pass


def get_db_template(schema):
    f_path = lookup.DB_TEMPLATES[schema]
    data = ""
    with open(f_path, encoding='utf-8') as data_file:
        data = json.loads(data_file.read())
    return data


def get_sample_attributes():
    sample_attributes = json_to_pytype(lookup.X_FILES["SAMPLE_ATTRIBUTES"])
    # maybe some logic here to filter the returned attributes,
    # for instance, based on the tags?
    return sample_attributes


def get_isajson_refactor_type(key):
    out_dict = {}
    if key in json_to_pytype(lookup.X_FILES["ISA_JSON_REFACTOR_TYPES"]):
        out_dict = json_to_pytype(
            lookup.X_FILES["ISA_JSON_REFACTOR_TYPES"])[key]
    return out_dict


def json_to_pytype(path_to_json, compatibility_mode=True):
    return helpers.json_to_pytype(path_to_json, compatibility_mode)


def get_repo_type_options():
    return lookup.DROP_DOWNS['REPO_TYPE_OPTIONS']


def get_dataverse_subject_dropdown():
    return lookup.DROP_DOWNS['DATAVERSE_SUBJECTS']


def get_study_type_options():
    return lookup.DROP_DOWNS['STUDY_TYPES']


def get_assembly_type_option():
    return lookup.DROP_DOWNS['ASSEMBLY_TYPES']


def get_sample_type_options():
    data = lookup.DROP_DOWNS_SOURCE.get("sample_type_options", str())
    if isinstance(data, str) and data:  # it's only a path, resolve to get actual data
        data = helpers.json_to_pytype(data)
    return data


def get_repository_options():
    data = lookup.DROP_DOWNS_SOURCE.get("repository_options", str())
    if isinstance(data, str) and data:  # it's only a path, resolve to get actual data
        data = helpers.json_to_pytype(data)
    return data


def get_omics_type_options():
    return lookup.DROP_DOWNS['OMICS_TYPE']


def get_growth_area_options():
    return lookup.DROP_DOWNS['GROWTH_AREAS']


def get_rooting_medium_options():
    return lookup.DROP_DOWNS['ROOTING_MEDIUM']


def get_nutrient_control_options():
    return lookup.DROP_DOWNS['GROWTH_NUTRIENTS']


def get_watering_control_options():
    return lookup.DROP_DOWNS['WATERING_OPTIONS']


'''
def get_copo_id():
    # todo: remove this and uncomment the below try block!!!
    import uuid
    u = uuid.uuid4()
    return int(str(u.time_low) + str(u.time_mid))
    # make unique copo id
    # try:
    #     return get_uid()
    # except ConnectionError:
    #     return "0" * 13


def get_user_id():
    if settings.UNIT_TESTING:
        User.objects.get(username=settings.TEST_USER_NAME).id
    else:
        return ThreadLocal.get_current_user().id


def get_current_user():
    if settings.UNIT_TESTING:
        return User.objects.get(username=settings.TEST_USER_NAME)
    else:
        return ThreadLocal.get_current_user()


def get_current_request():
    return ThreadLocal.get_current_request()


def get_base_url():
    r = ThreadLocal.get_current_request()
    scheme = r.scheme
    domain = r.get_host()
    return scheme + "://" + domain


def get_datetime():
    """
    provides a consistent way of storing fields of this type across modules
    :return:
    """
    return datetime.utcnow()


def get_not_deleted_flag():
    """
    provides a consistent way of setting records as not deleted
    :return:
    """
    return "0"


def get_deleted_flag():
    """
    provides a consistent way of setting records as not deleted
    :return:
    """
    return "1"
'''


def get_button_templates():
    return copy.deepcopy(lookup.BUTTON_TEMPLATES)["templates"]


def get_db_json_schema(component):
    """
    function returns a JSON schema configuration given a component.
    Note: these are db models schemas, necessarily conforming to the JSON Schema specification and different from
    UI schemas, which provide the configuration that COPO UI schemas map to.
    :param component:
    :return:
    """

    isa_path = RESOLVER['isa_json_db_models']

    # ...set other paths accordingly depending on where the actual files reside or, conceptually, the schema provider

    schema_dict = dict(
        publication=json_to_pytype(os.path.join(
            isa_path, 'publication_schema.json')).get("properties", dict()),
        person=json_to_pytype(os.path.join(isa_path, 'person_schema.json')).get(
            "properties", dict()),
        datafile=json_to_pytype(os.path.join(
            isa_path, 'data_schema.json')).get("properties", dict()),
        sample=json_to_pytype(os.path.join(isa_path, 'sample_schema.json')).get(
            "properties", dict()),
        source=json_to_pytype(os.path.join(isa_path, 'source_schema.json')).get(
            "properties", dict()),
        material=json_to_pytype(os.path.join(
            isa_path, 'material_schema.json')).get("properties", dict()),
        comment=json_to_pytype(os.path.join(
            isa_path, 'comment_schema.json')).get("properties", dict()),
        material_attribute_value=json_to_pytype(os.path.join(isa_path, 'material_attribute_value_schema.json')).get(
            "properties", dict()),
        material_attribute=json_to_pytype(os.path.join(isa_path, 'material_attribute_schema.json')).get("properties",
                                                                                                        dict()),
        investigation=json_to_pytype(os.path.join(
            isa_path, 'investigation_schema.json')).get("properties", dict()),
        ontology_annotation=json_to_pytype(os.path.join(isa_path, 'ontology_annotation_schema.json')).get("properties",
                                                                                                          dict()),
        study=json_to_pytype(os.path.join(isa_path, 'study_schema.json')).get(
            "properties", dict()),
        assay=json_to_pytype(os.path.join(isa_path, 'assay_schema.json')).get(
            "properties", dict()),
        protocol=json_to_pytype(os.path.join(
            isa_path, 'protocol_schema.json')).get("properties", dict()),
        protocol_parameter=json_to_pytype(os.path.join(isa_path, 'protocol_parameter_schema.json')).get("properties",
                                                                                                        dict()),
        factor=json_to_pytype(os.path.join(isa_path, 'factor_schema.json')).get(
            "properties", dict()),
        factor_value=json_to_pytype(os.path.join(
            isa_path, 'factor_value_schema.json')).get("properties", dict()),
        process=json_to_pytype(os.path.join(
            isa_path, 'process_schema.json')).get("properties", dict()),
        process_parameter_value=json_to_pytype(os.path.join(isa_path, 'process_parameter_value_schema.json')).get(
            "properties", dict()),
        ontology_source_reference=json_to_pytype(os.path.join(isa_path, 'ontology_source_reference_schema.json')).get(
            "properties", dict()),

    )

    return schema_dict.get(component, dict())


def get_isa_schema_xml(file_name):
    pth = RESOLVER['isa_xml_db_models']

    output_dict = dict(status=str(), content=str())

    try:
        output_dict["status"] = "success"
        output_dict["content"] = ET.parse(os.path.join(pth, file_name))

    except:
        output_dict["status"] = "error"
        output_dict["content"] = "Couldn't find any resource that corresponds to the supplied parameter!"

    return output_dict


'''
def get_ena_remote_path(submission_token):
    """
    defines the path for datafiles uploaded to ENA Dropbox
    :param submission_token: the submission id
    :return:
    """
    remote_path = os.path.join(submission_token, str(get_current_user()))

    return remote_path
'''


def get_ena_submission_url(user_name, password):
    """
    function builds the submission url to point to the specified (test or live box) ENA service
    :param user_name:
    :param password:
    :return:
    """


def get_copo_schema(component, as_object=False):
    """
    function retrieves a required UI schema from the DB.
    :param component: a key in the schema_dict to be retrieved
    :param as_object: True returns the schema as an object whose element can be accessed using the '.' notation. False
            for the traditional python dictionary access
    :return:
    """
    from common.dal.copo_base_da import DataSchemas
        
    schema_base = DataSchemas.get_ui_template("COPO").get("copo")

    """
    schema_dict = dict(
        publication=DataSchemas.get_ui_template_node("COPO", "publication"),
        person=DataSchemas.get_ui_template_node("COPO", "person"),
        datafile=DataSchemas.get_ui_template_node("COPO", "datafile"),
        sample=DataSchemas.get_ui_template_node("COPO", "sample"),  
        source=DataSchemas.get_ui_template_node("COPO", "source"), 
        ontology_annotation=DataSchemas.get_ui_template_node("COPO", "ontology_annotation"), 
        comment=DataSchemas.get_ui_template_node("COPO", "comment"),
        material_attribute_value=DataSchemas.get_ui_template_node("COPO", "material_attribute_value"), 
        duration=DataSchemas.get_ui_template_node("COPO", "duration"),
        miappe_rooting_greenhouse=schema_base.get('miappe').get(
            'rooting').get('greenhouse').get("fields", list()),
        miappe_rooting_field=schema_base.get('miappe').get(
            'rooting').get('field').get("fields", list()),
        hydroponics=schema_base.get('miappe').get(
            'nutrients').get('hydroponics').get('fields', list()),
        soil=schema_base.get('miappe').get(
            'nutrients').get('soil').get('fields', list()),
        phenotypic_variables=schema_base.get("miappe").get(
            "phenotypic_variables").get("fields", list()),
        environment_variables=schema_base.get("miappe").get(
            "environment_variables").get("fields", list()),
        metadata_template=DataSchemas.get_ui_template_node("COPO", "metadata_template"),
        approval=DataSchemas.get_ui_template_node("COPO", "approval")
    )
    schema = schema_dict.get(component, list())
    """

    schema = list()
    match component:
        case "miappe_rooting_greenhouse":
            schema = schema_base.get('miappe').get('rooting').get('greenhouse').get("fields", list())
        case "miappe_rooting_field":
            schema = schema_base.get('miappe').get('rooting').get('field').get("fields", list())
        case "hydroponics":
            schema = schema_base.get('miappe').get('nutrients').get('hydroponics').get('fields', list())
        case "soil":
            schema = schema_base.get('miappe').get('nutrients').get('soil').get('fields', list())
        case "phenotypic_variables":
            schema = schema_base.get("miappe").get("phenotypic_variables").get("fields", list())
        case "environment_variables":
            schema = schema_base.get("miappe").get("environment_variables").get("fields", list())
        case _:
            schema = DataSchemas.get_ui_template_node("COPO", component)

    if schema and as_object:
        schema = json_to_object(dict(fields=schema)).fields

    return schema

def object_type_control_map():
    """
    function keeps a mapping of object type controls to schema names
    :return:
    """
    control_dict = dict()
    control_dict["copo-characteristics"] = "material_attribute_value"
    control_dict["ontology term"] = "ontology_annotation"
    control_dict["copo-comment"] = "comment"
    control_dict["copo-duration"] = "duration"
    control_dict["copo-environmental-characteristics"] = "environment_variables"
    control_dict["copo-phenotypic-characteristics"] = "phenotypic_variables"

    return control_dict


def get_object_array_schema():
    control_dict = dict()
    control_dict["copo-characteristics"] = get_copo_schema(
        "material_attribute_value")
    control_dict["copo-comment"] = get_copo_schema("comment")
    control_dict["copo-environmental-characteristics"] = get_copo_schema(
        "environment_variables")
    control_dict["copo-phenotypic-characteristics"] = get_copo_schema(
        "phenotypic_variables")

    return control_dict


'''
def default_jsontype(type):
    d_type = str()

    if type == "object":
        d_type = dict()
    elif type == "array":
        d_type = list()
    elif type == "boolean":
        d_type = False

    return d_type
'''


def get_studies():
    return lookup.DROP_DOWNS['OMICS_TYPE']
    # data = {
    #    "value": "na",
    #    "label": "Not Applicable"
    # }
    # return data


def get_args_from_parameter(parameter, param_value_dict):
    """
    given a comma seprated parameter string, function returns an argument tuple
    :param parameter:
    :param param_value_dict:
    :return:
    """

    parameter = parameter.split(",")
    parameter = [x.strip() for x in parameter]  # remove unwanted whitespaces
    args = [param_value_dict[p] for p in parameter if p in param_value_dict]
    args = tuple(args)

    return args

def san_check(val):
    return val if val is not None else ''

def get_compliant_fields(component, project, can_be_exported=False):
    schema = get_copo_schema(component)
    project = project.lower()
    
    compliant_fields = [
        x['id'].split('.')[-1]
        for x in schema
        if x.get('is_compliant', False) # Field is compliant
        and project in x.get('specifications', [])
        and (x.get('show_in_api', False) if can_be_exported else True)
    ]

    return compliant_fields

def get_export_fields(component, project):
    schema = get_copo_schema(component)
    output = set()
    for x in schema:
        if x.get('show_in_api', False) and not x.get('is_sensitive', False):
            if not x.get('specifications', list()) or project.lower() in x.get('specifications', list()):
                output.add(x['id'].split('.')[-1])
    return list(output)

def get_non_compliant_fields(component, project, can_be_exported=False):
    schema = get_copo_schema(component)
    project = project.lower()
    
    non_compliant_fields = [
        x['id'].split('.')[-1]
        for x in schema
        if not x.get('is_compliant_field', False)  # Field is non-compliant
        and project in x.get('specifications', [])
        and (x.get('show_in_api', False) if can_be_exported else True)
    ]
    
    return non_compliant_fields

def get_sensitive_fields(component):
    schema = get_copo_schema(component)
    sensitive_fields = [x['id'].split('.')[-1] for x in schema if x.get('is_sensitive', False)]
    return sensitive_fields

def get_unqualified_id(qual):
    return qual.split(".")[-1]

class DecoupleFormSubmission:
    def __init__(self, auto_fields, schema):
        """
        :param auto_fields: fields/values list from form submission
        :param schema: the particular schema used for resolving DB fields
        """

        # clear None types
        auto_fields = {k: san_check(v) for k, v in auto_fields.items() if k}

        self.auto_fields = auto_fields
        self.schema = schema
        self.object_has_value = False
        self.global_key_split = "___0___"

    def get_schema_fields_updated(self):

        auto_dict = dict()

        for f in self.schema:
            if f.type == "array":
                # handle array types
                value_list = list()

                object_type_control = object_type_control_map().get(f.control.lower(), str())

                if object_type_control:
                    # handle object type controls e.g., ontology term, comment

                    object_fields = get_copo_schema(object_type_control, True)

                    # get the primary data...and secondary data
                    primary_data = dict()
                    secondary_data_list = list()

                    for o_f in object_fields:
                        comp_key = f.id + "." + o_f.id.split(".")[-1]

                        # and even the field may, also, very well be of type object, decouple further...
                        decoupled_list = self.decouple_object(
                            comp_key, list(), o_f)

                        # match decoupled elements
                        key_list = list()
                        key_value_list = list()

                        for key in decoupled_list:
                            if key in self.auto_fields.keys():

                                c = [k for k in self.auto_fields.keys() if k.startswith(
                                    key + self.global_key_split)]
                                if c:
                                    secondary_data_list.append(c)

                                key_split = (key.split(f.id + ".")
                                             [-1]).split(".")
                                if len(key_split) == 1:
                                    primary_data[key_split[0]
                                                 ] = self.auto_fields[key]
                                else:
                                    key_list.append(key_split[:-1])
                                    key_value_list.append(
                                        dict(keys=key_split, value=self.auto_fields[key]))

                        if key_list:
                            object_model = self.form_object_model(
                                dict(), key_list)

                            for kvl in key_value_list:
                                object_model = self.set_object_fields(
                                    object_model, kvl["keys"], kvl["value"])

                            for kk, vv in object_model.items():
                                primary_data[kk] = vv

                    # don't save empty objects
                    self.object_has_value = False
                    self.has_value(primary_data)

                    if self.object_has_value:

                        # sanitise schema: make it compliant with schema provider's specifications
                        target_schema = get_db_json_schema(object_type_control)
                        if target_schema:
                            for kx in target_schema:
                                target_schema = ISAHelpers().resolve_schema_key(target_schema, kx, object_type_control,
                                                                                primary_data)
                            value_list.append(target_schema)
                        else:
                            value_list.append(primary_data)

                    # sort secondary data
                    if secondary_data_list:
                        for s in secondary_data_list:
                            s.sort()

                        grouped_indx = list()
                        for i in range(0, len(secondary_data_list[0])):
                            h_indx = list()

                            for sdl in secondary_data_list:
                                h_indx.append(sdl[i])

                            grouped_indx.append(h_indx)

                        for g_i in grouped_indx:
                            primary_data = dict()
                            key_list = list()
                            key_value_list = list()

                            for key in g_i:
                                if key in self.auto_fields.keys():

                                    key_split = (
                                        key.split(f.id + ".")[-1]).split(".")
                                    if len(key_split) == 1:
                                        primary_data[(key_split[0]).rsplit(self.global_key_split, 1)[0]] = \
                                            self.auto_fields[key]
                                    else:
                                        key_list.append(key_split[:-1])
                                        key_split[-1] = key_split[-1].rsplit(
                                            self.global_key_split, 1)[0]
                                        key_value_list.append(
                                            dict(keys=key_split, value=self.auto_fields[key]))

                            if key_list:
                                object_model = self.form_object_model(
                                    dict(), key_list)

                                for kvl in key_value_list:
                                    object_model = self.set_object_fields(
                                        object_model, kvl["keys"], kvl["value"])

                                for kk, vv in object_model.items():
                                    primary_data[kk] = vv

                            # don't save empty objects
                            self.object_has_value = False
                            self.has_value(primary_data)

                            if self.object_has_value:
                                # sanitise schema: make it compliant with schema provider's specifications
                                target_schema = get_db_json_schema(
                                    object_type_control)

                                if target_schema:
                                    for kx in target_schema:
                                        target_schema = ISAHelpers().resolve_schema_key(target_schema, kx,
                                                                                        object_type_control,
                                                                                        primary_data)
                                    value_list.append(target_schema)
                                else:
                                    value_list.append(primary_data)

                    auto_dict[f.id.split(".")[-1]] = value_list
                else:
                    # handle array non-object type control

                    # get the primary field...and secondary data
                    if f.id in self.auto_fields.keys():
                        value_list.append(self.auto_fields[f.id])
                        secondary_data_list = [k for k in self.auto_fields.keys() if
                                               k.startswith(f.id + self.global_key_split)]

                        # sort secondary data, keeping the input order
                        if secondary_data_list:
                            secondary_data_list.sort()

                            for sdl in secondary_data_list:
                                value_list.append(self.auto_fields[sdl])

                        auto_dict[f.id.split(".")[-1]] = value_list
            else:
                # handle non-array types
                # l.log("line808: " + str(f))
                object_type_control = object_type_control_map().get(f.control.lower(), str())

                if object_type_control:
                    # handle object type controls e.g., ontology term, comment

                    object_fields = get_copo_schema(object_type_control, True)

                    # get the data
                    primary_data = dict()

                    for o_f in object_fields:
                        comp_key = f.id + "." + o_f.id.split(".")[-1]

                        # and even the field may, also, very well be of type object, decouple further...
                        decoupled_list = self.decouple_object(
                            comp_key, list(), o_f)

                        # match decoupled elements
                        key_list = list()
                        key_value_list = list()

                        for key in decoupled_list:
                            if key in self.auto_fields.keys():

                                key_split = (key.split(f.id + ".")
                                             [-1]).split(".")
                                if len(key_split) == 1:
                                    primary_data[key_split[0]
                                                 ] = self.auto_fields[key]
                                else:
                                    key_list.append(key_split[:-1])
                                    key_value_list.append(
                                        dict(keys=key_split, value=self.auto_fields[key]))

                        if key_list:
                            object_model = self.form_object_model(
                                dict(), key_list)

                            for kvl in key_value_list:
                                object_model = self.set_object_fields(
                                    object_model, kvl["keys"], kvl["value"])

                            for kk, vv in object_model.items():
                                primary_data[kk] = vv

                        # don't save empty objects
                        self.object_has_value = False
                        auto_dict[f.id.split(".")[-1]] = dict()

                        self.has_value(primary_data)

                        if self.object_has_value:
                            auto_dict[f.id.split(".")[-1]] = primary_data

                    # sanitise schema: make it compliant with schema provider's specifications
                    target_schema = get_db_json_schema(object_type_control)

                    if target_schema:
                        for kx in target_schema:
                            target_schema = ISAHelpers().resolve_schema_key(target_schema, kx, object_type_control,
                                                                            primary_data)
                        auto_dict[f.id.split(".")[-1]] = target_schema
                    else:
                        auto_dict[f.id.split(".")[-1]] = primary_data

                else:
                    # not an object type control

                    if f.id in self.auto_fields.keys():
                        auto_dict[f.id.split(".")[-1]] = self.auto_fields[f.id]

        return auto_dict

    def get_schema_fields_updated_dict(self):
        """
        this is an alternative to the '.' object version - converting to object '.' seems expensive
        :return:
        """

        auto_dict = dict()

        for f in self.schema:
            f_id = f['id']
            f_type = f['type']
            f_control = f['control']
            if f_type == "array":
                # handle array types
                value_list = list()

                object_type_control = object_type_control_map().get(f_control.lower(), str())

                if object_type_control:
                    # handle object type controls e.g., ontology term, comment

                    object_fields = get_copo_schema(object_type_control)

                    # get the primary data...and secondary data
                    primary_data = dict()
                    secondary_data_list = list()

                    for o_f in object_fields:
                        comp_key = f_id + "." + o_f['id'].split(".")[-1]

                        # and even the field may very well be of type object, decouple further...
                        decoupled_list = self.decouple_object_dict(
                            comp_key, list(), o_f)

                        # match decoupled elements
                        key_list = list()
                        key_value_list = list()

                        for key in decoupled_list:
                            if key in self.auto_fields.keys():

                                c = [k for k in self.auto_fields.keys() if k.startswith(
                                    key + self.global_key_split)]
                                if c:
                                    secondary_data_list.append(c)

                                key_split = (key.split(f_id + ".")
                                             [-1]).split(".")
                                if len(key_split) == 1:
                                    primary_data[key_split[0]
                                                 ] = self.auto_fields[key]
                                else:
                                    key_list.append(key_split[:-1])
                                    key_value_list.append(
                                        dict(keys=key_split, value=self.auto_fields[key]))

                        if key_list:
                            object_model = self.form_object_model(
                                dict(), key_list)

                            for kvl in key_value_list:
                                object_model = self.set_object_fields(
                                    object_model, kvl["keys"], kvl["value"])

                            for kk, vv in object_model.items():
                                primary_data[kk] = vv

                    # don't save empty objects
                    self.object_has_value = False
                    self.has_value(primary_data)

                    if self.object_has_value:

                        # sanitise schema: make it compliant with schema provider's specifications
                        target_schema = get_db_json_schema(object_type_control)
                        if target_schema:
                            for kx in target_schema:
                                target_schema = ISAHelpers().resolve_schema_key(target_schema, kx, object_type_control,
                                                                                primary_data)
                            value_list.append(target_schema)
                        else:
                            value_list.append(primary_data)

                    # sort secondary data
                    if secondary_data_list:
                        for s in secondary_data_list:
                            s.sort()

                        grouped_indx = list()
                        for i in range(0, len(secondary_data_list[0])):
                            h_indx = list()

                            for sdl in secondary_data_list:
                                h_indx.append(sdl[i])

                            grouped_indx.append(h_indx)

                        for g_i in grouped_indx:
                            primary_data = dict()
                            key_list = list()
                            key_value_list = list()

                            for key in g_i:
                                if key in self.auto_fields.keys():

                                    key_split = (
                                        key.split(f_id + ".")[-1]).split(".")
                                    if len(key_split) == 1:
                                        primary_data[(key_split[0]).rsplit(self.global_key_split, 1)[0]] = \
                                            self.auto_fields[key]
                                    else:
                                        key_list.append(key_split[:-1])
                                        key_split[-1] = key_split[-1].rsplit(
                                            self.global_key_split, 1)[0]
                                        key_value_list.append(
                                            dict(keys=key_split, value=self.auto_fields[key]))

                            if key_list:
                                object_model = self.form_object_model(
                                    dict(), key_list)

                                for kvl in key_value_list:
                                    object_model = self.set_object_fields(
                                        object_model, kvl["keys"], kvl["value"])

                                for kk, vv in object_model.items():
                                    primary_data[kk] = vv

                            # don't save empty objects
                            self.object_has_value = False
                            self.has_value(primary_data)

                            if self.object_has_value:
                                # sanitise schema: make it compliant with schema provider's specifications
                                target_schema = get_db_json_schema(
                                    object_type_control)

                                if target_schema:
                                    for kx in target_schema:
                                        target_schema = ISAHelpers().resolve_schema_key(target_schema, kx,
                                                                                        object_type_control,
                                                                                        primary_data)
                                    value_list.append(target_schema)
                                else:
                                    value_list.append(primary_data)

                    auto_dict[f_id.split(".")[-1]] = value_list
                else:
                    # handle array non-object type control

                    # get the primary field...and secondary data
                    if f_id in self.auto_fields.keys():
                        if type(self.auto_fields[f_id]) == list:
                            value_list = self.auto_fields[f_id]
                        else:
                            value_list.append(self.auto_fields[f_id])
                        secondary_data_list = [k for k in self.auto_fields.keys() if
                                               k.startswith(f_id + self.global_key_split)]

                        # sort secondary data, keeping the input order
                        if secondary_data_list:
                            secondary_data_list.sort()

                            for sdl in secondary_data_list:
                                tmp_val = self.auto_fields.get(
                                    sdl, str()).strip()
                                if tmp_val:
                                    value_list.append(tmp_val)

                        auto_dict[f_id.split(".")[-1]] = value_list
            else:
                # handle non-array types
                object_type_control = object_type_control_map().get(f_control.lower(), str())

                if object_type_control:
                    # handle object type controls e.g., ontology term, comment

                    object_fields = get_copo_schema(object_type_control)

                    # get the data
                    primary_data = dict()

                    for o_f in object_fields:
                        comp_key = f_id + "." + o_f['id'].split(".")[-1]

                        # and even the field may, also, very well be of type object, decouple further...
                        decoupled_list = self.decouple_object_dict(
                            comp_key, list(), o_f)

                        # match decoupled elements
                        key_list = list()
                        key_value_list = list()

                        for key in decoupled_list:
                            if key in self.auto_fields.keys():

                                key_split = (key.split(f_id + ".")
                                             [-1]).split(".")
                                if len(key_split) == 1:
                                    primary_data[key_split[0]
                                                 ] = self.auto_fields[key]
                                else:
                                    key_list.append(key_split[:-1])
                                    key_value_list.append(
                                        dict(keys=key_split, value=self.auto_fields[key]))

                        if key_list:
                            object_model = self.form_object_model(
                                dict(), key_list)

                            for kvl in key_value_list:
                                object_model = self.set_object_fields(
                                    object_model, kvl["keys"], kvl["value"])

                            for kk, vv in object_model.items():
                                primary_data[kk] = vv

                        # don't save empty objects
                        self.object_has_value = False
                        auto_dict[f_id.split(".")[-1]] = dict()

                        self.has_value(primary_data)

                        if self.object_has_value:
                            auto_dict[f_id.split(".")[-1]] = primary_data

                    # sanitise schema: make it compliant with schema provider's specifications
                    target_schema = get_db_json_schema(object_type_control)

                    if target_schema:
                        for kx in target_schema:
                            target_schema = ISAHelpers().resolve_schema_key(target_schema, kx, object_type_control,
                                                                            primary_data)
                        auto_dict[f_id.split(".")[-1]] = target_schema
                    else:
                        auto_dict[f_id.split(".")[-1]] = primary_data

                else:
                    # not an object type control

                    if f_id in self.auto_fields.keys():
                        auto_dict[f_id.split(".")[-1]] = self.auto_fields[f_id]

        return auto_dict

    def decouple_object(self, comp_key, decoupled_list, field):
        base_key = comp_key

        object_type_control = object_type_control_map().get(field.control.lower(), str())

        if object_type_control:  # handle object type controls e.g., ontology term, comment
            object_fields = get_copo_schema(object_type_control, True)

            for o_f in object_fields:
                comp_key = base_key + "." + o_f.id.split(".")[-1]
                decoupled_list = self.decouple_object(
                    comp_key, decoupled_list, o_f)
        else:
            decoupled_list.append(comp_key)

        return decoupled_list

    def decouple_object_dict(self, comp_key, decoupled_list, field):
        """
        this function mirrors the '.' notation function with similar name (without _dict)
        :param comp_key:
        :param decoupled_list:
        :param field:
        :return:
        """
        base_key = comp_key

        object_type_control = object_type_control_map().get(
            field['control'].lower(), str())

        if object_type_control:  # handle object type controls e.g., ontology term, comment
            object_fields = get_copo_schema(object_type_control)

            for o_f in object_fields:
                comp_key = base_key + "." + o_f['id'].split(".")[-1]
                decoupled_list = self.decouple_object_dict(
                    comp_key, decoupled_list, o_f)
        else:
            decoupled_list.append(comp_key)

        return decoupled_list

    def set_object_fields(self, d, keys, val):
        out = d
        for k in keys[:-2]:
            out = out[k]
        out[keys[-2]][keys[-1]] = val

        return d

    def form_object_model(self, d, key_list):
        object_model = d
        for path in key_list:
            current_level = object_model
            for part in path:
                if part not in current_level:
                    current_level[part] = dict()
                current_level = current_level[part]

        return d

    def has_value(self, d):
        for k, v in d.items():
            if isinstance(v, dict):
                self.has_value(v)
            else:
                if v:
                    # set some global value
                    self.object_has_value = True
