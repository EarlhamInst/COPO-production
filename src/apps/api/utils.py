__author__ = 'felix.shaw@tgac.ac.uk - 20/01/2016'

from common.lookup.lookup import API_RETURN_TEMPLATES
from common.schema_versions.lookup.dtol_lookups import STANDARDS, STANDARDS_MAPPING_FILE_PATH
from django.http import HttpResponse
from django_tools.middlewares import ThreadLocal

import bson.json_util as jsonb
import json
import pandas as pd
import shortuuid
import uuid

def get_return_template(type):
    """
    Method to return a python object representation of the given api template return type
    :param type: a string naming the template type
    :return: an python object representation of the json contained in the template
    """
    path = API_RETURN_TEMPLATES[type.upper()]
    with open(path) as data_file:
        data = json.load(data_file)
    return data

def extract_to_template(object=None, template=None):
    """
    Method to examine fields in object and extract those which match the field names in template along with their values
    :param object: the object to search
    :param template: the fields to look for
    :return: the template with the values completed
    """
    for f in object:
        for t in template:
            if f == t:
                template[t] = object[t]

    return template

def generate_rocrate_person_object(df, personlist, person_field_name, prefix, person_map):
    # items = []
    for people in personlist:
        person_affiliations = df[df[person_field_name] == people][f"{prefix}_AFFILIATION"].unique(
        )[0] if f"{prefix}_AFFILIATION" in df.columns else str()
        orcid_ids = df[df[person_field_name] == people][f"{prefix}_ORCID_ID"].unique(
        )[0] if f"{prefix}_ORCID_ID" in df.columns else str()

        person_list = people.split("|")
        person_affiliation_list = person_affiliations.split("|")
        orcid_id_list = orcid_ids.split("|")
        affiliation = ""

        for index,  person in enumerate(person_list):
            name = person.strip()
            item = person_map.get(name, dict())
            person_map[name] = item

            if orcid_ids and len(orcid_id_list) > index:
                item["@id"] = "http://orcid.org/" + \
                    orcid_id_list[index].strip()
            else:
                item["@id"] = uuid.uuid4().hex
            item["name"] = name
            item["@type"] = "Person"

            affiliation_list = item.get("affiliation", list())
            item["affiliation"] = affiliation_list

            # item["role"] = person_field_name
            if person_affiliation_list:
                if len(person_affiliation_list) > index:
                    affiliation = person_affiliation_list[index].strip()
                    if affiliation not in item["affiliation"]:
                        item["affiliation"].append(affiliation)
                elif affiliation:
                    if affiliation not in item["affiliation"]:
                        item["affiliation"].append(affiliation)

            # items.append(item)
    return person_map

def generate_rocrate_response(samples):
    '''
    result_list = []
    manifest_map = dict()
    for samples in data:
        if type(samples) != dict or  "manifest_id" not in samples:
            result_list = ["Not Implemented"]
            return result_list
        manifest_id = samples.get("manifest_id","")
        if manifest_id:
            if manifest_id not in manifest_map:
                manifest_map[manifest_id] = []
            manifest_map[manifest_id].append(samples)
    if len(manifest_map.keys()) > 1:
        result_list = ["Not Implemented"]
        return result_list
    '''
    rocrate_json = {}
    manifest_id = samples[0].get("manifest_id", "")

    if not manifest_id:
        rocrate_json["error"] = "Not Implemented"
    else:
        # @type: context
        # for key, samples in manifest_map.items():
        # rocrate_json = {}
        context = [
            "https://w3id.org/ro/crate/1.1/context",
            "https://w3id.org/ro/terms/sample",
            "https://w3id.org/ro/terms/copo"
        ]

        info_dict = {
            "dwc":  "http://rs.tdwg.org/dwc/terms/",
            "dwciri": "http://rs.tdwg.org/dwc/iri/",
            "BioSample": "https://bioschemas.org/BioSample",
            "collector": "https://bioschemas.org/BioSample#collector",
            "custodian": "https://bioschemas.org/BioSample#custodian",
            "isControl": "https://bioschemas.org/BioSample#isControl",
            "samplingAge": "https://bioschemas.org/BioSample#samplingAge",
            "taxonomicRange": "http://schema.org/taxonomicRange"
        }

        context.append(info_dict)
        rocrate_json["@context"] = context
        graph_list = list()

        # @type: CreativeWork
        creativeWork = {"@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork"}
        creativeWork["conformsTo"] = {"@id": "https://w3id.org/ro/crate/1.1"}
        # {"@id":f"https://copo-project.org/api/manifest/{key}" }
        creativeWork["about"] = {
            "@id": f"https://copo-project.org/api/manifest/{manifest_id}"}
        graph_list.append(creativeWork)

        df = pd.DataFrame(samples)
        dateCreated = df["time_created"].min()
        dateModifed = df["time_updated"].max()

        # @type: Person
        # updatedby = df["updated_by"].unique()
        # author = df["created_by"].unique()
        collectedby = df["COLLECTED_BY"].unique()
        coordinator = df["SAMPLE_COORDINATOR"].unique(
        ) if "SAMPLE_COORDINATOR" in df.columns else []
        perservedby = df["PERSERVED_BY"].unique(
        ) if "PERSERVED_BY" in df.columns else []
        identifiedby = df["IDENTIFIED_BY"].unique(
        ) if "IDENTIFIED_BY" in df.columns else []

        rocrate_person = dict()
        rocrate_person = generate_rocrate_person_object(
            df, collectedby, "COLLECTED_BY", "COLLECTOR", rocrate_person)
        rocrate_person = generate_rocrate_person_object(
            df, coordinator, "SAMPLE_COORDINATOR", "SAMPLE_COORDINATOR", rocrate_person)
        rocrate_person = generate_rocrate_person_object(
            df, perservedby, "PERSERVED_BY", "PERSERVER", rocrate_person)
        rocrate_person = generate_rocrate_person_object(
            df, identifiedby, "IDENTIFIED_BY", "IDENTIFIER", rocrate_person)

        # @type: Dataset
        # f"https://copo-project.org/api/manifest/{key}
        manifest_item = {"@id": f"https://copo-project.org/api/manifest/{manifest_id}",
                         "@type": "Dataset", "dateCreated": dateCreated, "datedModified": dateModifed}
        manifest_item["contributor"] = []
        manifest_item["contributor"].extend(
            {"@id":  p["@id"]} for p in rocrate_person.values())

        manifest_item["hasPart"] = [
            {"@id": f"https://copo-project.org/api/sample/copo_id/{x['copo_id']}"} for x in samples]
        manifest_item["taxonomicRange"] = [
            {"@id": f"http://identifiers.org/taxonomy:{x}"} for x in df["TAXON_ID"].unique()]
        location_mapping = [{f"#place{str(shortuuid.ShortUUID().random(length=10))}": x}
                            for x in df["COLLECTION_LOCATION"].unique()]
        manifest_item["location"] = [
            {"@id": k} for element in location_mapping for k, v in element.items()]

        graph_list.append(manifest_item)
        graph_list.extend(rocrate_person.values())

        # @type: Taxon
        for x in df["TAXON_ID"].unique():
            item = {"@id": f"http://identifiers.org/taxonomy:{x}"}
            item["@type"] = "TAXON"
            item["name"] = df[df["TAXON_ID"] == x]["SCIENTIFIC_NAME"].unique(
            )[0] if "SCIENTIFIC_NAME" in df.columns else ""
            item["parentTaxon"] = {
                "@id": f"https://copo-project.org/api/sample_field/ORDER_OR_GROUP/{df[df['TAXON_ID']== x ]['ORDER_OR_GROUP'].unique()[0]}"}
            graph_list.append(item)

        # @type: Place
        lat_prefix = "LATITUDE"
        long_prefix = "LONGITUDE"
        latLong_startEnd_lst = [
            f'{lat_prefix}_START', f'{lat_prefix}_END', f'{long_prefix}_START', f'{long_prefix}_END']

        for element in location_mapping:
            for key, value in element.items():
                item = {"@id": key}
                item["@type"] = "Place"
                item["name"] = value
                item["latitude"] = df[df["COLLECTION_LOCATION"] == value]["DECIMAL_LATITUDE"].unique()[
                    0] if "DECIMAL_LATITUDE" in df.columns else ""
                item["logitude"] = df[df["COLLECTION_LOCATION"] == value]["DECIMAL_LONGITUDE"].unique()[
                    0] if "DECIMAL_LONGITUDE" in df.columns else ""

                if all(element in df.columns for element in latLong_startEnd_lst):
                    for i in latLong_startEnd_lst:
                        item[i] = df[df["COLLECTION_LOCATION"] == value][i].unique()[
                            0] if i in df.columns else ""

                graph_list.append(item)

        # @type: BioSample
        # Function to insert a key, value pair into a dictionary at a specified position/index
        def insert(_dict, obj, pos): return {k: v for k, v in (
            list(_dict.items())[:pos] + list(obj.items()) + list(_dict.items())[pos:])}

        for x in samples:
            sample_item = {
                "@id": f"https://copo-project.org/api/sample/copo_id/{x['copo_id']}", "@type": "BioSample"}
            keys_lst = list(x.keys())
            biosampleAccession = x.get("biosampleAccession", "")

            if "TAXON_ID" in x:
                # sample_item["taxonomicRange"] = {"@id": f"http://identifiers.org/taxonomy:{x['TAXON_ID']}"}
                # Get index/position to insert new key, value into sample item dictionary
                index = keys_lst.index("TAXON_ID") + 1
                # Insert new key, value into dictionary at specified position
                x = insert(x, {'taxonomicRange': {
                           "@id": f"https://identifiers.org/taxonomy:{x['TAXON_ID']}"}}, index)

            if biosampleAccession:
                sample_item["identifier"] = {
                    "@id": f"http://identifiers.org/biosample:{biosampleAccession}"}

            for p in "COLLECTOR:COLLECTED_BY", "SAMPLE_COORDINATOR:SAMPLE_COORDINATOR", "PERSERVER:PERSERVED_BY", "IDENTIFIER:IDENTIFIED_BY":
                pp = p.split(":")
                collectors = x.get(pp[1], "")
                if collectors:
                    # sample_item[pp[0].lower()] = []
                    # Recalculate the list of keys since it has been adjusted since a key, value pair insertion was done
                    keys_lst = list(x.keys())

                    field = ""
                    if (pp[0].lower() != "collector"):
                        field = "custodian"
                        # Get index/position to insert new key, value pair into sample item dictionary
                        index = keys_lst.index(f"{pp[0]}_AFFILIATION") + 1
                    else:
                        field = pp[0].lower()
                        # Get index/position to insert new key, value pair into sample item dictionary
                        index = keys_lst.index(pp[1]) + 1

                    # Insert new key, value into dictionary at specified position
                    x = insert(x, {field: []}, index)

                    for c in collectors.split("|"):
                        collector = rocrate_person.get(c.strip(), dict())
                        if collector:
                            # sample_item[pp[0].lower()].append({"@id": collector["@id"]})
                            x[field].append({"@id": collector["@id"]})

            sample_item.update(x)
            graph_list.append(sample_item)

        rocrate_json["@graph"] = graph_list
        # result_list.append(rocrate_json)
    return rocrate_json

def get_standard_mapping(standard_list, template):    
    with open(STANDARDS_MAPPING_FILE_PATH) as f:
        standards_data = json.load(f)
        standards_data = standards_data[0]

        output_dict = dict()
        output_list = list()

        for d in template:
            outer_dict = dict()
            lst_dict = dict()
            inner_dict = dict()
            
            copo_id = d.get('copo_id', str())       
            
            for key, value in d.items():
                tol_column_dict = dict()
                tol_column_dict[key] = dict()

                for standard in standard_list:
                    inner_dict = dict()

                    if key in standards_data.keys():
                        inner_dict[standard] = {'key': key, 'value': value} if standard == "tol" else {'key':  standards_data.get(key, str()).get(standard, str()).get('field', str()), 'value': value}
                    else:
                        inner_dict[standard] = {'key': key, 'value': value} if standard == "tol" else {'key': str(), 'value': value}
                    
                    # Merge dictionaries
                    tol_column_dict[key] |= inner_dict

                    # Merge dictionaries that reference the same TOL column name
                    lst_dict |= tol_column_dict

            outer_dict[copo_id] = lst_dict
            
            # Merge dictionaries that reference the same 'copo_id'
            output_dict |= outer_dict

            output_list.append(output_dict)

    return output_list #standard_json

def finish_request(template=None, error=None, num_found=None, return_http_response=True):
    """
    Method to tidy up data before returning API caller
    :param template: completed template of resource data
    :param error_info: error created if any
    :return: the complete API return
    """
    request = ThreadLocal.get_current_request()
    return_type = request.GET.get('return_type', "json").lower()

    standard = request.GET.get('standard', "tol")

    # Split the 'standard' string into a list
    standard_list = standard.split(',')
    standard_list = list(map(lambda x: x.strip().lower(), standard_list))

    # Remove any empty elements in the list e.g.
    # where 2 or more commas have been typed in error
    standard_list[:] = [x for x in standard_list if x]

    '''
    if is_csv == 'True' or is_csv == 'true' or is_csv == '1' or is_csv == 1 :
        is_csv = True
    else:
        is_csv = False
    '''
    wrapper = get_return_template('WRAPPER')

    # Set template with standard data if standard is one of the 
    # standards - dwc, ena or mixs identified in the STANDARDS list
    if any(x in standard_list for x in STANDARDS) and standard_list != ["tol"] and return_http_response:
        template = get_standard_mapping(standard_list, template)

    if error is None:
        if num_found == None:
            if template == None:
                wrapper["number_found"] = 0
            if type(template) == type(list()):
                wrapper['number_found'] = len(template)
            else:
                wrapper['number_found'] = 1
        else:
            wrapper['number_found'] = num_found
        wrapper['data'] = template
        wrapper['status'] = "OK"
    else:
        wrapper['status']['error'] = True
        wrapper['status']['error_detail'] = error
        wrapper['number_found'] = None
        wrapper['data'] = None

    output = jsonb.dumps(wrapper)
    
    if return_http_response:
        if return_type == "csv" and standard_list == ["tol"]:
            # Create the HttpResponse object with the appropriate CSV header
            # only if the standard is TOL
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename=export.csv'
            df = pd.DataFrame(template)
            df.to_csv(response, index=False)
            return response
        elif return_type == "csv" and standard_list != ["tol"]:
            return HttpResponse(content="Not Implemented")
        elif return_type == "rocrate":
            df = pd.DataFrame(template)
            manifest_ids = df['manifest_id'].unique() if 'manifest_id' in df.columns else []
            if len(manifest_ids) == 1 :
                rocrate_objs = generate_rocrate_response(template)
                return HttpResponse(content=jsonb.dumps(rocrate_objs),content_type="application/json" )
            return HttpResponse(content="Not Implemented")
        else:
            return HttpResponse(output, content_type="application/json")

    else:
        return output
