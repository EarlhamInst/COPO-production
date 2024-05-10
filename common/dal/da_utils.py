from django_tools.middlewares import ThreadLocal
from .copo_da import DataFile
from .sample_da import Sample, Source
from .submission_da import Submission


def get_current_request():
    return ThreadLocal.get_current_request()

def get_samples_options():
    profile_id = get_current_request().session['profile_id']
    samples = Sample(profile_id).get_all_records()

    option_values = []
    for sd in samples:
        label = sd["name"]
        option_values.append({"value": str(sd["_id"]), "label": label})

    return option_values

def get_existing_study_options():
    subs = Submission().get_complete()
    out = list()
    out.append({
        "value": "required",
        "label": "-- select one --"
    })
    out.append({
        "value": "none",
        "label": "Not in COPO"
    })
    for s in subs:
        try:
            out.append(
                {
                    "value": s['profile_id'],
                    "label": s['accessions']['project']['accession']
                }
            )
        except:
            pass
    return out

def get_isasamples_json():
    '''
    returns isa samples in a profile
    :return
    '''
    profile_id = get_current_request().session['profile_id']
    samples = Sample(profile_id).get_all_records()

    value_field = str("id")
    label_field = str("sample_name")
    search_field = ["id", "sample_name"]
    secondary_label_field = ["meta_sample_name"]

    elem_json = dict(value_field=value_field,
                     label_field=label_field,
                     secondary_label_field=secondary_label_field,
                     search_field=search_field,
                     options=list())

    for sd in samples:
        if sd["sample_type"] == "isasample":
            elem_json.get("options").append(
                {
                    value_field: str(sd["_id"]),
                    label_field: sd["name"],
                    secondary_label_field[0]: sd["name"],
                    "sample_type": sd.get("sample_type", str())
                }
            )

    return elem_json


def generate_sources_json(target_id=None):
    profile_id = get_current_request().session['profile_id']

    if target_id:
        sources = list()
        sources.append(Source().get_record(target_id))
    else:
        sources = Source(profile_id).get_all_records()

    schema = Source().get_schema().get("schema_dict")

    value_field = str("id")
    label_field = str("source_name")
    search_field = ["id", "source_name"]
    secondary_label_field = ["meta_source_name"]
    component_records = dict()

    elem_json = dict(value_field=value_field,
                     label_field=label_field,
                     secondary_label_field=secondary_label_field,
                     search_field=search_field,
                     options=list(),
                     component_records=component_records)

    for src in sources:
        elem_json.get("options").append(
            {
                value_field: str(src["_id"]),
                label_field: src["name"],
                secondary_label_field[0]: src["name"]
            })

    return elem_json


def get_samples_json(target_id=None):
    '''
    returns all samples in a profile (i.e isa and biosamples)
    :return:
    '''
    profile_id = get_current_request().session['profile_id']

    if target_id:
        samples = list()
        samples.append(Sample().get_record(target_id))
    else:
        samples = Sample(profile_id).get_all_records()

    value_field = str("id")
    label_field = str("sample_name")
    search_field = ["id", "sample_name"]
    secondary_label_field = ["meta_sample_name"]

    elem_json = dict(value_field=value_field,
                     label_field=label_field,
                     secondary_label_field=secondary_label_field,
                     search_field=search_field,
                     options=list())

    for sd in samples:
        elem_json.get("options").append(
            {
                value_field: str(sd["_id"]),
                label_field: sd["name"],
                secondary_label_field[0]: sd["name"],
                "sample_type": sd.get("sample_type", str())
            })

    return elem_json


def get_datafiles_json(target_id=None):
    '''
    returns all datafile record
    :return:
    '''
    profile_id = get_current_request().session['profile_id']

    if target_id:
        datafiles = list()
        datafiles.append(DataFile().get_record(target_id))
    else:
        datafiles = DataFile(profile_id).get_all_records()

    value_field = str("id")
    label_field = str("datafile_name")
    search_field = ["id", "datafile_name"]
    secondary_label_field = ["meta_datafile_name"]

    elem_json = dict(value_field=value_field,
                     label_field=label_field,
                     secondary_label_field=secondary_label_field,
                     search_field=search_field,
                     options=list())

    for sd in datafiles:
        elem_json.get("options").append(
            {
                value_field: str(sd["_id"]),
                label_field: sd["name"],
                secondary_label_field[0]: sd["name"]
            })

    return elem_json

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def filter_non_sample_accession_dict_lst(lst, search_value):
    '''
    filter non sample accession dict from list 
    based on search value
    :param lst:
    :param search_value:
    :return:
    '''
    search_value = search_value.lower()
    result = list(filter(lambda item: search_value in item.get("accession_type", str()).lower() or search_value in item.get("accession", str()).lower() or search_value in item.get("alias", str()).lower() or search_value in item.get("profile_title", str()).lower(), lst))

    return result

