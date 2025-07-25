import pandas
import pymongo

from bson import json_util
from django.contrib.auth.models import User
from django.http import HttpResponse
from rest_framework import status

import common.dal.copo_base_da as da
from common.dal.sample_da import Sample
from common.dal.mongo_util import cursor_to_list
from common.utils.helpers import get_excluded_associated_projects
from src.apps.copo_core.models import AssociatedProfileType, ProfileType
from ..utils import finish_request, validate_date_from_api, validate_project


def get_number_of_users(request):
    users = User.objects.all()
    number = len(users)
    return HttpResponse(number)


def get_number_of_samples_by_sample_type(request, sample_type):
    d_from = request.GET.get('d_from', None)
    d_to = request.GET.get('d_to', None)

    # Validate optional date fields
    result = validate_date_from_api(d_from, d_to, optional=True)

    # Return response if result is an error
    if isinstance(result, HttpResponse):
        return result

    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    # Strange Swagger API bug where sample_type is being passed
    # as a comma instead of an empty string if it is not specified/provided
    sample_type = str() if sample_type == ',' else sample_type

    # Validate optional sample type field
    project_issues = validate_project(sample_type, field_name='sample_type', optional=True)
    if project_issues:
        return project_issues

    number = Sample().get_number_of_samples_by_sample_type(
        sample_type.lower(), d_from_parsed, d_to_parsed
    )
    return HttpResponse(number)


def get_number_of_samples(request):
    number = Sample().get_number_of_samples()
    return HttpResponse(number)


def get_number_of_profiles(request):
    number = da.handle_dict["profile"].count_documents({})
    return HttpResponse(number)


def get_number_of_datafiles(request):
    number = da.handle_dict["datafile"].count_documents({})
    return HttpResponse(number)


def combined_stats_json(request):
    stats = cursor_to_list(
        da.handle_dict["stats"].find({}, {"_id": 0}).sort('date', pymongo.DESCENDING)
    )
    df = pandas.DataFrame(stats, index=None)
    return HttpResponse(df.reset_index().to_json(orient='records'))


def samples_stats_csv(request):
    stats = cursor_to_list(
        da.handle_dict["stats"]
        .find(
            {},
            {
                "_id": 0,
                "date": 1,
                "samples": 1,
            },
        )
        .sort('date', pymongo.ASCENDING)
    )
    df = pandas.DataFrame(stats, index=None)
    df = df.rename(columns={"samples": "num"})
    x = df.to_json(orient="records")
    return HttpResponse(x, content_type="text/json")


def samples_hist_json(request, metric):
    # get counts for each label in the supplied variable (metric) across tol samples
    if metric == "GAL":
        # need to merge PARTNER and GAL columns as this field has different names between tol types
        projection = {"GAL": 1, "PARTNER": 1, "_id": 0}
        s_list = list(
            Sample()
            .get_collection_handle()
            .find(
                {
                    "$or": [
                        {"TOL_PROJECT": "DTOL"},
                        {"TOL_PROJECT": "ASG"},
                        {"sample_type": "dtol"},
                        {"sample_type": "asg"},
                    ]
                },
                projection,
            )
        )
        df = pandas.DataFrame(s_list)
        df["GAL"][df["GAL"].isnull()] = df["PARTNER"][df["GAL"].isnull()]
    else:
        projection = {metric: 1, "_id": 0}
        s_list = list(
            Sample()
            .get_collection_handle()
            .find(
                {
                    "$or": [
                        {"TOL_PROJECT": "DTOL"},
                        {"TOL_PROJECT": "ASG"},
                        {"sample_type": "dtol"},
                        {"sample_type": "asg"},
                    ]
                },
                projection,
            )
        )
        df = pandas.DataFrame(s_list)

    # now get counts of each value label in dataframe
    if not df.empty:
        u = df[metric].value_counts()
        out = list()
        for x in u.keys():
            out.append({"k": x, "v": int(u[x])})
        return HttpResponse(json_util.dumps(out))
    return HttpResponse(
        json_util.dumps([]), content_type="application/json"
    )


def get_tol_projects(request):
    tol_project_lst = sorted(
        {
            item.description
            for item in ProfileType().get_profile_types()
            if item.is_dtol_profile
        }
    )
    return finish_request(tol_project_lst)


def get_associated_tol_projects(request):
    excluded_associated_projects = get_excluded_associated_projects()

    # Filter associated projects based on exclusion list
    associated_projects = sorted(
        item.label
        for item in AssociatedProfileType().get_associated_profile_types()
        if item.name not in excluded_associated_projects
    )
    return finish_request(associated_projects)
