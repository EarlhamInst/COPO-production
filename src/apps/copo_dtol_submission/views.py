from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseBadRequest
from common.dal.copo_da import Sample, Profile, Submission
from bson import json_util
from common.utils.helpers import get_group_membership_asString, notify_frontend
from src.apps.copo_core.models import ViewLock
from src.apps.copo_core.views import web_page_access_checker
from common.utils.helpers import  get_group_membership_asString, get_current_user
import json
from bson.objectid import ObjectId
from django.conf import settings
from .utils.copo_email import Email

# Create your views here.


@web_page_access_checker
@login_required
def copo_sample_accept_reject(request):
    sample_manager_groups = list()
    is_bge_checker = False
    user_groups = get_group_membership_asString()
    for group in user_groups:
        idx = group.rfind("_sample_managers")
        if idx > 0:
            sample_manager_groups.append(group[0:idx])
        if "bge_checkers" == group:
            is_bge_checker =  True
            
    return render(request, 'copo/copo_sample_accept_reject.html', {"sample_manager_groups":sample_manager_groups, "is_bge_checker":is_bge_checker})


@login_required
def get_samples_column_names(request):
    columnanmes = Sample().get_sample_display_column_names()
    return HttpResponse(json_util.dumps(columnanmes))


@login_required
def update_pending_samples_table(request):
    profile_filter = request.GET.get("profiles", "")
    group_filter = request.GET.get("group", "")

    # samples = Sample().get_unregistered_dtol_samples()
    member_groups = get_group_membership_asString()
    # todo control for someone being both
    profiles = []
    if f"{group_filter}_sample_managers" in member_groups:
        profiles = Profile().get_profiles(filter=profile_filter, group_filter=group_filter)

    """     
    if "dtol_sample_managers" in member_groups:
        profiles = Profile().get_dtol_profiles(filter=profile_filter)
    if "erga_sample_managers" in member_groups:
        profiles += Profile().get_erga_profiles(filter=profile_filter)
    if "dtolenv_sample_managers" in member_groups:
        profiles += Profile().get_dtolenv_profiles(filter=profile_filter) """

    return HttpResponse(json_util.dumps(profiles))


@login_required
def get_samples_for_profile(request):
    url = request.build_absolute_uri()
    if not ViewLock().isViewLockedCreate(url=url):
        profile_id = request.GET["profile_id"]
        filter = request.GET["filter"]
        start = request.GET.get("start", "0")
        length = request.GET.get("length", "10")
        draw = request.GET.get("draw", "1")
        sort_by = request.GET.get("order[0][column]", "")
        direction = request.GET.get("order[0][dir]", "")
        search = request.GET.get("search", "")
        dir = 1
        if direction == "desc":
            dir = -1

        samples = Sample().get_dtol_from_profile_id(
            profile_id, filter, draw, start, length, sort_by, dir, search)
        # notify_frontend(msg="Creating Sample: " + "sprog", action="info",
        #                     html_id="dtol_sample_info")

        return HttpResponse(json_util.dumps(samples))
    else:
        return HttpResponse(json_util.dumps({"locked": True}))


@login_required
def mark_sample_rejected(request):
    sample_ids = request.GET.get("sample_ids")
    sample_ids = json.loads(sample_ids)
    if sample_ids:
        for sample_id in sample_ids:
            d1 = Sample().mark_rejected(sample_id)
            d2 = Sample().timestamp_dtol_sample_updated(sample_id)
            if not d1 and d2:
                return HttpResponse(status=500)
        return HttpResponse(status=200)
    return HttpResponse(status=500)


@login_required
def add_sample_to_dtol_submission(request):
    sample_ids = request.GET.get("sample_ids")
    sample_ids = json.loads(sample_ids)
    profile_id = request.GET.get("profile_id")
    profile = Profile().get_record(profile_id)
    group = get_group_membership_asString()
    is_bge_checker =  "bge_checkers" in group
 
    # check we have required params
    if sample_ids and profile:
        # check for submission object, and create if absent
        sub = Submission().get_dtol_submission_for_profile(profile_id)
        type_sub = profile["type"]
        #is_bge_profile = "BGE" in [ x.get("value","") for x in profile.get("associated_type",[]) ]
        sequence_centres = profile.get("sequencing_centre", [])
        is_sanger_profile = "erga" in type_sub.lower() and settings.SANGER_SEQUENCING_CENTER in sequence_centres

        if not sub:
            if type_sub == "Aquatic Symbiosis Genomics (ASG)":
                sub = Submission(profile_id).save_record(
                    dict(), **{"type": "asg"})
            elif type_sub == "European Reference Genome Atlas (ERGA)":
                sub = Submission(profile_id).save_record(
                    dict(), **{"type": "erga"})
            else:
                sub = Submission(profile_id).save_record(
                    dict(), **{"type": "dtol"})
        sub["dtol_status"] = "pending"
        sub["target_id"] = sub.pop("_id")

        sample_oids = list(map(lambda id: ObjectId(id), sample_ids))
        samples = Sample().get_all_records_columns(filter_by=dict(_id={"$in": sample_oids}), projection=dict(status=1))
        processing_sample_ids = []
        pending_sample_ids = []
        bge_pending_sample_ids = []
        for sample in samples:
            sample_id = str(sample["_id"])
            notify_frontend(action="delete_row", html_id=str(sample_id), data={})

            if is_sanger_profile and sample["status"] != "pending":
                if is_bge_checker and sample["status"] == "bge_pending":
                    pending_sample_ids.append(sample_id)
                else:
                    bge_pending_sample_ids.append(sample_id)
            else:
                processing_sample_ids.append(sample_id)
                # iterate over samples and add to submission
                if not sample_id in sub["dtol_samples"]:
                    sub["dtol_samples"].append(sample_id)
                    # Sample().get_all_records_columns()
        if processing_sample_ids:
            Sample().mark_processing(sample_ids=processing_sample_ids)
        if pending_sample_ids:
            Sample().mark_pending(sample_ids=pending_sample_ids)
            uri = request.build_absolute_uri('/')
            #send email to Sangar to notify them of new samples
            Email().notify_manifest_pending_for_sequencing_center(data=uri + 'copo/dtol_submission/accept_reject_sample/', profile_id=profile_id,  title=profile["title"], description=profile["description"] )
        if bge_pending_sample_ids:
            Sample().mark_pending(sample_ids=bge_pending_sample_ids, is_erga=True)
            uri = request.build_absolute_uri('/')
            #send email to BGE to notify them of new samples
            Email().notify_manifest_pending_for_bge_checker(data=uri + 'copo/dtol_submission/accept_reject_sample/', profile_id=profile_id,  title=profile["title"], description=profile["description"] )
        Sample().timestamp_dtol_sample_updated(sample_ids=sample_ids)
        # sample_ids_bson = list(map(lambda id: ObjectId(id), sample_ids))
        # sepciment_ids = Sample().get_collection_handle().distinct( 'SPECIMEN_ID', {"_id": {"$in": sample_ids_bson}});
        # if "dtol_specimen" not in sub:
        #    sub["dtol_specimen"] = []
        # for speciment_id in sepciment_ids:
        #    if speciment_id not in sub["dtol_specimen"]:
        #        sub["dtol_specimen"].append(speciment_id)
        if processing_sample_ids:
            if Submission().save_record(dict(), **sub):
                return HttpResponse(status=200)
            else:
                return HttpResponse(status=500)
        else:
            notify_frontend(data={"profile_id": profile_id}, msg="", action="hide_sub_spinner",
                html_id="dtol_sample_info")
        return HttpResponse(status=200)
    else:
        return HttpResponse(status=500, content="Sample IDs or profile_id not provided")


@login_required
def delete_dtol_samples(request):
    ids = json.loads(request.POST.get("sample_ids"))
    report = list()
    for s in ids:
        r = Sample().delete_sample(s)
        report.append(r)
    notify_frontend(data={"profile_id": ""}, msg=report,
                    action="info",
                    html_id="sample_info")

    return HttpResponse(json.dumps({}))
