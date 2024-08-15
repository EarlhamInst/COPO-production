from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from common.dal.sample_da import Sample 
from common.dal.profile_da import Profile
from common.dal.submission_da import Submission
from bson import json_util
from common.utils.helpers import get_group_membership_asString, notify_frontend
from src.apps.copo_core.models import AssociatedProfileType, ViewLock, ProfileType
from src.apps.copo_core.views import web_page_access_checker
from common.utils.helpers import  get_group_membership_asString, get_current_user
import json
from bson.objectid import ObjectId
from django.conf import settings
from .utils.copo_email import Email
from common.utils.helpers import get_datetime
from jsonpickle import encode

# Create your views here.
lg = settings.LOGGER

@web_page_access_checker
@login_required
def copo_sample_accept_reject(request):
    sample_manager_groups = list()
    associated_profiles_type_approval_for = [assoicated_profile_type.name for assoicated_profile_type in AssociatedProfileType.objects.filter(is_approval_required=True, users=get_current_user())]
 
    user_groups = get_group_membership_asString()
    for group in user_groups:
        idx = group.rfind("_sample_managers")
        if idx > 0:
            sample_manager_groups.append(group[0:idx])
            
    return render(request, 'copo/copo_sample_accept_reject.html', {"sample_manager_groups":sample_manager_groups, "associated_profiles_type_approval_for": associated_profiles_type_approval_for})


@login_required
def get_samples_column_names(request):
    group_filter = request.GET.get("group", "")
    column_names = Sample().get_sample_display_column_names(group_filter=group_filter)
    return HttpResponse(json_util.dumps(column_names))

@web_page_access_checker
@login_required
def update_pending_samples_table(request):
    profile_filter = request.GET.get("profiles", "")
    group_filter = request.GET.get("group", "")
    search_filter = request.GET.get("search", "")
    sort_by = request.GET.get("order", "")
    direction = request.GET.get("dir", "")
    dir = 1
    if direction == "desc":
        dir = -1
    # samples = Sample().get_unregistered_dtol_samples()
    member_groups = get_group_membership_asString()
    # todo control for someone being both
    profiles = []
 
    if f"{group_filter}_sample_managers" in member_groups :
        profiles = Profile().get_profiles(filter=profile_filter, group_filter=group_filter, search_filter=search_filter, sort_by=sort_by, dir=dir)
 
    result = {"data": profiles}
    return HttpResponse(json_util.dumps(result))

@web_page_access_checker
@login_required
def get_dtol_samples_for_profile(request):
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

        current_user = get_current_user()
        profile = Profile().get_record(profile_id)
        is_associated_project_type_checker = False
        is_sequencing_centre_checker = False

        # Sometimes profile is None
        if isinstance(profile, dict):
            associated_profiles = profile.get("associated_type",[])
            #sequencing_centres = profile.get('sequencing_centre',[])
        
            #is_sequencing_centre_sample_manager = any(SequencingCentre.objects.filter( users=current_user, name__in = sequencing_centres))
            is_associated_project_type_checker = any(AssociatedProfileType.objects.filter(is_approval_required=True, users=current_user, name__in = associated_profiles))
            #is_sequencing_centre_checker = any(SequencingCentre.objects.filter(is_approval_required=True, users=current_user, name__in = sequencing_centres))
            
        if direction == "desc":
            dir = -1
            
        samples = {}
        if profile_id and profile_id != 'None':
           profile_type = Profile().get_type(profile_id).lower()
            
           if profile_type:
              type = profile_type
      
              if ProfileType.objects.get(type=profile_type).is_dtol_profile:   #if it is dtol_type
                 if not is_associated_project_type_checker:
                    samples["data"] = []
                 else:
                     samples = Sample().get_dtol_from_profile_id(
                        profile_id, filter, draw, start, length, sort_by, dir, search, type, is_associated_project_type_checker, is_sequencing_centre_checker)
 
        out = encode(samples, unpicklable=False)
        return HttpResponse(out, content_type='application/json')

        #return HttpResponse(json_util.dumps(samples))
    else:
        return HttpResponse(json_util.dumps({"locked": True}))

@web_page_access_checker
@login_required
def mark_sample_rejected(request):
    sample_ids = request.GET.get("sample_ids")
    sample_ids = json.loads(sample_ids)
    if sample_ids:
        Sample().mark_rejected(sample_ids=sample_ids)
        return HttpResponse(status=200)
    return HttpResponse(status=400, content="Sample IDs not provided")

@web_page_access_checker
@login_required
def add_sample_to_dtol_submission(request):
    sample_ids = request.POST.get("sample_ids")
    sample_ids = json.loads(sample_ids)
    profile_id = request.POST.get("profile_id")
    profile = Profile().get_record(profile_id)
    associated_profiles = profile.get("associated_type",[])
    sequencing_centres = profile.get("sequencing_centre", [])
    group = get_group_membership_asString()
    #is_bge_checker =  "bge_checkers" in group
    current_user = get_current_user()
    is_sample_manager = True #assume user is a sample manager as it has been checked in the decorator
    assoicated_profiles_type_require_approval = AssociatedProfileType.objects.filter(is_approval_required=True,  name__in =  associated_profiles)
    associated_profiles_type_approval_for = AssociatedProfileType.objects.filter(is_approval_required=True,  users=current_user,  name__in = associated_profiles)


    # check we have required params
    if sample_ids and profile:
        # check for submission object, and create if absent
        sub = Submission().get_dtol_submission_for_profile(profile_id)
        type_sub = profile["type"]
 
        if not sub:
            sub = Submission(profile_id).save_record(
                                dict(), **{"type": type_sub.lower()})     
        sub["dtol_status"] = "pending"
        sub["target_id"] = sub.pop("_id")

        sample_oids = [ObjectId(id) for id in sample_ids]
        samples = Sample().get_all_records_columns(filter_by=dict(_id={"$in": sample_oids}), projection=dict(status=1,approval=1))
        processing_sample_ids = []
        pending_sample_ids = []
 
        update_approval_for_samples = []
        update_approval = {}

        for sample in samples:
            sample_id = str(sample["_id"])
            notify_frontend(action="delete_row", html_id=str(sample_id), data={})

            now = get_datetime()
            match sample["status"]:
                case "pending":
                    all_approved = True
                    for associated_profile in assoicated_profiles_type_require_approval:
                        if associated_profile in associated_profiles_type_approval_for:
                            update_approval[f"approval.{associated_profile.name}"] = now
                            update_approval_for_samples.append(sample["_id"])
                        elif not sample.get("approval",{}).get(associated_profile.name,""):
                            all_approved = False
                    if all_approved:
                        processing_sample_ids.append(sample_id)
                case "rejected":
                    pending_sample_ids.append(sample_id)
                case _:
                    lg.error(f"Sample {sample_id} has an invalid status {sample['status']} for the current profile type {type_sub} and user {current_user}")
 
        # Processing samples
        if processing_sample_ids:
            Sample().mark_processing(sample_ids=processing_sample_ids) 
            for sample_id in processing_sample_ids:
                if not sample_id in sub["dtol_samples"]:
                    sub["dtol_samples"].append(sample_id)
            
        if update_approval_for_samples:
                Sample().update_field(field_values=update_approval, oids=update_approval_for_samples),

        # Pending samples
        if pending_sample_ids:
            Sample().mark_pending(sample_ids=pending_sample_ids)
            uri = request.build_absolute_uri('/')
            #send email to Sangar to notify them of new samples
            Email().notify_manifest_pending_for_associated_project_type_checker(data=uri + 'copo/dtol_submission/accept_reject_sample/', profile_id=profile_id,  title=profile["title"], description=profile["description"] )

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

@web_page_access_checker
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
