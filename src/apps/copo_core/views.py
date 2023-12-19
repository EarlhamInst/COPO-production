from common.schema_versions.lookup.dtol_lookups import TOL_PROFILE_TYPES, SAMPLE_MANAGERS_ACCESSIBLE_WEB_PAGES
from common.utils import helpers
from django.db.models import Q
import bson.json_util as json_util
from django.http import HttpResponse, HttpResponseBadRequest
from common.utils.copo_lookup_service import COPOLookup
import json
from allauth.socialaccount.models import SocialAccount
from bson import json_util as j
from bson import ObjectId
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from jsonpickle import encode
from src.apps.api.views.general import *
from common.dal.mongo_util import cursor_to_list
from .broker_da import BrokerDA, BrokerVisuals
from common.dal.copo_da import DataFile, CopoGroup, MetadataTemplate
from common.dal.profile_da import ProfileInfo, Profile
from common.dal.sample_da import Sample
from common.dal.submission_da import Submission
from common.dal.copo_base_da import   DAComponent

from src.apps.copo_barcoding_submission.utils.da import TaggedSequence
from src.apps.copo_assembly_submission.utils.da import Assembly
from src.apps.copo_seq_annotation_submission.utils.da import SequenceAnnotation
from common.s3.s3Connection import S3Connection as s3
from common.lookup.lookup import REPO_NAME_LOOKUP
from .models import banner_view
from common.schemas.utils import data_utils
from common.utils.helpers import get_env, get_group_membership_asString
LOGGER = settings.LOGGER

da_dict = dict(
    profile=Profile,
    submission=Submission,
    seqannotation=SequenceAnnotation,
    assembly=Assembly,
    files=s3,
    taggedseq=TaggedSequence,
    read=Sample          
)
"""
@login_required
def index(request):
    print(get_env("MEDIA_ROOT"))
    banner = banner_view.objects.all()
    if len(banner) > 0:
        context = {'user': request.user, "banner": banner[0]}
    else:
        context = {'user': request.user}
    groups = get_group_membership_asString()
    context['groups'] = groups
    return render(request, 'copo/index.html', context)


def login(request):
    context = {
        'login_form': LoginForm(),
    }
    return render(request, 'copo/auth/login.html', context)
"""


def web_page_access_checker(func):
    # Custom decorator to check if the current web page should be
    # viewed/accessed by the current web page viewer
    def verify_view_access(request, *args, **kwargs):
        member_groups = get_group_membership_asString()
        current_view = request.resolver_match.view_name

        profile_id = kwargs.get('profile_id', '')

        if not profile_id:
            # Check if current web page viewer is a sample manager and current web page is
            # 'accept/reject samples' web page, if 'yes', grant web page access if not, deny access
            if any(x.endswith('sample_managers') for x in member_groups) and 'accept_reject' in current_view:
                return func(request, *args, **kwargs)
            profile_id = request.session.get("profile_id", str())
            if not profile_id:
                    response = handler403(request)
                
        user_id = Profile().get_record(ObjectId(profile_id))['user_id']
        profile_type = Profile().get_type(profile_id).lower()

        shared_profiles = list(
            Profile().get_shared_for_user(id_only=True))

        # The ID output from get_user_id() i.e. current_user_id is
        # the same output from 'request.user.id'
        rightful_page_viewerID = User.objects.get(pk=user_id).id
        current_page_viewerID = request.user.id

        if not (current_page_viewerID == rightful_page_viewerID):
            if any(str(x['_id']) == profile_id for x in shared_profiles):
                # Grant web page access if the profile (ID) associated with the current web page
                # belongs to a profile that is shared with the current web page viewer
                response = func(request, *args, **kwargs)
            elif 'data_managers' in member_groups:
                # Grant web page access if the current web page viewer is a data manager
                # i.e. a COPO developer/team member
                # with permission to view the web page
                # NB: Useful for viewing web pages related to 'Stand-alone' profiles
                if profile_type:
                    request.session['profile_id'] = profile_id

                response = func(request, *args, **kwargs)
                
            elif any(x.endswith('sample_managers') for x in member_groups):
                # Check if current web page viewer is a sample manager
                # with permission to view the web page
                if any(x in current_view for x in SAMPLE_MANAGERS_ACCESSIBLE_WEB_PAGES):
                    if 'asg' in profile_type and 'dtol_sample_managers' in member_groups:
                        request.session['profile_id'] = profile_id
                        response = func(request, *args, **kwargs)
                    elif 'dtol_env' in profile_type and 'dtolenv_sample_managers' in member_groups:
                        request.session['profile_id'] = profile_id
                        response = func(request, *args, **kwargs)
                    elif 'dtol' in profile_type and 'dtol_sample_managers' in member_groups:
                        request.session['profile_id'] = profile_id
                        response = func(request, *args, **kwargs)
                    elif 'erga' in profile_type and 'erga_sample_managers' in member_groups:
                        request.session['profile_id'] = profile_id
                        response = func(request, *args, **kwargs)
                else:
                    response = handler403(request)
            else:
                # Deny web page access if the profile (ID) associated with the current web page
                # is not owned/created by the current web page viewer
                response = handler403(request)
            
            # Deny access to the Files' web page because
            # it depends on the ECS bucket name of the loggged-in user               
#            if 'copo_files' in current_view:              
#                response = handler403(request)
        else:
            # Grant web page access if the profile (ID) associated with the current web page
            # is owned/created by the current web page viewer
            response = func(request, *args, **kwargs)
            
        return response
    return verify_view_access


def test(request):
    return render(request, template_name='copo/test.html')


def error_page(request):
    return render(request, context={}, template_name="copo/error_page.html")


def test(request):
    return render(request, "copo/test.html")


def forward_to_info(request):
    message = request.GET['message']
    control = request.GET['control']
    return render(request, 'copo/info_page.html', {'message': message, 'control': control})


@login_required
def get_profile_counts(request):
    profile_id = request.session["profile_id"]
    counts = ProfileInfo(profile_id).get_counts()
    return HttpResponse(encode(counts))


@login_required
def view_templates(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)

    return render(request, 'copo/metadata_templates.html', {'profile_id': profile_id, 'profile': profile})


@login_required
def author_template(request, template_id):
    record = MetadataTemplate().get_by_id(template_id)
    context = {
        "template_name": record["template_name"], "template_id": template_id}
    return render(request, "copo/author_metadata_template.html", context)


@login_required
def copo_repositories(request):
    user = request.user.id
    return render(request, 'copo/my_repositories.html')


"""
@login_required
def copo_samples(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    groups = get_group_membership_asString()
    return render(request, 'copo/copo_sample.html', {'profile_id': profile_id, 'profile': profile, 'groups': groups})
"""


def copo_docs(request):
    context = dict()
    return render(request, 'copo/copo_docs.html', {'context': context})


def resolve_submission_id(request, submission_id):
    sub = Submission().get_record(submission_id)
    # get all file metadata
    output = dict()
    files = list()
    for f in sub.get("bundle", list()):
        file = DataFile().get_record(f)
        files.append(file["description"]["attributes"])
    output["files"] = files
    output["accessions"] = sub["accessions"]
    output["metadata"] = {}
    output["metadata"]["dc"] = sub["meta"]["fields"]
    return HttpResponse(j.dumps(output))


@login_required
def copo_visualize(request):
    context = dict()

    task = request.POST.get("task", str())

    profile_id = request.session.get("profile_id", str())

    context["quick_tour_flag"] = request.session.get("quick_tour_flag", True)
    # for displaying tour message across site
    request.session["quick_tour_flag"] = context["quick_tour_flag"]
    component = request.POST.get("component", str())
    da_object = DAComponent(profile_id=profile_id, component=request.POST.get("component", str()))
    if component in da_dict:
        da_object = da_dict[component](profile_id=profile_id)
        
    target_id=request.POST.get("target_id", None)    
    if component == "read" and target_id:
        target_id = target_id.split("_")[0]

    broker_visuals = BrokerVisuals(context=context,
                                   profile_id=profile_id,
                                   request=request,
                                   user_id=request.user.id,
                                   component=request.POST.get(
                                       "component", str()),
                                   target_id=target_id,
                                   quick_tour_flag=request.POST.get(
                                       "quick_tour_flag", False),
                                   datafile_ids=json.loads(
                                       request.POST.get("datafile_ids", "[]")),
                                   request_dict=request.POST.dict(),
                                   da_object=da_object
                                   )

    task_dict = dict(table_data=broker_visuals.do_table_data,
                     server_side_table_data=broker_visuals.do_server_side_table_data,
                     profiles_counts=broker_visuals.do_profiles_counts,
                     # wizard_messages=broker_visuals.do_wizard_messages,  deprecated
                     # metadata_ratings=broker_visuals.do_metadata_ratings,
                     # description_summary=broker_visuals.do_description_summary,
                     # un_describe=broker_visuals.do_un_describe,
                     attributes_display=broker_visuals.do_attributes_display,
                     help_messages=broker_visuals.get_component_help_messages,
                     update_quick_tour_flag=broker_visuals.do_update_quick_tour_flag,
                     get_component_info=broker_visuals.do_get_component_info,
                     get_profile_info=broker_visuals.do_get_profile_info,
                     get_submission_accessions=broker_visuals.do_get_submission_accessions,
                     get_submission_datafiles=broker_visuals.do_get_submission_datafiles,
                     # get_destination_repo=broker_visuals.do_get_destination_repo,
                     # get_repo_stats=broker_visuals.do_get_repo_stats,
                     # managed_repositories=broker_visuals.do_managed_repositories,
                     # get_submission_meta_repo=broker_visuals.do_get_submission_meta_repo,
                     view_submission_remote=broker_visuals.do_view_submission_remote,
                     )

    if task in task_dict:
        context = task_dict[task]()

    out = jsonpickle.encode(context, unpicklable=False)
    return HttpResponse(out, content_type='application/json')

@web_page_access_checker
@login_required
def copo_forms(request):
    context = dict()
    task = request.POST.get("task", str())

    profile_id = request.session.get("profile_id", str())

    if request.POST.get("profile_id", str()):
        profile_id = request.POST.get("profile_id")
        request.session["profile_id"] = profile_id

    component = request.POST.get("component", str())
    da_object = DAComponent(profile_id=profile_id, component=request.POST.get("component", str()))
    if component in da_dict:
        da_object = da_dict[component](profile_id=profile_id)

    broker_da = BrokerDA(context=context,
                         profile_id=profile_id,
                         component=request.POST.get("component", str()),
                         referenced_field=request.POST.get(
                             "referenced_field", str()),
                         referenced_type=request.POST.get(
                             "referenced_type", str()),
                         target_id=request.POST.get("target_id", str()),
                         target_ids=json.loads(
                             request.POST.get("target_ids", "[]")),
                         datafile_ids=json.loads(
                             request.POST.get("datafile_ids", "[]")),
                         auto_fields=request.POST.get("auto_fields", dict()),
                         visualize=request.POST.get("visualize", str()),
                         id_handle=request.POST.get("id_handle", str()),
                         user_id=request.user.id,
                         action_type=request.POST.get("action_type", str()),
                         id_type=request.POST.get("id_type", str()),
                         data_source=request.POST.get("data_source", str()),
                         user_email=request.POST.get("user_email", str()),
                         bundle_name=request.POST.get("bundle_name", str()),
                         request_dict=request.POST.dict(),
                         da_object=da_object
                         )

    task_dict = dict(resources=broker_da.do_form_control_schemas,
                     save=broker_da.do_save_edit,
                     edit=broker_da.do_save_edit,
                     delete=broker_da.do_delete,
                     validate_and_delete=broker_da.validate_and_delete,
                     form=broker_da.do_form,
                     # form_and_component_records=broker_da.do_form_and_component_records,
                     # doi=broker_da.do_doi,
                     # initiate_submission=broker_da.do_initiate_submission,
                     user_email=broker_da.do_user_email,
                     component_record=broker_da.do_component_record,
                     component_form_record=broker_da.component_form_record,
                     # sanitise_submissions=broker_da.do_sanitise_submissions,
                     # create_rename_description_bundle=broker_da.create_rename_description_bundle,
                     # clone_description_bundle=broker_da.do_clone_description_bundle,
                     lift_submission_embargo=broker_da.do_lift_submission_embargo,
                     submit_assembly=broker_da.do_submit_assembly,
                     submit_annotation=broker_da.do_submit_annotation,
                     submit_read=broker_da.do_submit_read,
                     delete_read=broker_da.do_delete_read,
                     submit_tagged_seq=broker_da.do_submit_tagged_seq,
                     )

    if task in task_dict:
        context = task_dict[task]()

    out = jsonpickle.encode(context, unpicklable=False)
    status = context.get("action_feedback", dict()).get("status", "success")
    if status == "success":
        return HttpResponse(status=200, content=out, content_type='application/json')

    return HttpResponse(out, content_type='application/json')


"""
@login_required()
def delete_profile(request):
    context = dict()
    task = request.POST.get("task", str())

    x = 0
    profile_ids = []
    while request.POST.get("target_id[" + str(x) + "][record_id]", ""):
        profile_ids.append(request.POST.get("target_id[" + str(x) + "][record_id]", ""))
        x += 1

    response = HttpResponse(content_type="application/json")
    response.status_code = 200
    profiles_undeleted = []
    if not profile_ids:
        response.status_code = 405
    else:
        for profile in profile_ids:
            if not Profile().validate_and_delete(profile):
                profiles_undeleted.append(profile)
                response.status_code = 405
    undeleted_json = json.dumps({"undeleted": profiles_undeleted})
    response.write(undeleted_json)
    return response
"""


@login_required
@staff_member_required
def copo_admin(request):
    context = dict()
    task = request.POST.get("task", str())

    out = jsonpickle.encode(context)
    return HttpResponse(out, content_type='application/json')


@login_required
def copo_submissions(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)

    return render(request, 'copo/copo_submission.html', {'profile_id': profile_id, 'profile': profile})


@login_required
def copo_get_submission_table_data(request):
    profile_id = request.POST.get('profile_id')
    submission = Submission(profile_id=profile_id).get_all_records(
        sort_by="date_created", sort_direction="-1")
    for s in submission:
        s['date_created'] = s['date_created'].strftime('%d %b %Y - %I:%M %p')
        s['date_modified'] = s['date_modified'].strftime('%d %b %Y - %I:%M %p')
        s['display_name'] = REPO_NAME_LOOKUP[s['repository']]
        if s['complete'] == 'false' or s['complete'] == False:
            s['status'] = 'Pending'
        else:
            s['status'] = 'Submitted'

    out = j.dumps(submission)
    return HttpResponse(out)


@login_required
def goto_error(request, message="Something went wrong, but we're not sure what!"):
    try:
        LOGGER.log(message)
    finally:
        context = {'message': message}
        return render(request, 'copo/error_page.html', context)


"""
def copo_logout(request):
    logout(request)
    return render(request, 'copo/auth/logout.html', {})

def copo_register(request):
    if request.method == 'GET':
        return render(request, 'copo/register.html')
    else:
        # create user and return to auth page
        firstname = request.POST['frm_register_firstname']
        lastname = request.POST['frm_register_lastname']
        email = request.POST['frm_register_email']
        username = request.POST['frm_register_username']
        password = request.POST['frm_register_password']

        user = User.objects.create_user(username, email, password)
        user.set_password(password)
        user.last_name = lastname
        user.first_name = firstname
        user.save()

        return render(request, 'copo/templates/account/auth.html')
"""


@login_required
def view_user_info(request):
    user = helpers.get_current_user()
    # op = Orcid().get_orcid_profile(user)
    d = SocialAccount.objects.get(user_id=user.id)
    op = json.loads(json.dumps(d.extra_data).replace("-", "_"))
    repos = []
    # repo_ids = user.userdetails.repo_submitter
    # repos = Repository().get_by_ids(repo_ids)
    # data_dict = jsonpickle.encode(data_dict)
    data_dict = {'orcid': op, "repos": repos}

    return render(request, 'copo/user_info.html', data_dict)


@login_required()
def view_groups(request):
    # g = Group().create_group(description="test descrition")
    profile_list = cursor_to_list(Profile().get_for_user())
    group_list = cursor_to_list(CopoGroup().get_by_owner(request.user.id))
    # Get a list of uppercase manifest types
    manifest_types_lst = list(map(str.upper, TOL_PROFILE_TYPES))

    # Separate uppercase profile types by commas
    manifest_types_str = ' or '.join([', '.join(
        manifest_types_lst[:-1]), manifest_types_lst[-1]] if len(manifest_types_lst) > 2 else manifest_types_lst)

    return render(request, 'copo/copo_group.html',
                  {'request': request, 'profile_list': profile_list, 'group_list': group_list, 'manifest_types': manifest_types_str})


"""
# @login_required()
@user_is_staff
def administer_repos(request):
    return render(request, 'copo/copo_repository.html', {'request': request})


@user_is_staff
def copo_repositories_admin(request):
    return render(request, 'copo/copo_repository_admin.html', {'request': request})


def manage_repos(request):
    return render(request, 'copo/copo_repo_management.html', {'request': request})


def manage_repositories(request):
    return render(request, 'copo/copo_repository_manage.html', {'request': request})
"""


def handler404(request, exception):
    return error_page(request)


def handler500(request):
    return error_page(request)


def handler403(request, message="Apologies, you do not have permission to view this web page"):
    try:
        LOGGER.log(message)
    finally:
        context = {'message': message}
        return render(request, 'copo/unauthorised_page.html', context)


def get_source_count(self):
    profile_id = data_utils.get_current_request().session['profile_id']
    num_sources = ProfileInfo(profile_id).source_count()
    return HttpResponse(encode({'num_sources': num_sources}))


def search_copo_components(request, data_source):
    """
    function does local lookup of items given data_source
    :param request:
    :param data_source:
    :return:
    """

    search_term = request.GET.get("q", str())
    accession = request.GET.get("accession", str())
    profile_id = request.GET.get("profile_id", str())
    referenced_field = request.GET.get("referenced_field", str())

    if request.method == 'POST':
        search_term = request.POST.get("q", str())
        accession = request.POST.get("accession", str())
        profile_id = request.POST.get("profile_id", str())
        referenced_field = request.POST.get("referenced_field", str())

    data = COPOLookup(
        search_term=search_term,
        accession=accession,
        data_source=data_source,
        profile_id=profile_id,
        referenced_field=referenced_field
    ).broker_component_search()

    return HttpResponse(jsonpickle.encode(data), content_type='application/json')


def create_group(request):
    name = request.POST['group_name']
    description = request.POST['description']

    if not name or not description:
        return HttpResponseBadRequest(
            'Error Creating Group - Form field(s) cannot be empty!')

    uid = CopoGroup().create_shared_group(name=name, description=description)

    if uid:
        return HttpResponse(json.dumps({'id': str(uid), 'name': name}))
    else:
        return HttpResponseBadRequest('Forbidden - Group with the same name already exists!')


def edit_group(request):
    group_id = request.POST['group_id']
    name = request.POST['group_name']
    description = request.POST['description']

    if not name or not description:
        return HttpResponseBadRequest(
            'Error Updating Group - Form field(s) cannot be empty!')

    if name and description:
        document = CopoGroup().edit_group(
            group_id=group_id, name=name, description=description)

        if document:
            return HttpResponse(json.dumps({'id': str(group_id), 'name': name}))
        else:
            return HttpResponseBadRequest('Forbidden - Group with the same name already exists!')


def delete_group(request):
    id = request.GET.get('group_id','')
    deleted = CopoGroup().delete_group(group_id=id)
    if deleted:
        return HttpResponse(json.dumps({'deleted': True}))
    else:
        return HttpResponseBadRequest('Error Deleting Group - Try Again')


def view_group(request):
    id = request.GET.get('group_id','')
    group_info = CopoGroup().view_shared_group(group_id=id)
    if group_info:
        return HttpResponse(json_util.dumps({'resp': group_info}))
    else:
        return HttpResponseBadRequest('Error Viewing Group - Try Again')


def add_profile_to_group(request):
    group_id = request.GET.get('group_id','')
    profile_id = request.GET.get('profile_id','')
    resp = CopoGroup().add_profile(group_id=group_id, profile_id=profile_id)
    if resp:
        return HttpResponse(json.dumps({'resp': 'Added to Group'}))
    else:
        return HttpResponseBadRequest(json.dumps({'resp': 'Server Error - Try again'}))


def remove_profile_from_group(request):
    group_id = request.GET.get('group_id','')
    profile_id = request.GET.get('profile_id','')
    resp = CopoGroup().remove_profile(group_id=group_id, profile_id=profile_id)
    if resp:
        return HttpResponse(json.dumps({'resp': 'Removed from Group'}))
    else:
        return HttpResponseBadRequest(json.dumps({'resp': 'Server Error - Try again'}))


def get_profiles_in_group(request):
    group_id = request.GET.get('group_id','')
    grp_info = CopoGroup().get_profiles_for_group_info(group_id=group_id)
    return HttpResponse(json_util.dumps({'resp': grp_info}))


def get_users_in_group(request):
    group_id = request.GET.get('group_id','')
    usr_info = CopoGroup().get_users_for_group_info(group_id=group_id)
    return HttpResponse(json_util.dumps({'resp': usr_info}))


def get_users(request):
    q = request.GET['q']
    x = list(User.objects.filter(
        Q(first_name__istartswith=q) | Q(last_name__istartswith=q) | Q(username__istartswith=q))
        .values_list('id', 'first_name', 'last_name', 'email', 'username'))
    if not x:
        return HttpResponse()
    return HttpResponse(json.dumps(x))


def add_user_to_group(request):
    group_id = request.GET.get('group_id','')
    user_id = request.GET.get('user_id','')
    grp_info = CopoGroup().add_user_to_group(group_id=group_id, user_id=user_id)
    return HttpResponse(json_util.dumps({'resp': grp_info}))


def remove_user_from_group(request):
    group_id = request.GET.get('group_id','')
    user_id = request.GET.get('user_id','')
    grp_info = CopoGroup().remove_user_from_group(
        group_id=group_id, user_id=user_id)
    return HttpResponse(json_util.dumps({'resp': grp_info}))
