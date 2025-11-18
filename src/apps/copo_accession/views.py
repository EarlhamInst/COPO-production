from bson import json_util
from common.dal.profile_da import Profile
from common.dal.sample_da import Sample
from common.dal.submission_da import Submission
from common.schema_versions.lookup.dtol_lookups import EXCLUDED_SAMPLE_TYPES, NON_SAMPLE_ACCESSION_TYPES, GENOMICS_PROJECT_SAMPLE_TYPE_DICT, TOL_PROFILE_TYPES
from common.utils.helpers import get_group_membership_asString
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import render
from src.apps.copo_core.models import ViewLock, ProfileType
from src.apps.copo_core.views import web_page_access_checker
from .utils.helpers import set_column_class_name, generate_column_title
import common.schemas.utils.data_utils as d_utils
import json


@web_page_access_checker
@login_required
def copo_accessions(request, profile_id, ui_component):
    request.session['profile_id'] = profile_id
    profile = Profile().get_record(profile_id)
    groups = get_group_membership_asString()

    return render(request, 'copo/accessions/copo_accessions.html',
                  {'profile_id': profile_id, 'profile': profile, 'groups': groups, 'showAllCOPOAccessions': False, "ui_component": ui_component})

def copo_accessions_dashboard(request):
    # Determine if users are in the appropriate membership group to view the web page
    groups = get_group_membership_asString()

    return render(request, 'copo/accessions/copo_accessions.html',
                  {'profile_id': '999', 'groups': groups, 'showAllCOPOAccessions': True})

def get_accession_records_column_names(request):
    isOtherAccessionsTabActive = d_utils.convertStringToBoolean(request.GET.get('isOtherAccessionsTabActive', str()))
    
    columns = list()
    columns.append(dict(data='DT_RowId', visible=False))
    columns.append(dict(data='accession_type', title='Submission Type', visible=True))

    if isOtherAccessionsTabActive:
        columns.append(dict(data='record_id', visible=False))
        columns.append(dict(data='profile_id', visible=False))

        # Set column names
        labels = ['accession', 'alias', 'profile_title']
        
        for x in labels:
            class_name = set_column_class_name(x)
            title=d_utils.convertStringToTitleCase(x)
            columns.append(dict(data=x, title=title, visible=True,  className=class_name))
    else:
        # Set column names
        labels = ['record_id', 'biosampleAccession', 'sraAccession', 'submissionAccession', 'manifest_id', 'SCIENTIFIC_NAME', 'SPECIMEN_ID', 'TAXON_ID']
        copo_api_labels = ['record_id', 'manifest_id']

        for x in labels:
            class_name = set_column_class_name(x, copo_api_labels)
            title = generate_column_title(x, copo_api_labels)
            columns.append(dict(data=x, title=title, className=class_name))

    return HttpResponse(json_util.dumps(columns))

def generate_accession_records(request):
    isUserProfileActive = d_utils.convertStringToBoolean(request.POST.get('isUserProfileActive',str()))
    isOtherAccessionsTabActive = d_utils.convertStringToBoolean(request.POST.get('isOtherAccessionsTabActive', str()))
    showAllCOPOAccessions = d_utils.convertStringToBoolean(request.POST.get('showAllCOPOAccessions', str()))
    filter_accessions = json.loads(request.POST.get('filter_accessions', '[]')) # 'json.loads()' expects a string
    # Convert items in the 'filter_accessions' list to lowercase
    filter_accessions = [x.lower() for x in filter_accessions] if filter_accessions else list()

    # Replace 'genomics' with 'isasample' in the 'filter_accessions' list
    filter_accessions = ['isasample' if 'genomics' in filter_accessions else accession for accession in filter_accessions]

    profile_id = request.session.get('profile_id', str()) if isUserProfileActive else str()
    start = request.POST.get('start', '0')
    length = request.POST.get('length', '10')
    draw = request.POST.get('draw', '1')
    sort_by = request.POST.get('order[0][column]', '')
    direction = request.POST.get('order[0][dir]', '')
    search = request.POST.get('search', '')
    dir = -1

    if direction == 'asc':
        dir = 1

    # Create a dictionary to store the elements
    element_dict = dict()
    element_dict['draw'] = draw
    element_dict['start'] = start
    element_dict['length'] = length
    element_dict['sort_by'] = sort_by
    element_dict['dir'] = dir
    element_dict['search'] = search
    element_dict['showAllCOPOAccessions'] = showAllCOPOAccessions
    element_dict['isUserProfileActive'] = isUserProfileActive
    element_dict['profile_id'] = profile_id
    element_dict['filter_accessions'] = filter_accessions

    records = None

    if isOtherAccessionsTabActive:
        records = Submission().get_non_sample_accessions(element_dict)          
    else:
        records = Sample().get_sample_accessions(element_dict)

    return HttpResponse(json_util.dumps(records))

def get_accession_types(element_dict):
    filter = dict()
    showAllCOPOAccessions = element_dict['showAllCOPOAccessions']
    isUserProfileActive = element_dict['isUserProfileActive']
    profile_id = element_dict['profile_id']
    isOtherAccessionsTabActive = element_dict['isOtherAccessionsTabActive']

    if not showAllCOPOAccessions:
        if isUserProfileActive and profile_id:
            filter['profile_id'] = profile_id

    all_sample_types =  Sample().get_distinct_sample_types(filter)

    # Remove excluded sample types
    all_sample_types = [sample_type for sample_type in all_sample_types if sample_type not in EXCLUDED_SAMPLE_TYPES]

    project_types = list()
    
    for x in all_sample_types:
        if x in GENOMICS_PROJECT_SAMPLE_TYPE_DICT:
            project_types.append(x)

        if x in TOL_PROFILE_TYPES:
            project_types.append(x)

    # Remove duplicates from the list
    project_types = list(set(project_types))

    accession_types = NON_SAMPLE_ACCESSION_TYPES if isOtherAccessionsTabActive else project_types

    # Check if any item in the 'GENOMICS_PROJECT_SAMPLE_TYPE_DICT' dictionary is in the 'accession_types' list
    # If yes, replace the item with the value in the 'GENOMICS_PROJECT_SAMPLE_TYPE_DICT' dictionary
    if any(item in GENOMICS_PROJECT_SAMPLE_TYPE_DICT for item in accession_types):
        accession_types = [GENOMICS_PROJECT_SAMPLE_TYPE_DICT.get(item, item) for item in accession_types]

    # Sort the list
    accession_types.sort()

    return accession_types


def get_filter_accession_titles(request):
    # Get the text and value for the filter accession checkboxes
    accession_titles = list()
    showAllCOPOAccessions = d_utils.convertStringToBoolean(
        request.POST.get('showAllCOPOAccessions', str()))
    isUserProfileActive = d_utils.convertStringToBoolean(
        request.POST.get('isUserProfileActive', str()))
    isOtherAccessionsTabActive = d_utils.convertStringToBoolean(
        request.POST.get('isOtherAccessionsTabActive', str()))
    
    profile_id = request.session.get('profile_id', str()) if isUserProfileActive else str()

    # Create a dictionary to store the elements
    element_dict = dict()
    element_dict['showAllCOPOAccessions'] = showAllCOPOAccessions
    element_dict['isUserProfileActive'] = isUserProfileActive
    element_dict['profile_id'] = profile_id
    element_dict['isOtherAccessionsTabActive'] = isOtherAccessionsTabActive

    accession_types = get_accession_types(element_dict)

    if isOtherAccessionsTabActive:
        # Reorder the list of accessions types accorsing to the order of 
        # the accession types in NON_SAMPLE_ACCESSION_TYPES list
        accession_titles = [{'title': d_utils.convertStringToTitleCase(
            item), 'value': item} for item in NON_SAMPLE_ACCESSION_TYPES if item in accession_types]
    else:
        # Sample project types including Genomics
        # since samples might exist in Genomics projects
        profile_types = list()

        for i in accession_types:
            profile_type_dict = Profile().get_collection_handle().find_one({'type': {'$regex': i.lower(), '$options': 'i'}},
                                                                      {'_id': 0, 'type': 1})
            if profile_type_dict:
                # Get the description i.e the name of the profile based on the profile type
                profile_type_obj = ProfileType.objects.get(type=profile_type_dict.get('type', str()))
                profile_types.append(
                    {'title':  profile_type_obj.description, 'value': i.upper()})

        accession_titles = profile_types

    return HttpResponse(json_util.dumps(accession_titles))

def are_accession_records_available(request):
    # Check if accession records are available in the SubmissionCollection of the database
    showAllCOPOAccessions = d_utils.convertStringToBoolean(request.GET.get('showAllCOPOAccessions', str()))
    isUserProfileActive = d_utils.convertStringToBoolean(request.GET.get('isUserProfileActive',str()))
    profile_id = request.session.get('profile_id', str()) if isUserProfileActive else str()

    # Create a dictionary to store the elements
    element_dict = dict()
    element_dict['showAllCOPOAccessions'] = showAllCOPOAccessions
    element_dict['isUserProfileActive'] = isUserProfileActive
    element_dict['profile_id'] = profile_id

    records_count = Submission().get_accession_records_count(element_dict)
    return HttpResponse(json_util.dumps(records_count))
