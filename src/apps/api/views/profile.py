import bson.json_util as jsonb
from django.http import HttpResponse
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

import common.schemas.utils.data_utils as d_utils
from common.dal.mongo_util import cursor_to_list
from common.dal.profile_da import Profile
from common.dal.sample_da import Sample
from ..utils import (
    generate_csv_response,
    generate_wrapper_response,
    validate_associated_project,
    validate_date_from_api,
    validate_project,
    validate_return_type,
)
from .sample import format_date
from jsonpickle import encode
from src.apps.copo_core.broker_da import BrokerDA
from bson.json_util import dumps
from common.dal.profile_da import Profile

class APIProfile(APIView):

    def post(self, request):
        if request.method == 'POST':
            request_data = request.data
            auto_fields = { f"copo.profile.{k}" : v for k, v in request_data.items()}

            broker_da = BrokerDA(component="profile", da_object=Profile(), task="save", auto_fields=auto_fields)

            context = broker_da.do_save_edit()
            out = encode(context, unpicklable=False)
            status = context.get("action_feedback", dict()).get("status", "success")
            if status == "success" or status=="warning":
                profiles = Profile().get_all_records_columns(
                    filter_by={'title': request_data['title'], 'user_id': request.user.id}, projection={'_id':1}
                )
                if profiles:
                    return HttpResponse(status=201, content=dumps({"id": str(profiles[0].get("_id"))}), content_type='application/json')

            return HttpResponse(status="400", content=out, content_type='application/json')
        
    def get(self, request):
        if request.method == 'GET':
            uid = request.user.id
            existing_profiles = Profile().get_all_profiles(user=uid)
            
            #existing_profiles = Profile().get_collection_handle().find({'user_id': uid})
            out = list()
            for el in existing_profiles:
                out.append(
                    {'title': el['title'], "description":el['description'],'type': el['type'],  'id': str(el['_id'])}
                )
            return Response(out)
    

def associate_profiles_with_tubes_or_well_ids(request):
    filter_dict = {}
    projection = {
        '_id': 1,
        'title': 1,
        'type': 1,
        'associated_type': 1,
        'first_manifest_date_created': 1,
        'last_manifest_date_modified': 1,
    }

    return_type = request.GET.get('return_type', 'json').lower()
    profile_type = request.GET.get('project', '').lower()
    associated_profile_type = request.GET.get('associated_tol_project', '')

    # Validate required profile_type
    project_issues = validate_project(profile_type)
    if project_issues:
        return project_issues

    filter_dict['type'] = profile_type

    # Validate optional associated_profile_type
    associated_project_issues = validate_associated_project(
        associated_profile_type, optional=True
    )
    if associated_project_issues:
        return associated_project_issues

    if associated_profile_type:
        filter_dict['associated_type'] = {'$in': [associated_profile_type]}

    # Validate optional date fields
    d_from = request.GET.get('d_from', None)
    d_to = request.GET.get('d_to', None)

    result = validate_date_from_api(d_from, d_to, optional=True)
    if isinstance(result, HttpResponse):
        return result

    # Unpack parsed date values from the result
    d_from_parsed, d_to_parsed = result

    # Add date filters
    if d_from_parsed is not None:
        filter_dict['first_manifest_date_created'] = {'$gte': d_from_parsed}

    if d_to_parsed is not None:
        filter_dict['last_manifest_date_modified'] = {'$lt': d_to_parsed}

    profiles = cursor_to_list(
        Profile().get_collection_handle().find(filter_dict, projection)
    )

    profile_count = len(profiles)

    # Create a dictionary to store the final structure
    profile_well_associations = []

    for profile in profiles:
        # Fetch samples associated with the profile
        samples = Sample().get_by_field('profile_id', [str(profile['_id'])])

        if not samples:
            # If no samples are found, continue to the next profile
            continue

        # Gather the TUBE_OR_WELL_IDs from the associated samples
        tube_or_well_ids = [
            sample['TUBE_OR_WELL_ID']
            for sample in samples
            if 'TUBE_OR_WELL_ID' in sample
        ]

        # Add the profile and its associated well IDs to the result structure
        profile_well_associations.append(
            {
                'TUBE_OR_WELL_IDs': tube_or_well_ids,
                'associated_profile_type': profile['associated_type'],
                'first_manifest_date_created': format_date(
                    profile['first_manifest_date_created']
                ),
                'last_manifest_date_modified': format_date(
                    profile['last_manifest_date_modified']
                ),
                'profile_title': profile['title'],
                'profile_type': profile['type'],
            }
        )

    # Validate optional return_type
    return_type_issues = validate_return_type(return_type)
    if return_type_issues:
        return return_type_issues

    if return_type == 'csv':
        template = []
        for profile in profile_well_associations:
            well_ids = ' '.join(profile['TUBE_OR_WELL_IDs'])
            template.append(
                {
                    'copo_profile_title': profile['profile_title'],
                    'TUBE_OR_WELL_IDs': well_ids,
                }
            )
        return generate_csv_response(standard='tol', template=template)
    else:
        output = generate_wrapper_response(
            num_found=profile_count, template=profile_well_associations
        )
        output = jsonb.dumps([output])

        return HttpResponse(output, content_type='application/json')
