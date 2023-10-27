from django.shortcuts import render
from common.dal.copo_da import Profile
from django.contrib.auth.decorators import login_required
from common.utils.helpers import get_group_membership_asString
from src.apps.copo_core.views import web_page_access_checker

# Create your views here.


@web_page_access_checker
@login_required
def copo_samples(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    groups = get_group_membership_asString()
    return render(request, 'copo/copo_sample.html', {'profile_id': profile_id, 'profile': profile, 'groups': groups})
