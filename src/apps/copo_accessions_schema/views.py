from common.dal.profile_da import Profile
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from src.apps.copo_core.views import web_page_access_checker
from src.apps.copo_single_cell_submission.utils.da import Singlecell
from common.utils.helpers import get_not_deleted_flag

@web_page_access_checker
@login_required
def copo_accessions_schema(request, profile_id, ui_component):
    request.session['profile_id'] = profile_id
    profile = Profile().get_record(profile_id)

    studies = Singlecell(profile_id=profile_id).get_all_records_columns(filter_by={"profile_id": profile_id, "deleted": get_not_deleted_flag()}, projection={"study_id": 1, "checklist_id":1, "schema_name":1 , "_id":0, "components.study": 1})
    for study in studies:
        study["title"] = study["components"]["study"][0].get("title", "")
    return render(request, 'copo/copo_accessions_schema.html',
                {'profile_id': profile_id, 'profile': profile,  "ui_component": ui_component, "studies": studies})

    