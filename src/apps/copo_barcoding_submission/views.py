from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from common.dal.copo_da import EnaChecklist
from common.dal.profile_da import Profile
from src.apps.copo_core.views import web_page_access_checker


@web_page_access_checker
@login_required
def copo_taggedseq(request, profile_id, ui_component):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    checklists = EnaChecklist().get_barcoding_checklists_no_fields()

    return render(request, 'copo/copo_tagged_seq.html', {'profile_id': profile_id, 'profile': profile,  'checklists': checklists, "ui_component": ui_component})


@login_required()
def ena_taggedseq_manifest_validate(request, profile_id):
    request.session["profile_id"] = profile_id
    checklist_id = request.GET.get("checklist_id")
    data = {"profile_id": profile_id}
    if checklist_id:
        checklist = EnaChecklist().execute_query({"primary_id": checklist_id})
        if checklist:
            data["checklist_id"] = checklist_id
            data["checklist_name"] = checklist[0]["name"]

    return render(request, "copo/ena_taggedseq_manifest_validate.html", data)
