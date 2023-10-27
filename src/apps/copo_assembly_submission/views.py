from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from common.dal.copo_da import Submission, Assembly, Profile
from common.utils.helpers import notify_assembly_status
from django.http import HttpResponse, JsonResponse
from .forms import AssemblyForm
from .utils import EnaAssembly
from common.s3.s3Connection import S3Connection
from src.apps.copo_core.views import web_page_access_checker


@login_required()
def ena_assembly(request, profile_id, assembly_id=None):
    is_error = False
    request.session["profile_id"] = profile_id
    assembly = None

    if assembly_id:
        assembly = Assembly().get_record(assembly_id)
        if not assembly:
            return HttpResponse(content="Assembly not exists", status=400)

    study_accession = ""
    sample_accession = []

    existing_sub = Submission().get_records_by_field("profile_id", profile_id)
    existing_accessions = ""
    if existing_sub:
        existing_accessions = existing_sub[0].get("accessions", "")
    if existing_accessions:
        study = existing_accessions.get("project", "")
        if study:
            if isinstance(study, dict):
                study_accession = study.get("accession", "")
            elif isinstance(study, list):
                study_accession = study[0].get("accession", "")
        else:
            study_accession = ""
        samples = existing_accessions.get("sample", "")
        if samples:
            for sample in samples:
                if sample.get("sample_accession", ""):
                    sample_accession.append(sample.get("sample_accession", ""))

    ecs_files = []
    s3obj = S3Connection()
    bucket_name = str(request.user.id) + "_" + request.user.username
    if s3obj.check_for_s3_bucket(bucket_name):
        files = s3obj.list_objects(bucket_name)
        if files:
            for file in files:
                ecs_files.append(file["Key"])

    if request.method == 'POST' or request.method == 'PUT':
        form = AssemblyForm(request.POST, request.FILES,
                            sample_accession=sample_accession, assembly=assembly, ecs_files=ecs_files)
        if form.is_valid():
            notify_assembly_status(data={"profile_id": profile_id},
                                   msg="Intitialising Assembly Submission",
                                   action="info",
                                   html_id="assembly_info")
            # this is a dict
            formdata = form.cleaned_data
            # uploading files to folder in COPO
            notify_assembly_status(data={"profile_id": profile_id}, msg="", action="show",
                                   html_id="loading_span")
            # EnaAssembly.upload_assembly_files(files)
            assembly_id = request.POST.get("assembly_id", "")

            sub_result = EnaAssembly.validate_assembly(
                formdata, profile_id, assembly_id)
            if sub_result.get("error", ""):
                notify_assembly_status(data={"profile_id": profile_id},
                                       msg=sub_result.get("error", ""),
                                       action="error",
                                       html_id="assembly_info")
                is_error = True
                # messages.error(request,sub_result)
            else:
                notify_assembly_status(data={"profile_id": profile_id},
                                       msg="The assembly has been created with accession: " + sub_result.get(
                    "accession", "Success"),
                    action="info",
                    html_id="assembly_info")

                return JsonResponse(status=200, data=sub_result)
                # form = AssemblyForm(study_accession=study_accession, sample_accession=sample_accession)
            # return HttpResponse()
        else:
            notify_assembly_status(data={"profile_id": profile_id},
                                   msg=str(form.errors),
                                   action="error",
                                   html_id="assembly_info")
            is_error = True
            # messages.error(request, form.errors)
        # if is_error:
        return HttpResponse(content="Validation Error", status=400)

    else:

        # todo I'm probably out of time to do this, but we need to account -maybe?- for a situation in which we have
        # multiple assemblies submitted as part of the same profile, probably the structure in the database need to
        # change slightly so that it is possible for us to link accession and relative sample
        # eg. accessions: {assembly: {accession:,alias:, SAMPLE}} in copo_da add_assembly_accession
        #
        # pass the accessions as "study_accession" and "sample_ccession" to the form so that they are
        # set authomatically and cannot be changed by the user
        form = AssemblyForm(study_accession=study_accession, sample_accession=sample_accession, assembly=assembly, ecs_files=ecs_files
                            # initial={"assemblyname": "jdklsad", "coverage": 1, "program": "jiwjd", "platform": "kkfjoep", "mingaplength": 10,
                            #         "description": "jfksjkdlfs"}
                            )
        return render(request, "copo/ena_assembly_form.html",
                      {"profile_id": profile_id, "form": form, "hide_form": False})


@web_page_access_checker
@login_required
def copo_assembly(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    return render(request, 'copo/copo_assembly.html', {'profile_id': profile_id, 'profile': profile})
