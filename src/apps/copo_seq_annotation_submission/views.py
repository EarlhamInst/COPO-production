from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from common.dal.copo_da import Submission, Profile, Sequnece_annotation
from common.utils.helpers import notify_annotation_status
from django.http import HttpResponse, JsonResponse
from .forms import AnnotationFilesForm, AnnotationForm
from .utils import EnaAnnotation
from common.s3.s3Connection import S3Connection
from django.forms import formset_factory
from functools import partial, wraps
from src.apps.copo_core.views import web_page_access_checker


@web_page_access_checker
@login_required
def copo_seq_annotation(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    return render(request, 'copo/copo_seq_annotation.html', {'profile_id': profile_id, 'profile': profile})


@login_required()
def ena_annotation(request, profile_id, seq_annotation_id=None):
    request.session["profile_id"] = profile_id
    is_error = False
    seq_annotation = None

    if seq_annotation_id:
        seq_annotation = Sequnece_annotation().get_record(seq_annotation_id)
        if not seq_annotation:
            return HttpResponse(content="Sequence Annotation not exists", status=400)

    study_accession = ""
    sample_accession = []
    run_accession = []
    experiment_accession = []
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
        runs = existing_accessions.get("run", "")
        if runs:
            for run in runs:
                if run.get("accession", ""):
                    run_accession.append(run.get("accession", ""))
        experiments = existing_accessions.get("experiment", "")
        if experiments:
            for experiment in experiments:
                if experiment.get("accession", ""):
                    experiment_accession.append(
                        experiment.get("accession", ""))
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

    AnnotationFilesFormSet = formset_factory(
        wraps(AnnotationFilesForm)(partial(AnnotationFilesForm, ecs_files=ecs_files)), extra=3)
    if request.method == 'POST' or request.method == 'PUT':
        # return render(request, "copo/ena_assembly.html", {"profile_id": profile_id, "form": [], "hide_form": False})
        form = AnnotationForm(request.POST, request.FILES, sample_accession=sample_accession,
                              study_accession=study_accession, run_accession=run_accession,
                              experiment_accession=experiment_accession, seq_annotation=seq_annotation)
        formset = AnnotationFilesFormSet(
            request.POST, request.FILES, prefix="annotation_files")
        if form.is_valid() and formset.is_valid():
            notify_annotation_status(data={"profile_id": profile_id},
                                     msg="Intitialising Annotation Submission",
                                     action="info",
                                     html_id="annotation_info")
            # this is a dict
            formdata = form.cleaned_data
            seq_annotation_id = request.POST.get("seq_annotation_id", "")

            sub_result = EnaAnnotation.validate_annotation(
                formdata, formset, profile_id, seq_annotation_id)
            if sub_result.get("error", ""):
                notify_annotation_status(data={"profile_id": profile_id},
                                         msg=sub_result.get("error", ""),
                                         action="error",
                                         html_id="annotation_info")
            else:
                notify_annotation_status(data={"profile_id": profile_id},
                                         msg=sub_result.get("success", ""),
                                         action="info",
                                         html_id="annotation_info")
                return JsonResponse(status=201, data=sub_result)

        else:
            msg = ""
            if not form.is_valid():
                msg = str(form.errors)
            if not formset.is_valid():
                for f in formset:
                    if f.errors:
                        msg = msg + str(f.errors)
            notify_annotation_status(data={"profile_id": profile_id},
                                     msg=msg,
                                     action="error",
                                     html_id="annotation_info")
        return HttpResponse(content="Validation Error", status=400)

    else:

        form = AnnotationForm(study_accession=study_accession, sample_accession=sample_accession,
                              run_accession=run_accession, experiment_accession=experiment_accession,
                              seq_annotation=seq_annotation)
        # AnnotationFilesFormSet = formset_factory(AnnotationFilesForm(ecs_files), extra=3 )
        formset = AnnotationFilesFormSet(prefix="annotation_files")
        if seq_annotation:
            filenames = seq_annotation.get("filenames", list())
            filetypes = seq_annotation.get("filetypes", list())
            if filenames and filetypes:
                formset = AnnotationFilesFormSet(prefix="annotation_files", initial=[
                                                 {'file': filenames[i], 'type': filetypes[i]} for i in range(len(filenames))])

        return render(request, "copo/ena_annotation_form.html",
                      {"profile_id": profile_id, "form": form, "formset": formset, "hide_form": False})


@web_page_access_checker
@login_required
def copo_seq_annotation(request, profile_id):
    request.session["profile_id"] = profile_id
    profile = Profile().get_record(profile_id)
    return render(request, 'copo/copo_seq_annotation.html', {'profile_id': profile_id, 'profile': profile})
