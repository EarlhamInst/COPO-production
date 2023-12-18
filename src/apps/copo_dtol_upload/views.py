from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from .utils.da import   ValidationQueue
from .utils.Dtol_Spreadsheet import DtolSpreadsheet
from common.utils import html_tags_utils as htags
import pickle
import jsonpickle
from datetime import datetime 
import json
from common.utils.logger import Logger 
l = Logger()

@login_required
# Create your views here.
def sample_spreadsheet(request, report_id=""):
    file = request.FILES["file"]
    name = file.name
    if "profile_id" in request.POST:
        p_id = request.POST["profile_id"]
    else:
        p_id = request.session["profile_id"]
    dtol = DtolSpreadsheet(file=file, p_id=p_id)
    if name.endswith("xlsx") or name.endswith("xls"):
        fmt = 'xls'
    elif name.endswith("csv"):
        fmt = 'csv'
    else:
        msg = "Unrecognised file format for spreadsheet. " \
              "File format should be either <strong>.xls</strong>, <strong>.xlsx</strong> or <strong>.csv</strong>."
        l.log(msg)        
        return HttpResponse(status=400, content=msg)

    if dtol.loadManifest(m_format=fmt):
        srlz_dtol = pickle.dumps(dtol.file)
        if "profile_id" in request.POST:
            p_id = request.POST["profile_id"]
        else:
            p_id = request.session["profile_id"]
        r = {"$set": {"manifest_data": srlz_dtol, "profile_id": p_id, "schema_validation_status": "pending",
                      "taxon_validation_status": "pending", "err_msg": [],
                      "time_added": datetime.utcnow(),
                      "file_name": name,
                      "isupdate": False,
                      "report_id": report_id,
                      "user_id": request.user.id,
                      }}
        ValidationQueue().get_collection_handle().update_one({"profile_id": p_id}, r, upsert=True)

    return HttpResponse()

@login_required
def sample_images(request):
    files = request.FILES
    dtol = DtolSpreadsheet(validation_record_id=request.POST["validation_record_id"])
    matchings = dtol.check_image_names(files)

    return HttpResponse(json.dumps(matchings))    

@login_required
def sample_permits(request):
    files = request.FILES
    dtol = DtolSpreadsheet(validation_record_id=request.POST["validation_record_id"])
    matchings = dtol.check_permit_names(files)

    return HttpResponse(json.dumps(matchings))    

@login_required
def create_spreadsheet_samples(request):
    validation_record_id = request.GET["validation_record_id"]
    # note calling DtolSpreadsheet without a spreadsheet object will attempt to load one from the session
    dtol = DtolSpreadsheet(validation_record_id=validation_record_id)
    dtol.save_records()

    # Save table_data
    context = dict()
    context["table_data"] = htags.generate_table_records(profile_id=request.session["profile_id"], component="sample")
    context["component"] = "sample"
    out = jsonpickle.encode(context, unpicklable=False)
    return HttpResponse(status=200, content=out, content_type='application/json')

@login_required
def update_spreadsheet_samples(request):
    validation_record_id = request.GET["validation_record_id"]
    # note calling DtolSpreadsheet without a spreadsheet object will attempt to load one from the session
    dtol = DtolSpreadsheet(validation_record_id=validation_record_id)
    dtol.update_records()

    # Save table_data
    context = dict()
    context["table_data"] = htags.generate_table_records(profile_id=request.session["profile_id"], component="sample")
    context["component"] = "sample"
    out = jsonpickle.encode(context, unpicklable=False)
    return HttpResponse(status=200, content=out, content_type='application/json')
