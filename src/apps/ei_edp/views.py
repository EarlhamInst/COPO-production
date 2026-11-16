from django.contrib.auth.decorators import login_required
from django.http import JsonResponse

from common.utils.logger import Logger
from src.apps.copo_core.views import web_page_access_checker

from .utils import edp_utils
from .utils.constants import BATCH_FIELDS, BATCH_ID
from .utils.edp_validation_messages import MESSAGES
from .utils.lims import LIMSUnavailable

l = Logger()


@login_required
def get_sample_batch_options(request):
    # Options for the dialogue that registers a batch of samples under an EDP profile
    try:
        return JsonResponse(edp_utils.get_sample_batch_form_options())
    except LIMSUnavailable as error:
        l.error(str(error))
        return JsonResponse({'status': 'error', 'message': str(error)}, status=503)


@login_required
@web_page_access_checker
def sample_batches(request, profile_id):
    # `GET` lists an EDP profile's sample batches. `POST` adds a batch or if a
    # batch ID is given, changes that batch. Only EDP managers (and data
    # managers) can register samples. The users that the profile is shared with
    # can only upload the samples'and LIMS manifest metadata.
    result = {'status': 'success', 'message': ''}
    if request.method == 'POST':
        if not edp_utils.is_edp_manager(request.user):
            return JsonResponse(
                {'status': 'error', 'message': MESSAGES['edp_manager_required_for_samples']}, status=403
            )
        fields = {field: request.POST.get(field, '') for field in BATCH_FIELDS}
        batch_id = request.POST.get(BATCH_ID, '')
        if batch_id:
            result = edp_utils.update_sample_batch(profile_id, batch_id, fields)
        else:
            result = edp_utils.add_sample_batch(profile_id, fields)

    try:
        batches = edp_utils.describe_sample_batches(profile_id)
    except Exception as error:
        l.exception(error)
        return JsonResponse(
            {'status': 'error', 'message': 'The sample batches could not be loaded from Sapio.'},
            status=503,
        )
    if batches is None:
        return JsonResponse({'status': 'error', 'message': 'Profile not found.'}, status=404)

    return JsonResponse(
        {'status': result['status'], 'message': result.get('message', ''), 'batches': batches}
    )
