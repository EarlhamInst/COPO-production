from datetime import datetime
from src.apps.copo_core.models import ProfileType, Component, TitleButton, RecordActionButton, Banner
from django.conf import settings

def latest_message(request):
    '''
    sm = StatusMessage(message_owner=request.user, message="")
    sm.save()
    '''
    try:
        if request.user.userdetails.active_task:
            status_msgs = request.user.statusmessage_set.latest()
            return {"latest_message": status_msgs.message, "created": status_msgs.created}
        else:
            return {"latest_message": "No Active Tasks", "created": datetime.utcnow()}
    except:
        return {}


def copo_context(request):
    # Get active banners
    banner_objects = Banner.objects.filter(active=True)

    return {
        "banner": banner_objects[0] if banner_objects else None,
        "component_def": Component.objects.all(),
        "copo_email": settings.MAIL_ADDRESS,
        "profile_type_def": ProfileType.objects.all(),
        "title_button_def": TitleButton.objects.all(),
        "record_action_button_def": RecordActionButton.objects.all(),
        "media_url": settings.MEDIA_URL,
        "upload_url": settings.UPLOAD_URL,
        "image_file_extensions": settings.IMAGE_FILE_EXTENSIONS
    }
