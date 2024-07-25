from datetime import datetime
from src.apps.copo_core.models import ProfileType, Component, TitleButton, RecordActionButton
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
    return {
        "profile_type_def": ProfileType.objects.all(),
        "component_def": Component.objects.all(),
        "title_button_def": TitleButton.objects.all(),
        "record_action_button_def": RecordActionButton.objects.all(),
        "copo_email": settings.MAIL_ADDRESS  
    }
