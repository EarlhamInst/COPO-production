from datetime import datetime
from django.contrib.auth.decorators import login_required

@login_required
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
