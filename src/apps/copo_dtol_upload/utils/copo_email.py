from src.apps.copo_core.models import User, AssociatedProfileType
from common.utils.copo_email import CopoEmail
from common.dal.profile_da import Profile
from common.utils.logger import Logger

logger = Logger()

class Email:

    def __init__(self):
        self.messages = {
            "new_manifest" : "<h4>Manifest Available</h4><p>A manifest has been uploaded for approval. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>",
        }

    def notify_manifest_pending_approval(self, data, **kwargs):
        
        # get email addresses of users in sequencing centre
        users = set()
        p_id = kwargs.get("profile_id", "")
        profile = Profile().get_record(p_id) if p_id else None
        if profile:
            type = profile.get("type", "").upper()
            #if type == "ERGA":
            associated_profiles = profile.get("associated_type", [])
            assoicated_profiles_type_require_approval = AssociatedProfileType.objects.filter(is_approval_required=True,  name__in =  associated_profiles)
            for ap in assoicated_profiles_type_require_approval:
                users.update(ap.users.all())
            #else :
            #    users = set(User.objects.filter(groups__name=f'{type.lower()}_sample_notifiers'))    
 
        email_addresses = set()
        sub = ""
        if users:
            email_addresses.update([u.email for u in users])

            demo_notification = ""
            is_new = "New "
            if "demo" in data:
                demo_notification = "DEMO SERVER NOTIFICATION: "
            if not kwargs.get("is_new", True) :            
                is_new = "Modified "
            msg = self.messages["new_manifest"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
            sub = demo_notification + is_new + kwargs["project"] + " Manifest - " + kwargs["title"]
            CopoEmail().send(to=list(email_addresses), sub=sub, content=msg, html=True)
        else:
            logger.log("No users found for associated_project_type_checker" if type =="ERGA" else "No users found for sample_notifiers")
