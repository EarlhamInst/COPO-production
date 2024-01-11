from src.apps.copo_core.models import User, SequencingCentre
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
        users = []
        p_id = kwargs.get("profile_id", "")
        type = kwargs.get("project", "")
        sequencing_centres = []
        if p_id:
            profile = Profile().get_record(p_id)
            logger.debug(profile)
            checker_users = User.objects.filter(groups__name='bge_checkers')
            logger.debug(checker_users)
            all_seq_centres = SequencingCentre.objects.all()
            for centre in all_seq_centres:
                logger.debug(centre)
                logger.debug(centre.users.all())

            sequencing_centres = profile.get("sequencing_centre", [])

            '''
            is_bge_profile = "BGE" in [ x.get("value","") for x in profile.get("associated_type",[]) ]
            if is_bge_profile:
                users = User.objects.filter(groups__name='bge_checkers')
            elif type in ["ERGA"]:
                for sc in sequencing_centres:
                    centre = SequencingCentre.objects.get(name=sc)
                    users += centre.users.all()
            '''        
            if type in ["ERGA"]:
                for sc in sequencing_centres:
                    centre = SequencingCentre.objects.get(name=sc)
                    users += centre.users.all()
                    logger.debug(users)
                checker_users = User.objects.filter(groups__name='bge_checkers')
                users = list(set(users) & set(checker_users))
            elif type in ["DTOL", "ASG"]:
                users = User.objects.filter(groups__name='dtol_sample_notifiers')
            elif type in ["DTOL_ENV"]:
                users = User.objects.filter(groups__name='dtolenv_sample_notifiers')
            else:
                users = []     

        email_addresses = list()
        sub = ""
        if len(users) > 0:
            for u in users:
                email_addresses.append(u.email)

            demo_notification = ""
            is_new = "New "
            if "demo" in data:
                demo_notification = "DEMO SERVER NOTIFICATION: "
            if not kwargs.get("is_new", True) :            
                is_new = "Modified "
            msg = self.messages["new_manifest"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
            sub = demo_notification + is_new + kwargs["project"] + " Manifest - " + kwargs["title"]
            CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)
        else:
            logger.log("No users found for sequencing centre " )
