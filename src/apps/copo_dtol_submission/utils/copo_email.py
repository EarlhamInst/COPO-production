import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from common.dal.copo_da import Profile
from django.conf import settings
from src.apps.copo_core.models import SequencingCentre
from common.utils.copo_email import CopoEmail
from common.utils.logger import Logger
from src.apps.copo_core.models import User
logger = Logger()

class Email:

    def __init__(self):
        self.messages = {
            "sample_rejected" : "<h4>Following samples are rejected by ENA</h4><h5>{} - {}</h5><ul>{}</ul>",
            "pending_samples" : "<h4>Samples Available for approve</h4><p>sample(s) has/have been approved by BGE checkers. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>",
            "bge_pending_samples" : "<h4>Samples Available for approve</h4><p>The previous rejected sample(s) are now ready for approve. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>"

        }


    def notify_sample_rejected_after_approval(self, **kwargs):
        # get email addresses of users in sequencing centre
        users = []
        p_id = kwargs.get("profile_id", "")
        if p_id != "":
            profile = Profile().get_record(p_id)
            sequencing_centres = profile.get("sequencing_centre", [])
            for sc in sequencing_centres:
                centre = SequencingCentre.objects.get(name=sc)
                users += centre.users.all()
        email_addresses = list()
        sub = ""
        samples = kwargs["rejected_sample"] 
        sample_arr = [f"<li>{key} : {samples[key]}</li>" for key in samples.keys()]
        sample_str = ' '.join(sample_arr)

        if len(users) > 0:
            for u in users:
                email_addresses.append(u.email)
            
            msg = self.messages["sample_rejected"].format(kwargs.get("title"," ") , kwargs.get("description"," "), sample_str)
            sub = settings.ENVIRONMENT_TYPE + " " + kwargs.get("project"," ") + " Manifest - " + kwargs.get("title"," ")
            CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)


    def notify_manifest_pending_for_sequencing_center(self, data, **kwargs):        
        # get email addresses of users in sequencing centre
        users = []
        p_id = kwargs.get("profile_id", "")
        if p_id:
            profile = Profile().get_record(p_id)
            sequencing_centres = profile.get("sequencing_centre", [])
            for sc in sequencing_centres:
                centre = SequencingCentre.objects.get(name=sc)
                users += centre.users.all()
        email_addresses = list()
        sub = ""
        if len(users) > 0:
            for u in users:
                email_addresses.append(u.email)

            demo_notification = ""
            if "demo" in data:
                demo_notification = "DEMO SERVER NOTIFICATION: "
            msg = self.messages["pending_samples"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
            sub = demo_notification + " Manifest - " + kwargs["title"]
            CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)
        else:
            logger.log("No users found for sequencing centre")

    def notify_manifest_pending_for_bge_checker(self, data, **kwargs):        
        # get email addresses of users in sequencing centre
        p_id = kwargs.get("profile_id", "")
        profile = Profile().get_record(p_id) if p_id else None 
        if profile:    
            sequencing_centres = profile.get("sequencing_centre", [])
            for sc in sequencing_centres:
                centre = SequencingCentre.objects.get(name=sc)
                users += centre.users.all()
            checker_users = User.objects.filter(groups__name='bge_checkers')        
            users = list(set(users) & set(checker_users))
            email_addresses = list()
            sub = ""
            if len(users) > 0:
                for u in users:
                    email_addresses.append(u.email)

                demo_notification = ""
                if "demo" in data:
                    demo_notification = "DEMO SERVER NOTIFICATION: "
                msg = self.messages["bge_pending_samples"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
                sub = demo_notification + " Manifest - " + kwargs["title"]
                CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)
                return
        logger.log("No users found for bge_checkers")