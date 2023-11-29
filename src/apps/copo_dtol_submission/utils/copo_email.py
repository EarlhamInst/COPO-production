from common.dal.copo_da import Profile
from common.schema_versions.lookup.dtol_lookups import ACCEPTED_SAMPLES_DEFAULT_CC_EMAIL_RECIPIENTS
from common.utils.copo_email import CopoEmail
from common.utils.logger import Logger
from django.conf import settings
from src.apps.copo_core.models import SequencingCentre
from src.apps.copo_core.models import User

logger = Logger()

class Email:
    def __init__(self):
        # Message content for 'accepted samples' email notifications
        sample_accepted_msg_content = "Dear Manifest Submitter/Sample Provider,"
        sample_accepted_msg_content += "\n\nWe have now accepted your manifest submission of {} in Collaborative OPen Omics (COPO)."
        sample_accepted_msg_content += "<ul>"
        sample_accepted_msg_content += "\n\n<li>In case you haven\'t provided barcoding, biobanking, and voucher information yet, with the submitted manifest, please provide it as soon as available, by uploading a new manifest updating solely the respective fields."
        sample_accepted_msg_content += "\nThis information is required to be displayed with the Sample data in ENA and will be part of the Genome note.</li>"
        sample_accepted_msg_content += "\n\n<>"
        sample_accepted_msg_content += "If you have not done it yet, please complete the two Material Transfer Agreement (MTAs) documents. After completing your part of the documents, please send them to Thomas Marcussen [<a href='mailto:thomarc@ibv.uio.no'>thomarc@ibv.uio.no</a>] and Rita Monteiro [<a href='mailto:r.monteiro@leibniz-lib.de'>r.monteiro@leibniz-lib.de</a>]."
        sample_accepted_msg_content += "<ul>"
        sample_accepted_msg_content += "\n<li>MTA1 - Material Transfer Agreement for PROVISION OF MATERIAL, with no change in ownership (between Sample Provider and Sequencing Centre): <a href='https://eu.twk.pm/jsmsel4ird'>https://eu.twk.pm/jsmsel4ird</a></li>"
        sample_accepted_msg_content += "\n<li>MTA2 - Material Transfer Agreement for RECEIPT OF MATERIAL, with change in ownership (between Sample Provider and LIB Biobank): <a href='https://eu.twk.pm/kq8aosg2n6'>https://eu.twk.pm/kq8aosg2n6</a></li>"
        sample_accepted_msg_content += "</ul>"
        sample_accepted_msg_content += "</li>"
        sample_accepted_msg_content += "\n\n<li>Please proceed to sample shipping with {}. The contact person is in cc.</li>"
        sample_accepted_msg_content += "\n\n<li><b>Please do NOT ship the samples before the MTAs have been signed or without prior agreement with the Sequencing Centre.</b></li>"
        sample_accepted_msg_content += "</ul>"
        sample_accepted_msg_content += "\n\n\nIf you have any questions, please do not hesitate to get in touch."
        sample_accepted_msg_content += "\n\nBest regards,"
        sample_accepted_msg_content += "\n\nCollaborative OPen Omics (COPO) Project Team"

        self.sample_accepted_msg_content = sample_accepted_msg_content
        
        # Messages for email notifications
        self.messages = {
            "sample_accepted" : sample_accepted_msg_content,
            "sample_rejected" : "<h4>Following samples are rejected by ENA</h4><h5>{} - {}</h5><ul>{}</ul>",
            "pending_samples" : "<h4>Samples Available for Approval</h4><p>The following sample(s) has/have been approved by the Biodiversity Genomics Europe (BGE) checkers. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>",
            "bge_pending_samples" : "<h4>Samples Available for Approval</h4><p>The previous rejected sample(s) are now ready for approval. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>"
        }

    def notify_sample_accepted_after_approval(self, **kwargs):
        users = list()
        centre_contact_details = list()

        # Send email if "ERGA" samples have been accepted
        if 'ERGA' in kwargs.get('project', ''):
            for sc in kwargs.get('sequencing_centres', []):
                centre = SequencingCentre.objects.get(name=sc)
                centre_contact_details += centre.contact_details.all() # Contact name and email address of the partner for the sequencing centre
                users += centre.users.all() # Sample/manifest submitters
    
            users = list(set(users))
            centre_contact_details = list(set(centre_contact_details))
            to_email_addresses = list()
            cc = list()
            sub = ""

            if len(users) > 0:
                for u in users:
                    to_email_addresses.append(u.email)

                for sc_contact_details in centre_contact_details:
                    cc.append(sc_contact_details.contact_email)

                # 'to' email address recipients
                to_email_addresses = list(set(to_email_addresses)) # Remove duplicates

                # 'cc' email address recipients
                cc.extend(ACCEPTED_SAMPLES_DEFAULT_CC_EMAIL_RECIPIENTS)
                cc = list(set(cc))  # Remove duplicates

                msg = self.messages["sample_accepted"].format(kwargs.get('title',' '), ', '.join(kwargs.get('sequencing_centres', [])))
                sub = f"{kwargs.get('title',' ')} Sample Manifest accepted in COPO"
                
                CopoEmail().send(to=to_email_addresses, sub=sub, content=msg, html=True, is_cc_required=True, cc=cc)
                return
            

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

            email_addresses = list(set(email_addresses)) # Remove duplicates

            msg = self.messages["sample_rejected"].format(kwargs.get("title"," ") , kwargs.get("description"," "), sample_str)
            sub = settings.ENVIRONMENT_TYPE + " " + kwargs.get("project"," ") + " Manifest - " + kwargs.get("title"," ")
            CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)


    def notify_manifest_pending_for_sequencing_centre(self, data, **kwargs):        
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

            email_addresses = list(set(email_addresses)) # Remove duplicates

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
        users = list()

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
                    
                email_addresses = list(set(email_addresses)) # Remove duplicates

                demo_notification = ""
                if "demo" in data:
                    demo_notification = "DEMO SERVER NOTIFICATION: "
                msg = self.messages["bge_pending_samples"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
                sub = demo_notification + " Manifest - " + kwargs["title"]
                CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)
                return
        logger.log("No users found for bge_checkers")