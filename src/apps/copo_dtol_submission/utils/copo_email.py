from common.dal.profile_da import Profile
from common.dal.copo_da import CopoGroup
from common.utils.copo_email import CopoEmail
from common.utils.logger import Logger
from django.conf import settings
from src.apps.copo_core.models import AssociatedProfileType, SequencingCentre
from src.apps.copo_core.models import User

import common.schemas.utils.data_utils as d_utils
import json

logger = Logger()

class Email:
    def __init__(self):
        # Message content for 'accepted samples' email notifications

        sample_accepted_msg_content = "Dear {gretting},<br><br>We have now accepted your manifest submission of <b>{title}</b> in Collaborative OPen Omics (COPO).<br><br><br>If you have any questions, please do not hesitate to get in touch.<br><br><br>Best regards,<br>Collaborative OPen Omics (COPO) Project Team"
        """
        sample_accepted_msg_content = "Dear {},"
        sample_accepted_msg_content += "<br><br>We have now accepted your manifest submission of <b>{}</b> in Collaborative OPen Omics (COPO).<br>"
        sample_accepted_msg_content += "<ul>"
        sample_accepted_msg_content += '<li>In case you have not provided barcoding, biobanking and voucher information yet, with the submitted manifest, please provide it as soon as available, by uploading a new manifest updating solely the respective fields.'
        sample_accepted_msg_content += "<br>This information is required to be displayed with the Sample data in ENA and will be part of the Genome note.</li>"
        sample_accepted_msg_content += "</li>"
        sample_accepted_msg_content += "<br><li>If you have not done it yet, please complete the two Material Transfer Agreement (MTAs) documents. After completing your part of the documents, please send them to Thomas Marcussen [<a href='mailto:thomarc@ibv.uio.no'>thomarc@ibv.uio.no</a>] and Rita Monteiro [<a href='mailto:r.monteiro@leibniz-lib.de'>r.monteiro@leibniz-lib.de</a>]."
        sample_accepted_msg_content += "<ul>"
        sample_accepted_msg_content += "<br><li>MTA1 - Material Transfer Agreement for PROVISION OF MATERIAL, with no change in ownership (between Sample Provider and Sequencing Centre): <a href='https://eu.twk.pm/jsmsel4ird'>https://eu.twk.pm/jsmsel4ird</a></li>"
        sample_accepted_msg_content += "<br><li>MTA2 - Material Transfer Agreement for RECEIPT OF MATERIAL, with change in ownership (between Sample Provider and LIB Biobank): <a href='https://eu.twk.pm/h3nyix8dk9'>https://eu.twk.pm/h3nyix8dk9</a></li>"
        sample_accepted_msg_content += "</ul>"
        sample_accepted_msg_content += "</li>"
        sample_accepted_msg_content += "<br><li>Please proceed to sample shipping with {}. The contact person is in cc.</li>"
        sample_accepted_msg_content += "<br><li>Please <b>do not</b> ship the samples before the MTAs have been signed or without prior agreement with the Sequencing Centre.</li>"
        sample_accepted_msg_content += "</ul>"
        sample_accepted_msg_content += "<br><br>If you have any questions, please do not hesitate to get in touch."
        sample_accepted_msg_content += "<br><br><br>Best regards,"
        sample_accepted_msg_content += "<br>Collaborative OPen Omics (COPO) Project Team"
        """
        
        # Messages for email notifications
        self.messages = {
            "sample_accepted" : sample_accepted_msg_content,
            "sample_rejected" : "<h4>Following samples are rejected by ENA</h4><h5>{} - {}</h5><ul>{}</ul>",
            "pending_samples" : "<h4>Samples Available for Approval</h4><p>The following sample(s) has/have been approved by the Biodiversity Genomics Europe (BGE) checkers. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>",
            "bge_pending_samples" : "<h4>Samples Available for Approval</h4><p>The previous rejected sample(s) are now ready for approval. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>",
            "associated_project_samples" : "<h4>Samples Available for Approval</h4><p>The following sample(s) are now ready for approval. Please follow the link to proceed</p><h5>{} - {}</h5><p><a href='{}'>{}</a></p>"
        }

    def notify_sample_accepted_after_approval(self, **kwargs):
        profile = kwargs.get('profile', '')
        accepted_samples_notifiers = set()
        email_addresses = set()
        p_id = kwargs.get("profile_id", "")
        profile = Profile().get_record(p_id) if p_id else None
        if profile: 
            profile_owner_userID = profile.get('user_id','')   
            title = profile.get('title','')
            apts = profile.get("associated_type", [])
            #apts = [apt.get("value", "") for apt in associated_profile_types]

            apt_objs = AssociatedProfileType.objects.filter(name__in=apts, is_acceptance_email_notification_required=True)
            if not apt_objs:
                logger.debug(f"No acceptance notificaion email required for the associated profile types {apts}. Skipping email notification.")    
                return
            
            sequencing_centres = profile.get('sequencing_centre',[])

            centre_contact_details = list()
            centre_labels = list()

            for sc in sequencing_centres:
                centre = SequencingCentre.objects.get(name=sc)
                centre_labels.append(centre.label)
                
                # Get contact details of the sequencing centre only if the environment is production
                if settings.ENVIRONMENT_TYPE == "prod" and centre.contact_details and len(centre.contact_details) > 0:
                    # Contact name and email address of the partner for the sequencing centre
                    centre_contact_details.extend(json.loads(centre.contact_details)) 

            # Join the list of sequencing centre labels with commas then, have 'and' as the last entry
            centre_labels = d_utils.join_list_with_and_as_last_entry(centre_labels) 


        
            # Send email if "ERGA" samples have been accepted

            '''
                Persons who should receive an email are:
                    1. User who submitted the manifest/samples ('to' email recipient)
                       i.e. the owner of the profile through which the manifest/sample has been accepted
                    2. Sequencing centre contact person(s) ('cc' email recipient)
                    3. Person(s) within the erga_accepted_samples_notifiers Django group ('cc' email recipient)
            '''
            
            # Get list of persons who should be notified by default
            #erga_accepted_samples_notifiers = User.objects.filter(groups__name='erga_accepted_samples_notifiers') 

            # User who submitted the manifest i.e. the owner of the profile
            user = User.objects.get(pk=profile_owner_userID)
            
            # Get list of emails of shared users who also have access to the profile
            shared_profile_users_info = CopoGroup().get_shared_users_info_by_owner_and_profile_id(owner_id=profile_owner_userID, profile_id=profile.get('_id', ''))
            shared_profile_users_emails = [element['email'] for element in shared_profile_users_info if element.get('email','')]
            shared_profile_users_names = [element['name'] for element in shared_profile_users_info if element.get('name','')]

            # Extract the email address for the sequencing centre from its contact details
            centre_contact_emails = [element['contact_email'] for element in centre_contact_details if element.get('contact_email','')]

            if user:
                # 'to' email address recipient(s)
                to_email_addresses = set([user.email])
                to_email_addresses.update(shared_profile_users_emails) # Add shared profile user(s) email address(es)

                # Get profile owner name and shared profile user name
                profile_owner_name = f"{user.first_name} {user.last_name}"
                greetings_name = list(profile_owner_name)
                greetings_name.extend(shared_profile_users_names)
                greetings_name = d_utils.join_list_with_and_as_last_entry(greetings_name)
                sub = f"{title} Sample Manifest accepted in COPO"

                for apt_obj in apt_objs:
                    # 'cc' email address recipients
                    msg = apt_obj.acceptance_email_body.format(gretting=greetings_name, title=title, centre_labels=centre_labels) if  apt_obj.acceptance_email_body else self.messages["sample_accepted"].format(greetings_name, title)
                    accepted_samples_notifiers = set(apt_obj.users.all())
                    cc_email_addresses = set(centre_contact_emails)
                    cc_email_addresses.update([x.email for x in accepted_samples_notifiers])
                    #msg = apt_obj.acceptance_email_body.format(gretting=greetings_name, title=title, centre_labels=centre_labels)
                    CopoEmail().send(to=list(to_email_addresses), sub=sub, content=msg, html=True, is_cc_required=True, cc=list(cc_email_addresses))
            else:
                logger.log('No users found to send email to. Perhaps the user has been deleted.')    


    def notify_sample_rejected_after_approval(self, profile=None, rejected_sample=dict()):
        # get email addresses of users in sequencing centre
        users = set()
        email_addresses = set()
 
        sample_arr = [f"<li>{key} : {rejected_sample[key]}</li>" for key in rejected_sample.keys()]
        sample_str = ' '.join(sample_arr)

        if profile:    
            #type = profile.get("type", "").upper()
            #if type == "ERGA":
            apts = profile.get("associated_type", [])
            #apts = [apt.get("value", "") for apt in associated_profile_types]
            apt_objs = AssociatedProfileType.objects.filter(name__in=apts, is_approval_required=True)
            for apt_obj in apt_objs:
                users.update(apt_obj.users.all())
            #else:
            #    users = set(User.objects.filter(groups__name=f'{type.lower()}_sample_notifiers'))

        if users:
            email_addresses.update([u.email for u in users])
            msg = self.messages["sample_rejected"].format(profile.get("title"," ") , profile.get("description"," "), sample_str)
            sub = settings.ENVIRONMENT_TYPE + " " + profile.get("type"," ").upper() + " Manifest - " + profile.get("title"," ")
            CopoEmail().send(to=list(email_addresses), sub=sub, content=msg, html=True)

    def notify_manifest_pending_for_associated_project_type_checker(self, data, **kwargs):        
        # get email addresses of users in sequencing centre
        p_id = kwargs.get("profile_id", "")
        profile = Profile().get_record(p_id) if p_id else None
        users = set()

        if profile:    
            apts = profile.get("associated_type", [])
            #apts = [apt.get("value", "") for apt in associated_profile_types]
            apt_objs = AssociatedProfileType.objects.filter(name__in=apts, is_approval_required=True)
            for apt_obj in apt_objs:
                users.update(apt_obj.users.all())
            email_addresses = set()
            sub = ""
            if len(users) > 0:
                email_addresses.update([u.email for u in users])
                demo_notification = ""
                if "demo" in data:
                    demo_notification = "DEMO SERVER NOTIFICATION: "
                msg = self.messages["associated_project_samples"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
                sub = demo_notification + " Manifest - " + kwargs["title"]
                CopoEmail().send(to=list(email_addresses), sub=sub, content=msg, html=True)
                return
        logger.log("No users found for associated_project_type_checker")
    