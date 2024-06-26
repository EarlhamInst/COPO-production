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
        profile_owner_userID = profile.get('user_id','')
        project= d_utils.get_profile_type(profile.get('type',''))
        sequencing_centres = profile.get('sequencing_centre',[])
        title = profile.get('title','')
        
        if 'ERGA' in project:
            # Send email if "ERGA" samples have been accepted

            '''
                Persons who should receive an email are:
                    1. User who submitted the manifest/samples ('to' email recipient)
                       i.e. the owner of the profile through which the manifest/sample has been accepted
                    2. Sequencing centre contact person(s) ('cc' email recipient)
                    3. Person(s) within the erga_accepted_samples_notifiers Django group ('cc' email recipient)
            '''

            centre_contact_details = list()
            centre_labels = list()

            for sc in sequencing_centres:
                centre = SequencingCentre.objects.get(name=sc)
                centre_labels.append(centre.label)
                
                # Get contact details of the sequencing centre only if the environment is production
                if settings.ENVIRONMENT_TYPE == "prod" and centre.contact_details and len(centre.contact_details) > 0:
                    # Contact name and email address of the partner for the sequencing centre
                    centre_contact_details.extend(json.loads(centre.contact_details)) 
            
            to_email_addresses = list()
            sub = ""
            greetings_name = list()

            # Join the list of sequencing centre labels with commas then, have 'and' as the last entry
            centre_labels = d_utils.join_list_with_and_as_last_entry(centre_labels) 
            
            # Get list of persons who should be notified by default
            erga_accepted_samples_notifiers = User.objects.filter(groups__name='erga_accepted_samples_notifiers') 

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
                to_email_addresses.append(user.email)
                to_email_addresses.extend(shared_profile_users_emails) # Add shared profile user(s) email address(es)
                to_email_addresses = list(set(to_email_addresses)) # Remove duplicates

                # 'cc' email address recipients
                cc_email_addresses = centre_contact_emails
                cc_email_addresses.extend([x.email for x in erga_accepted_samples_notifiers])
                cc_email_addresses = list(set(cc_email_addresses))  # Remove duplicates

                # Get profile owner name and shared profile user name
                profile_owner_name = f"{user.first_name} {user.last_name}"
                greetings_name.append(profile_owner_name)
                greetings_name.extend(shared_profile_users_names)
                greetings_name = d_utils.join_list_with_and_as_last_entry(greetings_name)

                msg = self.messages["sample_accepted"].format(greetings_name, title, centre_labels)
                sub = f"{title} Sample Manifest accepted in COPO"
                
                CopoEmail().send(to=to_email_addresses, sub=sub, content=msg, html=True, is_cc_required=True, cc=cc_email_addresses)
            else:
                logger.log('No users found to send email to. Perhaps the user has been deleted.')    

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

            centres = SequencingCentre.objects.filter(name__in=sequencing_centres, is_approval_required=True)
            for sc in centres:
                users += sc.users.all()

            """
            for sc in sequencing_centres:
                # Try catch block to handle the case where the sequencing centre object does not exist
                try:
                    centre = SequencingCentre.objects.get(is_approval_required=True, name=sc)
                except SequencingCentre.DoesNotExist:
                   continue
                users += centre.users.all()
            """ 
   
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

    """
    def notify_manifest_pending_for_bge_checker(self, data, **kwargs):        
        # get email addresses of users in sequencing centre
        p_id = kwargs.get("profile_id", "")
        profile = Profile().get_record(p_id) if p_id else None
        users = list()

        if profile:    
            sequencing_centres = profile.get("sequencing_centre", [])
            centres = SequencingCentre.objects.filter(name__in=sequencing_centres)
            for sc in centres:
                users += sc.users.all()

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
    """
    def notify_manifest_pending_for_associated_project_type_checker(self, data, **kwargs):        
        # get email addresses of users in sequencing centre
        p_id = kwargs.get("profile_id", "")
        profile = Profile().get_record(p_id) if p_id else None
        users = list()

        if profile:    
            associated_profile_types = profile.get("associated_type", [])
            apts = [apt.get("value", "") for apt in associated_profile_types]
            apt_objs = AssociatedProfileType.objects.filter(name__in=apts, is_approval_required=True)
            for apt_obj in apt_objs:
                users += apt_obj.users.all()

            """
            for apt in associated_profile_types:
                # Try catch block to handle the case where the associated project type object does not exist
                try:
                    apt_obj = AssociatedProfileType.objects.get(is_approval_required=True, name=apt.get("value", ""))
                except SequencingCentre.DoesNotExist:
                   continue
                users += apt_obj.users.all()
            """     
            users = list(set(users))
            email_addresses = list()
            sub = ""
            if len(users) > 0:
                for u in users:
                    email_addresses.append(u.email)
                    
                email_addresses = list(set(email_addresses)) # Remove duplicates

                demo_notification = ""
                if "demo" in data:
                    demo_notification = "DEMO SERVER NOTIFICATION: "
                msg = self.messages["associated_project_samples"].format(kwargs["title"], demo_notification + kwargs["description"], data, data)
                sub = demo_notification + " Manifest - " + kwargs["title"]
                CopoEmail().send(to=email_addresses, sub=sub, content=msg, html=True)
                return
        logger.log("No users found for associated_project_type_checker")
    