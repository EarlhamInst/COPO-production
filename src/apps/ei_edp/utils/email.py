from common.utils.copo_email import CopoEmail
from common.utils.logger import Logger
from django.conf import settings
from django_tools.middlewares import threadlocal as ThreadLocal
from django.urls import reverse

logger = Logger()


class Email:
    # Both `notify` methods in this class, send one email per recipient and record.
    # `skipped_emails` relates to when an email is not set up on a server like CI
    #                  and as a result no email will be sent.
    #                  This is expected so the user is only made aware in this case.
    # `failed_emails` relates to when emails fail to be sent. The failure is 
    #                 reported as a warning instead of an error.
    #
    # Neither of these two things stop email recipients from being notified nor the profile 
    # in COPO from being created so these failures are logged, recorded and reported to the 
    # user rather than raised.

    def __init__(self):
        self.skipped_emails = []
        self.failed_emails = []

    def notify_shared_profile_to_non_existent_user(
        self, profile, customer_emails_token
    ):
        subj = "COPO Profile Shared with You"
        uri = self._get_uri()
        if settings.ENVIRONMENT_TYPE != "prod":
            subj = f'[{settings.ENVIRONMENT_TYPE.upper()} SERVER NOTIFICATION] - {subj}'

        if profile and customer_emails_token:
            for token, email in customer_emails_token.items():
                msg = f"<h4>COPO Profile Shared with You</h4><p>A COPO profile named '{profile.get("title", "")}' has been shared with you. Please use the link below to create your COPO account and access the profile.</p><p><a href='{uri}{reverse("copo:join_shared_profile", kwargs={"profile_id":str(profile["_id"]), "token":token})}'>Create an account on COPO</a></p>"
                self._send(email, subj, msg)

    def notify_shared_profile_to_existing_user(self, profile, users):
        subj = "COPO Profile Shared with You"
        # uri = self._get_uri()
        if settings.ENVIRONMENT_TYPE != "prod":
            subj = f'[{settings.ENVIRONMENT_TYPE.upper()} SERVER NOTIFICATION] - {subj}'

        if profile and users:
            for user in users:
                msg = f"Dear {user["first_name"]}, <h4>COPO Profile Shared with You</h4><p>A COPO profile named '{profile.get('title', '')}' has been shared with you. Please log in to your COPO account to access the profile.</p>"
                self._send(user["email"], subj, msg)

    def _send(self, email, subj, msg):
        try:
            sent = CopoEmail().send(to=[email], sub=subj, content=msg, html=True)
        except Exception as error:
            logger.exception(error)
            logger.error(f'Failed to send the shared profile email to {email}')
            self.failed_emails.append(email)
            return
        
        if not sent:
            self.skipped_emails.append(email)

    def _get_uri(self):
        uri = ThreadLocal.get_current_request().build_absolute_uri('/')
        if uri.endswith('/'):
            uri = uri[:-1]
        return uri
