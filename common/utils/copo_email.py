import smtplib
import zipfile

from django.conf import settings
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

from common.utils.logger import Logger

l = Logger()

# These email domains are reserved for testing and documentation only.
# Emails sent to them can never be delivered. Some mail servers like Office 365
# refuse an email message when one recipient uses them so as a result, 
# they are dropped before being used to send emails.
#
# Tests use them (e.g. 'copo-playwright@example.invalid') so
# that they can execute email-sending workflows without actually 
# sending an email to anyone.
RESERVED_EMAIL_DOMAINS = ('example.com', 'example.net', 'example.org')
RESERVED_EMAIL_TLDS = ('.example', '.invalid', '.localhost', '.test')


def is_reserved_email_address(address):
    domain = str(address).rsplit('@', 1)[-1].strip().lower()
    # A reserved domain covers its subdomains like `mail.example.com`
    return (
        domain in RESERVED_EMAIL_DOMAINS
        or domain.endswith(tuple('.' + d for d in RESERVED_EMAIL_DOMAINS))
        or domain.endswith(RESERVED_EMAIL_TLDS)
    )


def is_email_configured():
    # Email is set up on a server when it has a mail server to send
    # through. Environments that do not send email like CI, leave MAIL_SERVER
    # unset and as a result, sending is skipped rather than failing.
    return bool(settings.MAIL_SERVER)


class CopoEmail:

    def send(
        self,
        to,
        sub,
        content,
        html=False,
        cc=list(),
        bcc=list(),
        attachment_path=None
    ):
        # Returns `False` when an email is not set on a server so that nothing
        # is sent and `True` otherwise. A failure that occurs while sending
        # an email is raised.
        if not is_email_configured():
            l.log('Email is not set up on this server (because `MAIL_SERVER` is empty) so no email was sent.')
            return False

        msg = MIMEMultipart()
        msg['From'] = settings.MAIL_ADDRESS

        msg['Subject'] = sub

        if cc:
            msg['CC'] = ",".join(cc)  # Convert list to string
            to.extend(cc)

        if bcc:
            # 'bcc' recipients cannot be added to 'msg' multipart/* type message
            # because the 'to' and 'cc' receivers will know  who the 'bcc' recipients are
            # so, the 'bcc' recipients are only added to the 'to' email address list
            to.extend(bcc)

        if html:
            msg.attach(MIMEText(content, 'html'))
        else:
            msg.attach(MIMEText(content, "plain"))

        if attachment_path:
            self._attach_file(msg, attachment_path)

        # Emails are never sent to reserved test domains
        skipped = [address for address in to if is_reserved_email_address(address)]
        if skipped:
            l.log(f"Not sending email to reserved test address(es): {', '.join(skipped)}")
            to = [address for address in to if not is_reserved_email_address(address)]
        if not to:
            return True

        self.mailserver = smtplib.SMTP(settings.MAIL_SERVER, settings.MAIL_SERVER_PORT)
        # identify ourselves to smtp gmail client
        self.mailserver.ehlo()
        # secure our email with tls encryption
        self.mailserver.starttls()
        # re-identify ourselves as an encrypted connection
        self.mailserver.ehlo()
        self.mailserver.login(settings.MAIL_USERNAME, settings.MAIL_PASSWORD)
        self.mailserver.sendmail(settings.MAIL_ADDRESS, to, msg.as_string())
        self.mailserver.quit()
        return True

    def _attach_file(self, msg, file_path):
        # 20 * 1024 * 1024  == 20 MB
        max_attachment_size = 20 * 1024 * 1024
        file_name = Path(file_path).name

        if Path(file_path).stat().st_size > max_attachment_size:
            # Compress and zip the file if its size exceeds the 20 MB limit
            zip_path = Path(file_path).with_suffix('.zip')

            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.write(file_path, arcname=file_name)

            if zip_path.stat().st_size <= max_attachment_size:
                file_path = zip_path
        
        # Attach the file to the email
        with open(file_path, 'rb') as file:
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(file.read())

        encoders.encode_base64(attachment)

        attachment.add_header(
                'Content-Disposition', f'attachment; filename={file_name}'
            )

        msg.attach(attachment)
