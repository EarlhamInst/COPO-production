import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from django.conf import settings


class CopoEmail:

    def send(self, to, sub, content, html=False, is_cc_required=False, cc=list(), is_bcc_required=False, bcc=list()):
        msg = MIMEMultipart()
        msg['From'] = settings.MAIL_ADDRESS

        msg['Subject'] = sub

        if is_cc_required:
            msg['CC'] = ",".join(cc) # Convert list to string
            to.extend(cc)
           
        if is_bcc_required:
            # 'bcc' recipients cannot be added to 'msg' multipart/* type message
            # because the 'to' and 'cc' receivers will know  who the 'bcc' recipients are 
            # so, the 'bcc' recipients are only added to the 'to' email address list
            to.extend(bcc)
            
        if html:
            msg.attach(MIMEText(content, 'html'))
        else:
            msg.attach(MIMEText(content, "plain"))

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

