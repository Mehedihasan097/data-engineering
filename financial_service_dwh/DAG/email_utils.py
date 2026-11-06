# dags/utils/email_utils.py
# email_utils.py
import os
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_custom_email(to_emails, subject, html_content, cc_emails=None, from_email=None, attachment_path=None):
    to_emails_str = ", ".join(to_emails) if isinstance(to_emails, list) else to_emails
    cc_emails_str = ", ".join(cc_emails) if isinstance(cc_emails, list) else (cc_emails or "")

    # Initialize Airflow SMTP Hook
    smtp_hook = SmtpHook(smtp_conn_id='smtp_default')
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['To'] = to_emails_str
    
    if cc_emails_str:
        msg['Cc'] = cc_emails_str
    if from_email:
        msg['From'] = from_email

    msg.attach(MIMEText(html_content, 'html'))

    # Handle file attachment if path is provided
    if attachment_path and os.path.exists(attachment_path):
        try:
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            filename = os.path.basename(attachment_path)
            part.add_header("Content-Disposition", f"attachment; filename= {filename}")
            msg.attach(part)
        except Exception as e:
            print(f"⚠️ Warning: Failed to attach file: {e}")

    all_recipients = [email.strip() for email in to_emails_str.split(',')]
    if cc_emails_str:
        all_recipients.extend([email.strip() for email in cc_emails_str.split(',')])

    # FIX: Use 'with smtp_hook as smtp:' to properly initialize and open the client session
    try:
        with smtp_hook as smtp:
            smtp.send_smtp_message(
                to=all_recipients,
                subject=subject,
                html_content=msg.as_string()
            )
        print(f"✅ Email successfully sent to {to_emails_str}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        raise e