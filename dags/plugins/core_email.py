import argparse
import smtplib
import traceback
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os.path import basename

host_email_ip = os.env["HOST_EMAIL_IP"]
host_email_port = os.env["HOST_EMAIL_PORT"]
host_email = os.env["HOST_EMAIL"]
host_email_secret = os.env["HOST_EMAIL_SECRET"]

def send_email(send_to: str, msg_body: str, subject: str, cc_to: str = None, files: list = None):
    smtp_server = host_email_ip  # "smtp.office365.com"
    port = host_email_port # 25 / 587
    sender_email = host_email
    password = host_email_secret

    try:
        # email body
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = send_to

        if cc_to:
            message["Cc"] = cc_to

        # read msg body from file
        with open(msg_body, "r") as f:
            msg_body = f.read()

        # Create the plain-text and HTML version of your message
        text = msg_body

        html = text if "</html>" in text else f"""\
        <html>
          <body>
            <p>
            {text}
            </p>
          </body>
        </html>
        """

        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html", _charset=None)

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)
        message.attach(part2)

        # attach file to email:
        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(fil.read(), Name=basename(f))
            # After the file is closed
            part['Content-Disposition'] = f'attachment; filename="{basename(f)}"'
            message.attach(part)

        # Create a secure SSL context
        server = smtplib.SMTP(smtp_server, port)

        server.login(sender_email, password)

        # Send email
        list_send_cc_to = send_to.split(";")
        list_send_cc_to += cc_to.split(";") if cc_to else []

        server.sendmail(sender_email, list_send_cc_to, message.as_string())

    except Exception as e:
        print(f"[send_email] error: {e}")
        print(f"[send_email] traceback: {traceback.format_exc()}")
    finally:
        print("[send_email] finally")
        server.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='email tool')

    parser.add_argument('-sendto', type=str, help='list of email receipts')
    parser.add_argument('-msgbody', type=str, help='message body, html type')
    parser.add_argument('-subject', type=str, help='email subject')
    parser.add_argument('-ccto', type=str, help='cc to', default=None)
    parser.add_argument('-files', type=str, help='list of attachment files', default=None)

    args = parser.parse_args()
    send_email(send_to=args.sendto, msg_body=args.msgbody, subject=args.subject, cc_to=args.ccto,
               files=json.loads(args.files) if args.files else args.files)
