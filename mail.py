# general use imports
import email
import smtplib
# email related
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# -----------------------------------------------------------#

def attach_file_to_mail(raw_mail, message_type, file_path_dict: dict):
    '''
        This function attaches files to an email.
        It takes a dictionary of filenames and the path to them,
        As such -> {"ori.pdf": "c:/data/general_pdf.pdf", "123.pdf": "c:/data/etc.pdf"}
    '''

    for filename, file_path in file_path_dict.items():
        try:
            # Open the file in binary mode
            with open(file_path, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                file_stream = MIMEBase("application", "octet-stream")
                file_stream.set_payload(attachment.read())

            # Encode file in ASCII characters to send by email    
            encoders.encode_base64(file_stream)

            # Add header as key/value pair to attachment part
            file_stream.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )

            # Add attachment to message and convert message to string
            raw_mail.attach(file_stream)
        except FileNotFoundError:
            print(f"ERROR: No such file as {file_path} found, skipping it")
            raw_mail.attach(MIMEText("\nWARNING: A file meant to be sent could not be attached.", message_type))
        except PermissionError:
            print(f"ERROR: Permission denied to {file_path}, skipping it")
            raw_mail.attach(MIMEText("\nWARNING: A file meant to be sent could not be attached.", message_type))
    return raw_mail    



def send_mail(
        smtp_ip: str, smtp_port: int, sender_username: str, 
        sender_password: str, receiver_username: str, 
        subject: str = "", message: str = "", message_type: str = "plain", use_tls: bool = False, attachment_dict: dict = None):
    '''
        A function that sends mail.\n
        Example of use:\n
            send_mail(
                "smtp.gmail.com", 587,
                "ori@gmail.com", "gever", 
                "receiver@gmail.com", 
                "example_subject", "example_test", 
                attachment_dict={"example.pdf": "./data/random.pdf"})
        The function will use the given info to create a mail object, 
        then open a connection the the smtp server and send it.

        Params
        ------
            1.  smtp_ip - A string of the server's DNS/hostname/IP you'll send the mail to. 
                Example -> "smtp.google.com"
            2.  smtp_port - An int of the server's port you'll send the mail to. 
                The usual ports are 25 or 465 or 587 or 2525.
            3.  sender_username - Self-explanatory.
            4.  sender_password - Self-explanatory.
            5.  receiver_username - Self-explanatory.
            6.  subject - The mail's subject will be added to it. Defaults to an empty string.
            7.  message - The body of the mail. defaults to an empty string.
            8.  message_type - decides the body's styling. Either plain or html.
                Defaults to plain.
            9.  use_tls - If it is True, the client will attempt to use tls around the connection. 
                Defaults to False.
            10. attachment_dict - add a dictionary where the keys are the names of the 
                file attachments desired, and the value is the path to the file on the client.
                Example -> {"my_attachment.txt": "c:/data/random_file.txt"}
    '''

    # Creating a mail object and formatting its "headers".
    raw_mail = MIMEMultipart()
    raw_mail["From"] = sender_username
    raw_mail["To"] = receiver_username
    raw_mail["Subject"] = subject

    # Confirms the message type is either plain text, or with html formatting.
    # Then attaches the message to the mail object's body.
    if message_type != "html":
        message_type = "plain"
    raw_mail.attach(MIMEText(message, message_type))

    # If a dictionary was added, an attempt will be made to attach the files.
    if attachment_dict != None:
        raw_mail = attach_file_to_mail(raw_mail=raw_mail, message_type=message_type,file_path_dict=attachment_dict)

    # The email object is turned to a string to be sent as the message later.
    ready_mail = raw_mail.as_string()
    
    # A connection to the smtp server is made, if specified it will use tls.
    with smtplib.SMTP(smtp_ip, smtp_port) as mail_server:
        if use_tls == True:
            mail_server.starttls()
        mail_server.login(user=sender_username, password=sender_password)
        mail_server.sendmail(from_addr=sender_username, to_addrs=receiver_username , msg=ready_mail)
    
    return ready_mail