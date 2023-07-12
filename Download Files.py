import pandas as pd
import pysftp
import os
import shutil
import glob
import pandas
import logging
import smtplib
import ssl
import imaplib
import datetime
import time
import numpy as np
import phonenumbers

from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from jinja2 import Environment
from datetime import datetime
from email_validator import validate_email, EmailNotValidError

def set_directory():
    global owd

    owd = os.getcwd()
    os.chdir(owd)

def load_env():
    logging.info('Setting Environment variables')

    global HOST, PORT, USERNAME, PASSWORD, SOURCE, MAIL_USERN, MAIL_PASSWORD, IMAP_URL, IMAP_PORT, SMTP_URL, SMTP_PORT, SEND_TO

    load_dotenv()

    # Retrieve contents from .env file
    HOST = os.getenv("HOST")
    PORT = int(os.getenv("PORT"))  # Change port from string to integer
    USERNAME = os.getenv("USERN")
    PASSWORD = os.getenv("PASSWORD")
    SOURCE = os.getenv("SOURCE_PATH")
    MAIL_USERN = os.getenv('MAIL_USERN')
    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD')
    IMAP_URL = os.getenv('IMAP_URL')
    IMAP_PORT = os.getenv('IMAP_PORT')
    SMTP_URL = os.getenv('SMTP_URL')
    SMTP_PORT = os.getenv('SMTP_PORT')
    SEND_TO = os.getenv('SEND_TO')

def start_logging():
    global process_name

    # Get File Name of existing script
    process_name = os.path.basename(__file__).replace('.py', '').replace(' ', '_')

    logging.basicConfig(filename=f'Logs/{process_name}.log', format='%(asctime)s %(levelname)s %(process)d %(message)s', filemode='w', level=logging.INFO)

    # Printing the output to file for debugging
    logging.info('Starting the Script')

def stop_logging():
    logging.info('Stopping the Script')

def housekeeping():
    logging.info('Doing Housekeeping')

    # Change Directory
    os.chdir('Files')

    # Housekeeping
    multiple_files = glob.glob('*.csv')

    # Iterate over the list of filepaths & remove each file.
    logging.info('Removing old CSV files')
    for each_file in multiple_files:
        try:
            os.remove(each_file)
        except:
            pass

    # Change Directory
    os.chdir(owd)

def connect_to_sftp():
    logging.info('Connecting to SFTP Server')

    # Change Directory
    os.chdir('Files')

    # Connect to Download files from SFTP Server
    with pysftp.Connection(host=HOST, port=PORT, username=USERNAME, password=PASSWORD) as sftp:
        # Change directory in remote
        with sftp.cd(SOURCE):
            # List all files in that directory
            files = sftp.listdir()
            logging.info('Downloading files from server')
            for each_file in files:
                # Download file
                sftp.get(each_file)
                # Remove file
                # sftp.remove(each_file)

    # Change Directory
    os.chdir(owd)

def create_dataframe():
    logging.info('Creating a Dataframe of the Stay Connected files')

    multiple_files = glob.glob('Files/*.csv')

    data = pd.DataFrame()

    # Iterate over the list of filepaths & load each file.
    for each_file in multiple_files:
        df = pd.read_csv(each_file, on_bad_lines='skip', engine='python')
        data = pd.concat([data, df])

    data = data.drop_duplicates().reset_index(drop=True).copy()

    # Data Processing
    data = pre_process(data).copy()

    # Save to a file
    data.drop_duplicates().reset_index(drop=True).to_parquet('Database/Stay Connected.parquet', index=False)

    # Change Directory
    os.chdir(owd)

def pre_process(data):
    # Remove column
    data = data.drop(columns=['Unnamed: 27']).copy()

    # Country code
    data['countrycode'] = data['countrycode'].fillna(0)
    data['countrycode'] = data['countrycode'].astype(int)
    data['countrycode'] = data['countrycode'].replace(0, '')

    # Unnecessary Email columns
    data = data.drop(columns=['email1', 'email2', 'email3', 'email4', 'email5']).copy()

    # Emails
    data['email'] = data['email'].apply(lambda x: check_email(x))
    data['alternateemail'] = data['alternateemail'].apply(lambda x: check_email(x))

    # Country code
    data['countrycode'] = data['countrycode'].replace('', np.NaN)
    data['countrycode'] = data[['country', 'contact', 'countrycode']].apply(lambda x: add_missing_country_code(*x),
                                                                            axis=1)
    data['countrycode'] = data['countrycode'].fillna(0)
    data['countrycode'] = data['countrycode'].astype(int)

    # Phone
    data['phone'] = data['countrycode'].replace(0, '').astype(str) + data['contact'].astype(str)
    data['phone'] = data['phone'].replace('nan', np.NaN)

    # Remove columns
    data = data.drop(columns=['countrycode', 'contact']).copy()

    # Phones
    data['phone'] = data['phone'].astype(str)
    data['phone'] = data[['phone', 'country']].apply(lambda x: check_phone(*x), axis=1)

    # Batch
    data['batch'] = data['batch'].fillna(0)
    data['batch'] = data['batch'].astype(int)

    # Department
    data['dept'] = data['dept'].replace('Other', np.NaN)

    # Position
    data['position'] = data['position'].apply(lambda x: str(x)[-50:])
    data['position'] = data['position'].astype(str).replace('nan', np.NaN)

    # Clean LinkedIn URL
    data['linkedIn'] = data['linkedIn'].apply(lambda x: clean_linkedin(x))

    # Drop Columns
    data = data.drop(columns=['status', 'created_on', 'interest', 'concentdetails']).copy()

    return data

def check_email(email):
    email = str(email).strip()

    try:
        # Validate.
        valid = validate_email(email, check_deliverability=False, dns_resolver=None, allow_quoted_local=True)

        # Update with the normalized form.
        email = valid.email
        return email
    except: return np.NaN

def add_missing_country_code(country, phone, code):
    if country == 'India' and pd.isnull(code) and len(str(phone)) == 10: return 91
    else: return code

def check_phone(phone, country):
    phone = ''.join(filter(str.isdigit, phone))

    if country == 'India': phone = str(phone)[-12:]

    try:
        phone = '+' + str(phone).replace('+','')
        phone = phonenumbers.parse(phone)

        if phonenumbers.is_possible_number(phone): return phonenumbers.format_number(phone, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        else: return np.NaN
    except: return np.NaN

def clean_linkedin(url):
    url = str(url)

    if 'linkedin' in url:
        url = url.replace('https://', '').replace('http://', '').replace('www.', '')
        url = url.split('?', 1)[0]
        if url[-1] == '/': url = url[0:len(url)-1]
        return url
    else: return np.NaN

def send_error_emails(subject):
    logging.info('Sending email for an error')

    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = MAIL_USERN
    message["To"] = SEND_TO

    # Adding Reply-to header
    message.add_header('reply-to', MAIL_USERN)

    TEMPLATE = """
    <table style="background-color: #ffffff; border-color: #ffffff; width: auto; margin-left: auto; margin-right: auto;">
    <tbody>
    <tr style="height: 127px;">
    <td style="background-color: #363636; width: 100%; text-align: center; vertical-align: middle; height: 127px;">&nbsp;
    <h1><span style="color: #ffffff;">&nbsp;Raiser's Edge Automation: {{job_name}} Failed</span>&nbsp;</h1>
    </td>
    </tr>
    <tr style="height: 18px;">
    <td style="height: 18px; background-color: #ffffff; border-color: #ffffff;">&nbsp;</td>
    </tr>
    <tr style="height: 18px;">
    <td style="width: 100%; height: 18px; background-color: #ffffff; border-color: #ffffff; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #455362;">This is to notify you that execution of Auto-updating Alumni records has failed.</span>&nbsp;</td>
    </tr>
    <tr style="height: 18px;">
    <td style="height: 18px; background-color: #ffffff; border-color: #ffffff;">&nbsp;</td>
    </tr>
    <tr style="height: 61px;">
    <td style="width: 100%; background-color: #2f2f2f; height: 61px; text-align: center; vertical-align: middle;">
    <h2><span style="color: #ffffff;">Job details:</span></h2>
    </td>
    </tr>
    <tr style="height: 52px;">
    <td style="height: 52px;">
    <table style="background-color: #2f2f2f; width: 100%; margin-left: auto; margin-right: auto; height: 42px;">
    <tbody>
    <tr>
    <td style="width: 50%; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #ffffff;">Job :</span>&nbsp;</td>
    <td style="background-color: #ff8e2d; width: 50%; text-align: center; vertical-align: middle;">&nbsp;{{job_name}}&nbsp;</td>
    </tr>
    <tr>
    <td style="width: 50%; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #ffffff;">Failed on :</span>&nbsp;</td>
    <td style="background-color: #ff8e2d; width: 50%; text-align: center; vertical-align: middle;">&nbsp;{{current_time}}&nbsp;</td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    <tr style="height: 18px;">
    <td style="height: 18px; background-color: #ffffff;">&nbsp;</td>
    </tr>
    <tr style="height: 18px;">
    <td style="height: 18px; width: 100%; background-color: #ffffff; text-align: center; vertical-align: middle;">Below is the detailed error log,</td>
    </tr>
    <tr style="height: 217.34375px;">
    <td style="height: 217.34375px; background-color: #f8f9f9; width: 100%; text-align: left; vertical-align: middle;">{{error_log_message}}</td>
    </tr>
    </tbody>
    </table>
    """

    # Create a text/html message from a rendered template
    emailbody = MIMEText(
        Environment().from_string(TEMPLATE).render(
            job_name=subject,
            current_time=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            error_log_message=Argument
        ), "html"
    )

    # Add HTML parts to MIMEMultipart message
    # The email client will try to render the last part first
    try:
        message.attach(emailbody)
        attach_file_to_email(message, f'Logs/{process_name}.log')
        emailcontent = message.as_string()

    except:
        message.attach(emailbody)
        emailcontent = message.as_string()

    # Create secure connection with server and send email
    context = ssl._create_unverified_context()
    with smtplib.SMTP_SSL(SMTP_URL, SMTP_PORT, context=context) as server:
        server.login(MAIL_USERN, MAIL_PASSWORD)
        server.sendmail(
            MAIL_USERN, SEND_TO, emailcontent
        )

    # Save copy of the sent email to sent items folder
    with imaplib.IMAP4_SSL(IMAP_URL, IMAP_PORT) as imap:
        imap.login(MAIL_USERN, MAIL_PASSWORD)
        imap.append('Sent', '\\Seen', imaplib.Time2Internaldate(time.time()), emailcontent.encode('utf8'))
        imap.logout()

def attach_file_to_email(message, filename):
    logging.info('Attach file to email')

    # Open the attachment file for reading in binary mode, and make it a MIMEApplication class
    with open(filename, "rb") as f:
        file_attachment = MIMEApplication(f.read())

    # Add header/name to the attachments
    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename.replace('Logs', '')}",
    )

    # Attach the file to the message
    message.attach(file_attachment)

try:
    # Set current directory
    set_directory()

    # Start Logging
    start_logging()

    # Load ENV
    load_env()

    # Housekeeping
    housekeeping()

    # Connect to SFTP Server and Download files
    connect_to_sftp()

    # Merge to a single CSV
    create_dataframe()

except Exception as Argument:
    logging.error(Argument)

    # Send emails
    send_error_emails('Downloading files for Stay Connected updates')

finally:
    # Housekeeping
    housekeeping()

    # Stop Logging
    stop_logging()

    exit()
