import pandas as pd
import pysftp
import os
import glob
import logging
import datetime
import numpy as np
import phonenumbers
import csv
import base64
import msal
import requests

from dotenv import load_dotenv
from datetime import datetime
from email_validator import validate_email, EmailNotValidError

def set_directory():
    global owd

    owd = os.getcwd()
    os.chdir(owd)

def load_env():
    logging.info('Setting Environment variables')

    global HOST, PORT, USERNAME, PASSWORD, SOURCE, O_CLIENT_ID, CLIENT_SECRET, TENANT_ID, FROM, CC_TO, ERROR_EMAILS_TO, SEND_TO

    load_dotenv()

    # Retrieve contents from .env file
    HOST = os.getenv("HOST")
    PORT = int(os.getenv("PORT"))  # Change port from string to integer
    USERNAME = os.getenv("USERN")
    PASSWORD = os.getenv("PASSWORD")
    SOURCE = os.getenv("SOURCE_PATH")
    O_CLIENT_ID = os.getenv('O_CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    TENANT_ID = os.getenv('TENANT_ID')
    FROM = os.getenv('FROM')
    SEND_TO = os.getenv('SEND_TO')
    CC_TO = eval(os.getenv('CC_TO'))
    ERROR_EMAILS_TO = eval(os.getenv('ERROR_EMAILS_TO'))

def start_logging():
    global process_name

    # Get File Name of existing script
    process_name = os.path.basename(__file__).replace('.py', '').replace(' ', '_')

    logging.basicConfig(filename=f'Logs/{process_name}.log', format='%(asctime)s %(levelname)s %(process)d %(message)s', filemode='w', level=logging.DEBUG)

    # Printing the output to file for debugging
    logging.info('Starting the Script')

def stop_logging():
    logging.info('Stopping the Script')

def housekeeping():
    logging.info('Doing Housekeeping')

    # Change Directory
    os.chdir(owd)
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

    # Change Directory
    os.chdir(owd)

def create_dataframe():
    logging.info('Creating a Dataframe of the Stay Connected files')

    multiple_files = glob.glob('Files/*.csv')

    data = pd.DataFrame()

    # Iterate over the list of filepaths & load each file.
    for each_file in multiple_files:
        df = pd.read_csv(each_file, quoting=csv.QUOTE_NONE)
        data = pd.concat([data, df])

    data = data.drop_duplicates().reset_index(drop=True).copy()

    # Data Processing
    data = pre_process(data).copy()
    data = data.drop_duplicates().reset_index(drop=True).copy()

    return data

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

    # Convert object to Datetime
    data['created_on'] = pd.to_datetime(data['created_on'], format='%m/%d/%Y %I:%M:%S %p')

    # Drop Columns
    data = data.drop(columns=['status', 'interest', 'concentdetails']).copy()

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

    authority = f'https://login.microsoftonline.com/{TENANT_ID}'

    app = msal.ConfidentialClientApplication(
        client_id=O_CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=authority
    )

    scopes = ["https://graph.microsoft.com/.default"]

    result = None
    result = app.acquire_token_silent(scopes, account=None)

    if not result:
        result = app.acquire_token_for_client(scopes=scopes)

        TEMPLATE = """
        <table style="background-color: #ffffff; border-color: #ffffff; width: auto; margin-left: auto; margin-right: auto;">
        <tbody>
        <tr style="height: 127px;">
        <td style="background-color: #363636; width: 100%; text-align: center; vertical-align: middle; height: 127px;">&nbsp;
        <h1><span style="color: #ffffff;">&nbsp;Raiser's Edge Automation: {job_name} Failed</span>&nbsp;</h1>
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
        <td style="background-color: #ff8e2d; width: 50%; text-align: center; vertical-align: middle;">&nbsp;{job_name}&nbsp;</td>
        </tr>
        <tr>
        <td style="width: 50%; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #ffffff;">Failed on :</span>&nbsp;</td>
        <td style="background-color: #ff8e2d; width: 50%; text-align: center; vertical-align: middle;">&nbsp;{current_time}&nbsp;</td>
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
        <td style="height: 217.34375px; background-color: #f8f9f9; width: 100%; text-align: left; vertical-align: middle;">{error_log_message}</td>
        </tr>
        </tbody>
        </table>
        """
    
        # Create a text/html message from a rendered template
        emailbody = TEMPLATE.format(
            job_name=subject,
            current_time=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            error_log_message=Argument
        )

        # Set up attachment data
        with open(f'Logs/{process_name}.log', 'rb') as f:
            attachment_content = f.read()
        attachment_content = base64.b64encode(attachment_content).decode('utf-8')

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': subject,
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': emailbody
                    },
                    'ToRecipients': get_recipients(ERROR_EMAILS_TO),
                    'Attachments': [
                        {
                            '@odata.type': '#microsoft.graph.fileAttachment',
                            'name': f'{process_name}.log',
                            'contentBytes': attachment_content
                        }
                    ]
                },
                'SaveToSentItems': 'true'
            }

            requests.post(
                endpoint,
                headers={
                    'Authorization': 'Bearer ' + result['access_token']
                },
                json=email_msg
            )

        else:
            logging.info(result.get('error'))
            logging.info(result.get('error_description'))
            logging.info(result.get('correlation_id'))

def get_recipients(email_list):
    value = []

    for email in email_list:
        email = {
            'emailAddress': {
                'address': email
            }
        }

        value.append(email)

    return value

def find_remaining_data(all_df, partial_df):
    logging.info('Identifying missing data between two Dataframes')

    # Change directory
    os.chdir(owd)

    # Identify data present in all_df but missing in partial_df
    remaining_data = all_df[~all_df['id'].isin(partial_df['id'])].copy()

    # Change the datetime format

    remaining_data.to_parquet('Database/To be uploaded.parquet', index=False)

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
    data = create_dataframe().copy()

    # Load data that's already uploaded
    completed = pd.read_parquet('Database/Completed.parquet')

    # Find data to be uploaded
    find_remaining_data(data, completed)

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
