import requests
import os
import json
import re
import datetime
import logging
import random
import string
import msal
import base64
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from dotenv import load_dotenv
from fuzzywuzzy import process
from nameparser import HumanName

def set_directory():
    global owd

    owd = os.getcwd()
    os.chdir(owd)

def load_env():
    logging.info('Setting Environment variables')

    global O_CLIENT_ID, CLIENT_SECRET, TENANT_ID, FROM, CC_TO, ERROR_EMAILS_TO, SEND_TO, RE_API_KEY

    load_dotenv()

    # Retrieve contents from .env file
    RE_API_KEY = os.getenv('RE_API_KEY')
    O_CLIENT_ID = os.getenv('O_CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    TENANT_ID = os.getenv('TENANT_ID')
    FROM = os.getenv('FROM')
    SEND_TO = eval(os.getenv('SEND_TO'))
    CC_TO = eval(os.getenv('CC_TO'))
    ERROR_EMAILS_TO = eval(os.getenv('ERROR_EMAILS_TO'))
    RE_API_KEY = os.getenv('RE_API_KEY')

def start_logging():
    global process_name

    # Get File Name of existing script
    process_name = os.path.basename(__file__).replace('.py', '').replace(' ', '_')

    logging.basicConfig(filename=f'Logs/{process_name}.log', format='%(asctime)s %(levelname)s %(process)d %(message)s', filemode='w', level=logging.DEBUG)

    # Printing the output to file for debugging
    logging.info('Starting the Script')

def stop_logging():
    logging.info('Stopping the Script')

def set_api_request_strategy():
    logging.info('Setting API Request strategy')

    global http

    # API Request strategy
    logging.info('Setting API Request Strategy')

    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['HEAD', 'GET', 'OPTIONS'],
        backoff_factor=10
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount('https://', adapter)
    http.mount('http://', adapter)

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

def retrieve_token():

    logging.info('Retrieve token for API connections')

    with open('access_token_output.json') as access_token_output:
        data = json.load(access_token_output)
        access_token = data['access_token']

    return access_token

def get_request_re(url, params):
    logging.info('Running GET Request from RE function')

    # Retrieve access_token from file
    access_token = retrieve_token()

    # Request headers
    headers = {
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + access_token,
    }

    re_api_response = http.get(url, params=params, headers=headers)

    return re_api_response

def post_request_re(url, params):
    logging.info('Running POST Request to RE function')

    # Retrieve access_token from file
    access_token = retrieve_token()

    # Request headers
    headers = {
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json',
    }

    re_api_response = http.post(url, params=params, headers=headers, json=params)

    if not re_api_response.ok:
        raise Exception

    return re_api_response

def patch_request_re(url, params):
    logging.info('Running PATCH Request to RE function')

    # Retrieve access_token from file
    access_token = retrieve_token()

    # Request headers
    headers = {
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json'
    }

    re_api_response = http.patch(url, headers=headers, data=json.dumps(params))

    return re_api_response

def add_tags(source, tag, update, constituent_id):
    logging.info('Adding Tags to constituent record')

    params = {
        'category': tag,
        'comment': update,
        'parent_id': constituent_id,
        'value': source,
        'date': attribute_date
    }

    url = 'https://api.sky.blackbaud.com/constituent/v1/constituents/customfields'

    post_request_re(url, params)

def search_alum(email):
    logging.info(f'Searching for Alum with email address: {email}')

    url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/search?search_field=email_address&search_text={email}'

    params = {}

    api_response = get_request_re(url, params).json()

    df = api_to_df(api_response).copy()

    if not df.empty:
        return df['id'].values
    else:
        return []

def api_to_df(response):
    logging.info('Loading API response to a DataFrame')

    # Load from JSON to pandas
    try:
        api_response = pd.json_normalize(response['value'])
    except:
        try:
            api_response = pd.json_normalize(response)
        except:
            api_response = pd.json_normalize(response, 'value')

    # Load to a dataframe
    df = pd.DataFrame(data=api_response)

    return df

def update_emails(each_row, re_id):
    logging.info('Proceeding to update email')

    email_1 = each_row['email']
    email_2 = each_row['alternateemail']

    # Converting to lowercase
    email_1 = str(email_1).lower()
    email_2 = str(email_2).lower()

    # Email 1
    if email_1:

        # Get Email address present in RE
        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{re_id}/emailaddresses'
        params = {}

        api_response = get_request_re(url, params).json()

        # Load to Dataframe
        re_data = api_to_df(api_response).copy()

        # Mark as Primary
        re_data = re_data[['id', 'address']].drop_duplicates('address').copy()

        # Converting all emails to lowercase
        re_data['address'] = re_data['address'].apply(lambda x: str(x).lower())

        email_address_id = re_data[re_data['address'] == email_1]['id'].values[0]

        url = f'https://api.sky.blackbaud.com/constituent/v1/emailaddresses/{email_address_id}'

        params = {
            'primary': True
        }

        patch_request_re(url, params)

        # Update Sync tags
        add_tags('Stay Connected - Auto | Email address', 'Sync source', email_1, re_id)

        # Update Verified Tags
        add_tags(email_1, 'Verified Email', 'Stay Connected', re_id)

        # Email 2
        if email_2 != '':

            # Check if email address already exists
            emails_in_re = re_data['address'].to_list()

            if email_2 not in emails_in_re:

                # Upload Email to RE
                params = {
                    'address': email_2,
                    'constituent_id': re_id,
                    'type': 'Email'
                }

                url = 'https://api.sky.blackbaud.com/constituent/v1/emailaddresses'

                post_request_re(url, params)

                # Update Sync tags
                add_tags('Stay Connected - Auto | Email address', 'Sync source', email_2, re_id)

                # Update Verified Tags
                add_tags(email_2, 'Verified Email', 'Stay Connected', re_id)

def update_phones(each_row, re_id):
    logging.info('Proceeding to update phone')

    phone = each_row['phone']

    if phone:

        # Get Phone numbers present in RE
        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{re_id}/phones'
        params = {}

        api_response = get_request_re(url, params).json()

        # Load to Dataframe
        re_data = api_to_df(api_response).copy()

        if re_data.shape[0] != 0:
            # Check whether it exists
            # Remove non-numeric characters
            re_data['number'] = re_data['number'].apply(lambda x: re.sub(r'[^0-9]', '', x))
            new_phone = re.sub(r'[^0-9]', '', phone)

            phones_in_re = re_data['number'].to_list()

            # Adding last 10 characters of phone to the list as well
            counter = len(phones_in_re)
            i = 0

            for p in phones_in_re:
                i += 1
                phones_in_re.append(p[-10:])

                if i == counter:
                    break

            # If exists, mark as primary
            if new_phone in phones_in_re or new_phone[-10:] in phones_in_re:
                re_data = re_data[['id', 'number']].drop_duplicates('number').copy()

                phone_id = re_data[
                    re_data['number'].str.contains(new_phone[10:])
                ]['id'].values[0]

                url = f'https://api.sky.blackbaud.com/constituent/v1/phones/{int(phone_id)}'

                params = {
                    'primary': True,
                    'number': phone
                }

                patch_request_re(url, params)

            # Else, add in RE
            else:
                params = {
                    'constituent_id': re_id,
                    'number': phone,
                    'primary': True,
                    'type': 'Mobile'
                }

                url = 'https://api.sky.blackbaud.com/constituent/v1/phones'

                post_request_re(url, params)

        # Else, add in RE
        else:
            params = {
                'constituent_id': re_id,
                'number': phone,
                'primary': True,
                'type': 'Mobile'
            }

            url = 'https://api.sky.blackbaud.com/constituent/v1/phones'

            post_request_re(url, params)

        # Update Sync tags
        add_tags('Stay Connected - Auto | Phone', 'Sync source', phone, re_id)

        # Update Verified Tags
        add_tags(phone, 'Verified Phone', 'Stay Connected', re_id)

def update_location(each_row, re_id):
    logging.info('Proceeding to update location')

    city = each_row['city']
    state = each_row['state']
    country = each_row['country']

    # Remove non-alphabetic characters
    city = re.sub('[^a-zA-Z ]+', '', city)
    state = re.sub('[^a-zA-Z ]+', '', state)
    country = re.sub('[^a-zA-Z ]+', '', country)

    if country != '' or ~(country == 'India' and city == '' and state == ''):
        address = str(city) + ', ' + str(state) + ', ' + str(country)
        address = address.replace('nan', '').strip().replace(', ,', ', ')

        try:
            location = geolocator.geocode(address, addressdetails=True, language='en')

            if location is None:
                i = 0
            else:
                i = 1

            while i == 0:
                address_split = address[address.index(' ') + 1:]
                address = address_split
                location = geolocator.geocode(address_split, addressdetails=True, language='en')

                if location is None:
                    address = address_split
                    try:
                        if address == '':
                            break
                    except:
                        break
                    i = 0

                if location is not None:
                    break

            address = location.raw['address']

            try:
                city = address.get('city', '')
                if city == '':
                    try:
                        city = address.get('state_district', '')
                        if city == '':
                            try:
                                city = address.get('county', '')
                            except:
                                city = ''
                    except:
                        try:
                            city = address.get('county', '')
                        except:
                            city = ''
            except:
                try:
                    city = address.get('state_district', '')
                    if city == '':
                        try:
                            city = address.get('county', '')
                        except:
                            city = ''
                except:
                    try:
                        city = address.get('county', '')
                    except:
                        city = ''

            state = address.get('state', '')
            country = address.get('country', '')

        except:
            pass

        url = 'https://api.sky.blackbaud.com/constituent/v1/addresses'

        # Ignore state for below countries
        if country == 'Mauritius' or country == 'Switzerland' or country == 'France' or country == 'Bahrain':
            state = ''

        params = {
            'city': city,
            'state': state,
            'county': state,
            'country': country,
            'constituent_id': re_id,
            'type': 'Home',
            'preferred': True
        }

        # Delete blank values from JSON
        for i in range(10):
            params = del_blank_values_in_json(params.copy())

        try:
            api_response = post_request_re(url, params).json()

            # Update Sync tags
            address = str(city + ', ' + state + ', ' + country).replace(', ,', ', ').strip()
            add_tags('Stay Connected - Auto | Location', 'Sync source', address, re_id)

            # Update Verified Tags
            add_tags(address, 'Verified Location', 'Stay Connected', re_id)

        except:
            if 'county of value' in str(api_response).lower():
                add_county(state)
                post_request_re(url, params)

                # Update Sync tags
                address = str(city + ', ' + state + ', ' + country).replace(', ,', ', ').strip()
                add_tags('Stay Connected - Auto | Location', 'Sync source', address, re_id)

                # Update Verified Tags
                add_tags(address, 'Verified Location', 'Stay Connected', re_id)
            else:
                raise Exception(f'API returned an error: {api_response}')

def add_county(county):
    # counties = 5001
    # States = 5049
    i = 0
    code_table_ids = [5001, 5049]

    for code_table_id in code_table_ids:

        if i == 1:

            now = datetime.datetime.now()

            # Generate either a 2-digit or 3-digit number randomly
            if random.random() < 0.5:
                unique_num = int(now.strftime('%j%H%M%S%f')[:9])
            else:
                unique_num = int(now.strftime('%j%H%M%S%f')[:10])

            # Generate a random suffix character (either an alphabet or a special character)
            suffix_char = random.choice(string.ascii_lowercase + string.digits + '!@#$%^&*()')

            # Concatenate the unique number and the suffix character
            short_description = str(unique_num % 1000) + suffix_char

            if len(short_description) > 3:
                short_description = short_description[1:]

        else:
            short_description = ''

        url = f'https://api.sky.blackbaud.com/nxt-data-integration/v1/re/codetables/{code_table_id}/tableentries'

        params = {
            'long_description': county,
            'short_description': short_description
        }

        # Delete blank values from JSON
        for i in range(10):
            params = del_blank_values_in_json(params.copy())

        post_request_re(url, params)

        i += 1

def del_blank_values_in_json(d):
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    for key, value in list(d.items()):
        if value == "" or value == {} or value == [] or value == [""]:
            del d[key]
        elif isinstance(value, dict):
            del_blank_values_in_json(value)
    return d

def initialize_nominatim():
    logging.info('Initialize Nominatim API for Geocoding')

    global geolocator, geocode

    # initialize Nominatim API
    geolocator = Nominatim(user_agent="geoapiExercises")

    # adding 1 second padding between calls
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, return_value_on_exception=None)

def update_employment(each_row, re_id):
    logging.info('Proceeding to update employment')

    org = each_row['organization']
    position = each_row['position']

    if org:
        # Get existing relationships in RE

        # Get relationship from RE
        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{re_id}/relationships'
        params = {}

        # API request
        api_response = get_request_re(url, params).json()

        # Load to Dataframe
        re_data = api_to_df(api_response).copy()

        if re_data.shape[0] != 0:
            # Get list of employees in RE
            re_employer_list = re_data['name'].to_list()

            best_match = process.extractOne(org, re_employer_list)

            if best_match[1] >= 90:
                relationship_id = re_data[re_data['name'] == best_match[0]]['id'].values[0]

                url = f'https://api.sky.blackbaud.com/constituent/v1/relationships/{int(relationship_id)}'

                params = {
                    'is_primary_business': True,
                    'position': position
                }

                # Delete blank values from JSON
                for i in range(10):
                    params = del_blank_values_in_json(params.copy())

                patch_request_re(url, params)

            else:
                # Add new employment in RE

                # Check if organisation is a University
                school_matches = ['school', 'college', 'university', 'institute', 'iit', 'iim']

                if any(x in str(org).lower() for x in school_matches):
                    relationship = 'University'

                else:
                    relationship = 'Employer'

                params = {
                    'constituent_id': re_id,
                    'relation': {
                        'name': str(org)[:60],
                        'type': 'Organization'
                    },
                    'position': position,
                    'type': relationship,
                    'reciprocal_type': 'Employee',
                    'is_primary_business': True
                }

                # Delete blank values from JSON
                for i in range(10):
                    params = del_blank_values_in_json(params.copy())

                url = 'https://api.sky.blackbaud.com/constituent/v1/relationships'

                # Update in RE
                post_request_re(url, params)

        else:
            # Add new employment in RE

            # Check if organisation is a University
            school_matches = ['school', 'college', 'university', 'institute', 'iit', 'iim']

            if any(x in str(org).lower() for x in school_matches):
                relationship = 'University'

            else:
                relationship = 'Employer'

            params = {
                'constituent_id': re_id,
                'relation': {
                    'name': str(org)[:60],
                    'type': 'Organization'
                },
                'position': position,
                'type': relationship,
                'reciprocal_type': 'Employee',
                'is_primary_business': True
            }

            # Delete blank values from JSON
            for i in range(10):
                params = del_blank_values_in_json(params.copy())

            url = 'https://api.sky.blackbaud.com/constituent/v1/relationships'

            # Update in RE
            post_request_re(url, params)

        # Update Tags
        add_tags('Stay Connected - Auto | Employment', 'Sync source', str(org)[:50], re_id)

def update_linkedin(each_row, re_id):
    logging.info('Proceeding to update LinkedIn')

    # Get the new data
    linkedin = each_row['linkedIn']

    if linkedin:

        params = {
            'address': linkedin,
            'constituent_id': re_id,
            'primary': True,
            'type': 'LinkedIn'
        }

        url = 'https://api.sky.blackbaud.com/constituent/v1/onlinepresences'

        # Update in RE
        post_request_re(url, params)

        # Update Tags
        add_tags('Stay Connected - Auto | Online Presence', 'Sync source', str(linkedin)[:50], re_id)

def update_chapter(each_row, re_id):
    logging.info('Proceeding to update chapter')

    chapter = each_row['chapter']

    if chapter:
        url = 'https://api.sky.blackbaud.com/constituent/v1/constituents/customfields'

        params = {
            'category': 'Chapter',
            'value': chapter,
            'comment': 'Stay Connected',
            'date': datetime.now().replace(microsecond=0).isoformat(),
            'parent_id': re_id
        }

        try:
            post_request_re(url, params)
        except:
            pass

def update_education(each_row, re_id):
    logging.info('Proceeding to update Education')

    class_of = each_row['batch']
    department = each_row['dept']
    hostel = each_row['hostel']

    if class_of != '' or department != '' or hostel != '':
        # Get education present in RE
        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{re_id}/educations'
        params = {}

        api_response = get_request_re(url, params).json()

        # Load to a dataframe
        re_data = api_to_df(api_response).copy()

        # Check if any education data exists
        if not re_data.empty:

            re_data = re_data[re_data['school'] == 'Indian Institute of Technology Bombay'].reset_index(drop=True)

            if re_data.shape[0] == 1:

                education_id = int(re_data['id'][0])

                try:
                    re_class_of = int(re_data['class_of'][0])
                except:
                    re_class_of = class_of

                # Check if existing Class of is blank or invalid
                if class_of != '' and class_of == re_class_of:

                    url = f'https://api.sky.blackbaud.com/constituent/v1/educations/{education_id}'

                    params = {
                        'class_of': class_of,
                        'date_graduated': {
                            'y': class_of
                        },
                        'date_left': {
                            'y': class_of
                        },
                        'majors': [
                            department
                        ],
                        'social_organization': hostel
                    }

                    # Delete blank values from JSON
                    for i in range(10):
                        params = del_blank_values_in_json(params.copy())

                    if params:
                        patch_request_re(url, params)

                        # Update Sync tags
                        education = str(class_of) + ', ' + str(department) + ', ' + str(hostel)
                        add_tags('Stay Connected - Auto | Education', 'Sync source', education.replace(', , ', ', ').strip()[:50], re_id)

                else:
                    # Different Education exists
                    send_mail_different_education(re_data, each_row, 'Different education data exists in RE and the one provided by Alum', re_id)
            else:
                # Multiple education exists than what's provided
                re_data_html = re_data.to_html(index=False, classes='table table-stripped')
                each_row_html = pd.DataFrame(each_row).T.to_html(index=False, classes='table table-stripped')
                send_mail_different_education(re_data_html, each_row_html, 'Multiple education data exists in RE', re_id)

        else:
            logging.info('Adding new education')
            # Upload Education
            url = 'https://api.sky.blackbaud.com/constituent/v1/educations'

            params = {
                'campus': department[:50],
                'class_of': class_of,
                'date_graduated': {
                    'y': int(class_of)
                },
                'date_left': {
                    'y': int(class_of)
                },
                'majors': [
                    department[:50]
                ],
                'primary': True,
                'school': 'Indian Institute of Technology Bombay',
                'social_organization': hostel[:50],
                'constituent_id': re_id
            }

            post_request_re(url, params)

def send_mail_different_education(re_data, each_row, subject, re_id):

    logging.info('Sending email for different education')

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
        <p>Hi,</p>
        <p>This is to inform you that the Education data provided by Alum is different than that exists in Raisers Edge.</p>
        <p><a href="https://host.nxt.blackbaud.com/constituent/records/{constituent_id}?envId=p-dzY8gGigKUidokeljxaQiA&amp;svcId=renxt" target="_blank"><strong>Open in RE</strong></a></p>
        <p>&nbsp;</p>
        <p>Below is the data for your comparison:</p>
        <h3>Raisers Edge Data:</h3>
        <p>{re_data}</p>
        <p>&nbsp;</p>
        <h3>Provided by Alum:</h3>
        <p>{education_data}</p>
        <p>&nbsp;</p>
        <p>Thanks &amp; Regards</p>
        <p>A Bot.</p>
        """

        # Create a text/html message from a rendered template
        emailbody = TEMPLATE.format(
            constituent_id=re_id,
            re_data=re_data.to_html(index=False),
            education_data=pd.DataFrame(each_row).T.to_html(index=False)
        )

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': subject,
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': emailbody
                    },
                    'ToRecipients': get_recipients(SEND_TO),
                    'CcRecipients': get_recipients(CC_TO),
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

def update_names(each_row, re_id):
    logging.info('Proceeding to update names')

    first = each_row['fname']
    last = each_row['lname']

    name = str(first) + ' ' + str(last)
    name = name.strip()
    name_bak = name

    # Get First, Middle and Last Name
    name = HumanName(str(name))
    first_name = str(name.first).title()

    # In case there's no middle name
    try:
        middle_name = str(name.middle).title()
    except:
        middle_name = ''

    last_name = str(name.last).title()
    title = str(name.title).title()

    # Get existing name from RE
    url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{re_id}'
    params = {}

    api_response = get_request_re(url, params).json()

    # Load to a DataFrame
    re_data = api_to_df(api_response).copy()

    # When there's no middle name in RE
    try:
        re_data = re_data[['name', 'title', 'first', 'middle', 'last']]
    except:
        re_data = re_data[['name', 'title', 'first', 'last']]

    # RE Data
    re_f_name = re_data['first'][0]

    try:
        re_m_name = re_data['middle'][0]
    except:
        re_m_name = ''

    re_l_name = re_data['last'][0]
    re_title = re_data['title'][0]

    # Check if name needs an update
    if re_title == title: title = ''
    if re_f_name == first_name: first_name = ''
    if re_m_name == middle_name: middle_name = ''
    if re_l_name == last_name: last_name = ''
    former_name = str(str(re_title) + ' ' + str(re_f_name) + ' ' + str(re_m_name) + ' ' + str(re_l_name)).replace('  ', ' ').strip()[:100]

    if first_name:
        params = {
            'title': str(title),
            'first': str(first_name)[:50],
            'middle': str(middle_name)[:50],
            'last': str(last_name)[:50],
            'former_name': former_name
        }

        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{re_id}'

        # Delete blank values from JSON
        for i in range(10):
            params = del_blank_values_in_json(params.copy())

        # Update in RE
        patch_request_re(url, params)

        # Update Tags
        add_tags('Stay Connected - Auto | Name', 'Sync source', str(name_bak)[:50], re_id)

        # Send email for different name
        send_mail_different_name(
            str(str(re_title) + ' ' + str(re_f_name) + ' ' + str(re_m_name) + ' ' + str(re_l_name)),
            str(str(title) + ' ' + str(first_name) + ' ' + str(middle_name) + ' ' + str(last_name)),
            'Different name exists in Raisers Edge and the one shared by Alum', re_id)

def send_mail_different_name(re_name, new_name, subject, re_id):
    logging.info('Sending email for different names')

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
        <p>Hi,</p>
        <p>This is to inform you that the name provided by Alum is different than that exists in Raisers Edge.</p>
        <p>The new one has been updated in Raisers Edge, and the Existing name is stored as &#39;<u>Former Name</u>&#39; in RE.</p>
        <p><a href="https://host.nxt.blackbaud.com/constituent/records/{constituent_id}?envId=p-dzY8gGigKUidokeljxaQiA&amp;svcId=renxt" target="_blank"><strong>Open in RE</strong></a></p>
        <table align="left" border="1" cellpadding="1" cellspacing="1" style="width:500px">
            <thead>
                <tr>
                    <th scope="col">Existing Name</th>
                    <th scope="col">New Name</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="text-align:center">{re_name}</td>
                    <td style="text-align:center">{new_name}</td>
                </tr>
            </tbody>
        </table>
        <p>&nbsp;</p>
        <p>&nbsp;</p>
        <p>&nbsp;</p>
        <p>&nbsp;</p>
        <p>Thanks &amp; Regards</p>
        <p>A Bot.</p>
        """

        # Create a text/html message from a rendered template
        emailbody = TEMPLATE.format(
            constituent_id=re_id,
            re_name=re_name,
            new_name=new_name
        )

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': subject,
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': emailbody
                    },
                    'ToRecipients': get_recipients(SEND_TO),
                    'CcRecipients': get_recipients(CC_TO),
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

def constituent_not_found(data, subject, re_id):
    logging.info('Sending email for record not found')

    data = pd.DataFrame(data).T.to_html(index=False)

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
        <p>Hi,</p>
        <p>This is to inform you that the application couldn't find an exact match for the constituent in Raisers Edge.</p>
        <p>Below is the data from Stay Connected:</p>
        {data}
        <p>&nbsp;</p>
        <table align="left" border="1" cellpadding="1" cellspacing="1" style="width:500px">
            <thead>
                <tr>
                    <th scope="col">System Record IDs identified</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="text-align:center">{re_id}</td>
                </tr>
            </tbody>
        </table>
        <p>&nbsp;</p>
        <p>&nbsp;</p>
        <p>&nbsp;</p>
        <p>&nbsp;</p>
        <p>Thanks &amp; Regards</p>
        <p>A Bot.</p>
        """

        # Create a text/html message from a rendered template
        emailbody = TEMPLATE.format(
            re_id=re_id,
            data=data
        )

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': subject,
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': emailbody
                    },
                    'ToRecipients': get_recipients(SEND_TO),
                    'CcRecipients': get_recipients(CC_TO),
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

try:
    # Set current directory
    set_directory()

    # Start Logging
    start_logging()

    # Load ENV
    load_env()

    # Set API Strategy
    set_api_request_strategy()

    # Load Geocode API
    initialize_nominatim()

    # Load the synced files
    completed = pd.read_parquet('Database/Completed.parquet')

    # Load Stay Connected Data yet to be uploaded
    data = pd.read_parquet('Database/To be uploaded.parquet')

    # Loop over Dataframe
    for index, row in data.iterrows():

        email = row['email']

        # Converting to lowercase
        email = str(email).lower()

        row = row.fillna('').copy()

        # Locate Alum
        re_id = search_alum(email)

        # Check if there's only 1 match
        if len(re_id) == 1:
            re_id = re_id[0]

            # If identified, proceed (only one match found)
            logging.info(f'Preparing to update record with System record ID: {re_id}')

            attribute_date = row['created_on'].isoformat()

            # Update Emails
            update_emails(row, re_id)

            # Update Phone
            update_phones(row, re_id)

            # Update Address
            update_location(row, re_id)

            # Update Employment
            update_employment(row, re_id)

            # Update LinkedIn
            update_linkedin(row, re_id)

            # Update Chapter
            update_chapter(row, re_id)

            # Update Education data (if missing in RE)
            update_education(row, re_id)

            # Update Bio
            update_names(row, re_id)

            # Update completed dataframe
            logging.info('Updating Database of synced records')
            row['created_on'] = np.NaN
            row = pd.DataFrame(row).T
            completed = pd.concat([completed, row], ignore_index=True)
            completed = completed.drop_duplicates().copy()
            completed.to_parquet('Database/Completed.parquet', index=False)

            break

        else:
            # Else, send Email to DB Admin
            logging.info('Multiple or no constituent found in Raisers Edge')
            constituent_not_found(row, 'Multiple or no constituent found in Raisers Edge', re_id)

except Exception as Argument:
    logging.error(Argument)
    send_error_emails('Error while uploading data to RE | Stay Connected')

finally:
    # Stop Logging
    stop_logging()

    exit()
