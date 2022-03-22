#!/usr/bin/env python3

from cgitb import reset
import email
import pysftp, json, requests, os, sys, csv, shutil, glob, pandas, psycopg2, smtplib, ssl, json, time, imaplib, re
from soupsieve import match
from unittest import case
from urllib import response
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment

# Set current directory
#os.chdir(os.path.dirname(sys.argv[0]))
os.chdir(os.getcwd())

from dotenv import load_dotenv

load_dotenv()

# Retrieve contents from .env file
DB_IP = os.getenv("DB_IP")
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
RE_API_KEY = os.getenv("RE_API_KEY")
MAIL_USERN = os.getenv("MAIL_USERN")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
IMAP_URL = os.getenv("IMAP_URL")
IMAP_PORT = os.getenv("IMAP_PORT")
SMTP_URL = os.getenv("SMTP_URL")
SMTP_PORT = os.getenv("SMTP_PORT")

# PostgreSQL DB Connection
conn = psycopg2.connect(host=DB_IP, dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD)

# Open connection
cur = conn.cursor()

# Query the next data to uploaded in RE
extract_sql = """
        SELECT * FROM updates_from_stayconnected EXCEPT SELECT * FROM updated_in_raisers_edge FETCH FIRST 1 ROW ONLY;
        """
cur.execute(extract_sql)

# Header of CSV file
header = ['id', 'first_name', 'last_name', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'email_6', 'phone_1', 'class_of', 'dept', 'hostel', 'country', 'state', 'city', 'organization', 'position', 'status', 'created_on', 'interest']

# Extract the next data to uploaded in RE to CSV file
with open('update.csv', 'w', encoding='UTF8') as update:
    # Write the data
    writer = csv.writer(update)
    # Write the header
    writer.writerow(header)
    writer.writerows(cur)

# Convert from CSV to JSON
os.system("csvjson --stream update.csv > update.json")

# Retrieve values to be updated in RE from update.json
with open('update.json') as update:
    data = json.load(update)
    first_name = data["first_name"]
    last_name = data["last_name"]
    email_1 = data["email_1"]
    email_2 = data["email_2"]
    email_3 = data["email_3"]
    email_4 = data["email_4"]
    email_5 = data["email_5"]
    email_6 = data["email_6"]
    phone_1 = data["phone_1"]
    class_of = data["class_of"]
    dept = data["dept"]
    hostel = data["hostel"]
    country = data["country"]
    state = data["state"]
    city = data["city"]
    organization = data["organization"]
    position = data["position"]

# Identify the constituent ID
# Search based on email and phone number
# Retrieve access_token from file
with open('access_token_output.json') as access_token_output:
  data = json.load(access_token_output)
  access_token = data["access_token"]

def get_request():
    # Request Headers for Blackbaud API request
    headers = {
    # Request headers
    'Bb-Api-Subscription-Key': RE_API_KEY,
    'Authorization': 'Bearer ' + access_token,
    }
    
    global api_response
    api_response = requests.get(url, params=params, headers=headers).json()
  
def post_request():
    headers = {
    # Request headers
    'Bb-Api-Subscription-Key': RE_API_KEY,
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/json',
    }
    
    global api_response
    api_response = requests.post(url, params=params, headers=headers, json=params).json()
    
    check_for_errors()
    
def patch_request():
    headers = {
    # Request headers
    'Bb-Api-Subscription-Key': RE_API_KEY,
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/json',
    }
    
    global params
    params = {
        #'search_text':search_text
    }
    
    global api_response
    api_response = requests.patch(url, params=params, headers=headers, json=params).json()
def check_for_errors():
    error_keywords = ["invalid", "error", "bad", "Unauthorized", "Forbidden", "Not Found", "Unsupported Media Type", "Too Many Requests", "Internal Server Error", "Service Unavailable"]
    
    if any(x in api_response for x in error_keywords):
        # Send emails
        send_error_emails()

def send_error_emails():
    with open('email_details.json') as f:
        email = json.load(f)
        sender_email = email["from"]
        reply_to = email["reply_to"]
        receiver_email = email["to"]
        subject = email["subject"]
        
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = receiver_email

        # Adding Reply-to header
        message.add_header('reply-to', reply_to)
        
        TEMPLATE="""
        """
    
def search_for_constituent_id():
  global params
  params = {
    'search_text':search_text
  }

  global url
  url = "https://api.sky.blackbaud.com/constituent/v1/constituents/search"

  # Blackbaud API GET request
  get_request()
  
  global api_response_constituent_search
  api_response_constituent_search = api_response
  
  global count
  # Get the count of results
  count = api_response["count"]

def constituent_not_found_email():
  with open('email_details.json') as f:
    email = json.load(f)
    sender_email = email["from"]
    reply_to = email["reply_to"]
    receiver_email = email["to"]
    subject = email["subject"]

  message = MIMEMultipart("alternative")
  message["Subject"] = subject
  message["From"] = sender_email
  message["To"] = receiver_email

  # Adding Reply-to header
  message.add_header('reply-to', reply_to)

  TEMPLATE = """
  <!DOCTYPE html>
  <html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
  <head>
      <meta charset="utf-8"> <!-- utf-8 works for most cases -->
      <meta name="viewport" content="width=device-width"> <!-- Forcing initial-scale shouldn't be necessary -->
      <meta http-equiv="X-UA-Compatible" content="IE=edge"> <!-- Use the latest (edge) version of IE rendering engine -->
      <meta name="x-apple-disable-message-reformatting">  <!-- Disable auto-scale in iOS 10 Mail entirely -->
      <title>Create a Donor?</title> <!-- The title tag shows in email notifications, like Android 4.4. -->

      <!-- Web Font / @font-face : BEGIN -->
      <!-- NOTE: If web fonts are not required, lines 10 - 27 can be safely removed. -->

      <!-- Desktop Outlook chokes on web font references and defaults to Times New Roman, so we force a safe fallback font. -->
      <!--[if mso]>
          <style>
              * {
                  font-family: Arial, sans-serif !important;
              }
          </style>
      <![endif]-->

      <!-- All other clients get the webfont reference; some will render the font and others will silently fail to the fallbacks. More on that here: http://stylecampaign.com/blog/2015/02/webfont-support-in-email/ -->
      <!--[if !mso]><!-->
          <link href="https://fonts.googleapis.com/css?family=Montserrat:300,500" rel="stylesheet">
      <!--<![endif]-->

      <!-- Web Font / @font-face : END -->

      <!-- CSS Reset -->
      <style>

          /* What it does: Remove spaces around the email design added by some email clients. */
          /* Beware: It can remove the padding / margin and add a background color to the compose a reply window. */
          html,
          body {
              margin: 0 auto !important;
              padding: 0 !important;
              height: 100% !important;
              width: 100% !important;
          }

          /* What it does: Stops email clients resizing small text. */
          * {
              -ms-text-size-adjust: 100%;
              -webkit-text-size-adjust: 100%;
          }

          /* What it does: Centers email on Android 4.4 */
          div[style*="margin: 16px 0"] {
              margin:0 !important;
          }

          /* What it does: Stops Outlook from adding extra spacing to tables. */
          table,
          td {
              mso-table-lspace: 0pt !important;
              mso-table-rspace: 0pt !important;
          }

          /* What it does: Fixes webkit padding issue. Fix for Yahoo mail table alignment bug. Applies table-layout to the first 2 tables then removes for anything nested deeper. */
          table {
              border-spacing: 0 !important;
              border-collapse: collapse !important;
              table-layout: fixed !important;
              margin: 0 auto !important;
          }
          table table table {
              table-layout: auto;
          }

          /* What it does: Uses a better rendering method when resizing images in IE. */
          img {
              -ms-interpolation-mode:bicubic;
          }

          /* What it does: A work-around for email clients meddling in triggered links. */
          *[x-apple-data-detectors],  /* iOS */
          .x-gmail-data-detectors,    /* Gmail */
          .x-gmail-data-detectors *,
          .aBn {
              border-bottom: 0 !important;
              cursor: default !important;
              color: inherit !important;
              text-decoration: none !important;
              font-size: inherit !important;
              font-family: inherit !important;
              font-weight: inherit !important;
              line-height: inherit !important;
          }

          /* What it does: Prevents Gmail from displaying an download button on large, non-linked images. */
          .a6S {
              display: none !important;
              opacity: 0.01 !important;
          }
          /* If the above doesn't work, add a .g-img class to any image in question. */
          img.g-img + div {
              display:none !important;
            }

          /* What it does: Prevents underlining the button text in Windows 10 */
          .button-link {
              text-decoration: none !important;
          }

          /* What it does: Removes right gutter in Gmail iOS app: https://github.com/TedGoas/Cerberus/issues/89  */
          /* Create one of these media queries for each additional viewport size you'd like to fix */
          /* Thanks to Eric Lepetit @ericlepetitsf) for help troubleshooting */
          @media only screen and (min-device-width: 375px) and (max-device-width: 413px) { /* iPhone 6 and 6+ */
              .email-container {
                  min-width: 375px !important;
              }
          }

      </style>

      <!-- Progressive Enhancements -->
      <style>

          /* What it does: Hover styles for buttons */
          .button-td,
          .button-a {
              transition: all 100ms ease-in;
          }
          .button-td:hover,
          .button-a:hover {
              background: #555555 !important;
              border-color: #555555 !important;
          }

          /* Media Queries */
          @media screen and (max-width: 480px) {

              /* What it does: Forces elements to resize to the full width of their container. Useful for resizing images beyond their max-width. */
              .fluid {
                  width: 100% !important;
                  max-width: 100% !important;
                  height: auto !important;
                  margin-left: auto !important;
                  margin-right: auto !important;
              }

              /* What it does: Forces table cells into full-width rows. */
              .stack-column,
              .stack-column-center {
                  display: block !important;
                  width: 100% !important;
                  max-width: 100% !important;
                  direction: ltr !important;
              }
              /* And center justify these ones. */
              .stack-column-center {
                  text-align: center !important;
              }

              /* What it does: Generic utility class for centering. Useful for images, buttons, and nested tables. */
              .center-on-narrow {
                  text-align: center !important;
                  display: block !important;
                  margin-left: auto !important;
                  margin-right: auto !important;
                  float: none !important;
              }
              table.center-on-narrow {
                  display: inline-block !important;
              }

              /* What it does: Adjust typography on small screens to improve readability */
              .email-container p {
                  font-size: 17px !important;
                  line-height: 22px !important;
              }
          }

      </style>

      <!-- What it does: Makes background images in 72ppi Outlook render at correct size. -->
      <!--[if gte mso 9]>
      <xml>
          <o:OfficeDocumentSettings>
              <o:AllowPNG/>
              <o:PixelsPerInch>96</o:PixelsPerInch>
          </o:OfficeDocumentSettings>
      </xml>
      <![endif]-->

  </head>
  <body width="100%" bgcolor="#F1F1F1" style="margin: 0; mso-line-height-rule: exactly;">
      <center style="width: 100%; background: #F1F1F1; text-align: left;">

          <!-- Visually Hidden Preheader Text : BEGIN -->
          <div style="display:none;font-size:1px;line-height:1px;max-height:0px;max-width:0px;opacity:0;overflow:hidden;mso-hide:all;font-family: sans-serif;">
              Hello Kamal, I couldn't find below donor in Raiser's Edge. Let me know what you want me to do now.
          </div>
          <!-- Visually Hidden Preheader Text : END -->

          <!--
              Set the email width. Defined in two places:
              1. max-width for all clients except Desktop Windows Outlook, allowing the email to squish on narrow but never go wider than 680px.
              2. MSO tags for Desktop Windows Outlook enforce a 680px width.
              Note: The Fluid and Responsive templates have a different width (600px). The hybrid grid is more "fragile", and I've found that 680px is a good width. Change with caution.
          -->
          <div style="max-width: 680px; margin: auto;" class="email-container">
              <!--[if mso]>
              <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="680" align="center">
              <tr>
              <td>
              <![endif]-->

              <!-- Email Body : BEGIN -->
              <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center" width="100%" style="max-width: 680px;" class="email-container">


                  <!-- HEADER : BEGIN -->
                  <tr>
                      <td bgcolor="#333333">
                          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                              <tr>
                                  <td style="padding: 10px 40px 10px 40px; text-align: center;">
                                      <img src="https://i.ibb.co/fk6J37P/iitblogowhite.png" width="57" height="13" alt="alt_text" border="0" style="height: auto; font-family: sans-serif; font-size: 15px; line-height: 20px; color: #555555;">
                                  </td>
                              </tr>
                          </table>
                      </td>
                  </tr>
                  <!-- HEADER : END -->

                  <!-- HERO : BEGIN -->
                  <tr>
                      <!-- Bulletproof Background Images c/o https://backgrounds.cm -->
                      <td background="https://i.ibb.co/y8dhxm3/Background.png" bgcolor="#222222" align="center" valign="top" style="text-align: center; background-position: center center !important; background-size: cover !important;">
                          <!--[if gte mso 9]>
                          <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="true" stroke="false" style="width:680px; height:380px; background-position: center center !important;">
                          <v:fill type="tile" src="background.png" color="#222222" />
                          <v:textbox inset="0,0,0,0">
                          <![endif]-->
                          <div>
                              <!--[if mso]>
                              <table role="presentation" border="0" cellspacing="0" cellpadding="0" align="center" width="500">
                              <tr>
                              <td align="center" valign="middle" width="500">
                              <![endif]-->
                              <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%" style="max-width:500px; margin: auto;">

                                  <tr>
                                      <td height="20" style="font-size:20px; line-height:20px;">&nbsp;</td>
                                  </tr>

                                  <tr>
                                    <td align="center" valign="middle">

                                    <table>
                                      <tr>
                                          <td valign="top" style="text-align: center; padding: 60px 0 10px 20px;">

                                              <h1 style="margin: 0; font-family: 'Montserrat', sans-serif; font-size: 30px; line-height: 36px; color: #ffffff; font-weight: bold;">Hello Kamal,</h1>
                                          </td>
                                      </tr>
                                      <tr>
                                          <td valign="top" style="text-align: center; padding: 10px 20px 15px 20px; font-family: sans-serif;  font-size: 20px; line-height: 25px; color: #ffffff;">
                                              <p style="margin: 0;">I couldn't find below Alum in Raiser's Edge.</p>
                                          </td>
                                      </tr>
                                    </table>

                                    </td>
                                  </tr>

                                  <tr>
                                      <td height="20" style="font-size:20px; line-height:20px;">&nbsp;</td>
                                  </tr>

                              </table>
                              <!--[if mso]>
                              </td>
                              </tr>
                              </table>
                              <![endif]-->
                          </div>
                          <!--[if gte mso 9]>
                          </v:textbox>
                          </v:rect>
                          <![endif]-->
                      </td>
                  </tr>
                  <!-- HERO : END -->

                  <!-- INTRO : BEGIN -->
                  <tr>
                      <td bgcolor="#ffffff">
                          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                              <tr>
                                  <td style="padding: 40px 40px 20px 40px; text-align: left;">
                                      <h1 style="margin: 0; font-family: arial, cochin, sans-serif; font-size: 20px; line-height: 26px; color: #333333; font-weight: bold;">Below are the Alum details,</h1>
                                  </td>
                              </tr>
                              <tr>
                                  <td align="left" style="padding: 0px 40px 20px 40px;">
                              <table cellspacing="0" cellpadding="0" border="0" width="100%">
                                  <tr>
                                      <td width="30%" align="left" bgcolor="#eeeeee" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 800; line-height: 24px; padding: 10px;">
                                          Name
                                      </td>
                                      <td width="70%" align="left" bgcolor="#eeeeee" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 800; line-height: 24px; padding: 10px;">
                                          {{first_name}} {{last_name}}
                                      </td>
                                  </tr>
                                  <tr>
                                      <td width="30%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 15px 10px 5px 10px;">
                                          Email Addresses
                                      </td>
                                      <td width="70%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 15px 10px 5px 10px;">
                                          <p>{{email_1}}</p>
                                          <p>{{email_2}}</p>
                                          <p>{{email_3}}</p>
                                          <p>{{email_4}}</p>
                                          <p>{{email_5}}</p>
                                          <p>{{email_6}}</p>
                                      </td>
                                  </tr>
                                  <tr>
                                      <td width="30%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          Graduation details
                                      </td>
                                      <td width="70%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          <p>{{dept}}</p>
                                          <p>{{class_of}}</p>
                                          <p>{{hostel}}</p>
                                      </td>
                                  </tr>
                                  <tr>
                                      <td width="30%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          Address
                                      </td>
                                      <td width="70%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          <p>{{city_name}}</p>
                                          <p>{{state_name}}</p>
                                          <p>{{country_name}}</p>
                                      </td>
                                  </tr>
                                  <tr>
                                      <td width="30%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          Employment
                                      </td>
                                      <td width="70%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          <p>{{org_name}}</p>
                                          <p>{{position}}</p>
                                      </td>
                                  </tr>
                                  <tr>
                                      <td width="30%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          Phone
                                      </td>
                                      <td width="70%" align="left" style="font-family: Open Sans, Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 24px; padding: 5px 10px;">
                                          {{phone_1}}
                                      </td>
                                  </tr>
                              </table>
                          </td>
                      </tr>
                      <tr>
                          <td align="left" style="padding-top: 20px;">
                          </td>
                              </tr>
                              <tr>
                              </tr>
                              <tr>
                                  <td style="padding: 0px 40px 0px 50px; font-family: sans-serif; font-size: 15px; line-height: 20px; color: #555555; text-align: left; font-weight:normal;">
                                      <p style="margin: 0;">Yours sincerely,</p>
                                  </td>
                              </tr>

                              <tr>
                                  <td align="left" style="padding: 0px 40px 40px 40px;">

                                      <table width="200" align="left">
                                          <tr>
                                            <td width="90">
                                              <img src="https://i.ibb.co/J262D44/Bot.png" width="90" height="90" style="margin:0; padding:0; border:none; display:block;" border="0" alt="">
                                            </td>
                                            <td width="110">

                                              <table width="" cellpadding="0" cellspacing="0" border="0">
                                                <tr>
                                                  <td align="left" style="font-family: sans-serif; font-size:15px; line-height:20px; color:#222222; font-weight:bold;" class="body-text">
                                                    <p style="font-family: 'Montserrat', sans-serif; font-size:15px; line-height:20px; color:#222222; font-weight:bold; padding:0; margin:0;" class="body-text">A Bot,</p>
                                                  </td>
                                                </tr>
                                                <tr>
                                                  <td align="left" style="font-family: sans-serif; font-size:15px; line-height:20px; color:#666666;" class="body-text">
                                                    <p style="font-family: sans-serif; font-size:15px; line-height:20px; color:#666666; padding:0; margin:0;" class="body-text">made by Kamal.</p>
                                                  </td>
                                                </tr>
                                              </table>

                                            </td>
                                          </tr>
                                      </table>

                                  </td>
                              </tr>

                          </table>
                      </td>
                  </tr>
                  <!-- INTRO : END -->

                  <!-- CTA : BEGIN -->
                  <tr>
                      <td bgcolor="#26a4d3">
                          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                              <tr>
                                  <td style="padding: 40px 40px 5px 40px; text-align: center;">
                                      <h1 style="margin: 0; font-family: 'Montserrat', sans-serif; font-size: 20px; line-height: 24px; color: #ffffff; font-weight: bold;">YOU CAN CHECK IF THIS ALUMNI EXISTS ALREADY</h1>
                                  </td>
                              </tr>
                              <tr>
                                  <td style="padding: 0px 40px 20px 40px; font-family: sans-serif; font-size: 17px; line-height: 23px; color: #aad4ea; text-align: center; font-weight:normal;">
                                      <p style="margin: 0;">in Raiser's Edge</p>
                                  </td>
                              </tr>
                              <tr>
                                  <td valign="middle" align="center" style="text-align: center; padding: 0px 20px 40px 20px;">

                                      <!-- Button : BEGIN -->
                                      <table role="presentation" align="center" cellspacing="0" cellpadding="0" border="0" class="center-on-narrow">
                                          <tr>
                                              <td style="border-radius: 50px; background: #ffffff; text-align: center;" class="button-td">
                                                  <a href="https://renxt.blackbaud.com" style="background: #ffffff; border: 15px solid #ffffff; font-family: 'Montserrat', sans-serif; font-size: 14px; line-height: 1.1; text-align: center; text-decoration: none; display: block; border-radius: 50px; font-weight: bold;" class="button-a">
                                                      <span style="color:#26a4d3;" class="button-link">&nbsp;&nbsp;&nbsp;&nbsp;CHECK NOW&nbsp;&nbsp;&nbsp;&nbsp;</span>
                                                  </a>
                                              </td>
                                          </tr>
                                      </table>
                                      <!-- Button : END -->

                                  </td>
                              </tr>

                          </table>
                      </td>
                  </tr>
                  <!-- CTA : END -->

                  <!-- SOCIAL : BEGIN -->
                  <tr>
                      <td bgcolor="#292828">
                          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                              <tr>
                                  <td style="padding: 30px 30px; text-align: center;">

                                      <table align="center" style="text-align: center;">
                                          <tr>
                                              <td>
                                                  <h1 style="margin: 0; font-family: 'Montserrat', sans-serif; font-size: 20px; line-height: 24px; color: #ffffff; font-weight: bold;">Have a nice day! 😊</h1>
                                              </td>
                                          </tr>
                                      </table>

                                  </td>
                              </tr>

                          </table>
                      </td>
                  </tr>
                  <!-- SOCIAL : END -->

                  <!-- FOOTER : BEGIN -->
                  <tr>
                      <td bgcolor="#ffffff">
                          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                              <tr>
                                  <td style="padding: 40px 40px 0px 40px; font-family: sans-serif; font-size: 14px; line-height: 18px; color: #666666; text-align: center; font-weight:normal;">
                                      <p style="margin: 0;"><b>Indian Institute of Technology Bombay</b></p>
                                  </td>
                              </tr>
                              <tr>
                                  <td style="padding: 0px 40px 30px 40px; font-family: sans-serif; font-size: 12px; line-height: 18px; color: #666666; text-align: center; font-weight:normal;">
                                      <p style="margin: 0;">Powai, Mumbai, Maharashtra, India 400076</p>
                                  </td>
                              </tr>

                          </table>
                      </td>
                  </tr>
                  <!-- FOOTER : END -->

              </table>
              <!-- Email Body : END -->

              <!--[if mso]>
              </td>
              </tr>
              </table>
              <![endif]-->
          </div>

      </center>
  </body>
  </html>
  """

  # Create a text/html message from a rendered template
  emailbody = MIMEText(
      Environment().from_string(TEMPLATE).render(
          first_name=first_name,
          last_name=last_name,
          email_1=email_1,
          email_2=email_2,
          email_3=email_3,
          email_4=email_4,
          email_5=email_5,
          email_6=email_6,
          dept=dept,
          class_of=class_of,
          hostel=hostel,
          city_name=city,
          state_name=state,
          country_name=country,
          org_name=organization,
          position=position,
          phone_1=phone_1
      ), "html"
  )

  # Add HTML parts to MIMEMultipart message
  # The email client will try to render the last part first
  message.attach(emailbody)
  emailcontent = message.as_string()

  # Create secure connection with server and send email
  context = ssl._create_unverified_context()
  with smtplib.SMTP_SSL(SMTP_URL, SMTP_PORT, context=context) as server:
      server.login(MAIL_USERN, MAIL_PASSWORD)
      server.sendmail(
          sender_email, receiver_email,  emailcontent
      )

  # Save copy of the sent email to sent items folder
  with imaplib.IMAP4_SSL(IMAP_URL, IMAP_PORT) as imap:
      imap.login(MAIL_USERN, MAIL_PASSWORD)
      imap.append('Sent', '\\Seen', imaplib.Time2Internaldate(time.time()), emailcontent.encode('utf8'))
      imap.logout()

  # Close DB connection
  cur.close()
  conn.close()

  sys.exit()

def update_email():
    global params
    params = {
        'address': email_address,
        'constituent_id': constituent_id,
        'type': new_email_type
    }
    
    global url
    url = "https://api.sky.blackbaud.com/constituent/v1/emailaddresses"
    
    # Blackbaud API POST request
    post_request()

def update_record():
    global constituent_id
    constituent_id = api_response_constituent_search["value"][0]["id"]
  
    # Retrieve Email List
    global params
    params = {
        #'search_text':search_text
    }

    global url
    url = "https://api.sky.blackbaud.com/constituent/v1/constituents/%s/emailaddresses" % constituent_id

    # Blackbaud API GET request
    get_request()
    
    global email_search_api_response
    email_search_api_response = api_response
    
    # Check and update email
    global email_type_list
    email_type_list=[]
    email_list = [email_1, email_2, email_3, email_4, email_5, email_6]
            
    re_email_list = []
    for address in api_response['value']:
        try:
            emails = (address['address'])
            re_email_list.append(emails)
        except:
            pass
        
    # Finding missing email addresses to be added in RE
    set1 = set(email_list)
    set2 = set(re_email_list)
    
    missing = list(sorted(set1 - set2))
    
    for emails in missing:
        print ("Email to be added")
        global email_address
        email_address = emails
        # Figure the email type
        types = address['type']
        email_num = re.sub("[^0-9]", "", types)
        email_type_list.append(email_num)
        existing_max_count = int(max(email_type_list))
        print (existing_max_count)
        new_max_count = existing_max_count + 1
        print(new_max_count)
        try:
            incremental_max_count
        except:
            incremental_max_count = new_max_count
        else:
            incremental_max_count = incremental_max_count + 1            
        print (incremental_max_count)
        global new_email_type
        new_email_type = "Email " + str(incremental_max_count)
        #new_email_type = exec("Email %s" % str(ncount))
        print (new_email_type)
        update_email()
        
    # Mark email_1 address as primary
    url = "https://api.sky.blackbaud.com/constituent/v1/constituents/%s/emailaddresses" % constituent_id
    
    # Blackbaud API GET request
    get_request()
    
    for email_search in api_response['value']:
        if email_search['address'] == email_1:
            
            email_address_id = email_search['id']
            
            url = "https://api.sky.blackbaud.com/constituent/v1/emailaddresses/%s" % email_address_id
            
            patch_request()
            break
    
    
    
  
  # Check and  update phone
  # Check and update Education details
  # Check and update Organisation
  # Check and update name

  # Update the completed table in DB
  #with open('update.csv', 'r') as input_csv:

      # Skip the header row.
  #    next(input_csv)
  #    cur.copy_from(input_csv, 'updated_in_raisers_edge', sep=',')

  # Commit changes
  #conn.commit()

  # Close DB connection
  #cur.close()
  #conn.close()

    sys.exit()
    
    
count = 0
while count != 1:
    for emails in {email_1, email_2, email_3, email_4, email_5, email_6}:
        try:
            global search_text
            search_text = emails
            search_for_constituent_id()
        except:
            constituent_not_found_email()
else:
    print ("Got constituent ID")
    update_record()