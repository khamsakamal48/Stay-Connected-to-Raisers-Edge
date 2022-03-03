#!/usr/bin/env python3

from unittest import case
import pysftp, json, requests, os, sys, csv, shutil, glob, pandas, psycopg2
from soupsieve import match

# Set current directory
os.chdir(os.path.dirname(sys.argv[0]))

from dotenv import load_dotenv

load_dotenv()

# Create a .env file with below parameters
#HOST=
#PORT=
#USERN=
#PASSWORD=
#SOURCE_PATH=
#DESTINATION_PATH=
#DB_IP=
#DB_NAME=
#DB_USERNAME=
#DB_PASSWORD=
#AUTH_CODE=
#REDIRECT_URL=
#CLIENT_ID=
#RE_API_KEY=

# Retrieve contents from .env file
DB_IP = os.getenv("DB_IP")
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
RE_API_KEY = os.getenv("RE_API_KEY")

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

# Request Headers for Blackbaud API request
headers = {
    # Request headers
    'Bb-Api-Subscription-Key': RE_API_KEY,
    'Authorization': 'Bearer ' + access_token,
}

# Search on the basis of email_1
match email_1:
  case ""|0:
    print("email_1 is blank")
    match email_2:
      case ""|0:
        print("email_2 is blank")
        match email_3:
          case ""|0:
            print("email_3 is blank")
            match email_4:
              case ""|0:
                print("email_4 is blank")
                match email_5:
                  case ""|0:
                    print("email_5 is blank")
                    match email_6:
                      case ""|0:
                        print("email_6 is blank")
                        match phone_1:
                          case ""|0:
                            print("phone is blank. The script will exit.")
                            sys.exit()
                          case "*":
                            print("will search Alum based on phone")
                      case "*":
                        print("will search Alum based on email_6")
                  case "*":
                    print("will search Alum based on email_5")
              case "*":
                print("will search Alum based on email_4")
          case "*":
            print("will search Alum based on email_3")
      case "*":
        print("will search Alum based on email_2")
  case "*":
    print("will search Alum based on email_1")


# If search results not equal to 1, then send email to Admin
# When search results equal to 1, start updating Alumni record



# Update the completed table in DB
with open('update.csv', 'r') as input_csv:

    # Skip the header row.
    next(input_csv)
    cur.copy_from(input_csv, 'updated_in_raisers_edge', sep=',')

# Commit changes
conn.commit()

# Close DB connection
cur.close()
conn.close()