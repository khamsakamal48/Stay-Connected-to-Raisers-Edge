#!/usr/bin/env python3

import pysftp, json, requests, os, sys, csv, shutil, glob, pandas, psycopg2

# Set current directory
os.chdir(os.path.dirname(sys.argv[0]))

from dotenv import load_dotenv

load_dotenv()

# Set current directory
os.getcwd()

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

# Retrieve contents from .env file
DB_IP = os.getenv("DB_IP")
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

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
os.system("csvjson update.csv > update.json")

# Retrieve values to be updated in RE from update.json
with open('update.json') as update:
  data = json.load([update])
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