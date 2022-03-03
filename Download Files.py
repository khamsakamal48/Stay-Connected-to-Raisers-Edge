#!/usr/bin/env python3

import pysftp, os, csv, shutil, glob, pandas, psycopg2, sys

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

# Retrieve contents from .env file
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT")) # Change port from string to integer
USERNAME = os.getenv("USERN")
PASSWORD = os.getenv("PASSWORD")
SOURCE = os.getenv("SOURCE_PATH")
DESTINATION = os.getenv("DESTINATION_PATH")
DB_IP = os.getenv("DB_IP")
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Change Directory 
os.chdir(DESTINATION)

# Connect to Download files from SFTP Server
with pysftp.Connection(host=HOST, port=PORT, username=USERNAME, password=PASSWORD) as sftp:
  # Change directory in remote
  with sftp.cd(SOURCE):
    # List all files in that directory
    files = sftp.listdir()
    for each_file in files:
      # Download file
      sftp.get(each_file)
      # Remove file
      sftp.remove(each_file)

# Merge CSV files into one
os.system("awk '(NR == 1) || (FNR > 1)' *-*.csv > Stay_Connected_Merged.csv")

#Remove blank rows
os.system("sed -i 's/\s*;\s*/;/g' Stay_Connected_Merged.csv")
os.system("sed -i 's/\s*,\s*/,/g' Stay_Connected_Merged.csv")
os.system("sed -i '/^$/d' Stay_Connected_Merged.csv")
os.system("awk -F, 'length>NF+1' Stay_Connected_Merged.csv > Stay_Connected_Merged_All_New.csv")

# Delete last column from CSV file
with open("Stay_Connected_Merged_All_New.csv", "r") as fin:
    with open("Stay_Connected_Merged_All.csv", "w") as fout:
        writer=csv.writer(fout)
        for row in csv.reader(fin):
            writer.writerow(row[:-1])

# Move files
shutil.move("Stay_Connected_Merged_All.csv","../Stay_Connected_Merged_All.csv")

# Delete all CSV files
csv_files = glob.glob('*.csv')
for each_csv_file in csv_files:
  os.remove(each_csv_file)

# Change Directory back
os.chdir('..')

# Remove duplicates
merged_csv_file = pandas.read_csv('Stay_Connected_Merged_All.csv')
merged_csv_file.drop_duplicates(inplace=True)
merged_csv_file.to_csv('Stay_Connected_Merged_All.csv', index=False)

# PostgreSQL DB Connection
conn = psycopg2.connect(host=DB_IP, dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD)

# Open connection
cur = conn.cursor()

# Delete rows in table
cur.execute("truncate updates_from_stayconnected;")

# Commit changes
conn.commit()

# Copying contents of CSV file to PostgreSQL DB
with open('Stay_Connected_Merged_All.csv', 'r') as input_csv:

    # Skip the header row.
    next(input_csv)
    cur.copy_from(input_csv, 'updates_from_stayconnected', sep=',')

# Commit changes
conn.commit()