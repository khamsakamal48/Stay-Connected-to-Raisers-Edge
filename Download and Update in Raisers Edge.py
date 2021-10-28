import pysftp, json, requests, os, sys, csv, shutil, glob, pandas

from dotenv import load_dotenv

load_dotenv()

# Set current directory
os.getcwd()

# Download file from SFTP Server
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT")) # Change port from string to integer
USERNAME = os.getenv("USERN")
PASSWORD = os.getenv("PASSWORD")
SOURCE = os.getenv("SOURCE_PATH")
DESTINATION = os.getenv("DESTINATION_PATH")

# Change Directory 
os.chdir(DESTINATION)

# Connect to SFTP Server
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
os.system("touch Stay_Connected_Merged_All.csv")
os.system("awk '(NR == 1) || (FNR > 1)' *-*.csv > Stay_Connected_Merged_All.csv")

#Remove blank rows
os.system("sed -i 's/\s*;\s*/;/g' Stay_Connected_Merged_All.csv")
os.system("sed -i 's/\s*,\s*/,/g' Stay_Connected_Merged_All.csv")
os.system("sed -i '/^$/d' Stay_Connected_Merged_All.csv")
os.system("awk -F, 'length>NF+1' Stay_Connected_Merged_All.csv > Stay_Connected_Merged_All_New.csv")

# Move files
shutil.move("Stay_Connected_Merged_All_New.csv","../Stay_Connected_Merged_All.csv")

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