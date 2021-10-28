import pysftp, json, requests, os, sys

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

# Change Directory back
os.chdir('..')