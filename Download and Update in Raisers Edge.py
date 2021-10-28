import pysftp, json, requests, os, sys, paramiko

from dotenv import load_dotenv

load_dotenv()

# Set current directory
os.getcwd()

# Download file from SFTO Server
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
USERNAME = os.getenv("USERN")
PASSWORD = os.getenv("PASSWORD")
SOURCE = os.getenv("SOURCE_PATH")
DESTINATION = os.getenv("DESTINATION_PATH")

# Open a transport
transport = paramiko.Transport((HOST,PORT))

# Auth
transport.connect(None,USERNAME,PASSWORD)

# Connect Now
sftp = paramiko.SFTPClient.from_transport(transport)

# Download files
sftp.get(SOURCE, DESTINATION)

# Close
if sftp: sftp.close()
if transport: transport.close()
