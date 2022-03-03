#!/usr/bin/env python3

import json, requests, os, shutil, sys

# Set current directory
os.chdir(os.path.dirname(sys.argv[0]))

from dotenv import load_dotenv

load_dotenv()

# Set current directory
os.chdir(os.path.dirname(sys.argv[0]))

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
AUTH_CODE = os.getenv("AUTH_CODE")

# Blackbaud Token URL
url = 'https://oauth2.sky.blackbaud.com/token'

def access_token():
    # Retrieve access_token from file
    with open('access_token_output.json') as access_token_output:
      data = json.load(access_token_output)
      access_token = data["access_token"]
    return access_token

og_file = "access_token_output.json"
bak_file = "access_token_output.json.bak"

# Check if the output is empty
if access_token() == "":
    shutil.copyfile(bak_file, og_file)

# Take Backup
shutil.copyfile(og_file, bak_file)

# Retrieve refresh_token from file
with open('access_token_output.json') as access_token_output:
  data = json.load(access_token_output)
  refresh_token = data["refresh_token"]

# Request Headers for Blackbaud API request
headers = {
    # Request headers
    'Content-Type': 'application/x-www-form-urlencoded',
    'Authorization': 'Basic ' + AUTH_CODE
}

# Request parameters for Blackbaud API request
data = {
    'grant_type': 'refresh_token',
    'refresh_token': refresh_token
}

# API Request
response = requests.post(url, data=data, headers=headers).json()

# Write output to JSON file
with open("access_token_output.json", "w") as response_output:
    json.dump(response, response_output, ensure_ascii=False, sort_keys=True, indent=4)

# Check if the output is empty
if access_token() == "":
    shutil.copyfile(bak_file, og_file)