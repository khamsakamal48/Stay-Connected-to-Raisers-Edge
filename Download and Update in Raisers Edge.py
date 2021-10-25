import pysftp, json, requests

with open('sftp_cred.json', 'r') as sftp_cred_file:
    sftp_cred = json.load(sftp_cred_file)
    host = sftp_cred['host']
    port = sftp_cred['port']
    username = sftp_cred['username']
    password = sftp_cred['password']