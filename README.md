# Stay Connected to Raisers Edge

sudo apt install python3-pip
sudo apt install csvkit

pip3 install pandas
pip3 install pysftp
pip3 install psycopg2
pip3 install python-dotenv
pip3 install fuzzywuzzy
pip3 install python-Levenshtein


If you encounter error on installing pyscopg2, then try:

```bash
pip3 install psycopg2-binary
```

Install PostgreSQL

Create stay-connected database
CREATE DATABASE "stay-connected";

Create tables

CREATE TABLE updates_from_stayconnected
(
    id character varying,
    fname character varying,
    lname character varying,
    email character varying,
    email1 character varying,
    email2 character varying,
    email3 character varying,
    email4 character varying,
    email5 character varying,
    contact character varying,
    batch character varying,
    dept character varying,
    hostel character varying,
    country character varying,
    state character varying,
    city character varying,
    organization character varying,
    position character varying,
    status character varying,
    created_on character varying,
    interest character varying
);

CREATE TABLE updated_in_raisers_edge
(
    id character varying,
    fname character varying,
    lname character varying,
    email character varying,
    email1 character varying,
    email2 character varying,
    email3 character varying,
    email4 character varying,
    email5 character varying,
    contact character varying,
    batch character varying,
    dept character varying,
    hostel character varying,
    country character varying,
    state character varying,
    city character varying,
    organization character varying,
    position character varying,
    status character varying,
    created_on character varying,
    interest character varying
);

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
#MAIL_USERN=
#MAIL_PASSWORD=
#IMAP_URL=
#IMAP_PORT=
#SMTP_URL=
#SMTP_PORT=