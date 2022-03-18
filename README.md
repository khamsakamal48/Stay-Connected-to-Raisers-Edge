# Stay Connected to Raisers Edge

sudo apt install python3-pip
sudo apt install csvkit

pip3 install pandas
pip3 install pysftp
pip3 install psycopg2
pip3 install python-dotenv

If you encounter error on installing pyscopg2, then try:

```bash
pip3 install psycopg2-binary
```

Install PostgreSQL

Create stay_connected database

Create table

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