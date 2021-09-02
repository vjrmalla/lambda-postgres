#!/usr/bin/env python

# serverless database query - postgresql example

# Copyright 2016 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

import psycopg2
import logging
import traceback
from os import environ

endpoint=environ.get('ENDPOINT')
port=environ.get('PORT')
dbuser=environ.get('DBUSER')
password=environ.get('DBPASSWORD')
database=environ.get('DATABASE')

query_db_list = [
    "CREATE DATABASE QSR ENCODING = 'UTF8'",
    "CREATE DATABASE SenseServices ENCODING = 'UTF8'",
    "CREATE DATABASE QSMQ ENCODING = 'UTF8'",
    "CREATE DATABASE Licenses ENCODING = 'UTF8'"
    ]
query_after_db_list = [
    "CREATE ROLE qliksenserepository WITH LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity'",
    "ALTER ROLE qliksenserepository WITH ENCRYPTED PASSWORD 'qliksense'",
    "ALTER DATABASE QSR OWNER TO qliksenserepository",
    "ALTER DATABASE SenseServices OWNER TO qliksenserepository",
    "ALTER DATABASE QSMQ OWNER TO qliksenserepository",
    "ALTER DATABASE Licenses OWNER TO qliksenserepository",
    "GRANT TEMPORARY, CONNECT ON DATABASE QSMQ TO PUBLIC",
    "GRANT CREATE ON DATABASE QSMQ TO qliksenserepository",
    "GRANT TEMPORARY, CONNECT ON DATABASE SenseServices TO PUBLIC",
    "GRANT CREATE ON DATABASE SenseServices TO qliksenserepository",
    "GRANT TEMPORARY, CONNECT ON DATABASE Licenses TO PUBLIC",
    "GRANT CREATE ON DATABASE Licenses TO qliksenserepository"
    ]
query="CREATE DATABASE SenseServices ENCODING = 'UTF8'"

logger=logging.getLogger()
logger.setLevel(logging.INFO)


def make_connection():
    conn_str="host={0} dbname={1} user={2} password={3} port={4}".format(
        endpoint,database,dbuser,password,port)
    conn = psycopg2.connect(conn_str)
    conn.autocommit=True
    return conn


def log_err(errmsg):
    logger.error(errmsg)
    return {"body": errmsg , "headers": {}, "statusCode": 400,
        "isBase64Encoded":"false"}

logger.info("Cold start complete.")

def lambda_handler(event,context):

    try:
        cnx = make_connection()
        cursor=cnx.cursor()

        try:
            for query in query_db_list:
                cursor.execute(query)
        except:
            return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                traceback.format_exc()) )
        try:
            for query in query_after_db_list:
                cursor.execute(query)
        except:
            return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                traceback.format_exc()) )

        cursor.close()
        return {"body": "success", "headers": {}, "statusCode": 200,
        "isBase64Encoded":"false"}


    except:
        return log_err("ERROR: Cannot connect to database from handler.\n{}".format(
            traceback.format_exc()))


    finally:
        try:
            cnx.close()
        except:
            pass


if __name__== "__main__":
    handler(None,None)
