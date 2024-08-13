#!/usr/bin/env python3

# pip install azure-identity
# from https://stackoverflow.com/questions/58440480/connect-to-azure-sql-in-python-with-mfa-active-directory-interactive-authenticat

try:
    from azure.identity import AzureCliCredential
    azure_identitiy_available = True
except ImportError:
    azure_identitiy_available = False

import pyodbc
from time import perf_counter
from datetime import datetime

import sys, traceback
import struct
import argparse
import logging

def parse_args():
    parser = argparse.ArgumentParser(description='Execute an sql command on an sql server')

    parser.add_argument('--source-driver', dest='source_driver', default='{ODBC Driver 18 for SQL Server}', help='source database server driver (default: %(default)s)')
    parser.add_argument('--source-server', dest='source_server', help='source database server name', required=True)
    parser.add_argument('--source-db', dest='source_db', help='source database name', required=True)
    parser.add_argument('--source-authentication', dest='source_authentication', default='UsernamePassword', help='source database authentication. Possible to use AzureActiveDirectory (default: %(default)s)')
    parser.add_argument('--source-user', dest='source_user', help='source database username, if authentication is set to UsenamePassword')
    parser.add_argument('--source-password', dest='source_password', help='source database password, if authentication is set to UsenamePassword')

    parser.add_argument('--debug-sql', dest='debug_sql', default = False, action='store_true', help='If enabled, prints sql statements. (default: %(default)d)')

    parser.add_argument('sql_commands', type=str, nargs='+', help='The SQL command to execute. Use multiple strings for multiple sql commands to be executed sequentially.')

    return parser

# Function to create a connection',
def create_connection(config) -> pyodbc.Connection:
#    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};UID={config["user"]};PWD={config["password"]};Encrypt=Yes;TrustServerCertificate=Yes;'
# jdbc:sqlserver://portal-int-cl1-prod-sqlserver.database.windows.net:1433;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryPassword

    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};Encrypt=Yes;TrustServerCertificate=Yes;hostNameInCertificate=*.database.windows.net;loginTimeout=30'
    attrs_before = None

    if "authentication" in config and config["authentication"]  == 'AzureActiveDirectory':
        if not azure_identitiy_available:
            print("For AzureActiveDirectory authentication, please install azure-identity first!")
            sys.exit(-1)

        # Use the cli credential to get a token after the user has signed in via the Azure CLI 'az login' command.
        credential = AzureCliCredential()
        databaseToken = credential.get_token('https://database.windows.net/')

        # get bytes from token obtained
        tokenb = bytes(databaseToken[0], "UTF-16-LE")
        tokenstruct = struct.pack("=i", len(tokenb)) + tokenb;
        SQL_COPT_SS_ACCESS_TOKEN = 1256 
        attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct}
        print(f'using authentication {config["authentication"]}...', end="")

    else:
        # username/password:    
        conn_str = conn_str + f';UID={config["user"]};PWD={config["password"]}'
        print(f'using authentication username/password', end="")
    
    return pyodbc.connect(conn_str, attrs_before = attrs_before)

def execute_sql(connection, sql_commands):
    cursor = connection.cursor()
    for sql_command in sql_commands:
        print(f'executing SQL statement: {sql_command}', flush=True)

        start_time = perf_counter()

        cursor.execute(sql_command)

        duration_sec = perf_counter() - start_time

        if sql_command.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            
            if rows:
                print(f"Query returned {len(rows)} rows in {duration_sec:.1f}s", flush=True)
                columns = [column[0] for column in cursor.description]
                print(" | ".join(columns))
                
                for row in rows:
                    print(" | ".join(str(value) for value in row))
            else:
                print("No rows found.")
        else:
            # For non-SELECT queries, you can check the row count for feedback
            affected_rows = cursor.rowcount
            if affected_rows >= 0:
                print(f"Query executed successfully in {duration_sec:.1f}s. {affected_rows} rows affected.")
            else:
                print(f"Query executed successfully in {duration_sec:.1f}s, but the number of affected rows is unknown ")
        connection.commit()
            
    cursor.close()

# Main process
if __name__ == '__main__':
    logging.basicConfig() # initializiation needed!!!!

    current_datetime = datetime.now()
    print(f'starting at {current_datetime.isoformat()}')


    sql_logger = logging.getLogger('sql')

    parser = parse_args()
    ARGS = parser.parse_args()

    if ARGS.debug_sql:
        sql_logger.setLevel(logging.DEBUG)

    source_config = { 
        'driver': ARGS.source_driver,
        'server': ARGS.source_server,
        'database': ARGS.source_db,
        'authentication': ARGS.source_authentication,
        'user': ARGS.source_user,
        'password': ARGS.source_password,
    }

    connection = None
    try:
        # Create connections
        print(f'connecting to source server {source_config["server"]} db {source_config["database"]}... ', end="", flush=True)
        connection = create_connection(source_config)
        print(' - DONE')

        execute_sql(connection, ARGS.sql_commands)


    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc(file=sys.stdout)
    finally:
        if connection:
            connection.close()
