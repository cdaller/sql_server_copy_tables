#!/usr/bin/env python3

import pyodbc
from time import perf_counter

# Configuration for source and target databases
source_config = {
    'driver': '{ODBC Driver 18 for SQL Server}',
    'server': 'liferay-docker.dccsintra.net',
    'database': 'LRTransferTest',
    'user': 'sa',
    'password': 'xxx',
    'schema': 'HEIDE'
}

target_config = {
    'driver': '{ODBC Driver 18 for SQL Server}',
    'server': 'localhost',
    'database': 'liferay-db',
    'user': 'sa',
    'password': 'xxx',
    'schema': 'HEIDETRANSFER'
}

table_names = [
            # 'D7ADRESS', 
            'DH7ANS', 
            # 'DH7EINH', 
               'DH7MES', 
               'DH7NUTZ', 'DH7OBJ' 'PORTALFLAG', 
            #    'REP_CRE_KV', 'REP_CRE_MASTER3A', 
            #    'REP_CRE_MASTER3B', 
            #    'REP_CRE_MASTER3C', 
            #    'REP_CRE_MP', 'REP_CRE_OB_KZ', 'REP_CRE_VAZ'
               ]

# Function to create a connection
def create_connection(config):
    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};UID={config["user"]};PWD={config["password"]};Encrypt=Yes;TrustServerCertificate=Yes'
    return pyodbc.connect(conn_str)

# Function to get the create table query
def get_create_table_query(source_cursor, schema_name, table_name):
    source_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'")
    columns = source_cursor.fetchall()
    
    column_definitions = ', '.join([f"{column[3]} {column[7]}{' NOT NULL' if column[6] == 'NO' else ''}" for column in columns])
    return f"CREATE TABLE {schema_name}.{table_name} ({column_definitions})"

# Function to copy data from source to target
def copy_data(source_conn, target_conn, source_schema, table_name, target_schema):
    print(f"Copying table {table_name} ...", end="")
    start_time = perf_counter()

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    # Fetch data from source table
    source_cursor.execute(f"SELECT * FROM {source_schema}.{table_name}")
    rows = source_cursor.fetchall()
    row_count = len(rows)
    
    # Insert data into target table
    placeholders = ', '.join(['?' for _ in rows[0]])
    target_cursor.fast_executemany = True
    target_cursor.executemany(f"INSERT INTO {target_schema}.{table_name} VALUES ({placeholders})", rows)
    target_conn.commit()
    duration_sec = perf_counter() - start_time
    rows_per_sec = int(round(row_count / duration_sec))
    print(f" - done in {duration_sec:.1f} seconds for {row_count} rows ({rows_per_sec} rows/sec)")

def truncate_table(sql_conn, schema_name, table_name):
    print(f"Truncating table {table_name} ...", end="")
    cursor = sql_conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
    sql_conn.commit()
    print(f" - done")

# Main process
try:
    # Create connections
    source_conn = create_connection(source_config)
    target_conn = create_connection(target_config)

    source_schema = source_config['schema']
    target_schema = target_config['schema']

    # Create table in target database (not working!) - tables need to be created before!
    # source_cursor = source_conn.cursor()
    # create_table_query = get_create_table_query(source_cursor, target_schema, table_name)
    # target_cursor = target_conn.cursor()
    # target_cursor.execute(create_table_query)
    # target_conn.commit()

    for table_name in table_names:
        truncate_table(target_conn, target_schema, table_name)
        # Copy data from source to target
        copy_data(source_conn, target_conn, source_schema, table_name, target_schema)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    source_conn.close()
    target_conn.close()
