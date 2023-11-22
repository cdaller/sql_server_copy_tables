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
    'password': 'x',
    'schema': 'HEIDETRANSFER'
}

table_names = [
            # 'D7ADRESS', #done
            # 'DH7ANS', # done
            # 'DH7EINH', # done
            #'DH7MES', # done with paging
            # 'DH7NUTZ', # done
            # 'DH7OBJ', # done 
            # 'PORTALFLAG', # done
            # 'REP_CRE_KV', # done (removed 'identity'))
            'REP_CRE_MASTER3A', 
            'REP_CRE_MASTER3B', 
            'REP_CRE_MASTER3C', 
            # 'REP_CRE_MP', 'REP_CRE_OB_KZ', 'REP_CRE_VAZ'
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
    print(f"Copying table {table_name} ...", end="", flush=True)
    start_time = perf_counter()

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    total_row_count = get_row_count(source_conn, source_schema, table_name)
    print(f" {total_row_count} rows ...", end="")

    page_size = 500000
    page_count = 0
    offset = 0

    while True:

        page_count += 1

        # Fetch data from source table
        source_cursor.execute(f"SELECT * FROM {source_schema}.{table_name} ORDER BY (SELECT 1) OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY")
        rows = source_cursor.fetchall()

        if not rows:
            break  # Break the loop if there are no more rows to fetch

        row_count = len(rows)
        if row_count == page_size:
            if page_count == 1:
                print(f" paging {int(total_row_count / page_size)} pages each {page_size} rows, page", end="")
            print(f" {page_count}", end="", flush=True)
        else:
            print(f" writing {row_count} rows ...", end="", flush=True)
        
        # Insert data into target table
        placeholders = ', '.join(['?' for _ in rows[0]])
        target_cursor.fast_executemany = True
        target_cursor.executemany(f"INSERT INTO {target_schema}.{table_name} VALUES ({placeholders})", rows)

        target_conn.commit()

        offset += page_size

    duration_sec = perf_counter() - start_time
    rows_per_sec = int(round(total_row_count / duration_sec))
    print(f" - done in {duration_sec:.1f} seconds ({rows_per_sec} rows/sec)")

def truncate_table(connection, schema_name, table_name):
    print(f"Truncating table {table_name} ...", end="", flush=True)
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
    connection.commit()
    print(" - done")

def get_row_count(connection, schema_name, table_name) -> int:
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM  {schema_name}.{table_name}")
    total_rows = cursor.fetchone()[0]
    return total_rows

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
