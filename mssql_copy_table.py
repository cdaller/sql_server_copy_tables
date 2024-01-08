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
import sys
import struct
import argparse
import re

DEFAULT_PAGE_SIZE=50000

def parse_args():
    parser = argparse.ArgumentParser(description='Copy one or more tables from an sql server to another sql server', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--source-driver', dest='source_driver', default='{ODBC Driver 18 for SQL Server}', help='source database server driver')
    parser.add_argument('--source-server', dest='source_server', help='source database server name', required=True)
    parser.add_argument('--source-db', dest='source_db', help='source database name', required=True)
    parser.add_argument('--source-schema', dest='source_schema', default='dbo', help='source database schema name')
    parser.add_argument('--source-authentication', dest='source_authentication', default='UsernamePassword', help='source database authentication. default is UsernamePassword. Possible to use AzureActiveDirectory')
    parser.add_argument('--source-user', dest='source_user', help='source database username, if authentication is set to UsenamePassword')
    parser.add_argument('--source-password', dest='source_password', help='source database password, if authentication is set to UsenamePassword')
    parser.add_argument('--source-list-tables', dest='source_list_tables', default=False, action='store_true', help='If set, a list of tables is printed, no data is copied!')

    parser.add_argument('--target-driver', dest='target_driver', default='{ODBC Driver 18 for SQL Server}', help='target database server driver')
    parser.add_argument('--target-server', dest='target_server', help='target database server name', required=True)
    parser.add_argument('--target-db', dest='target_db', help='target database name', required=True)
    parser.add_argument('--target-schema', dest='target_schema', default='dbo', help='target database schema name')
    parser.add_argument('--target-authentication', dest='target_authentication', default='UsernamePassword', help='target database authentication. default is UsernamePassword. Possible to use AzureActiveDirectory')
    parser.add_argument('--target-user', dest='target_user', help='source database username, if authentication is set to UsenamePassword')
    parser.add_argument('--target-password', dest='target_password', help='source database password, if authentication is set to UsenamePassword')
    parser.add_argument('--target-list-tables', dest='target_list_tables', default=False, action='store_true', help='If set, a list of tables is printed, no data is copied!')

    parser.add_argument('--truncate-table', dest='truncate_table', default=False, action=argparse.BooleanOptionalAction, help='If set, truncate the target table before inserting rows from source table.')
    parser.add_argument('--drop-table', dest='drop_table', default=True, action=argparse.BooleanOptionalAction, help='If set, drop (if exists) and recreate the target table before inserting rows from source table. All columns, types and not-null and primary key constraints will also be copied. Indices of the table will also be recreated if not prevented by --no-copy-indices flag')
    parser.add_argument('--copy-indices', dest='copy_indices', default=True, action=argparse.BooleanOptionalAction, help='Create the indices for the target tables as they exist on the source table')
    parser.add_argument('--dry-run', dest='dry_run', default=False, action='store_true', help='Do not modify target database, just print what would happen')

    parser.add_argument('-t', '--table', nargs='*', dest='tables', help='Specify the tables you want to copy. Either repeat "-t <name> -t <name2>" or by "-t <name> <name2>"')
    parser.add_argument('--all-tables', dest='copy_all_tables', default=False, action='store_true', help='Copy all tables in the schema from the source db to the target db')
    parser.add_argument('--table-filter', dest='table_filter', default = None, help='Filter table names using this regular expression (regexp must match tables name). Use with "--all-tables" or one of the "list-tables" arguments.')

    return parser


def get_dry_run_text(dry_run: bool) -> str:
    if dry_run:
        return " (DRY RUN)"
    else:
        return ""

# Function to create a connection',
def create_connection(config):
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


# Function to get the create table query
def get_create_table_query(source_cursor, source_schema, table_name, target_schema):
    # Get column definitions
    source_cursor.execute(f"""
        SELECT 
            COLUMN_NAME, DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, 
            COLUMN_DEFAULT, DATETIME_PRECISION
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = N'{source_schema}' AND TABLE_NAME = N'{table_name}'
    """)
    columns = source_cursor.fetchall()

    column_definitions = []
    for column in columns:
        col_def = f"{column.COLUMN_NAME} {column.DATA_TYPE}"
        # Add length for character types and precision for datetime2
        if column.DATA_TYPE in ['varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary']:
            col_def += f"({column.CHARACTER_MAXIMUM_LENGTH})" if column.CHARACTER_MAXIMUM_LENGTH else "(max)"
        elif column.DATA_TYPE == 'datetime2':
            col_def += f"({column.DATETIME_PRECISION})"

        if column.IS_NULLABLE == 'NO':
            col_def += " NOT NULL"
        if column.COLUMN_DEFAULT:
            col_def += f" DEFAULT {column.COLUMN_DEFAULT}"

        column_definitions.append(col_def)


    # Get primary key information
    source_cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = N'{source_schema}' AND TABLE_NAME = N'{table_name}' AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    """)
    primary_keys = [row.COLUMN_NAME for row in source_cursor.fetchall()]
    
    pk_definition = ''
    if primary_keys:
        pk_definition = f", PRIMARY KEY ({', '.join(primary_keys)})"

    # Combine to form CREATE TABLE statement
    create_table_statement = f"CREATE TABLE {target_schema}.{table_name} ({', '.join(column_definitions)}{pk_definition})"
    return create_table_statement

# fetch input sizes for decimal columns (see https://github.com/mkleehammer/pyodbc/issues/845)
def get_input_sizes_for_decimal(conn, schema_name, table_name):
    cursor = conn.cursor()
    
    # Query column types, precision, and scale
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, (schema_name, table_name))

    input_sizes = []
    for row in cursor.fetchall():
        column_name, data_type, precision, scale = row
        
        # For decimal columns, add a tuple of (type, precision, scale)
        if data_type == 'decimal':
            input_sizes.append((pyodbc.SQL_DECIMAL, precision, scale))
        else:
            # For other data types, you can choose to add specific handling or use None
            input_sizes.append(None)

    return input_sizes

# Function to copy data from source to target
def copy_data(source_conn, target_conn, source_schema, table_name, target_schema, dry_run = False):
    print(f"Copying table {table_name} ...", end="", flush=True)
    start_time = perf_counter()


    with source_conn.cursor() as source_cursor:
        with target_conn.cursor() as target_cursor:

            total_row_count = get_row_count(source_conn, source_schema, table_name)
            print(f" {total_row_count} rows ..." + get_dry_run_text(dry_run), end="")

            input_sizes = get_input_sizes_for_decimal(source_conn, source_schema, table_name)
            target_cursor.fast_executemany = True
            # workaround for an odbc bug that cannot handle decimal values correctly when fast_executemany is True (https://github.com/mkleehammer/pyodbc/issues/845)
            target_cursor.setinputsizes(input_sizes)

            page_count = 0
            offset = 0

            while True:

                page_count += 1

                # Fetch data from source table
                source_cursor.execute(f"SELECT * FROM {source_schema}.{table_name} ORDER BY (SELECT 1) OFFSET {offset} ROWS FETCH NEXT {DEFAULT_PAGE_SIZE} ROWS ONLY")
                rows = source_cursor.fetchall()

                if not rows:
                    break  # Break the loop if there are no more rows to fetch

                row_count = len(rows)
                if row_count == DEFAULT_PAGE_SIZE:
                    if page_count == 1:
                        print(f" paging {int(total_row_count / DEFAULT_PAGE_SIZE + 1)} pages each {DEFAULT_PAGE_SIZE} rows, page", end="")
                    print(f" {page_count}", end="", flush=True)
                else:
                    print(f" writing {row_count} rows ...", end="", flush=True)
                
                # Insert data into target table
                if not dry_run:
                    placeholders = ', '.join(['?' for _ in rows[0]])
                    insert_sql = f"INSERT INTO {target_schema}.{table_name} VALUES ({placeholders})"
                    #print('execute sql ' + insert_sql + str(rows))
                    target_cursor.executemany(insert_sql, rows)

                    target_conn.commit()

                offset += DEFAULT_PAGE_SIZE

            duration_sec = perf_counter() - start_time
            rows_per_sec = int(round(total_row_count / duration_sec))
            print(f" - done in {duration_sec:.1f} seconds ({rows_per_sec} rows/sec)")

def truncate_table(connection, schema_name, table_name, dry_run = False):
    print(f"Truncating table {table_name} {get_dry_run_text(dry_run)} ...", end="", flush=True)
    if not dry_run:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
        connection.commit()
    print(" - done")

def get_row_count(connection, schema_name, table_name) -> int:
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM  {schema_name}.{table_name}")
    total_rows = cursor.fetchone()[0]
    return total_rows

def create_table(source_conn, target_conn, source_schema, table_name, target_schema, dry_run = False):
    # Create table in target database (including primary key and null constraints)
    with source_conn.cursor() as source_cursor:
        create_table_query = get_create_table_query(source_cursor, source_schema, table_name, target_schema)
        if not dry_run:
            target_cursor = target_conn.cursor()
            target_cursor.execute(create_table_query)
        print(f"Table {target_schema}.{table_name} created successfully." + get_dry_run_text(dry_run))
    target_conn.commit()

def drop_table_if_exists(conn, schema_name, table_name, dry_run = False):
    cursor = conn.cursor()

    # Check if the table exists in the given schema
    cursor.execute("""
        SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, (schema_name, table_name))

    if cursor.fetchone():
        # Table exists, drop it
        drop_query = f"DROP TABLE {schema_name}.{table_name}"
        if not dry_run:
            cursor.execute(drop_query)
            conn.commit()
        print(f"Table {schema_name}.{table_name} dropped successfully." + get_dry_run_text(dry_run))
    else:
        print(f"Table {schema_name}.{table_name} is not dropped - does not exist.")


def copy_indices(source_conn, target_conn, source_schema, table_name, target_schema, dry_run = False):
    with source_conn.cursor() as source_cursor:
        with target_conn.cursor() as target_cursor:

            # Query to get index information from the source database
            source_cursor.execute("""
            SELECT 
                i.name AS index_name,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS columns,
                i.is_unique
            FROM 
                sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE 
                t.name = ? AND s.name = ? AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
            GROUP BY i.name, i.is_unique, t.name
            """, (table_name, source_schema))

            # Construct and execute CREATE INDEX statements
            for row in source_cursor:
                index_name, columns, is_unique = row
                unique_clause = "UNIQUE" if is_unique else ""
                create_index_query = f"CREATE {unique_clause} INDEX [{index_name}] ON [{target_schema}].[{table_name}] ({columns})"
                # print('create index ' + create_index_query)
                if not dry_run:
                    target_cursor.execute(create_index_query)

    if not dry_run:
        target_conn.commit()

    print(f"Indices for table {table_name} in schema {target_schema} copied successfully." + get_dry_run_text(dry_run))

def get_table_names(conn, schema):
    cursor = conn.cursor()
    cursor.execute("""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = ?
        AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """, (schema))

    table_names = []
    for row in cursor:
        table_names.append(row.TABLE_NAME)
    return table_names

def filter_strings_by_regex(strings, pattern):
    if pattern is None:
        return strings
    regex = re.compile(pattern)
    filtered_strings = [s for s in strings if regex.match(s)]
    return filtered_strings


# Main process
if __name__ == '__main__':
    parser = parse_args()
    ARGS = parser.parse_args()

    source_config = { 
        'driver': ARGS.source_driver,
        'server': ARGS.source_server,
        'database': ARGS.source_db,
        'schema': ARGS.source_schema,
        'authentication': ARGS.source_authentication,
        'user': ARGS.source_user,
        'password': ARGS.source_password,
    }

    target_config = { 
        'driver': ARGS.target_driver,
        'server': ARGS.target_server,
        'database': ARGS.target_db,
        'schema': ARGS.target_schema,
        'authentication': ARGS.target_authentication,
        'user': ARGS.target_user,
        'password': ARGS.target_password,
    }

    source_conn = None
    target_conn = None
    try:
        source_schema = source_config['schema']
        target_schema = target_config['schema']

        # Create connections
        print(f'connecting to source server {source_config["server"]} db {source_config["database"]}... ', end="")
        source_conn = create_connection(source_config)
        print(' - DONE')

        if ARGS.source_list_tables:
            print(f'List of all tables (filter is applied if given):')
            table_names = get_table_names(source_conn, source_schema)
            for table_name in filter_strings_by_regex(table_names, ARGS.table_filter):
                print(table_name)
            sys.exit(0)

        print(f'connecting to target server {target_config["server"]} db {target_config["database"]}... ', end="")
        target_conn = create_connection(target_config)
        print(' - DONE')

        if ARGS.target_list_tables:
            print(f'List of all tables (filter is applied if given):')
            table_names = get_table_names(target_conn, target_schema)
            for table_name in filter_strings_by_regex(table_names, ARGS.table_filter):
                print(table_name)
            sys.exit(0)

        table_names = ARGS.tables
        if ARGS.copy_all_tables:
            table_names = get_table_names(source_conn, source_schema)

        table_names = filter_strings_by_regex(table_names, ARGS.table_filter)

        for table_name in table_names:
            if ARGS.drop_table:
                drop_table_if_exists(target_conn, target_schema, table_name, ARGS.dry_run)
                create_table(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run)
            if ARGS.copy_indices:
                copy_indices(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run)
            if ARGS.truncate_table:
                truncate_table(target_conn, target_schema, table_name, ARGS.dry_run)

            # Copy data from source to target
            copy_data(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()