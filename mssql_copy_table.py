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
import sys, traceback
import struct
import argparse
import re
import logging
from datetime import datetime
from typing import List, Dict, Tuple

STATUS_START = 'START'
STATUS_SUCCESS = 'SUCCESS'
STATUS_ERROR = 'ERROR'

def parse_args():
    parser = argparse.ArgumentParser(description='Copy one or more tables from an sql server to another sql server')
    parser.add_argument('--source-driver', dest='source_driver', default='{ODBC Driver 18 for SQL Server}', help='source database server driver (default: %(default)s)')
    parser.add_argument('--source-server', dest='source_server', help='source database server name', required=True)
    parser.add_argument('--source-db', dest='source_db', help='source database name', required=True)
    parser.add_argument('--source-schema', dest='source_schema', default='dbo', help='source database schema name (default: %(default)s)')
    parser.add_argument('--source-authentication', dest='source_authentication', default='UsernamePassword', help='source database authentication. Possible to use AzureActiveDirectory (default: %(default)s)')
    parser.add_argument('--source-user', dest='source_user', help='source database username, if authentication is set to UsernamePassword')
    parser.add_argument('--source-password', dest='source_password', help='source database password, if authentication is set to UsernamePassword')
    parser.add_argument('--source-list-tables', dest='source_list_tables', default=False, action='store_true', help='If set, a list of tables is printed, no data is copied! (default: %(default)s)')

    parser.add_argument('--target-driver', dest='target_driver', default='{ODBC Driver 18 for SQL Server}', help='target database server driver (default: %(default)s)')
    parser.add_argument('--target-server', dest='target_server', help='target database server name', required=True)
    parser.add_argument('--target-db', dest='target_db', help='target database name', required=True)
    parser.add_argument('--target-schema', dest='target_schema', default='dbo', help='target database schema name (default: %(default)s)')
    parser.add_argument('--target-authentication', dest='target_authentication', default='UsernamePassword', help='target database authentication. Possible to use AzureActiveDirectory (default: %(default)s)')
    parser.add_argument('--target-user', dest='target_user', help='source database username, if authentication is set to UsernamePassword')
    parser.add_argument('--target-password', dest='target_password', help='source database password, if authentication is set to UsernamePassword')
    parser.add_argument('--target-list-tables', dest='target_list_tables', default=False, action='store_true', help='If set, a list of tables is printed, no data is copied!')

    parser.add_argument('--truncate-table', dest='truncate_table', default=False, action=argparse.BooleanOptionalAction, help='If set, truncate the target table before inserting rows from source table. If this option is set, the tables are NOT recreated, even if --create-table is used! (default: %(default)s)')
    parser.add_argument('--create-table', dest='create_table', default=True, action=argparse.BooleanOptionalAction, help='If set, drop (if exists) and (re)create the target table before inserting rows from source table. All columns, types and not-null and primary key constraints will also be copied. Indices of the table will also be recreated if not prevented by --no-copy-indices flag (default: %(default)s)')
    parser.add_argument('--copy-indices', dest='copy_indices', default=True, action=argparse.BooleanOptionalAction, help='Create the indices for the target tables as they exist on the source table (default: %(default)s)')
    parser.add_argument('--drop-indices', dest='drop_indices', default=True, action=argparse.BooleanOptionalAction, help='Drop indices before copying data for performance reasons. The indices are created after copying by --copy-indices afterwards (default: %(default)s)')
    parser.add_argument('--copy-data', dest='copy_data', default=True, action=argparse.BooleanOptionalAction, help='Copy the data of the tables. Default True! Use --no-copy-data if you want to creat the indices only. (default: %(default)s)')
    parser.add_argument('--dry-run', dest='dry_run', default=False, action='store_true', help='Do not modify target database, just print what would happen. (default: %(default)s)')

    parser.add_argument('--compare-table', dest='compare_table', default=False, action=argparse.BooleanOptionalAction, help='If set, do not copy any data, but compare the source and the target table(s) and print if there are any differences in columns, indices or content rows. (default: %(default)s)')
    parser.add_argument('--compare-view', dest='compare_view', default=False, action=argparse.BooleanOptionalAction, help='If set, do not copy any data, but compare the source and the target view(s) and print if there are any differences in columns. (default: %(default)s)')

    parser.add_argument('-t', '--table', nargs='+', action='extend', dest='tables', help='Specify the tables you want to copy. Either repeat "-t <name> -t <name2>" or by "-t <name> <name2>"')
    parser.add_argument('--all-tables', dest='copy_all_tables', default=False, action='store_true', help='Copy all tables in the schema from the source db to the target db. (default: %(default)s)')
    parser.add_argument('--table-filter', dest='table_filter', default = None, help='Filter table names using this regular expression (regexp must match table names). Use with "--all-tables" or one of the "list-tables" arguments. (default: %(default)s)')
    parser.add_argument('--page-size', dest='page_size', default = 50000, type=int, help='Page size of rows that are copied in one step. Depending on the size of table, values between 50000 (default) and 500000 are working well (depending on the number of rows, etc.). (default: %(default)d)')
    parser.add_argument('--page-start', dest='page_start', default = 1, type=int, help='Page to start with. Please note that the first page number ist 1 to match the output during copying of the data. The output of a page number indicates the page is read. The "w" after the page number shows that the pages was successfully written. Please also note that this settings does not make much sense if you copy more than one table! (default: %(default)d)')

    parser.add_argument('--where', dest='where_clause', default = None, help='If set, this where clause is added to all queries executed on the source data source. If you only want to add some rows, use in combination with the params "--no-create-table --no-drop-indices --no-copy-indices". (default: %(default)s)')
    parser.add_argument('--delete-where', dest='delete_where', default = False, action=argparse.BooleanOptionalAction, help='Delete all rows in the target table using the given where clause if a where clause is set with the "--where" parameter. (default: %(default)s)')
    parser.add_argument('--join', nargs='+', action='extend', dest='joins', default = None, help='Add one or more joins to the selection of data (probably only useful in combination with the --where clause). The original table name is \"source_table\" to use in the joins. Either use the parameter multiple times or separate the joins with spaces.". (default: %(default)s)')

    parser.add_argument('--copy-view', dest='copy_view', default=False, action=argparse.BooleanOptionalAction, help='Copy the views. By default all views are copied if not limited by "--view <name>" "--view-filter <regepx>"! (default: %(default)s)')
    parser.add_argument('--view', nargs='+', action='extend', dest='views', help='Specify the views you want to copy. Either repeat "--view <name> --view <name2>" or by "--view <name> <name2>"')
    parser.add_argument('--view-filter', dest='view_filter', default = None, help='Filter view names using this regular expression (regexp must match view names). (default: %(default)s)')

    parser.add_argument('--debug-sql', dest='debug_sql', default = False, action='store_true', help='If enabled, prints sql statements. (default: %(default)d)')

    parser.add_argument('--progress-track-file', dest='progress_file_name', default = None, help='If set, a file with the given name is used to remember which tables/views it already processed sucessfully. If the script is restarted, all tables/views are not processed that were processed sucessfully before.". (default: %(default)s)')


    return parser


def get_dry_run_text(dry_run: bool) -> str:
    if dry_run:
        return " (DRY RUN)"
    else:
        return ""
    
def execute_sql(cursor, sql, *parameters) -> pyodbc.Cursor: 
    if sql_logger.isEnabledFor(logging.DEBUG):
        if parameters:
            sql_logger.debug(f"execute sql: {sql} with params: {parameters}")
        else:
            sql_logger.debug(f"execute sql: {sql}")
    return cursor.execute(sql, *parameters)

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

# Function to get the create table query
def get_create_table_query(source_cursor, source_schema, table_name, target_schema) -> str:
    # Get column definitions
    source_cursor.execute(f"""
        SELECT 
            c.COLUMN_NAME, c.DATA_TYPE, 
            c.CHARACTER_MAXIMUM_LENGTH, c.IS_NULLABLE, 
            c.COLUMN_DEFAULT, c.DATETIME_PRECISION,
            c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
            col.is_identity
        FROM INFORMATION_SCHEMA.COLUMNS c
        JOIN sys.columns col
            ON col.name = c.COLUMN_NAME
            AND col.object_id = OBJECT_ID(N'{source_schema}.{table_name}')
        WHERE c.TABLE_SCHEMA = N'{source_schema}' AND c.TABLE_NAME = N'{table_name}'
    """)
    columns = source_cursor.fetchall()

    column_definitions = []
    for column in columns:
        col_def = f"{column.COLUMN_NAME} {column.DATA_TYPE}"

        # Add details for specific data types
        if column.DATA_TYPE in ['varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary']:
            col_def += f"({column.CHARACTER_MAXIMUM_LENGTH})" if column.CHARACTER_MAXIMUM_LENGTH and column.CHARACTER_MAXIMUM_LENGTH > 0 else "(max)"
        elif column.DATA_TYPE == 'datetime2':
            col_def += f"({column.DATETIME_PRECISION})"
        elif column.DATA_TYPE in ['decimal', 'numeric']:
            precision = column.NUMERIC_PRECISION if column.NUMERIC_PRECISION is not None else 18
            scale = column.NUMERIC_SCALE if column.NUMERIC_SCALE is not None else 0
            col_def += f"({precision}, {scale})"

        # Add identity property
        if column.is_identity:
            col_def += " IDENTITY(1, 1)"
        # Add NOT NULL constraint
        if column.IS_NULLABLE == 'NO':
            col_def += " NOT NULL"
        # Add DEFAULT constraint if exists
        if column.COLUMN_DEFAULT:
            col_def += f" DEFAULT {column.COLUMN_DEFAULT}"

        column_definitions.append(col_def)

    # Improved query to fetch primary key information
    source_cursor.execute(f"""
        SELECT 
            kc.name AS PK_NAME, 
            i.type_desc AS INDEX_TYPE,
            STRING_AGG(col.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMN_NAMES
        FROM 
            sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
            JOIN sys.key_constraints kc ON t.object_id = kc.parent_object_id AND kc.type = 'PK' AND i.index_id = kc.unique_index_id
        WHERE 
            t.name = N'{table_name}' 
            AND s.name = N'{source_schema}'
            AND i.is_primary_key = 1
        GROUP BY kc.name, i.type_desc
    """)
    pk_info = source_cursor.fetchone()

    pk_definition = ''
    if pk_info:
        pk_name = pk_info.PK_NAME
        index_type = "CLUSTERED" if pk_info.INDEX_TYPE == "CLUSTERED" else "NONCLUSTERED"
        pk_definition = f", CONSTRAINT {pk_name} PRIMARY KEY {index_type} ({pk_info.COLUMN_NAMES})"

    # Combine to form CREATE TABLE statement
    create_table_statement = f"CREATE TABLE {target_schema}.{table_name} ({', '.join(column_definitions)}{pk_definition})"
    return create_table_statement


# fetch input sizes for decimal columns (see https://github.com/mkleehammer/pyodbc/issues/845)
def get_input_sizes(conn, schema_name, table_name) -> []:
    cursor = conn.cursor()
    
    # Query column types, precision, scale, and character maximum length
    execute_sql(cursor, f"""
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, (schema_name, table_name))

    # Define a large size for VARCHAR(MAX)
    varchar_max_size = 512000  # This is a large size that is typically used to represent VARCHAR(MAX)

    input_sizes = []
    for row in cursor.fetchall():
        column_name, data_type, precision, scale, char_max_length = row
        
        # Handle decimal columns
        if data_type == 'decimal':
            input_sizes.append((pyodbc.SQL_DECIMAL, precision, scale))
        # Handle varchar and nvarchar columns
        # elif data_type in ['varchar', 'nvarchar']:
        #     if char_max_length == -1:  # Indicates varchar(max) or nvarchar(max)
        #         input_sizes.append(varchar_max_size)
        #     else:
        #         input_sizes.append(char_max_length)
        # # Handle bigint columns
        # elif data_type == 'bigint':
        #     input_sizes.append(pyodbc.SQL_BIGINT)
        else:
            # For other data types, you can choose to add specific handling or use None
            input_sizes.append(None)

    return input_sizes

def get_primary_key(source_conn, source_schema, table_name):
    """
    Return all primar key columns comma separated

    :param source_conn: The database connection object.
    :param source_schema: The schema of the table.
    :param table_name: The name of the table.
    :return: The name of the numerical primary key column, or None if not found.
    """
    with source_conn.cursor() as cursor:
        # Query to find the primary key
        pk_query = f"""
            SELECT kcu.COLUMN_NAME, c.DATA_TYPE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c
                ON kcu.COLUMN_NAME = c.COLUMN_NAME
                AND kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND kcu.TABLE_NAME = c.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = '{source_schema}'
                AND tc.TABLE_NAME = '{table_name}'
        """
        execute_sql(cursor, pk_query)
        rows = cursor.fetchall()
        return rows


def get_numerical_primary_key(source_conn, source_schema, table_name) -> str:
    """
    Determine if the given table has a numerical primary key and return its column name.

    :param source_conn: The database connection object.
    :param source_schema: The schema of the table.
    :param table_name: The name of the table.
    :return: The name of the numerical primary key column, or None if not found.
    """
    rows = get_primary_key(source_conn, source_schema, table_name)
    if len(rows) != 1:
        # deny if none or more than one column (combined primary key)
        return None

    row = rows[0]
    # Check if primary key exists and is numerical
    if row and row.DATA_TYPE in ['int', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal']:
        return row.COLUMN_NAME

    return None

def get_primary_key_column_names(source_conn, source_schema, table_name):
    rows = get_primary_key(source_conn, source_schema, table_name)
    if len(rows) == 0:
        return None
    primary_key_columns = [row.COLUMN_NAME for row in rows]
    primary_key_columns_str = ', '.join(primary_key_columns)
    return primary_key_columns_str

# Function to copy data from source to target
def copy_data(source_conn, target_conn, source_schema, table_name, target_schema, page_start, dry_run=False, page_size=50000, where_clause=None, joins=None):
    print(f"Copying table {table_name} {'using where clause [' + where_clause + ']' if where_clause else ''}...", end="", flush=True)
    start_time = perf_counter()

    primary_key = get_numerical_primary_key(source_conn, source_schema, table_name)
    if primary_key:
        print(f" using primary key '{primary_key}' for optimization ...", end="", flush=True)

    with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:
        # Check if table has any identity columns
        execute_sql(source_cursor, f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{table_name}' AND COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
        """)
        identity_columns = [row.COLUMN_NAME for row in source_cursor.fetchall()]

        # Set IDENTITY_INSERT ON only if there are identity columns
        if identity_columns:
            execute_sql(target_cursor, f"SET IDENTITY_INSERT {target_schema}.{table_name} ON")

        # Get column names for the INSERT statement
        execute_sql(source_cursor,f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{table_name}'
        """)
        columns = [row.COLUMN_NAME for row in source_cursor.fetchall()]
        column_list = ", ".join(columns)

        # Get total row count
        total_row_count = get_row_count(source_conn, source_schema, table_name, where_clause, joins)
        print(f" {total_row_count:_} rows ..." + get_dry_run_text(dry_run), end="", flush=True)

        input_sizes = get_input_sizes(source_conn, source_schema, table_name)
        target_cursor.fast_executemany = True
        target_cursor.setinputsizes(input_sizes)

        page_count = page_start
        offset = page_start * page_size
        print_page_info = True

        while True:
            start_time_page = perf_counter()
            page_count += 1

            join_sql = " ".join([f'\nJOIN {join}' for join in joins]) if joins else ''

            if primary_key:
                # Use primary key for efficient paging
                execute_sql(source_cursor, f"""
                    WITH fetching AS (
                        SELECT source_table.{primary_key}, n=ROW_NUMBER() OVER ( ORDER BY source_table.{primary_key})
                        FROM {source_schema}.{table_name} source_table
                        {join_sql}
                        {'WHERE ' + where_clause if where_clause else ''}
                    )
                    SELECT source_table.* 
                    FROM fetching f 
                    JOIN {source_schema}.{table_name} source_table ON source_table.{primary_key} = f.{primary_key}
                    WHERE f.n > {offset} and f.n <= {offset + page_size}
                    OPTION (RECOMPILE)
                """)
            else:
                # Use OFFSET for paging when no numerical primary key is available
                primary_key_column_names = get_primary_key_column_names(source_conn, source_schema, table_name) or '(SELECT NULL)'
                execute_sql(source_cursor, f"""
                    SELECT * FROM {source_schema}.{table_name} source_table
                    {join_sql}
                    {'WHERE ' + where_clause if where_clause else ''}
                    ORDER BY {primary_key_column_names}
                    OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY
                """)

            rows = source_cursor.fetchall()
            if not rows:
                break

            duration_sec_page_read = perf_counter() - start_time_page
            row_count = len(rows)

            if row_count == page_size:
                if print_page_info:
                    print(f" paging {int(total_row_count / page_size + 1)} pages each {page_size:_} rows, page", end="")
                    print_page_info = False
                print(f" {page_count}r({duration_sec_page_read:.1f}s)", end="", flush=True)
            else:
                print(f" reading {row_count:_} rows ({duration_sec_page_read:.1f}s) ", end="", flush=True)

            if not dry_run:
                placeholders = ', '.join(['?' for _ in rows[0]])
                insert_sql = f"INSERT INTO {target_schema}.{table_name} ({column_list}) VALUES ({placeholders})"
                target_cursor.executemany(insert_sql, rows)
                target_conn.commit()
                duration_sec_page_write = perf_counter() - start_time_page - duration_sec_page_read
                print(f"w({duration_sec_page_write:.1f}s)", end="", flush=True)

            offset += page_size

        # Set IDENTITY_INSERT OFF after copying data
        if identity_columns:
            execute_sql(target_cursor, f"SET IDENTITY_INSERT {target_schema}.{table_name} OFF")

        duration_sec = perf_counter() - start_time
        rows_per_sec = int(round(total_row_count / duration_sec))
        print(f" - done in {duration_sec:.1f} seconds ({rows_per_sec} rows/sec)")

def delete_data(connection, schema_name, table_name, where_clause, joins, dry_run = False):
    print(f"Deleting data in table {table_name} using where clause \"{where_clause}\" {get_dry_run_text(dry_run)} ...", end="", flush=True)
    if not dry_run:
        with connection.cursor() as cursor:
            join_sql = " ".join([f'\nJOIN {join}' for join in joins]) if joins else ''
            execute_sql(cursor, f"DELETE source_table FROM {schema_name}.{table_name} source_table {join_sql} WHERE {where_clause}")
        connection.commit()
    print(" - done")

def truncate_table(connection, schema_name, table_name, dry_run = False):
    print(f"Truncating table {table_name} {get_dry_run_text(dry_run)} ...", end="", flush=True)
    if not dry_run:
        with connection.cursor() as cursor:
            execute_sql(cursor, f"TRUNCATE TABLE {schema_name}.{table_name}")
        connection.commit()
    print(" - done")

def get_row_count(connection, schema_name, table_name, where_clause, joins) -> int:
    with connection.cursor() as cursor:
        join_sql = " ".join([f'\nJOIN {join}' for join in joins]) if joins else ''
        sql = f"SELECT COUNT(*) FROM {schema_name}.{table_name} source_table {join_sql} {'WHERE ' + where_clause if where_clause else ''}"
        execute_sql(cursor, sql)
        total_rows = cursor.fetchone()[0]
        return total_rows

def create_table(source_conn, target_conn, source_schema, table_name, target_schema, dry_run = False):
    # Create table in target database (including primary key and null constraints)
    with source_conn.cursor() as source_cursor:
        create_table_query = get_create_table_query(source_cursor, source_schema, table_name, target_schema)
        if not dry_run:
            # print(f'Create query: {create_table_query}')
            target_cursor = target_conn.cursor()
            execute_sql(target_cursor, create_table_query)
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
            execute_sql(cursor, drop_query)
            conn.commit()
        print(f"Table {schema_name}.{table_name} dropped successfully." + get_dry_run_text(dry_run))
    else:
        print(f"Table {schema_name}.{table_name} is not dropped - does not exist.")


def copy_indices(source_conn, target_conn, source_schema, table_name, target_schema, dry_run = False):
    num_indices = 0
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
            indices = source_cursor.fetchall()
            num_indices = len(indices)
            if num_indices > 0:
                print(f"Create {len(indices)} index object(s) for table {target_schema}.{table_name}: ", end="")
                for row in indices:
                    index_name, columns, is_unique = row
                    unique_clause = "UNIQUE" if is_unique else ""
                    create_index_query = f"CREATE {unique_clause} INDEX [{index_name}] ON [{target_schema}].[{table_name}] ({columns})"
                    if not dry_run:
                        print(f"{index_name} ", end="", flush=True)
                        execute_sql(target_cursor, create_index_query)
                print("") # new line

    if not dry_run:
        target_conn.commit()

    if num_indices > 0:
        print(f"Indices for table {target_schema}.{table_name} created successfully." + get_dry_run_text(dry_run))
    else:
        print(f"No indices for table {target_schema}.{table_name} found - nothing done.")

def drop_all_indices(conn, schema_name, table_name, dry_run = False):
    """
    Drops all indices for a given table in a given schema, excluding primary key constraints, using a context manager.

    :param conn: A pyodbc connection object to the database.
    :param schema_name: The name of the schema containing the table.
    :param table_name: The name of the table whose indices will be dropped.
    """
    with conn.cursor() as cursor:
        # Query to retrieve all non-primary key indices for the table
        query_get_indices = f"""
            SELECT i.name AS IndexName
            FROM sys.indexes AS i
            JOIN sys.tables AS t ON i.object_id = t.object_id
            JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            WHERE t.name = '{table_name}'
              AND s.name = '{schema_name}'
              AND i.is_primary_key = 0
              AND i.type_desc <> 'HEAP'
        """

        execute_sql(cursor, query_get_indices)
        indices = [row.IndexName for row in cursor.fetchall()]

        # Drop each index
        for index_name in indices:
            try:
                drop_query = f"DROP INDEX {index_name} ON {schema_name}.{table_name}"
                execute_sql(cursor, drop_query)
                print(f"Dropped index: {index_name}")
            except Exception as e:
                print(f"Error dropping index {index_name}: {e}")

    if not dry_run:
        conn.commit()

    print(f"Indices for table {target_schema}.{table_name} dropped successfully." + get_dry_run_text(dry_run))

def alter_all_indices(conn, schema_name, table_name, command, dry_run = False):
    with conn.cursor() as cursor:
        # Query to retrieve all non-primary key indices for the table
        query_get_indices = f"ALTER INDEX ALL ON {schema_name}.{table_name} {command}"
        execute_sql(cursor, query_get_indices)
    if not dry_run:
        conn.commit()
    print(f"Indices for table {target_schema}.{table_name}: {command}." + get_dry_run_text(dry_run))

def ireplace(old, new, text) -> str:
    idx = 0
    while idx < len(text):
        index_l = text.lower().find(old.lower(), idx)
        if index_l == -1:
            return text
        text = text[:index_l] + new + text[index_l + len(old):]
        idx = index_l + len(new) 
    return text

def fetch_view_definitions(conn, source_schema, target_schema) -> Tuple[str, str]:
    views_query = f"""
        SELECT o.name AS view_name, m.definition AS view_definition
        FROM sys.sql_modules m
                INNER JOIN sys.objects o ON m.object_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = '{source_schema}' AND o.type = 'V';
    """
    with conn.cursor() as cursor:
        cursor.execute(views_query)
        view_definitions = cursor.fetchall()

        modified_views = []
        for name, definition in view_definitions:
            # Modify the view definition to replace source schema with target schema
            # Note: This simplistic replacement assumes that schema names do not appear in other contexts
            #       where replacement would be incorrect. Adjust logic as needed for complex cases.
            modified_definition = ireplace(f'{source_schema}.', f'{target_schema}.', definition)
            modified_definition = ireplace(f'[{source_schema}].', f'[{target_schema}].', modified_definition)
            modified_views.append((name, modified_definition))

        # print(f"modified_views {modified_views}")
        return modified_views

def create_views(conn, schema, view_definitions, dry_run = False):
    with conn.cursor() as cursor:
        for view_name, view_definition in view_definitions:            
            # Create the view in the destination database
            print(f"Create view {schema}.{view_name}", end="", flush=True)
            if 'create or alter view' not in view_definition.lower():
                view_definition = re.sub(r'create\s+view\s+', 'create or alter view ', view_definition, flags = re.IGNORECASE)
            #print(f"sql: {view_definition}")
            cursor.execute(view_definition)
            print(' - DONE')
    if not dry_run:
        conn.commit()
    print(f"Views in schema {target_schema} created" + get_dry_run_text(dry_run))


def get_table_names(conn, schema) -> List[str]:
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

def filter_strings_by_regex(strings, pattern) -> List[str]:
    if pattern is None:
        return strings
    regex = re.compile(pattern)
    filtered_strings = [s for s in strings if regex.match(s)]
    return filtered_strings

# Function to fetch index and corresponding columns
def get_indices(cursor, schema_name, table_name) -> Dict[str, str]:
    execute_sql(cursor, f"""
        SELECT i.name AS IndexName, COL_NAME(ic.object_id, ic.column_id) AS ColumnName
        FROM sys.indexes AS i
        INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.tables AS t ON i.object_id = t.object_id
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE t.name = '{table_name}' AND s.name = '{schema_name}' AND i.type_desc != 'HEAP'
        ORDER BY i.name, ic.key_ordinal
    """)
    indices = {}
    for row in cursor.fetchall():
        if row.IndexName not in indices:
            indices[row.IndexName] = []
        indices[row.IndexName].append(row.ColumnName)
    # Sort the columns for each index
    for index in indices:
        indices[index] = sorted(indices[index])
    return indices

def compare_table(source_conn, source_schema, table_name, target_conn, target_schema):
    print(f"Comparing table {source_schema}.{table_name} in source to target ...", end="", flush=True)

    with source_conn.cursor() as source_cursor:
        with target_conn.cursor() as target_cursor:

            # Check if the table exists in the target schema
            target_cursor.execute(f"""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{table_name}'
            """)
            target_table_exists = target_cursor.fetchone()

            if not target_table_exists:
                print(f"\n -  Table {table_name} exists in source but not in target.")
                return

            # Get column info for source and target tables
            source_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{table_name}'
            """)
            source_columns = {row.COLUMN_NAME: row.DATA_TYPE for row in source_cursor.fetchall()}

            target_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{table_name}'
            """)
            target_columns = {row.COLUMN_NAME: row.DATA_TYPE for row in target_cursor.fetchall()}

            # Compare column names and types
            for col, type in source_columns.items():
                if col not in target_columns:
                    print(f"\n - Column {col} exists in source table but not in target table.", end="")
                elif type != target_columns[col]:
                    print(f"\n - Column {col} has different type: Source({type}) vs Target({target_columns[col]})", end="")

            for col, type in target_columns.items():
                if col not in source_columns:
                    print(f"\n - Column {col} exists in target table but not in source table.", end="")

            # Compare indices
            source_indices = get_indices(source_cursor, source_schema, table_name)
            target_indices = get_indices(target_cursor, target_schema, table_name)

            source_index_columns = {tuple(sorted(columns)) for columns in source_indices.values()}
            target_index_columns = {tuple(sorted(columns)) for columns in target_indices.values()}

            for columns in source_index_columns:
                if columns not in target_index_columns:
                    print(f"\n - Index with columns {columns} exists in source table but not in target table.", end="")

            for columns in target_index_columns:
                if columns not in source_index_columns:
                    print(f"\n - Index with columns {columns} exists in target table but not in source table.", end="")

            # Compare the number of rows
            source_cursor.execute(f"SELECT COUNT(*) FROM {source_schema}.{table_name}")
            source_row_count = source_cursor.fetchone()[0]

            target_cursor.execute(f"SELECT COUNT(*) FROM {target_schema}.{table_name}")
            target_row_count = target_cursor.fetchone()[0]

            if source_row_count != target_row_count:
                print(f"\n - Row count differs: Source({source_row_count:_}) vs Target({target_row_count:_})", end="")

            print(" - DONE")


def compare_views(source_conn, source_schema, view_definitions, target_conn, target_schema):
    for view_name, view_definition in view_definitions:         
        compare_view(source_conn, source_schema, view_name, target_conn, target_schema)

def compare_view(source_conn, source_schema, view_name, target_conn, target_schema):
    print(f"Comparing view {source_schema}.{view_name} in source to target ...", end="", flush=True)

    with source_conn.cursor() as source_cursor:
        with target_conn.cursor() as target_cursor:

            # Check if the view exists in the target schema
            target_cursor.execute(f"""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{view_name}'
            """)
            target_view_exists = target_cursor.fetchone()

            if not target_view_exists:
                print(f"\n - View {view_name} exists in source but not in target.")
                return

            # Get column info for source and target views
            source_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{view_name}'
            """)
            source_columns = {row.COLUMN_NAME: row.DATA_TYPE for row in source_cursor.fetchall()}

            target_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{view_name}'
            """)
            target_columns = {row.COLUMN_NAME: row.DATA_TYPE for row in target_cursor.fetchall()}

            # Compare column names and types
            for col, type in source_columns.items():
                if col not in target_columns:
                    print(f"\n - Column {col} exists in source view but not in target view.", end="")
                elif type != target_columns[col]:
                    print(f"\n - Column {col} has different type: Source({type}) vs Target({target_columns[col]})", end="")

            for col, type in target_columns.items():
                if col not in source_columns:
                    print(f"\n - Column {col} exists in target view but not in source view.", end="")

            # Compare view definitions
            source_cursor.execute(f"""
                SELECT VIEW_DEFINITION
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = '{view_name}'
            """)
            source_view_definition = source_cursor.fetchone()[0]

            target_cursor.execute(f"""
                SELECT VIEW_DEFINITION
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{view_name}'
            """)
            target_view_definition = target_cursor.fetchone()[0]

            # Normalize both view definitions before comparing
            normalized_source_definition = normalize_definition(source_view_definition)
            normalized_target_definition = normalize_definition(target_view_definition)

            if normalized_source_definition != normalized_target_definition:
                print(f"\n - View definitions differ after normalization:\nSource:\n{source_view_definition}\nTarget:\n{target_view_definition}")

            print(" - DONE")


def normalize_definition(definition):
    """Normalize view definition by removing extra spaces and making it lowercase."""
    # Convert to lowercase
    definition = definition.lower()

    # Remove extra whitespace (multiple spaces, newlines, etc.)
    definition = re.sub(r'\s+', ' ', definition).strip()

    return definition


def has_progress_track_success(file_name, id) -> bool:
    if not file_name:
        return False
    
    with open(file_name, 'r') as file:
        for line in file:
            if line.startswith(f'{id}: {STATUS_SUCCESS}'):
                return True
    return False

def write_progress_track(file_name, id, status):
    if file_name:
        with open(file_name, "a") as file:
            now = datetime.now()
            now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
            file.write(f'{id}: {status} @{now_str}\n')

def execute_with_progress_track(track_file_name, id, function):
    write_progress_track(track_file_name, status_id, STATUS_START)
    if has_progress_track_success(track_file_name, id):
        print(f'Skipping {id}, was already processed sucessfully before (see {track_file_name}!')
    else:
        function() # passed as lambda
    write_progress_track(ARGS.progress_file_name, status_id, STATUS_SUCCESS)

# Main process
if __name__ == '__main__':
    logging.basicConfig() # initializiation needed!!!!

    sql_logger = logging.getLogger('sql')

    parser = parse_args()
    ARGS = parser.parse_args()

    if ARGS.debug_sql:
        sql_logger.setLevel(logging.DEBUG)

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
        print(f'connecting to source server {source_config["server"]} db {source_config["database"]}... ', end="", flush=True)
        source_conn = create_connection(source_config)
        print(' - DONE')

        if ARGS.source_list_tables:
            print(f'List of all tables (filter is applied if given):')
            table_names = get_table_names(source_conn, source_schema)
            for table_name in filter_strings_by_regex(table_names, ARGS.table_filter):
                print(table_name)
            sys.exit(0)

        print(f'connecting to target server {target_config["server"]} db {target_config["database"]}... ', end="", flush=True)
        target_conn = create_connection(target_config)
        print(' - DONE')

        if ARGS.target_list_tables:
            print(f'List of all tables (filter is applied if given):')
            table_names = get_table_names(target_conn, target_schema)
            for table_name in filter_strings_by_regex(table_names, ARGS.table_filter):
                print(table_name)
            sys.exit(0)

        table_names = ARGS.tables if ARGS.tables else []
        if ARGS.copy_all_tables or (table_names is None and ARGS.compare_table):
            table_names = get_table_names(source_conn, source_schema)

        table_names = filter_strings_by_regex(table_names, ARGS.table_filter)

        for table_name in table_names:

            if ARGS.compare_table:
                compare_table(source_conn, source_schema, table_name, target_conn, target_schema)
            
            if not ARGS.compare_table and not ARGS.compare_view:

                if ARGS.truncate_table:
                    if ARGS.page_start != 1:
                        print("WARNING: Setting a start page and truncating the table does not make sense! - ignore the truncation!")
                    else:
                        status_id = f'truncate_{target_schema}.{table_name}'
                        execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: truncate_table(target_conn, target_schema, table_name, ARGS.dry_run))
                        
                elif ARGS.create_table:
                    if ARGS.page_start != 1:
                        print("WARNING: Setting a start page and recreating the table does not make sense - ignore the table creation!")
                    else:
                        status_id = f'drop-table_{target_schema}.{table_name}'
                        execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: drop_table_if_exists(target_conn, target_schema, table_name, ARGS.dry_run))
                        
                        status_id = f'create-table_{target_schema}.{table_name}'
                        execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: create_table(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run))
                        

                # drop indices (no need if tables were dropped and recreated just before):
                if ARGS.drop_indices and not ARGS.create_table:
                    if ARGS.page_start != 1:
                        print("WARNING: Setting a start page results in ignoring index dropping!")
                    else:
                        status_id = f'drop_indices_{target_schema}.{table_name}'
                        execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: drop_all_indices(target_conn, target_schema, table_name, ARGS.dry_run))
                        

                # If a where clause is set and the rows should also be deleted first:
                if ARGS.where_clause and ARGS.delete_where:
                    id_where_clause = "." + ARGS.where_clause.replace("\r", " ").replace("\n", " ") if ARGS.where_clause else ''
                    status_id = f'delete_data_{target_schema}.{table_name}{id_where_clause}'
                    execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: delete_data(target_conn, target_schema, table_name, ARGS.where_clause, ARGS.joins, ARGS.dry_run))
                    

                # Copy data from source to target
                if ARGS.copy_data:
                    # clustered indices cannot be disabled (then insertion is not possible anymore!)
                    # alter_all_indices(target_conn, target_schema, table_name, 'DISABLE', ARGS.dry_run)
                    id_where_clause = "." + ARGS.where_clause.replace("\r", " ").replace("\n", " ") if ARGS.where_clause else ''
                    id_joins = "." + ".".join(ARGS.joins) if ARGS.joins else ''
                    status_id = f'copy_{source_schema}.{table_name}{id_where_clause}{id_joins}'
                    execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: copy_data(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.page_start - 1, ARGS.dry_run, ARGS.page_size, ARGS.where_clause, ARGS.joins))
                    # alter_all_indices(target_conn, target_schema, table_name, 'REBUILD', ARGS.dry_run)

                # create indices
                if ARGS.copy_indices:
                    if ARGS.page_start != 1 and not ARGS.drop_indices:
                        print("WARNING: Setting a start page results in ignoring index creation!")
                    else:
                        status_id = f'copy-indices_{source_schema}.{table_name}'
                        execute_with_progress_track(ARGS.progress_file_name, status_id, lambda: copy_indices(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run))
                

        # copy views
        if ARGS.copy_view or ARGS.compare_view:
            view_definitions = fetch_view_definitions(source_conn, source_schema, target_schema)
            view_names = []
            if ARGS.views:
                view_names = ARGS.views
                #print(f"view names: {view_names}")
            else:
                for view_name, view_definition in view_definitions:
                    view_names.append(view_name)
                view_names = filter_strings_by_regex(view_names, ARGS.view_filter)
                #print(f"view names: {view_names}")
            
            view_definitions = [(name, definition) for name, definition in view_definitions if name in view_names]
            # print(f"view definitions to process: {view_definitions}")

            if ARGS.compare_view:
                compare_views(source_conn, source_schema, view_definitions, target_conn, target_schema)    
            else:
                if ARGS.copy_view:
                    # ignore progress/status here, as operation is fast!
                    create_views(target_conn, target_schema, view_definitions, ARGS.dry_run)    

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(-1)
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()
