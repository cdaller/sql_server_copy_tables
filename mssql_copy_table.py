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
import logging

def parse_args():
    parser = argparse.ArgumentParser(description='Copy one or more tables from an sql server to another sql server')
    parser.add_argument('--source-driver', dest='source_driver', default='{ODBC Driver 18 for SQL Server}', help='source database server driver (default: %(default)s)')
    parser.add_argument('--source-server', dest='source_server', help='source database server name', required=True)
    parser.add_argument('--source-db', dest='source_db', help='source database name', required=True)
    parser.add_argument('--source-schema', dest='source_schema', default='dbo', help='source database schema name (default: %(default)s)')
    parser.add_argument('--source-authentication', dest='source_authentication', default='UsernamePassword', help='source database authentication. Possible to use AzureActiveDirectory (default: %(default)s)')
    parser.add_argument('--source-user', dest='source_user', help='source database username, if authentication is set to UsenamePassword')
    parser.add_argument('--source-password', dest='source_password', help='source database password, if authentication is set to UsenamePassword')
    parser.add_argument('--source-list-tables', dest='source_list_tables', default=False, action='store_true', help='If set, a list of tables is printed, no data is copied! (default: %(default)s)')

    parser.add_argument('--target-driver', dest='target_driver', default='{ODBC Driver 18 for SQL Server}', help='target database server driver (default: %(default)s)')
    parser.add_argument('--target-server', dest='target_server', help='target database server name', required=True)
    parser.add_argument('--target-db', dest='target_db', help='target database name', required=True)
    parser.add_argument('--target-schema', dest='target_schema', default='dbo', help='target database schema name (default: %(default)s)')
    parser.add_argument('--target-authentication', dest='target_authentication', default='UsernamePassword', help='target database authentication. Possible to use AzureActiveDirectory (default: %(default)s)')
    parser.add_argument('--target-user', dest='target_user', help='source database username, if authentication is set to UsenamePassword')
    parser.add_argument('--target-password', dest='target_password', help='source database password, if authentication is set to UsenamePassword')
    parser.add_argument('--target-list-tables', dest='target_list_tables', default=False, action='store_true', help='If set, a list of tables is printed, no data is copied!')

    parser.add_argument('--truncate-table', dest='truncate_table', default=False, action=argparse.BooleanOptionalAction, help='If set, truncate the target table before inserting rows from source table. If this option is set, the tables are NOT recreated, even if --create-table is used! (default: %(default)s)')
    parser.add_argument('--create-table', dest='create_table', default=True, action=argparse.BooleanOptionalAction, help='If set, drop (if exists) and (re)create the target table before inserting rows from source table. All columns, types and not-null and primary key constraints will also be copied. Indices of the table will also be recreated if not prevented by --no-copy-indices flag (default: %(default)s)')
    parser.add_argument('--copy-indices', dest='copy_indices', default=True, action=argparse.BooleanOptionalAction, help='Create the indices for the target tables as they exist on the source table (default: %(default)s)')
    parser.add_argument('--drop-indices', dest='drop_indices', default=True, action=argparse.BooleanOptionalAction, help='Drop indices before copying data for performance reasons. The indices are created after copying by --copy-indices afterwards (default: %(default)s)')
    parser.add_argument('--copy-data', dest='copy_data', default=True, action=argparse.BooleanOptionalAction, help='Copy the data of the tables. Default True! Use --no-copy-data if you want to creat the indices only. (default: %(default)s)')
    parser.add_argument('--dry-run', dest='dry_run', default=False, action='store_true', help='Do not modify target database, just print what would happen. (default: %(default)s)')

    parser.add_argument('--compare-table', dest='compare_table', default=False, action=argparse.BooleanOptionalAction, help='If set, do not copy any data, but compare the source and the target table(s) and print if there are any differences in columns, indices or content rows. (default: %(default)s)')

    parser.add_argument('-t', '--table', nargs='*', dest='tables', help='Specify the tables you want to copy. Either repeat "-t <name> -t <name2>" or by "-t <name> <name2>"')
    parser.add_argument('--all-tables', dest='copy_all_tables', default=False, action='store_true', help='Copy all tables in the schema from the source db to the target db. (default: %(default)s)')
    parser.add_argument('--table-filter', dest='table_filter', default = None, help='Filter table names using this regular expression (regexp must match table names). Use with "--all-tables" or one of the "list-tables" arguments. (default: %(default)s)')
    parser.add_argument('--page-size', dest='page_size', default = 50000, type=int, help='Page size of rows that are copied in one step. Depending on the size of table, values between 50000 (default) and 500000 are working well (depending on the number of rows, etc.). (default: %(default)d)')
    parser.add_argument('--page-start', dest='page_start', default = 1, type=int, help='Page to start with. Please note that the first page number ist 1 to match the output during copying of the data. The output of a page number indicates the page is read. The "w" after the page number shows that the pages was successfully written. Please also note that this settings does not make much sense if you copy more than one table! (default: %(default)d)')

    parser.add_argument('--debug-sql', dest='debug_sql', default = False, action='store_true', help='If enabled, prints sql statements. (default: %(default)d)')

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
            COLUMN_NAME, DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, 
            COLUMN_DEFAULT, DATETIME_PRECISION,
            NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = N'{source_schema}' AND TABLE_NAME = N'{table_name}'
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

def get_numerical_primary_key(source_conn, source_schema, table_name) -> str:
    """
    Determine if the given table has a numerical primary key and return its column name.

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
        if len(rows) != 1:
            # deny if none or more than one column (combined primary key)
            return None

        row = rows[0]
        # Check if primary key exists and is numerical
        if row and row.DATA_TYPE in ['int', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal']:
            return row.COLUMN_NAME

    return None

# Function to copy data from source to target
def copy_data(source_conn, target_conn, source_schema, table_name, target_schema, page_start, dry_run=False, page_size=50000):
    print(f"Copying table {table_name} ...", end="", flush=True)
    start_time = perf_counter()

    primary_key = get_numerical_primary_key(source_conn, source_schema, table_name)
    if primary_key:
        print(f" using primary key '{primary_key}' for optimization ...", end="", flush=True)

    with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:
        total_row_count = get_row_count(source_conn, source_schema, table_name)
        print(f" {total_row_count} rows ..." + get_dry_run_text(dry_run), end="", flush=True)

        input_sizes = get_input_sizes(source_conn, source_schema, table_name)
        target_cursor.fast_executemany = True
        # workaround for an odbc bug that cannot handle decimal values correctly when fast_executemany is True (https://github.com/mkleehammer/pyodbc/issues/845)
        target_cursor.setinputsizes(input_sizes)

        page_count = page_start
        offset = page_start * page_size
        print_page_info = True

        while True:
            start_time_page = perf_counter()

            page_count += 1

            if primary_key:
                # Use primary key for efficient paging
                # see https://erikdarling.com/considerations-for-paging-queries-in-sql-server-with-batch-mode-dont-use-offset-fetch/
                execute_sql(source_cursor,
                    f"WITH fetching AS ("
                    f"    select p.{primary_key}, n=ROW_NUMBER() OVER ( ORDER BY p.{primary_key})"
                    f"    from {source_schema}.{table_name} p"
                    f" )"
                    f" SELECT p.*"
                    f" FROM fetching f"
                    f" JOIN {source_schema}.{table_name} p ON p.{primary_key} = f.{primary_key}"
                    f" WHERE f.n > {offset} and f.n <= {offset + page_size}"
                    f" OPTION (RECOMPILE)"
                )
            else:
                # Use OFFSET for paging when no numerical primary key is available
                execute_sql(source_cursor, f"SELECT * FROM {source_schema}.{table_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY")

            rows = source_cursor.fetchall()
            if not rows:
                break

            duration_sec_page_read = perf_counter() - start_time_page

            row_count = len(rows)

            if row_count == page_size:
                if print_page_info:
                    print(f" paging {int(total_row_count / page_size + 1)} pages each {page_size} rows, page", end="")
                    print_page_info = False
                print(f" {page_count}r({duration_sec_page_read:.1f}s)", end="", flush=True)
            else:
                print(f" read {row_count} rows ({duration_sec_page_read:.1f}s) ", end="", flush=True)

            if not dry_run:
                placeholders = ', '.join(['?' for _ in rows[0]])
                insert_sql = f"INSERT INTO {target_schema}.{table_name} VALUES ({placeholders})"
                target_cursor.executemany(insert_sql, rows)
                target_conn.commit()
                duration_sec_page_write = perf_counter() - start_time_page - duration_sec_page_read

                print(f"w({duration_sec_page_write:.1f}s)", end="", flush=True)

            offset += page_size

        duration_sec = perf_counter() - start_time
        rows_per_sec = int(round(total_row_count / duration_sec))
        print(f" - done in {duration_sec:.1f} seconds ({rows_per_sec} rows/sec)")

def truncate_table(connection, schema_name, table_name, dry_run = False):
    print(f"Truncating table {table_name} {get_dry_run_text(dry_run)} ...", end="", flush=True)
    if not dry_run:
        with connection.cursor() as cursor:
            execute_sql(cursor, f"TRUNCATE TABLE {schema_name}.{table_name}")
        connection.commit()
    print(" - done")

def get_row_count(connection, schema_name, table_name) -> int:
    cursor = connection.cursor()
    execute_sql(cursor, f"SELECT COUNT(*) FROM  {schema_name}.{table_name}")
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


def get_table_names(conn, schema) -> []:
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

def filter_strings_by_regex(strings, pattern) -> []:
    if pattern is None:
        return strings
    regex = re.compile(pattern)
    filtered_strings = [s for s in strings if regex.match(s)]
    return filtered_strings

# Function to fetch index and corresponding columns
def get_indices(cursor, schema_name, table_name) -> {}:
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

def compare_tables(source_conn, source_schema, table_name, target_conn, target_schema):
    print(f"Comparing table {source_schema}.{table_name} in source to target ...", end="", flush=True)

    with source_conn.cursor() as source_cursor:
        with target_conn.cursor() as target_cursor:

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
                    print(f"\n-  Column {col} exists in source table but not in target table.")
                elif type != target_columns[col]:
                    print(f"\n-  Column {col} has different type: Source({type}) vs Target({target_columns[col]})")

            for col, type in target_columns.items():
                if col not in source_columns:
                    print(f"\n-  Column {col} exists in target table but not in source table.")

            # Compare indices
                    
            source_indices = get_indices(source_cursor, source_schema, table_name)
            target_indices = get_indices(target_cursor, target_schema, table_name)

            source_index_columns = {tuple(sorted(columns)) for columns in source_indices.values()}
            target_index_columns = {tuple(sorted(columns)) for columns in target_indices.values()}

            for columns in source_index_columns:
                if columns not in target_index_columns:
                    print(f"\n-  Index with columns {columns} exists in source table but not in target table.")

            for columns in target_index_columns:
                if columns not in source_index_columns:
                    print(f"\n-  Index with columns {columns} exists in target table but not in source table.")


            # Compare the number of rows
            source_cursor.execute(f"SELECT COUNT(*) FROM {source_schema}.{table_name}")
            source_row_count = source_cursor.fetchone()[0]

            target_cursor.execute(f"SELECT COUNT(*) FROM {target_schema}.{table_name}")
            target_row_count = target_cursor.fetchone()[0]

            if source_row_count != target_row_count:
                print(f"\n-  Row count differs: Source({source_row_count}) vs Target({target_row_count})")

            print(" - DONE")

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

        table_names = ARGS.tables
        if ARGS.copy_all_tables or (table_names is None and ARGS.compare_table):
            table_names = get_table_names(source_conn, source_schema)

        table_names = filter_strings_by_regex(table_names, ARGS.table_filter)

        for table_name in table_names:

            if ARGS.compare_table:
                compare_tables(source_conn, source_schema, table_name, target_conn, target_schema)
            else:

                if ARGS.truncate_table:
                    if ARGS.page_start != 1:
                        print("WARNING: Setting a start page and truncating the table does not make sense! - ignore the truncation!")
                    else:
                        truncate_table(target_conn, target_schema, table_name, ARGS.dry_run)
                elif ARGS.create_table:
                    if ARGS.page_start != 1:
                        print("WARNING: Setting a start page and recreating the table does not make sense - ignore the table creation!")
                    else:
                        drop_table_if_exists(target_conn, target_schema, table_name, ARGS.dry_run)
                        create_table(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run)

                # drop indices (no need if tables were dropped and recreated just before):
                if ARGS.drop_indices and not ARGS.create_table:
                    if ARGS.page_start != 1:
                        print("WARNING: Setting a start page results in ignoring index dropping!")
                    else:
                        drop_all_indices(target_conn, target_schema, table_name, ARGS.dry_run)

                # Copy data from source to target
                if ARGS.copy_data:
                    # clustered indices cannot be disabled (then insertion is not possible anymore!)
                    # alter_all_indices(target_conn, target_schema, table_name, 'DISABLE', ARGS.dry_run)
                    copy_data(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.page_start - 1, ARGS.dry_run, ARGS.page_size)
                    # alter_all_indices(target_conn, target_schema, table_name, 'REBUILD', ARGS.dry_run)

                # create indices
                if ARGS.copy_indices:
                    if ARGS.page_start != 1 and not ARGS.drop_indices:
                        print("WARNING: Setting a start page results in ignoring index creation!")
                    else:
                        copy_indices(source_conn, target_conn, source_schema, table_name, target_schema, ARGS.dry_run)


    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()
