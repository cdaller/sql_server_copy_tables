#!/usr/bin/env -S uv run --script

try:
    from azure.identity import AzureCliCredential
    azure_identity_available = True
except ImportError:
    azure_identity_available = False

import pyodbc
import sys
import struct
import argparse
import csv
import json
import re
from decimal import Decimal
from datetime import date, datetime, time
from time import perf_counter

from mssql_reorder_columns import format_column_def, format_key_constraint_sql, format_check_constraint_sql, format_index_sql, format_foreign_key_sql


INT_TYPES = {'tinyint', 'smallint', 'int', 'bigint'}
DECIMAL_TYPES = {'decimal', 'numeric', 'money', 'smallmoney'}
FLOAT_TYPES = {'float', 'real'}
DATETIME_TYPES = {'datetime', 'datetime2', 'smalldatetime'}
CHAR_TYPES = {'char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext'}


def parse_args():
    parser = argparse.ArgumentParser(description='Bulk import a CSV file into an existing SQL Server table. CSV header names are matched to table column names (case-insensitive).')
    parser.add_argument('--driver', dest='driver', default='{ODBC Driver 18 for SQL Server}', help='ODBC driver (default: %(default)s)')
    parser.add_argument('--server', dest='server', required=True, help='database server name')
    parser.add_argument('--db', dest='db', required=True, help='database name')
    parser.add_argument('--authentication', dest='authentication', default='UsernamePassword', help='authentication method (default: %(default)s)')
    parser.add_argument('--user', dest='user', help='username (for UsernamePassword auth)')
    parser.add_argument('--password', dest='password', help='password (for UsernamePassword auth)')
    parser.add_argument('--table', dest='table', required=True, help='table name, optionally schema-qualified (e.g. dbo.MyTable)')
    parser.add_argument('--file', dest='file', required=True, help='CSV file to import (first line must contain the column names)')
    parser.add_argument('--delimiter', dest='delimiter', default=',', help='CSV field delimiter (default: %(default)s)')
    parser.add_argument('--encoding', dest='encoding', default='utf-8-sig', help='CSV file encoding (default: %(default)s)')
    parser.add_argument('--create-table', dest='create_table', metavar='JSON_FILE', help='create the table from the table definition JSON file written by mssql_export_csv.py if it does not exist yet; indexes and foreign keys are created after the import')
    parser.add_argument('--page-size', dest='page_size', type=int, default=50000, help='number of rows inserted and committed per page (default: %(default)s)')
    before_import = parser.add_mutually_exclusive_group()
    before_import.add_argument('--truncate', dest='truncate', default=False, action=argparse.BooleanOptionalAction, help='truncate the table before the import (default: %(default)s)')
    before_import.add_argument('--delete-where', dest='delete_where', help='delete all rows matching this where clause before the import (e.g. "year = 2026")')
    before_import.add_argument('--auto-resume', dest='auto_resume', default=False, action=argparse.BooleanOptionalAction, help='continue an interrupted import: the current row count of the table is taken as the number of CSV rows already imported, these rows are skipped (default: %(default)s)')
    return parser


def create_connection(config) -> pyodbc.Connection:
    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};Encrypt=Yes;TrustServerCertificate=Yes;hostNameInCertificate=*.database.windows.net;loginTimeout=30;Packet Size=32767'
    attrs_before = None

    if config.get("authentication") == 'AzureActiveDirectory':
        if not azure_identity_available:
            print("For AzureActiveDirectory authentication, please install azure-identity first!")
            sys.exit(-1)
        credential = AzureCliCredential()
        databaseToken = credential.get_token('https://database.windows.net/')
        tokenb = bytes(databaseToken[0], "UTF-16-LE")
        tokenstruct = struct.pack("=i", len(tokenb)) + tokenb
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: tokenstruct}
        print(f'using authentication {config["authentication"]}...', end="")
    else:
        conn_str = conn_str + f';UID={config["user"]};PWD={config["password"]}'
        print(f'using authentication username/password', end="")

    return pyodbc.connect(conn_str, attrs_before=attrs_before)


def get_column_info(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            c.name          AS column_name,
            t.name          AS type_name,
            c.precision,
            c.scale,
            c.is_identity
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
    """, f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()
    return rows


def map_columns(csv_header, columns):
    """Map each CSV header name to a table column (case-insensitive). Exits if a CSV column is not in the table."""
    columns_by_name = {c.column_name.lower(): c for c in columns}
    mapped = []
    unknown = []
    for name in csv_header:
        column = columns_by_name.get(name.strip().lower())
        if column is None:
            unknown.append(name)
        else:
            mapped.append(column)
    if unknown:
        print(f"ERROR: CSV columns not found in table: {', '.join(unknown)}", file=sys.stderr)
        sys.exit(1)
    return mapped


def parse_bit(value):
    v = value.strip().lower()
    if v in ('1', 'true', 't', 'yes', 'y'):
        return True
    if v in ('0', 'false', 'f', 'no', 'n'):
        return False
    raise ValueError(f"invalid bit value '{value}'")


def get_converter(type_name):
    """Return a function converting a CSV string to a Python value suitable for the column type."""
    if type_name in INT_TYPES:
        return int
    if type_name in DECIMAL_TYPES:
        return Decimal
    if type_name in FLOAT_TYPES:
        return float
    if type_name == 'bit':
        return parse_bit
    if type_name == 'date':
        return date.fromisoformat
    if type_name in DATETIME_TYPES:
        return datetime.fromisoformat
    if type_name == 'time':
        return time.fromisoformat
    if type_name in ('binary', 'varbinary', 'image'):
        return lambda v: bytes.fromhex(v[2:] if v[:2].lower() == '0x' else v)
    # strings and everything else are passed as-is and converted by the server
    return None


# fetch input sizes for decimal columns (see https://github.com/mkleehammer/pyodbc/issues/845)
def get_input_sizes(columns):
    return [(pyodbc.SQL_DECIMAL, c.precision, c.scale) if c.type_name in ('decimal', 'numeric') else None for c in columns]


def convert_row(row, converters, is_char, line_number):
    values = []
    for value, convert, char_column in zip(row, converters, is_char):
        if value is None:
            # unquoted empty field
            values.append(None)
        elif value == '':
            # quoted empty field: empty string for character columns, NULL otherwise
            values.append('' if char_column else None)
        elif convert is None:
            values.append(value)
        else:
            try:
                values.append(convert(value))
            except ValueError as e:
                raise ValueError(f"line {line_number}: cannot convert '{value}': {e}") from None
    return tuple(values)


def table_exists(connection, schema, table) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT OBJECT_ID(?, 'U')", f'[{schema}].[{table}]')
        return cursor.fetchone()[0] is not None


def rename_constraints(definition, table):
    """Replace the exported table name in constraint and index names with the target table name, as constraint names
    must be unique within a schema (e.g. PK_MyTable -> PK_MyTable2)."""
    if definition['table'].lower() != table.lower():
        pattern = re.compile(re.escape(definition['table']), re.IGNORECASE)
        for key in ('key_constraints', 'indexes', 'check_constraints', 'foreign_keys'):
            for item in definition[key]:
                item['name'] = pattern.sub(table, item['name'])
    return definition


def create_table(connection, schema, table, definition):
    """Create the table with its columns, primary key/unique and check constraints (indexes and foreign keys are created after the import)."""
    full_table = f'[{schema}].[{table}]'
    print(f"Creating table {schema}.{table} from table definition...", end="", flush=True)
    col_defs = ',\n'.join(
        format_column_def(tuple(c[k] for k in ('name', 'type_name', 'max_length', 'precision', 'scale', 'is_nullable', 'column_id',
                                               'default_definition', 'is_identity', 'identity_seed', 'identity_incr')))
        for c in definition['columns'])
    statements = [f"CREATE TABLE {full_table} (\n{col_defs}\n);"]
    statements += [format_key_constraint_sql(full_table, kc) for kc in definition['key_constraints']]
    statements += [format_check_constraint_sql(full_table, ck) for ck in definition['check_constraints']]
    with connection.cursor() as cursor:
        for sql in statements:
            cursor.execute(sql)
    connection.commit()
    print(" - done")


def create_indexes_and_foreign_keys(connection, schema, table, definition):
    """Create the indexes and foreign keys of the table definition that do not exist yet."""
    full_table = f'[{schema}].[{table}]'
    with connection.cursor() as cursor:
        for index in definition['indexes']:
            cursor.execute("SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND name = ?", full_table, index['name'])
            if cursor.fetchone():
                continue
            print(f"Creating index {index['name']}...", end="", flush=True)
            start_time = perf_counter()
            cursor.execute(format_index_sql(full_table, index))
            connection.commit()
            print(f" - done in {perf_counter() - start_time:.1f} seconds")

        for fk in definition['foreign_keys']:
            cursor.execute("SELECT 1 FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID(?) AND name = ?", full_table, fk['name'])
            if cursor.fetchone():
                continue
            print(f"Creating foreign key {fk['name']}...", end="", flush=True)
            # references into the exported table's schema are redirected to the target schema (like mssql_copy_table.py)
            ref_schema = schema if fk['ref_schema'] == definition['schema'] else fk['ref_schema']
            try:
                cursor.execute(format_foreign_key_sql(full_table, f"[{ref_schema}].[{fk['ref_table']}]", fk))
                connection.commit()
                print(" - done")
            except pyodbc.Error as e:
                # e.g. the referenced table does not exist (yet) in the target database
                connection.rollback()
                print(f" - FAILED: {e}", file=sys.stderr)


def truncate_table(connection, schema, table):
    print(f"Truncating table {schema}.{table}...", end="", flush=True)
    with connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE [{schema}].[{table}]")
    connection.commit()
    print(" - done")


def delete_rows(connection, schema, table, where_clause):
    print(f"Deleting rows in table {schema}.{table} using where clause \"{where_clause}\"...", end="", flush=True)
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM [{schema}].[{table}] WHERE {where_clause}")
        deleted = cursor.rowcount
    connection.commit()
    print(f" - done ({deleted:_} rows deleted)")


def get_row_count(connection, schema, table) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM [{schema}].[{table}]")
        return cursor.fetchone()[0]


def import_csv(connection, schema, table, csv_columns, reader, page_size, skip_rows=0):
    full_table = f'[{schema}].[{table}]'
    column_list = ', '.join(f'[{c.column_name}]' for c in csv_columns)
    placeholders = ', '.join('?' for _ in csv_columns)
    insert_sql = f"INSERT INTO {full_table} WITH (TABLOCK) ({column_list}) VALUES ({placeholders})"

    converters = [get_converter(c.type_name) for c in csv_columns]
    is_char = [c.type_name in CHAR_TYPES for c in csv_columns]
    has_identity = any(c.is_identity for c in csv_columns)

    start_time = perf_counter()
    total_rows = 0
    page_count = 0

    with connection.cursor() as cursor:
        if has_identity:
            cursor.execute(f"SET IDENTITY_INSERT {full_table} ON")

        cursor.fast_executemany = True
        cursor.setinputsizes(get_input_sizes(csv_columns))

        page = []
        skipped = 0
        # line 1 is the header
        for line_number, row in enumerate(reader, start=2):
            if not row:
                continue
            if skipped < skip_rows:
                skipped += 1
                continue
            if len(row) != len(csv_columns):
                raise ValueError(f"line {line_number}: expected {len(csv_columns)} fields, got {len(row)}")
            page.append(convert_row(row, converters, is_char, line_number))
            if len(page) >= page_size:
                page_count += 1
                total_rows += insert_page(connection, cursor, insert_sql, page, page_count, total_rows, start_time)
                page = []
        if page:
            page_count += 1
            total_rows += insert_page(connection, cursor, insert_sql, page, page_count, total_rows, start_time)

        if has_identity:
            cursor.execute(f"SET IDENTITY_INSERT {full_table} OFF")

    duration_sec = perf_counter() - start_time
    rows_per_sec = int(round(total_rows / duration_sec)) if duration_sec > 0 else 0
    print(f" - done: {total_rows:_} rows in {duration_sec:.1f} seconds ({rows_per_sec} rows/sec)")


def insert_page(connection, cursor, insert_sql, page, page_count, rows_before, start_time):
    start_time_page = perf_counter()
    cursor.executemany(insert_sql, page)
    connection.commit()
    print(f" {page_count}w({perf_counter() - start_time_page:.1f}s, {rows_before + len(page):_} rows)", end="", flush=True)
    return len(page)


def main():
    parser = parse_args()
    ARGS = parser.parse_args()

    if '.' in ARGS.table:
        schema, table = ARGS.table.split('.', 1)
    else:
        schema = 'dbo'
        table = ARGS.table

    config = {
        'driver': ARGS.driver,
        'server': ARGS.server,
        'database': ARGS.db,
        'authentication': ARGS.authentication,
        'user': ARGS.user,
        'password': ARGS.password,
    }

    connection = None
    try:
        print(f'connecting to server {config["server"]} db {config["database"]}... ', end='', flush=True)
        connection = create_connection(config)
        print(' - DONE', flush=True)

        definition = None
        if ARGS.create_table:
            with open(ARGS.create_table, encoding='utf-8') as f:
                definition = rename_constraints(json.load(f), table)
            if table_exists(connection, schema, table):
                print(f"Table {schema}.{table} already exists, not creating it")
            else:
                create_table(connection, schema, table, definition)

        columns = get_column_info(connection, schema, table)
        if not columns:
            print(f"ERROR: Table '{schema}.{table}' not found or has no columns.", file=sys.stderr)
            sys.exit(1)

        with open(ARGS.file, newline='', encoding=ARGS.encoding) as f:
            reader = csv.reader(f, delimiter=ARGS.delimiter, quoting=csv.QUOTE_NOTNULL)
            header = next(reader, None)
            if not header:
                print(f"ERROR: CSV file '{ARGS.file}' is empty.", file=sys.stderr)
                sys.exit(1)
            csv_columns = map_columns(header, columns)

            if ARGS.truncate:
                truncate_table(connection, schema, table)
            if ARGS.delete_where:
                delete_rows(connection, schema, table, ARGS.delete_where)

            skip_rows = 0
            if ARGS.auto_resume:
                skip_rows = get_row_count(connection, schema, table)
                print(f"Auto-resume: table already contains {skip_rows:_} rows, skipping the first {skip_rows:_} CSV rows")

            print(f"Importing {ARGS.file} into {schema}.{table} ({len(csv_columns)} columns, page size {ARGS.page_size:_})...", end="", flush=True)
            import_csv(connection, schema, table, csv_columns, reader, ARGS.page_size, skip_rows)

        if definition:
            create_indexes_and_foreign_keys(connection, schema, table, definition)

    except Exception as e:
        print(f"\nAn error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    main()
