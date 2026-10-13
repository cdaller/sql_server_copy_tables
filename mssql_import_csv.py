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
from datetime import date, datetime, time, timedelta
from time import perf_counter

from mssql_reorder_columns import format_column_def, format_key_constraint_sql, format_check_constraint_sql, format_index_sql, format_foreign_key_sql
from mssql_reorder_columns import get_column_info as get_column_definitions, get_key_constraints, get_indexes, get_check_constraints, get_foreign_keys


INT_TYPES = {'tinyint', 'smallint', 'int', 'bigint'}
DECIMAL_TYPES = {'decimal', 'numeric', 'money', 'smallmoney'}
FLOAT_TYPES = {'float', 'real'}
DATETIME_TYPES = {'datetime', 'datetime2', 'smalldatetime'}
CHAR_TYPES = {'char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext'}
# types that cannot be compared (e.g. with EXCEPT) to detect changed rows on upsert
NOT_COMPARABLE_TYPES = {'text', 'ntext', 'image', 'xml', 'geography', 'geometry'}

STAGING_TABLE = '#upsert_staging'

# column keys in the order of the rows returned by mssql_reorder_columns.get_column_info
COLUMN_KEYS = ['name', 'type_name', 'max_length', 'precision', 'scale', 'is_nullable', 'column_id',
               'default_definition', 'is_identity', 'identity_seed', 'identity_incr']


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
    parser.add_argument('--upsert', dest='upsert', default=False, action=argparse.BooleanOptionalAction, help='update rows that already exist (matched by the primary key of the table definition given by --create-table) and insert the others; rows without changes are not updated (default: %(default)s)')
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


def get_table_definition(connection, schema, table):
    columns = []
    for row in get_column_definitions(connection, schema, table):
        column = dict(zip(COLUMN_KEYS, row))
        column['is_nullable'] = bool(column['is_nullable'])
        column['is_identity'] = bool(column['is_identity'])
        # IDENT_SEED/IDENT_INCR return decimals, which are not JSON serializable
        for key in ('identity_seed', 'identity_incr'):
            if column[key] is not None:
                column[key] = int(column[key])
        columns.append(column)

    return {
        'schema': schema,
        'table': table,
        'columns': columns,
        'key_constraints': get_key_constraints(connection, schema, table),
        'indexes': get_indexes(connection, schema, table),
        'check_constraints': get_check_constraints(connection, schema, table),
        'foreign_keys': get_foreign_keys(connection, schema, table),
    }


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
        format_column_def(tuple(c[k] for k in COLUMN_KEYS))
        for c in definition['columns'])
    statements = [f"CREATE TABLE {full_table} (\n{col_defs}\n);"]
    statements += [format_key_constraint_sql(full_table, kc) for kc in definition['key_constraints']]
    statements += [format_check_constraint_sql(full_table, ck) for ck in definition['check_constraints']]
    with connection.cursor() as cursor:
        for sql in statements:
            cursor.execute(sql)
    connection.commit()
    print(" - done")


def get_ref_schema(fk, definition, schema):
    """References into the exported table's schema are redirected to the target schema (like mssql_copy_table.py)."""
    return schema if fk['ref_schema'] == definition['schema'] else fk['ref_schema']


def compare_named(kind, expected, actual, suffix_missing=''):
    """Compare two lists of named items (constraints, indexes, ...) by name and return the differences."""
    differences = []
    expected_by_name = {item['name'].lower(): item for item in expected}
    actual_by_name = {item['name'].lower(): item for item in actual}
    for key, item in expected_by_name.items():
        other = actual_by_name.get(key)
        if other is None:
            differences.append(f"{kind} {item['name']}: missing in table{suffix_missing}")
            continue
        attrs = [f"{k} {item[k]!r} (definition) != {other.get(k)!r} (table)" for k in item if k != 'name' and item[k] != other.get(k)]
        if attrs:
            differences.append(f"{kind} {item['name']}: " + ', '.join(attrs))
    for key, item in actual_by_name.items():
        if key not in expected_by_name:
            differences.append(f"{kind} {item['name']}: not in table definition")
    return differences


def compare_table_definition(connection, schema, table, definition):
    """Compare the existing table with the table definition and return the differences."""
    # JSON round trip converts tuples to lists, like in the table definition file
    existing = json.loads(json.dumps(get_table_definition(connection, schema, table)))
    foreign_keys = [dict(fk, ref_schema=get_ref_schema(fk, definition, schema)) for fk in definition['foreign_keys']]

    differences = []
    expected_columns = [dict(c) for c in definition['columns']]
    for c in expected_columns + existing['columns']:
        del c['column_id']
    differences += compare_named('column', expected_columns, existing['columns'])
    existing_names = {c['name'].lower() for c in existing['columns']}
    expected_names = {c['name'].lower() for c in expected_columns}
    if ([c['name'].lower() for c in expected_columns if c['name'].lower() in existing_names]
            != [c['name'].lower() for c in existing['columns'] if c['name'].lower() in expected_names]):
        differences.append("column order differs")
    differences += compare_named('primary key/unique constraint', definition['key_constraints'], existing['key_constraints'])
    differences += compare_named('check constraint', definition['check_constraints'], existing['check_constraints'])
    differences += compare_named('index', definition['indexes'], existing['indexes'], ' (will be created after the import)')
    differences += compare_named('foreign key', foreign_keys, existing['foreign_keys'], ' (will be created after the import)')
    return differences


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
            try:
                cursor.execute(format_foreign_key_sql(full_table, f"[{get_ref_schema(fk, definition, schema)}].[{fk['ref_table']}]", fk))
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


def format_duration(seconds):
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


class Progress:
    """Prints per page progress and an ETA (like mssql_copy_table.py). The ETA is the *mutable tail* of the output,
    it is erased and reprinted in place after each page without touching the output of earlier pages."""

    def __init__(self, total_rows):
        self.total_rows = total_rows
        self.rows_done = 0
        self.start_time = perf_counter()
        self.start_datetime = datetime.now()
        self.tail_len = 0

    def erase_tail(self):
        if self.tail_len:
            print('\b' * self.tail_len + ' ' * self.tail_len + '\b' * self.tail_len, end="", flush=True)
            self.tail_len = 0

    def page_done(self, rows, msg):
        self.rows_done += rows
        self.erase_tail()
        print(msg, end="", flush=True)
        elapsed = perf_counter() - self.start_time
        rows_remaining = self.total_rows - self.rows_done
        if rows_remaining > 0 and self.rows_done > 0:
            remaining_sec = elapsed / self.rows_done * rows_remaining
            # anchor to the fixed start time (not datetime.now()) so the ETA stays stable
            eta = self.start_datetime + timedelta(seconds=elapsed + remaining_sec)
            tail = f" ETA {eta:%H:%M:%S} ({format_duration(remaining_sec)})"
            print(tail, end="", flush=True)
            self.tail_len = len(tail)

    def done(self, details=None):
        self.erase_tail()
        duration_sec = perf_counter() - self.start_time
        rows_per_sec = int(round(self.rows_done / duration_sec)) if duration_sec > 0 else 0
        print(f" - done: {self.rows_done:_} rows{f' ({details})' if details else ''} in {duration_sec:.1f} seconds ({rows_per_sec:_} rows/sec)")


def count_csv_rows(file, encoding, delimiter) -> int:
    """Count the data rows of the CSV file (without header and empty lines). Uses the csv reader, so quoted values
    containing line breaks are counted correctly."""
    with open(file, newline='', encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader, None)
        return sum(1 for row in reader if row)


def get_upsert_key_columns(connection, schema, table, definition, csv_columns, definition_file):
    """Return the primary key columns of the table definition (with the table's column names). Exits if the table
    definition has no primary key, the table's primary key differs or a key column is not in the CSV file."""
    primary_key = next((kc for kc in definition['key_constraints'] if kc['is_primary_key']), None)
    if primary_key is None:
        print(f"ERROR: --upsert needs a primary key, but the table definition in {definition_file} has none.", file=sys.stderr)
        sys.exit(1)
    key_names = [name.lower() for name, _ in primary_key['columns']]

    table_primary_key = next((kc for kc in get_key_constraints(connection, schema, table) if kc['is_primary_key']), None)
    table_key_names = [name.lower() for name, _ in table_primary_key['columns']] if table_primary_key else []
    if sorted(key_names) != sorted(table_key_names):
        print(f"ERROR: primary key of table {schema}.{table} ({', '.join(table_key_names) or 'none'}) differs from the "
              f"primary key in the table definition ({', '.join(key_names)}).", file=sys.stderr)
        sys.exit(1)

    csv_columns_by_name = {c.column_name.lower(): c for c in csv_columns}
    missing = [name for name in key_names if name not in csv_columns_by_name]
    if missing:
        print(f"ERROR: --upsert needs all primary key columns in the CSV file, missing: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return [csv_columns_by_name[name].column_name for name in key_names]


def get_upsert_statements(full_table, csv_columns, key_columns):
    """Return the SQL statements to create the staging table, load a page into it, update changed rows and insert new
    rows from the staging table into the target table."""
    column_list = ', '.join(f'[{c.column_name}]' for c in csv_columns)
    placeholders = ', '.join('?' for _ in csv_columns)
    key_join = ' AND '.join(f't.[{k}] = s.[{k}]' for k in key_columns)
    # identity columns cannot be updated (an identity column in the primary key is used for matching only)
    update_columns = [c for c in csv_columns if c.column_name not in key_columns and not c.is_identity]
    skipped_identity = [c.column_name for c in csv_columns if c.column_name not in key_columns and c.is_identity]
    if skipped_identity:
        print(f"WARNING: identity column(s) {', '.join(skipped_identity)} cannot be updated, only used for inserted rows", file=sys.stderr)

    update_sql = None
    if update_columns:
        set_list = ', '.join(f't.[{c.column_name}] = s.[{c.column_name}]' for c in update_columns)
        update_sql = f"UPDATE t SET {set_list} FROM {full_table} t JOIN {STAGING_TABLE} s ON {key_join}"
        if not any(c.type_name in NOT_COMPARABLE_TYPES for c in update_columns):
            # only update rows that actually changed (EXCEPT compares NULL values as equal)
            source_list = ', '.join(f's.[{c.column_name}]' for c in update_columns)
            target_list = ', '.join(f't.[{c.column_name}]' for c in update_columns)
            update_sql += f" WHERE EXISTS (SELECT {source_list} EXCEPT SELECT {target_list})"

    return {
        # SELECT INTO keeps types and collations of the target columns, the UNION drops the identity property
        'create': [f"SELECT TOP 0 {column_list} INTO {STAGING_TABLE} FROM {full_table} UNION ALL SELECT TOP 0 {column_list} FROM {full_table}",
                   f"ALTER TABLE {STAGING_TABLE} ADD PRIMARY KEY ({', '.join(f'[{k}]' for k in key_columns)})"],
        'load': f"INSERT INTO {STAGING_TABLE} ({column_list}) VALUES ({placeholders})",
        'update': update_sql,
        'insert': (f"INSERT INTO {full_table} WITH (TABLOCK) ({column_list}) SELECT {column_list} FROM {STAGING_TABLE} s "
                   f"WHERE NOT EXISTS (SELECT 1 FROM {full_table} t WHERE {key_join})"),
        'truncate': f"TRUNCATE TABLE {STAGING_TABLE}",
        'drop': f"DROP TABLE {STAGING_TABLE}",
    }


def import_csv(connection, schema, table, csv_columns, reader, page_size, total_rows, skip_rows=0, key_columns=None):
    """Import the CSV rows page by page. With key_columns, existing rows are updated and new rows inserted (upsert)."""
    full_table = f'[{schema}].[{table}]'
    column_list = ', '.join(f'[{c.column_name}]' for c in csv_columns)
    placeholders = ', '.join('?' for _ in csv_columns)
    insert_sql = f"INSERT INTO {full_table} WITH (TABLOCK) ({column_list}) VALUES ({placeholders})"
    upsert_statements = get_upsert_statements(full_table, csv_columns, key_columns) if key_columns else None
    counts = {'inserted': 0, 'updated': 0}

    converters = [get_converter(c.type_name) for c in csv_columns]
    is_char = [c.type_name in CHAR_TYPES for c in csv_columns]
    has_identity = any(c.is_identity for c in csv_columns)

    progress = Progress(total_rows - skip_rows)
    page_count = 0

    with connection.cursor() as cursor, connection.cursor() as sql_cursor:
        if has_identity:
            cursor.execute(f"SET IDENTITY_INSERT {full_table} ON")
        if upsert_statements:
            for sql in upsert_statements['create']:
                sql_cursor.execute(sql)

        def write_page(page, page_count):
            if upsert_statements:
                upsert_page(connection, cursor, sql_cursor, upsert_statements, page, page_count, progress, counts)
            else:
                insert_page(connection, cursor, insert_sql, page, page_count, progress)

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
                write_page(page, page_count)
                page = []
        if page:
            page_count += 1
            write_page(page, page_count)

        if has_identity:
            cursor.execute(f"SET IDENTITY_INSERT {full_table} OFF")
        if upsert_statements:
            sql_cursor.execute(upsert_statements['drop'])
            connection.commit()

    if upsert_statements:
        unchanged = progress.rows_done - counts['inserted'] - counts['updated']
        progress.done(f"{counts['inserted']:_} inserted, {counts['updated']:_} updated, {unchanged:_} unchanged")
    else:
        progress.done()


def insert_page(connection, cursor, insert_sql, page, page_count, progress):
    start_time_page = perf_counter()
    cursor.executemany(insert_sql, page)
    connection.commit()
    progress.page_done(len(page), f" {page_count}w({perf_counter() - start_time_page:.1f}s)")


def upsert_page(connection, cursor, sql_cursor, statements, page, page_count, progress, counts):
    """Load the page into the staging table, update changed rows and insert new rows in one transaction."""
    start_time_page = perf_counter()
    try:
        cursor.executemany(statements['load'], page)
    except pyodbc.IntegrityError as e:
        raise ValueError(f"page {page_count}: duplicate primary key in CSV file: {e}") from None
    if statements['update']:
        sql_cursor.execute(statements['update'])
        counts['updated'] += sql_cursor.rowcount
    sql_cursor.execute(statements['insert'])
    counts['inserted'] += sql_cursor.rowcount
    sql_cursor.execute(statements['truncate'])
    connection.commit()
    progress.page_done(len(page), f" {page_count}u({perf_counter() - start_time_page:.1f}s)")


def main():
    parser = parse_args()
    ARGS = parser.parse_args()
    if ARGS.upsert and not ARGS.create_table:
        parser.error("--upsert requires --create-table (the primary key is taken from the table definition)")
    if ARGS.upsert and (ARGS.truncate or ARGS.auto_resume):
        parser.error("--upsert cannot be combined with --truncate or --auto-resume")

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
                differences = compare_table_definition(connection, schema, table, definition)
                if differences:
                    print(f"WARNING: table {schema}.{table} differs from the table definition in {ARGS.create_table}:", file=sys.stderr)
                    for difference in differences:
                        print(f"  {difference}", file=sys.stderr)
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
            key_columns = get_upsert_key_columns(connection, schema, table, definition, csv_columns, ARGS.create_table) if ARGS.upsert else None

            if ARGS.truncate:
                truncate_table(connection, schema, table)
            if ARGS.delete_where:
                delete_rows(connection, schema, table, ARGS.delete_where)

            skip_rows = 0
            if ARGS.auto_resume:
                skip_rows = get_row_count(connection, schema, table)
                print(f"Auto-resume: table already contains {skip_rows:_} rows, skipping the first {skip_rows:_} CSV rows")

            print(f"{'Upserting' if key_columns else 'Importing'} {ARGS.file} into {schema}.{table} ({len(csv_columns)} columns, "
                  f"{f'primary key {", ".join(key_columns)}, ' if key_columns else ''}page size {ARGS.page_size:_})...", end="", flush=True)
            total_rows = count_csv_rows(ARGS.file, ARGS.encoding, ARGS.delimiter)
            print(f" {total_rows:_} rows{f' ({total_rows - skip_rows:_} to import)' if skip_rows else ''} ...", end="", flush=True)
            import_csv(connection, schema, table, csv_columns, reader, ARGS.page_size, total_rows, skip_rows, key_columns)

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
