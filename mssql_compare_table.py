#!/usr/bin/env -S uv run --script

# pip install azure-identity

try:
    from azure.identity import AzureCliCredential
    azure_identity_available = True
except ImportError:
    azure_identity_available = False

import pyodbc
import sys
import csv
import os
import re
import struct
import argparse

# fixed table of characters that are considered equivalent when --normalize-special-chars is used
SPECIAL_CHAR_REPLACEMENTS = {
    '„': '"',  # „ double low-9 quotation mark
    '“': '"',  # " left double quotation mark
    '”': '"',  # " right double quotation mark
    '‚': "'",  # ‚ single low-9 quotation mark
    '‘': "'",  # ' left single quotation mark
    '’': "'",  # ' right single quotation mark
    '–': '-',  # – en dash
    '—': '-',  # — em dash
}


def parse_args():
    parser = argparse.ArgumentParser(description='Compare two tables (same columns/types) - in different schemata, different databases, or different servers - and print differing rows as CSV.')

    parser.add_argument('--driver', '--driver1', dest='driver', default='{ODBC Driver 18 for SQL Server}', help='database server driver for the first table (default: %(default)s)')
    parser.add_argument('--server', '--server1', dest='server', required=True, help='database server name of the first table')
    parser.add_argument('--db', '--db1', dest='db', required=True, help='database name of the first table')
    parser.add_argument('--authentication','--authentication1', dest='authentication', default='UsernamePassword', help='authentication for the first table. Possible to use AzureActiveDirectory (default: %(default)s)')
    parser.add_argument('--user', '--user1', dest='user', help='username, if authentication is set to UsernamePassword')
    parser.add_argument('--password', '--password1', dest='password', help='password, if authentication is set to UsernamePassword')

    parser.add_argument('--driver2', dest='driver2', help='database server driver for the second table (default: same as --driver)')
    parser.add_argument('--server2', dest='server2', help='database server name of the second table (default: same as --server)')
    parser.add_argument('--db2', dest='db2', help='database name of the second table (default: same as --db)')
    parser.add_argument('--authentication2', dest='authentication2', help='authentication for the second table. Possible to use AzureActiveDirectory (default: UsernamePassword, or same as --authentication if no second connection is given)')
    parser.add_argument('--user2', dest='user2', help='username for the second table, if authentication2 is set to UsernamePassword (default: same as --user)')
    parser.add_argument('--password2', dest='password2', help='password for the second table, if authentication2 is set to UsernamePassword (default: same as --password)')

    parser.add_argument('--schema1', '--schema', dest='schema1', required=True, help='schema name of the first table')
    parser.add_argument('--table1', '--table', dest='table1', required=True, help='name of the first table')
    parser.add_argument('--schema2', dest='schema2', help='schema name of the second table (default: same as --schema1)')
    parser.add_argument('--table2', dest='table2', help='name of the second table (default: same as --table1)')

    parser.add_argument('--key-columns', dest='key_columns', nargs='+', help='column(s) that uniquely identify a row, used to match rows between the two tables. Default: the primary key columns of the table (auto-detected).')
    parser.add_argument('--compare-columns', dest='compare_columns', nargs='+', help='column(s) to compare for equality between matched rows. Default: all columns common to both tables.')
    parser.add_argument('--compare-columns-exclude', dest='compare_columns_exclude', nargs='+', help='column(s) to leave out of the comparison (applied after --compare-columns, e.g. to exclude a common column from the default "all columns" comparison).')

    parser.add_argument('--output', dest='output', default=None, help='write CSV output to this file instead of stdout')
    parser.add_argument('--diff-dir', dest='diff_dir', default=None, help='additionally write two CSV files (left.csv for table1, right.csv for table2) into this directory, one row per key value (sorted and aligned by key column(s), missing rows left blank). Open both in a diff view (e.g. VS Code "Compare Active File With..." or IntelliJ "Compare Files") to see differences highlighted per line/cell.')
    parser.add_argument('--max-diff-rows', dest='max_diff_rows', default=None, type=int, help='maximum number of rows to report per category (only-in-table1, only-in-table2, differing) to keep output manageable. Default: unlimited.')
    parser.add_argument('--ignore-whitespace-start-end', dest='ignore_whitespace_start_end', default=False, action='store_true', help='ignore leading/trailing whitespace differences in string values when comparing rows. Also applies to the values written to --diff-dir files. (default: %(default)s)')
    parser.add_argument('--normalize-special-chars', dest='normalize_special_chars', default=False, action='store_true', help=f'replace special characters using a fixed built-in table ({SPECIAL_CHAR_REPLACEMENTS}) before comparing values, to ignore differences caused by different encodings/sources of the same character (e.g. curly vs. straight quotes). Also applies to the values written to --diff-dir files. (default: %(default)s)')

    parser.add_argument('--where', dest='where_clause', default=None, help='If set, this where clause is added to the queries used to read rows from both tables for comparison. The original table name is "source_table" to use in the where clause. (default: %(default)s)')
    parser.add_argument('--join', nargs='+', action='extend', dest='joins', default=None, help='Add one or more joins to the selection of data read for comparison (probably only useful in combination with the --where clause). The original table name is "source_table" to use in the joins. Either use the parameter multiple times or separate the joins with spaces. (default: %(default)s)')

    return parser


def create_connection(config) -> pyodbc.Connection:
    conn_str = f'DRIVER={config["driver"]};SERVER={config["server"]};DATABASE={config["database"]};Encrypt=Yes;TrustServerCertificate=Yes;hostNameInCertificate=*.database.windows.net;loginTimeout=30'
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


def get_columns(connection, schema, table):
    """Return column names in ordinal order."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    columns = [row.COLUMN_NAME for row in cursor.fetchall()]
    cursor.close()
    return columns


def get_primary_key_columns(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
        ORDER BY kcu.ORDINAL_POSITION
    """, (schema, table))
    columns = [row.COLUMN_NAME for row in cursor.fetchall()]
    cursor.close()
    return columns


def determine_key_columns(connection1, schema1, table1, connection2, schema2, table2, requested):
    if requested:
        return list(requested)

    key_columns = get_primary_key_columns(connection1, schema1, table1)
    if key_columns:
        return key_columns

    key_columns = get_primary_key_columns(connection2, schema2, table2)
    if key_columns:
        return key_columns

    print(f"ERROR: Neither [{schema1}].[{table1}] nor [{schema2}].[{table2}] has a primary key. "
          f"Please specify --key-columns.", file=sys.stderr)
    sys.exit(1)


def describe_table(config1, schema1, table1, config2, schema2, table2):
    """Return a (label1, label2) pair identifying each table, including only the
    server/db/schema/table parts that actually differ between the two sides."""
    parts1 = []
    parts2 = []
    if config1['server'] != config2['server']:
        parts1.append(config1['server'])
        parts2.append(config2['server'])
    if config1['database'] != config2['database']:
        parts1.append(config1['database'])
        parts2.append(config2['database'])
    if schema1 != schema2:
        parts1.append(schema1)
        parts2.append(schema2)
    if table1 != table2:
        parts1.append(table1)
        parts2.append(table2)

    if not parts1:
        parts1 = [schema1, table1]
        parts2 = [schema2, table2]

    return '/'.join(parts1), '/'.join(parts2)


def sanitize_filename(name):
    return re.sub(r'[^A-Za-z0-9_-]+', '_', name).strip('_')


def normalize_value(value, trim, replace_special_chars):
    if not isinstance(value, str):
        return value
    if replace_special_chars:
        for old, new in SPECIAL_CHAR_REPLACEMENTS.items():
            value = value.replace(old, new)
    if trim:
        value = value.strip()
    return value


def normalize_row(row, trim, replace_special_chars):
    return tuple(normalize_value(v, trim, replace_special_chars) for v in row)


def fetch_rows(connection, schema, table, select_columns, where_clause=None, joins=None):
    col_list = ', '.join(f'source_table.[{c}]' for c in select_columns)
    join_sql = " ".join([f'\nJOIN {join}' for join in joins]) if joins else ''
    where_sql = f'WHERE {where_clause}' if where_clause else ''
    cursor = connection.cursor()
    cursor.execute(f'SELECT {col_list} FROM [{schema}].[{table}] source_table {join_sql} {where_sql}')
    rows = cursor.fetchall()
    cursor.close()
    return rows


def main():
    parser = parse_args()
    ARGS = parser.parse_args()

    schema1 = ARGS.schema1
    table1 = ARGS.table1
    schema2 = ARGS.schema2 if ARGS.schema2 else schema1
    table2 = ARGS.table2 if ARGS.table2 else table1

    config1 = {
        'driver': ARGS.driver,
        'server': ARGS.server,
        'database': ARGS.db,
        'authentication': ARGS.authentication,
        'user': ARGS.user,
        'password': ARGS.password,
    }
    # if no second connection info was given at all, compare both tables over the single connection
    same_connection = not (ARGS.driver2 or ARGS.server2 or ARGS.db2 or ARGS.authentication2 or ARGS.user2 or ARGS.password2)

    if same_connection:
        config2 = config1
    else:
        config2 = {
            'driver': ARGS.driver2 if ARGS.driver2 else ARGS.driver,
            'server': ARGS.server2 if ARGS.server2 else ARGS.server,
            'database': ARGS.db2 if ARGS.db2 else ARGS.db,
            'authentication': ARGS.authentication2 if ARGS.authentication2 else 'UsernamePassword',
            'user': ARGS.user2 if ARGS.user2 else ARGS.user,
            'password': ARGS.password2 if ARGS.password2 else ARGS.password,
        }

    connection1 = None
    connection2 = None
    try:
        print(f'connecting to server {config1["server"]} db {config1["database"]}... ', end='', flush=True)
        connection1 = create_connection(config1)
        print(' - DONE', flush=True)

        if same_connection:
            connection2 = connection1
        else:
            print(f'connecting to server {config2["server"]} db {config2["database"]}... ', end='', flush=True)
            connection2 = create_connection(config2)
            print(' - DONE', flush=True)

        columns1 = get_columns(connection1, schema1, table1)
        if not columns1:
            print(f"ERROR: Table [{schema1}].[{table1}] not found or has no columns.", file=sys.stderr)
            sys.exit(1)

        columns2 = get_columns(connection2, schema2, table2)
        if not columns2:
            print(f"ERROR: Table [{schema2}].[{table2}] not found or has no columns.", file=sys.stderr)
            sys.exit(1)

        common_columns = [c for c in columns1 if c in set(columns2)]
        if not common_columns:
            print(f"ERROR: [{schema1}].[{table1}] and [{schema2}].[{table2}] have no columns in common.", file=sys.stderr)
            sys.exit(1)

        key_columns = determine_key_columns(connection1, schema1, table1, connection2, schema2, table2, ARGS.key_columns)
        missing_key_columns = [c for c in key_columns if c not in set(common_columns)]
        if missing_key_columns:
            print(f"ERROR: key column(s) not found in both tables: {', '.join(missing_key_columns)}", file=sys.stderr)
            sys.exit(1)

        if ARGS.compare_columns:
            compare_columns = list(ARGS.compare_columns)
            missing_compare_columns = [c for c in compare_columns if c not in set(common_columns)]
            if missing_compare_columns:
                print(f"ERROR: compare column(s) not found in both tables: {', '.join(missing_compare_columns)}", file=sys.stderr)
                sys.exit(1)
        else:
            compare_columns = common_columns

        if ARGS.compare_columns_exclude:
            exclude = set(ARGS.compare_columns_exclude)
            missing_exclude_columns = exclude - set(common_columns)
            if missing_exclude_columns:
                print(f"ERROR: compare-columns-exclude column(s) not found in both tables: {', '.join(missing_exclude_columns)}", file=sys.stderr)
                sys.exit(1)
            compare_columns = [c for c in compare_columns if c not in exclude]
            if not compare_columns:
                print("ERROR: no columns left to compare after applying --compare-columns-exclude.", file=sys.stderr)
                sys.exit(1)

        # ensure key columns are always fetched, compare columns without key columns (avoid duplicate columns in select)
        compare_only_columns = [c for c in compare_columns if c not in set(key_columns)]
        select_columns = key_columns + compare_only_columns
        key_len = len(key_columns)

        print(f'comparing [{schema1}].[{table1}] to [{schema2}].[{table2}] using key column(s) {key_columns} '
              f'and comparing column(s) {compare_columns} ...', flush=True)

        rows1 = fetch_rows(connection1, schema1, table1, select_columns, where_clause=ARGS.where_clause, joins=ARGS.joins)
        rows2 = fetch_rows(connection2, schema2, table2, select_columns, where_clause=ARGS.where_clause, joins=ARGS.joins)

        by_key1 = {tuple(row[:key_len]): row for row in rows1}
        by_key2 = {tuple(row[:key_len]): row for row in rows2}

        keys1 = set(by_key1.keys())
        keys2 = set(by_key2.keys())

        only_in_1 = keys1 - keys2
        only_in_2 = keys2 - keys1
        common_keys = keys1 & keys2

        different_keys = []
        for key in common_keys:
            row1 = by_key1[key]
            row2 = by_key2[key]
            if ARGS.ignore_whitespace_start_end or ARGS.normalize_special_chars:
                left_values = normalize_row(row1[key_len:], ARGS.ignore_whitespace_start_end, ARGS.normalize_special_chars)
                right_values = normalize_row(row2[key_len:], ARGS.ignore_whitespace_start_end, ARGS.normalize_special_chars)
                values_differ = left_values != right_values
            else:
                values_differ = tuple(row1[key_len:]) != tuple(row2[key_len:])
            if values_differ:
                different_keys.append(key)

        sorted_only_in_1 = sorted(only_in_1)
        sorted_only_in_2 = sorted(only_in_2)
        sorted_different_keys = sorted(different_keys)

        truncated_only_in_1 = ARGS.max_diff_rows is not None and len(sorted_only_in_1) > ARGS.max_diff_rows
        truncated_only_in_2 = ARGS.max_diff_rows is not None and len(sorted_only_in_2) > ARGS.max_diff_rows
        truncated_different = ARGS.max_diff_rows is not None and len(sorted_different_keys) > ARGS.max_diff_rows

        if ARGS.max_diff_rows is not None:
            sorted_only_in_1 = sorted_only_in_1[:ARGS.max_diff_rows]
            sorted_only_in_2 = sorted_only_in_2[:ARGS.max_diff_rows]
            sorted_different_keys = sorted_different_keys[:ARGS.max_diff_rows]

        if truncated_only_in_1 or truncated_only_in_2 or truncated_different:
            print(f'NOTE: more than --max-diff-rows ({ARGS.max_diff_rows}) differing rows found, '
                  f'output is truncated per category.', file=sys.stderr)

        source1, source2 = describe_table(config1, schema1, table1, config2, schema2, table2)

        # if --diff-dir is used without an explicit --output, skip the combined CSV
        # (which would otherwise go to stdout) and only print the summary
        if ARGS.output or not ARGS.diff_dir:
            output_file = open(ARGS.output, 'w', newline='') if ARGS.output else sys.stdout
            try:
                writer = csv.writer(output_file)
                writer.writerow(['diff_type', 'source'] + select_columns)

                for key in sorted_only_in_1:
                    writer.writerow(['only_in_table1', source1] + list(by_key1[key]))

                for key in sorted_only_in_2:
                    writer.writerow(['only_in_table2', source2] + list(by_key2[key]))

                for key in sorted_different_keys:
                    writer.writerow(['different', source1] + list(by_key1[key]))
                    writer.writerow(['different', source2] + list(by_key2[key]))
            finally:
                if ARGS.output:
                    output_file.close()

        if ARGS.diff_dir:
            os.makedirs(ARGS.diff_dir, exist_ok=True)
            # filenames must always include the table name, even if describe_table() left it out
            # because it is identical on both sides
            file_label1 = source1 if table1 in source1.split('/') else f'{source1}/{table1}'
            file_label2 = source2 if table2 in source2.split('/') else f'{source2}/{table2}'
            left_path = os.path.join(ARGS.diff_dir, f'{sanitize_filename(file_label1)}.csv')
            right_path = os.path.join(ARGS.diff_dir, f'{sanitize_filename(file_label2)}.csv')
            blank_row = [''] * len(select_columns)

            with open(left_path, 'w', newline='') as left_file, open(right_path, 'w', newline='') as right_file:
                left_writer = csv.writer(left_file)
                right_writer = csv.writer(right_file)
                left_writer.writerow(select_columns)
                right_writer.writerow(select_columns)

                if ARGS.max_diff_rows is not None:
                    # a limit is set: only write the (capped) differing rows, not the full aligned table
                    diff_dir_keys = set(sorted_only_in_1) | set(sorted_only_in_2) | set(sorted_different_keys)
                else:
                    # unlimited: keep all matching (unchanged) rows too, for alignment context
                    diff_dir_keys = common_keys - set(different_keys) | set(sorted_only_in_1) | set(sorted_only_in_2) | set(sorted_different_keys)
                for key in sorted(diff_dir_keys):
                    left_row = list(by_key1[key]) if key in by_key1 else list(key) + blank_row[key_len:]
                    right_row = list(by_key2[key]) if key in by_key2 else list(key) + blank_row[key_len:]
                    if ARGS.ignore_whitespace_start_end or ARGS.normalize_special_chars:
                        left_row = list(normalize_row(left_row, ARGS.ignore_whitespace_start_end, ARGS.normalize_special_chars))
                        right_row = list(normalize_row(right_row, ARGS.ignore_whitespace_start_end, ARGS.normalize_special_chars))
                    left_writer.writerow(left_row)
                    right_writer.writerow(right_row)

            print(f'wrote diff files for use in an editor compare view: {left_path} <-> {right_path}', file=sys.stderr)

        limit_note = f' (showing at most {ARGS.max_diff_rows} row(s) per category, see --max-diff-rows)' if ARGS.max_diff_rows is not None else ''
        print(f'DONE: {len(only_in_1)} row(s) only in [{schema1}].[{table1}], '
              f'{len(only_in_2)} row(s) only in [{schema2}].[{table2}], '
              f'{len(different_keys)} row(s) with differing values{limit_note}.', file=sys.stderr)

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        if connection1:
            connection1.close()
        if connection2 and connection2 is not connection1:
            connection2.close()


if __name__ == '__main__':
    main()
