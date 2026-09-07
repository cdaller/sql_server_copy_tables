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


def parse_args():
    parser = argparse.ArgumentParser(description='Generate SQL to reorder columns of a SQL Server table')
    parser.add_argument('--driver', dest='driver', default='{ODBC Driver 18 for SQL Server}', help='ODBC driver (default: %(default)s)')
    parser.add_argument('--server', dest='server', required=True, help='database server name')
    parser.add_argument('--db', dest='db', required=True, help='database name')
    parser.add_argument('--authentication', dest='authentication', default='UsernamePassword', help='authentication method (default: %(default)s)')
    parser.add_argument('--user', dest='user', help='username (for UsernamePassword auth)')
    parser.add_argument('--password', dest='password', help='password (for UsernamePassword auth)')
    parser.add_argument('--table', dest='table', required=True, help='table name, optionally schema-qualified (e.g. dbo.MyTable)')
    parser.add_argument('--print-info', dest='print_info', default=True, action=argparse.BooleanOptionalAction, help='Print info about the current table (columns, defaults, indexes, constraints, foreign keys) (default: %(default)s)')
    parser.add_argument('--print-sql', dest='print_sql', default=True, action=argparse.BooleanOptionalAction, help='Print the generated SQL (default: %(default)s)')
    parser.add_argument('--execute-sql', dest='execute_sql', default=False, action=argparse.BooleanOptionalAction, help='Execute the generated SQL against the database (default: %(default)s)')
    parser.add_argument('column_order', nargs='+', help='columns in desired order (unspecified columns are appended at end)')
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


def get_column_info(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            c.name                  AS column_name,
            t.name                  AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.column_id,
            dc.definition           AS default_definition,
            c.is_identity,
            IDENT_SEED(QUOTENAME(s.name) + '.' + QUOTENAME(o.name)) AS identity_seed,
            IDENT_INCR(QUOTENAME(s.name) + '.' + QUOTENAME(o.name)) AS identity_incr
        FROM sys.columns c
        JOIN sys.types t        ON c.user_type_id = t.user_type_id
        JOIN sys.objects o      ON c.object_id = o.object_id
        JOIN sys.schemas s      ON o.schema_id = s.schema_id
        LEFT JOIN sys.default_constraints dc
                                ON dc.parent_object_id = c.object_id
                               AND dc.parent_column_id = c.column_id
        WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
    """, f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()
    return rows


def get_key_constraints(connection, schema, table):
    """Primary key and unique constraints (backed by an index)."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            kc.name         AS constraint_name,
            kc.type         AS constraint_type,
            i.type_desc     AS index_type_desc,
            c.name          AS column_name,
            ic.key_ordinal,
            ic.is_descending_key
        FROM sys.key_constraints kc
        JOIN sys.indexes i       ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE kc.parent_object_id = OBJECT_ID(?)
        ORDER BY kc.name, ic.key_ordinal
    """, f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()

    constraints = {}
    for name, ctype, index_type_desc, col_name, key_ordinal, is_desc in rows:
        c = constraints.setdefault(name, {
            'name': name,
            'is_primary_key': ctype == 'PK',
            'clustered': index_type_desc == 'CLUSTERED',
            'columns': [],
        })
        c['columns'].append((col_name, bool(is_desc)))
    return list(constraints.values())


def get_indexes(connection, schema, table):
    """Non-constraint indexes (regular CREATE INDEX indexes)."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            i.name          AS index_name,
            i.is_unique,
            i.type_desc,
            i.has_filter,
            i.filter_definition,
            c.name          AS column_name,
            ic.key_ordinal,
            ic.is_descending_key,
            ic.is_included_column
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE i.object_id = OBJECT_ID(?)
          AND i.is_primary_key = 0
          AND i.is_unique_constraint = 0
          AND i.type > 0
        ORDER BY i.name, ic.is_included_column, ic.key_ordinal
    """, f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()

    indexes = {}
    for name, is_unique, type_desc, has_filter, filter_def, col_name, key_ordinal, is_desc, is_included in rows:
        idx = indexes.setdefault(name, {
            'name': name,
            'is_unique': bool(is_unique),
            'clustered': type_desc == 'CLUSTERED',
            'filter_definition': filter_def if has_filter else None,
            'key_columns': [],
            'include_columns': [],
        })
        if is_included:
            idx['include_columns'].append(col_name)
        else:
            idx['key_columns'].append((col_name, bool(is_desc)))
    return list(indexes.values())


def get_check_constraints(connection, schema, table):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT name, definition
        FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID(?)
    """, f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()
    return [{'name': name, 'definition': definition} for name, definition in rows]


def get_foreign_keys(connection, schema, table):
    """Foreign keys defined on this table, referencing other tables."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            fk.name,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
            OBJECT_NAME(fk.referenced_object_id)         AS ref_table,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc,
            c.name  AS column_name,
            rc.name AS ref_column_name,
            fkc.constraint_column_id
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.columns c  ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
        JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
        WHERE fk.parent_object_id = OBJECT_ID(?)
        ORDER BY fk.name, fkc.constraint_column_id
    """, f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()

    fks = {}
    for name, ref_schema, ref_table, del_action, upd_action, col, ref_col, _ in rows:
        fk = fks.setdefault(name, {
            'name': name, 'ref_schema': ref_schema, 'ref_table': ref_table,
            'delete_action': del_action, 'update_action': upd_action, 'columns': [],
        })
        fk['columns'].append((col, ref_col))
    return list(fks.values())


def get_referencing_foreign_keys(connection, schema, table):
    """Foreign keys defined on OTHER tables that reference this table."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT
            fk.name,
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
            OBJECT_NAME(fk.parent_object_id)         AS child_table,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc,
            c.name  AS child_column,
            rc.name AS ref_column,
            fkc.constraint_column_id
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.columns c  ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
        JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
        WHERE fk.referenced_object_id = OBJECT_ID(?)
          AND fk.parent_object_id <> OBJECT_ID(?)
        ORDER BY fk.name, fkc.constraint_column_id
    """, f'{schema}.{table}', f'{schema}.{table}')
    rows = cursor.fetchall()
    cursor.close()

    fks = {}
    for name, child_schema, child_table, del_action, upd_action, child_col, ref_col, _ in rows:
        fk = fks.setdefault(name, {
            'name': name, 'child_schema': child_schema, 'child_table': child_table,
            'delete_action': del_action, 'update_action': upd_action, 'columns': [],
        })
        fk['columns'].append((child_col, ref_col))
    return list(fks.values())


def format_action(action_desc):
    return action_desc.replace('_', ' ')


def format_type(type_name, max_length, precision, scale):
    t = type_name.upper()
    if t in ('VARCHAR', 'CHAR', 'VARBINARY', 'BINARY'):
        length = 'MAX' if max_length == -1 else max_length
        return f'{t}({length})'
    if t in ('NVARCHAR', 'NCHAR'):
        length = 'MAX' if max_length == -1 else max_length // 2
        return f'{t}({length})'
    if t in ('DECIMAL', 'NUMERIC'):
        return f'{t}({precision},{scale})'
    if t == 'FLOAT':
        return f'{t}({precision})'
    if t in ('DATETIME2', 'TIME', 'DATETIMEOFFSET'):
        return f'{t}({scale})'
    return t


def format_column_def(col):
    (name, type_name, max_length, precision, scale,
     is_nullable, col_id, default_def, is_identity,
     identity_seed, identity_incr) = col

    type_str = format_type(type_name, max_length, precision, scale)
    identity_str = f' IDENTITY({int(identity_seed)},{int(identity_incr)})' if is_identity else ''
    default_str = f' DEFAULT {default_def}' if default_def else ''
    nullable_str = 'NULL' if is_nullable else 'NOT NULL'

    return f'    [{name}] {type_str}{identity_str}{default_str} {nullable_str}'


def format_key_constraint_sql(full_table, constraint):
    kind = 'PRIMARY KEY' if constraint['is_primary_key'] else 'UNIQUE'
    cluster = 'CLUSTERED' if constraint['clustered'] else 'NONCLUSTERED'
    cols = ',\n    '.join(f"[{c}] {'DESC' if desc else 'ASC'}" for c, desc in constraint['columns'])
    return (f"ALTER TABLE {full_table} ADD CONSTRAINT [{constraint['name']}] {kind} {cluster} (\n"
            f"    {cols}\n"
            f");")


def format_check_constraint_sql(full_table, check):
    return f"ALTER TABLE {full_table} ADD CONSTRAINT [{check['name']}] CHECK ({check['definition']});"


def format_index_sql(full_table, index):
    unique = 'UNIQUE ' if index['is_unique'] else ''
    cluster = 'CLUSTERED' if index['clustered'] else 'NONCLUSTERED'
    key_cols = ',\n    '.join(f"[{c}] {'DESC' if desc else 'ASC'}" for c, desc in index['key_columns'])
    sql = f"CREATE {unique}{cluster} INDEX [{index['name']}] ON {full_table} (\n    {key_cols}\n)"
    if index['include_columns']:
        include_cols = ', '.join(f"[{c}]" for c in index['include_columns'])
        sql += f"\nINCLUDE ({include_cols})"
    if index['filter_definition']:
        sql += f"\nWHERE {index['filter_definition']}"
    return sql + ';'


def format_foreign_key_sql(full_table, ref_full_table, fk, name_key='name'):
    cols = ', '.join(f"[{c}]" for c, _ in fk['columns'])
    ref_cols = ', '.join(f"[{rc}]" for _, rc in fk['columns'])
    sql = (f"ALTER TABLE {full_table} WITH CHECK ADD CONSTRAINT [{fk[name_key]}] FOREIGN KEY({cols})\n"
           f"REFERENCES {ref_full_table} ({ref_cols})\n"
           f"ON DELETE {format_action(fk['delete_action'])}\n"
           f"ON UPDATE {format_action(fk['update_action'])};\n"
           f"ALTER TABLE {full_table} CHECK CONSTRAINT [{fk[name_key]}];")
    return sql


def print_table_info(schema, table, columns, new_order, not_specified,
                      key_constraints, indexes, check_constraints,
                      foreign_keys, referencing_foreign_keys):
    print(f'\nCurrent column order for [{schema}].[{table}]:')
    for col in columns:
        (name, type_name, max_length, precision, scale,
         is_nullable, col_id, default_def, is_identity,
         identity_seed, identity_incr) = col
        type_str = format_type(type_name, max_length, precision, scale)
        extras = []
        if is_identity:
            extras.append(f'IDENTITY({int(identity_seed)},{int(identity_incr)})')
        if default_def:
            extras.append(f'DEFAULT {default_def}')
        extras.append('NULL' if is_nullable else 'NOT NULL')
        print(f'  {col_id}. {name} ({type_str}) {" ".join(extras)}')

    print(f'\nDesired new order: {", ".join(new_order)}')
    if not_specified:
        print(f"WARNING: Columns not included in new order (appended at end): {', '.join(not_specified)}", file=sys.stderr)

    print('\nPrimary key / unique constraints:')
    if key_constraints:
        for kc in key_constraints:
            kind = 'PRIMARY KEY' if kc['is_primary_key'] else 'UNIQUE'
            cluster = 'CLUSTERED' if kc['clustered'] else 'NONCLUSTERED'
            cols = ', '.join(f"{c} {'DESC' if desc else 'ASC'}" for c, desc in kc['columns'])
            print(f"  {kc['name']}: {kind} {cluster} ({cols})")
    else:
        print('  (none)')

    print('\nIndexes:')
    if indexes:
        for idx in indexes:
            unique = 'UNIQUE ' if idx['is_unique'] else ''
            cluster = 'CLUSTERED' if idx['clustered'] else 'NONCLUSTERED'
            cols = ', '.join(f"{c} {'DESC' if desc else 'ASC'}" for c, desc in idx['key_columns'])
            include = f" INCLUDE ({', '.join(idx['include_columns'])})" if idx['include_columns'] else ''
            filt = f" WHERE {idx['filter_definition']}" if idx['filter_definition'] else ''
            print(f"  {idx['name']}: {unique}{cluster} ({cols}){include}{filt}")
    else:
        print('  (none)')

    print('\nCheck constraints:')
    if check_constraints:
        for ck in check_constraints:
            print(f"  {ck['name']}: CHECK {ck['definition']}")
    else:
        print('  (none)')

    print('\nForeign keys defined on this table:')
    if foreign_keys:
        for fk in foreign_keys:
            cols = ', '.join(c for c, _ in fk['columns'])
            ref_cols = ', '.join(rc for _, rc in fk['columns'])
            print(f"  {fk['name']}: ({cols}) -> [{fk['ref_schema']}].[{fk['ref_table']}] ({ref_cols}) "
                  f"ON DELETE {format_action(fk['delete_action'])} ON UPDATE {format_action(fk['update_action'])}")
    else:
        print('  (none)')

    print('\nForeign keys referencing this table from other tables:')
    if referencing_foreign_keys:
        for fk in referencing_foreign_keys:
            cols = ', '.join(c for c, _ in fk['columns'])
            ref_cols = ', '.join(rc for _, rc in fk['columns'])
            print(f"  {fk['name']}: [{fk['child_schema']}].[{fk['child_table']}] ({cols}) -> ({ref_cols}) "
                  f"ON DELETE {format_action(fk['delete_action'])} ON UPDATE {format_action(fk['update_action'])}")
    else:
        print('  (none)')


def resolve_column_order(columns, new_order):
    existing_lower = {col[0].lower() for col in columns}
    specified_lower = [c.lower() for c in new_order]

    missing = set(specified_lower) - existing_lower
    if missing:
        print(f"ERROR: Columns not found in table: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Preserve original casing from DB for unspecified columns
    not_specified = [col[0] for col in columns if col[0].lower() not in set(specified_lower)]
    full_order_names = list(new_order) + not_specified
    return full_order_names, not_specified


def generate_reorder_sql(schema, table, columns, full_order_names,
                          key_constraints=None, indexes=None, check_constraints=None,
                          foreign_keys=None, referencing_foreign_keys=None):
    col_by_name = {col[0].lower(): col for col in columns}

    full_table = f'[{schema}].[{table}]'
    tmp_table  = f'[{schema}].[{table}__reorder_tmp]'
    tmp_name_for_rename = f'{schema}.{table}__reorder_tmp'  # sp_rename format

    has_identity = any(col_by_name[n.lower()][8] for n in full_order_names)

    col_defs = ',\n'.join(format_column_def(col_by_name[n.lower()]) for n in full_order_names)
    col_list  = ',\n    '.join(f'[{n}]' for n in full_order_names)
    # Exclude identity columns from INSERT column list
    non_identity_cols = [n for n in full_order_names if not col_by_name[n.lower()][8]]
    insert_col_list   = ',\n    '.join(f'[{n}]' for n in non_identity_cols)

    identity_insert_on  = f'\nSET IDENTITY_INSERT {tmp_table} ON;' if has_identity else ''
    identity_insert_off = f'\nSET IDENTITY_INSERT {tmp_table} OFF;' if has_identity else ''
    insert_cols = col_list if has_identity else insert_col_list
    select_cols = col_list if has_identity else insert_col_list

    key_constraints = key_constraints or []
    indexes = indexes or []
    check_constraints = check_constraints or []
    foreign_keys = foreign_keys or []
    referencing_foreign_keys = referencing_foreign_keys or []

    drop_incoming_fk_sql = '\n'.join(
        f"ALTER TABLE [{fk['child_schema']}].[{fk['child_table']}] DROP CONSTRAINT [{fk['name']}];"
        for fk in referencing_foreign_keys
    )
    drop_incoming_fk_section = (
        f"\n-- Step 0: Drop foreign keys from other tables referencing this table\n{drop_incoming_fk_sql}\n"
        if drop_incoming_fk_sql else ''
    )

    key_constraint_sql = '\n'.join(format_key_constraint_sql(full_table, kc) for kc in key_constraints)
    key_constraint_section = (
        f"\n-- Step 5: Recreate primary key / unique constraints\n{key_constraint_sql}\n"
        if key_constraint_sql else ''
    )

    check_constraint_sql = '\n'.join(format_check_constraint_sql(full_table, ck) for ck in check_constraints)
    check_constraint_section = (
        f"\n-- Step 6: Recreate check constraints\n{check_constraint_sql}\n"
        if check_constraint_sql else ''
    )

    fk_sql = '\n'.join(
        format_foreign_key_sql(full_table, f"[{fk['ref_schema']}].[{fk['ref_table']}]", fk)
        for fk in foreign_keys
    )
    fk_section = (
        f"\n-- Step 7: Recreate foreign keys defined on this table\n{fk_sql}\n"
        if fk_sql else ''
    )

    index_sql = '\n'.join(format_index_sql(full_table, idx) for idx in indexes)
    index_section = (
        f"\n-- Step 8: Recreate indexes\n{index_sql}\n"
        if index_sql else ''
    )

    incoming_fk_sql = '\n'.join(
        format_foreign_key_sql(f"[{fk['child_schema']}].[{fk['child_table']}]", full_table, fk)
        for fk in referencing_foreign_keys
    )
    incoming_fk_section = (
        f"\n-- Step 9: Recreate foreign keys from other tables referencing this table\n{incoming_fk_sql}\n"
        if incoming_fk_sql else ''
    )

    sql = f"""{drop_incoming_fk_section}-- Step 1: Create new table with desired column order
CREATE TABLE {tmp_table} (
{col_defs}
);
{identity_insert_on}
-- Step 2: Copy data
INSERT INTO {tmp_table} (
    {insert_cols}
)
SELECT
    {select_cols}
FROM {full_table};
{identity_insert_off}
-- Step 3: Drop original table
DROP TABLE {full_table};

-- Step 4: Rename new table to original name
EXEC sp_rename '{tmp_name_for_rename}', '{table}';
{key_constraint_section}{check_constraint_section}{fk_section}{index_section}{incoming_fk_section}"""
    return sql


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

        columns = get_column_info(connection, schema, table)
        if not columns:
            print(f"ERROR: Table '{schema}.{table}' not found or has no columns.", file=sys.stderr)
            sys.exit(1)

        full_order_names, not_specified = resolve_column_order(columns, ARGS.column_order)

        key_constraints = get_key_constraints(connection, schema, table)
        indexes = get_indexes(connection, schema, table)
        check_constraints = get_check_constraints(connection, schema, table)
        foreign_keys = get_foreign_keys(connection, schema, table)
        referencing_foreign_keys = get_referencing_foreign_keys(connection, schema, table)

        if ARGS.print_info:
            print_table_info(schema, table, columns, ARGS.column_order, not_specified,
                              key_constraints, indexes, check_constraints,
                              foreign_keys, referencing_foreign_keys)

        sql = generate_reorder_sql(schema, table, columns, full_order_names,
                                    key_constraints=key_constraints, indexes=indexes,
                                    check_constraints=check_constraints, foreign_keys=foreign_keys,
                                    referencing_foreign_keys=referencing_foreign_keys)

        if ARGS.print_sql:
            print('\n--- Generated SQL ---\n')
            print(sql)

        print("NOTE: triggers and permissions/grants on the table are not detected or recreated.", file=sys.stderr)

        if ARGS.execute_sql:
            print('\nExecuting generated SQL...', end='', flush=True)
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
            cursor.close()
            print(' - DONE', flush=True)

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    main()
