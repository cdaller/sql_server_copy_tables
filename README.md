# Sql Server Copy Tables

This python script is able to copy one or more tables from one sql server database to another.

If the tables already exist, it is able to truncate them or if they do not exist yet, if creates them and fills them with data.

The script is able to use username/password credentials or Azure Identity, so no credentials need to be passed to the script (just use ```az login``` before).

It also copies any indices that belong to the tables.

The script uses pages (not the most efficient way to read data, but it works).

## Help

```bash
./mssql_copy_table.py --help         
usage: mssql_copy_table.py [-h] [--source-driver SOURCE_DRIVER] --source-server SOURCE_SERVER --source-db SOURCE_DB [--source-schema SOURCE_SCHEMA] [--source-authentication SOURCE_AUTHENTICATION] [--source-user SOURCE_USER]
                           [--source-password SOURCE_PASSWORD] [--source-list-tables] [--target-driver TARGET_DRIVER] --target-server TARGET_SERVER --target-db TARGET_DB [--target-schema TARGET_SCHEMA] [--target-authentication TARGET_AUTHENTICATION]
                           [--target-user TARGET_USER] [--target-password TARGET_PASSWORD] [--target-list-tables] [--truncate-table | --no-truncate-table] [--create-table | --no-create-table] [--copy-indices | --no-copy-indices] [--dry-run]
                           [-t [TABLES ...]] [--all-tables] [--table-filter TABLE_FILTER] [--page-size PAGE_SIZE] [--page-start PAGE_START]

Copy one or more tables from an sql server to another sql server

options:
  -h, --help            show this help message and exit
  --source-driver SOURCE_DRIVER
                        source database server driver
  --source-server SOURCE_SERVER
                        source database server name
  --source-db SOURCE_DB
                        source database name
  --source-schema SOURCE_SCHEMA
                        source database schema name
  --source-authentication SOURCE_AUTHENTICATION
                        source database authentication. default is UsernamePassword. Possible to use AzureActiveDirectory
  --source-user SOURCE_USER
                        source database username, if authentication is set to UsenamePassword
  --source-password SOURCE_PASSWORD
                        source database password, if authentication is set to UsenamePassword
  --source-list-tables  If set, a list of tables is printed, no data is copied!
  --target-driver TARGET_DRIVER
                        target database server driver
  --target-server TARGET_SERVER
                        target database server name
  --target-db TARGET_DB
                        target database name
  --target-schema TARGET_SCHEMA
                        target database schema name
  --target-authentication TARGET_AUTHENTICATION
                        target database authentication. default is UsernamePassword. Possible to use AzureActiveDirectory
  --target-user TARGET_USER
                        source database username, if authentication is set to UsenamePassword
  --target-password TARGET_PASSWORD
                        source database password, if authentication is set to UsenamePassword
  --target-list-tables  If set, a list of tables is printed, no data is copied!
  --truncate-table, --no-truncate-table
                        If set, truncate the target table before inserting rows from source table. If this option is set, the tables are NOT recreated, even if --create-table is used!
  --create-table, --no-create-table
                        If set, drop (if exists) and (re)create the target table before inserting rows from source table. All columns, types and not-null and primary key constraints will also be copied. Indices of the table will also be recreated if not prevented by --no-copy-indices flag
  --copy-indices, --no-copy-indices
                        Create the indices for the target tables as they exist on the source table
  --dry-run             Do not modify target database, just print what would happen
  -t [TABLES ...], --table [TABLES ...]
                        Specify the tables you want to copy. Either repeat "-t <name> -t <name2>" or by "-t <name> <name2>"
  --all-tables          Copy all tables in the schema from the source db to the target db
  --table-filter TABLE_FILTER
                        Filter table names using this regular expression (regexp must match table names). Use with "--all-tables" or one of the "list-tables" arguments.
  --page-size PAGE_SIZE
                        Page size of rows that are copied in one step. Depending on the size of table, values between 50000 (default) and 500000 are working well.
  --page-start PAGE_START
                        Page to start with. Please note that the first page number ist 1 to match the output during copying of the data. The output of a page number indicates the page is read. The "w" after the page number shows that the pages was successfully written. Please also note that this settings does not make much sense if you copy more than one table!
```

## Examples

Copy all tables and indices from the source db to the target db and create all indices that belong to the tables:

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema dbo \
    --source-user xxx \
    --source-password xxx \
    --target-server anotherserver.foo.bar.com \
    --target-db my-db \
    --target-schema dbo \
    --target-user xxx \
    --target-password xxx \
    --create-table \
    --all-tables  
```

Only copy some tables and do not create them, but only truncate them before copying data. Also use dry-run to show what would be done, but do not do it!

```bash

./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema dbo \
    --source-user xxx \
    --source-password xxx \
    --target-server anotherserver.foo.bar.com \
    --target-db my-db \
    --target-schema dbo \
    --target-user xxx \
    --target-password xxx \
    --truncate-table \
    --no-copy-indices \
    --table table_a table_b table_c \
    --dry-run
```

Copy tables using a regular expression for the table names. Use azure databases with azure identity to login (```--target-authentication AzureActiveDirectory```). Also use a larger page size than the default 50000 rows:

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db liferay-db \
    --source-schema MYSCHEMA \
    --source-user sa \
    --source-password DSmdM@ORF1 \
    --target-server xyzserver.database.windows.net \
    --target-db azure-db \
    --target-schema OTHERSCHEMA \
    --target-authentication AzureActiveDirectory \
    --create-table \
    --table-filter "TABLE_.*|OTHER_.*" \
    --page-size 100000 \
    --all-tables
```

If the copy process breaks, one can restart the copy and start from a given page using ```--page-start 123``` parameter.
The copy process prints which pages have been read and written, so one knows exactly how many rows were already copied and what is missing:

```
Copying table TABLE_A ... 921070 rows ... paging 19 pages each 50000 rows, page 1rw 2rw 3rw 4rw 5rw 6rw 7rw 8rw 9rw 10rw 11rw 12rw 13rw 14r
```

This indicates that page 1 to 13 were read and written (```rw```), but page 14 was not written yet. So if the process somehow dies, one could restart the copy process by using ```--page-start 14```.
Please note that ```--page-start```does only make sense with a single table given!
