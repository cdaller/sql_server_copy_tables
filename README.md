# Sql Server Copy Tables and Views

This python script is able to copy one or more tables or views from one sql server database/schema to another.

If the tables already exist, it is able to truncate them or if they do not exist yet, if creates them and fills them with data.

The script is able to use username/password credentials or Azure Identity, so no credentials need to be passed to the script (just use ```az login``` before).

It also copies any indices that belong to the tables.

The script uses pages to bulk read and write data.

## Installation

The script uses pyodbc:

```bash
pip install pyodbc

# python >= 3.12:
mkdir .venv
python3 -m venv .venv 
source .venv/bin/activate
python3 -m pip install pyodbc
```

And it needs some odbc drivers from microsoft.

See either here how to install them: 

* Linux: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
* MacOS: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos

You might also need unixodbc:

```bash
brew install unixobc
```

### Azure Active Directory Authentication

Optional: If you want to use Azure Active Directory authentiation, you need to install ```azure-identity``` package:

```bash
pip install azure-identity

# python >= 3.12 using venv:
python3 -m pip install azure-identity
```

## Help

```bash
./mssql_copy_table.py --help
usage: mssql_copy_table.py [-h] [--source-driver SOURCE_DRIVER] --source-server SOURCE_SERVER --source-db SOURCE_DB [--source-schema SOURCE_SCHEMA] [--source-authentication SOURCE_AUTHENTICATION] [--source-user SOURCE_USER]
                           [--source-password SOURCE_PASSWORD] [--source-list-tables] [--target-driver TARGET_DRIVER] --target-server TARGET_SERVER --target-db TARGET_DB [--target-schema TARGET_SCHEMA] [--target-authentication TARGET_AUTHENTICATION]
                           [--target-user TARGET_USER] [--target-password TARGET_PASSWORD] [--target-list-tables] [--truncate-table | --no-truncate-table] [--create-table | --no-create-table] [--copy-indices | --no-copy-indices]
                           [--copy-data | --no-copy-data] [--dry-run] [--compare-table | --no-compare-table] [-t [TABLES ...]] [--all-tables] [--table-filter TABLE_FILTER] [--page-size PAGE_SIZE] [--page-start PAGE_START]

Copy one or more tables from an sql server to another sql server

options:
  -h, --help            show this help message and exit
  --source-driver SOURCE_DRIVER
                        source database server driver (default: {ODBC Driver 18 for SQL Server})
  --source-server SOURCE_SERVER
                        source database server name
  --source-db SOURCE_DB
                        source database name
  --source-schema SOURCE_SCHEMA
                        source database schema name (default: dbo)
  --source-authentication SOURCE_AUTHENTICATION
                        source database authentication. Possible to use AzureActiveDirectory (default: UsernamePassword)
  --source-user SOURCE_USER
                        source database username, if authentication is set to UsenamePassword
  --source-password SOURCE_PASSWORD
                        source database password, if authentication is set to UsenamePassword
  --source-list-tables  If set, a list of tables is printed, no data is copied! (default: False)
  --target-driver TARGET_DRIVER
                        target database server driver (default: {ODBC Driver 18 for SQL Server})
  --target-server TARGET_SERVER
                        target database server name
  --target-db TARGET_DB
                        target database name
  --target-schema TARGET_SCHEMA
                        target database schema name (default: dbo)
  --target-authentication TARGET_AUTHENTICATION
                        target database authentication. Possible to use AzureActiveDirectory (default: UsernamePassword)
  --target-user TARGET_USER
                        source database username, if authentication is set to UsenamePassword
  --target-password TARGET_PASSWORD
                        source database password, if authentication is set to UsenamePassword
  --target-list-tables  If set, a list of tables is printed, no data is copied!
  --truncate-table, --no-truncate-table
                        If set, truncate the target table before inserting rows from source table. If this option is set, the tables are NOT recreated, even if --create-table is used! (default: False)
  --create-table, --no-create-table
                        If set, drop (if exists) and (re)create the target table before inserting rows from source table. All columns, types and not-null and primary key constraints will also be copied. Indices of the table will also be recreated if not prevented by --no-copy-indices flag
                        (default: True)
  --copy-indices, --no-copy-indices
                        Create the indices for the target tables as they exist on the source table (default: True)
  --drop-indices, --no-drop-indices
                        Drop indices before copying data for performance reasons. The indices are created after copying by --copy-indices afterwards (default: True)
  --copy-data, --no-copy-data
                        Copy the data of the tables. Default True! Use --no-copy-data if you want to creat the indices only. (default: True)
  --dry-run             Do not modify target database, just print what would happen. (default: False)
  --compare-table, --no-compare-table
                        If set, do not copy any data, but compare the source and the target table(s) and print if there are any differences in columns, indices or content rows. (default: False)
  -t TABLES [TABLES ...], --table TABLES [TABLES ...]
                        Specify the tables you want to copy. Either repeat "-t <name> -t <name2>" or by "-t <name> <name2>"
  --all-tables          Copy all tables in the schema from the source db to the target db. (default: False)
  --table-filter TABLE_FILTER
                        Filter table names using this regular expression (regexp must match table names). Use with "--all-tables" or one of the "list-tables" arguments. (default: None)
  --page-size PAGE_SIZE
                        Page size of rows that are copied in one step. Depending on the size of table, values between 50000 (default) and 500000 are working well (depending on the number of rows, etc.). (default: 50000)
  --page-start PAGE_START
                        Page to start with. Please note that the first page number ist 1 to match the output during copying of the data. The output of a page number indicates the page is read. The "w" after the page number shows that the pages was successfully written. Please also note that
                        this settings does not make much sense if you copy more than one table! (default: 1)
  --copy-view, --no-copy-view
                        Copy the views. By default all views are copied if not limited by "--view <name>" "--view-filter <regepx>"! (default: False)
  --view VIEWS [VIEWS ...]
                        Specify the views you want to copy. Either repeat "--view <name> --view <name2>" or by "--view <name> <name2>"
  --view-filter VIEW_FILTER
                        Filter view names using this regular expression (regexp must match view names). (default: None)
  --debug-sql           If enabled, prints sql statements. (default: 0)
```

## Examples

### All Tables and Indices

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

### Copy Selected Tables and use Dry-Run Mode

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

### Copy Tables with Regexp Filter and Page Size


Copy tables using a regular expression for the table names. Use azure databases with azure identity to login (```--target-authentication AzureActiveDirectory```). Also use a larger page size than the default 50000 rows:

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema MYSCHEMA \
    --source-user xxx \
    --source-password xxx \
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
Copying table TABLE_A ... 921070 rows ... paging 19 pages each 50000 rows, page 1r(0.4s)w(9.6s) 2r(0.3s)w(4.1s) 3r(0.3s)w(4.9s) 4r(0.3s)w(4.4s) 5r(0.3s)w(4.0s) 6r(0.3s)w(3.8s) 7r(0.3s)w(5.6s) 8r(0.4s)w(5.0s) 9r(0.4s)w(6.2s) 10r(0.4s)w(4.8s) 11r(0.4s)w(4.0s) 12r(0.4s)w(7.1s) 13r(0.4s)w(5.0s) 14r(0.4s)
```

This indicates that page 1 to 13 were read and written (```rw``` including the seconds to read an write), but page 14 was not written yet. So if the process somehow dies at this specific moment, one could restart the copy process by using ```--page-start 14```.

Please note that ```--page-start```does only make sense with a single table given. If this parameter is used, the table is automatically NOT truncated, recreated nor are indices being copied.

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema MYSCHEMA \
    --source-user xxx \
    --source-password xxx \
    --target-server xyzserver.database.windows.net \
    --target-db azure-db \
    --target-schema OTHERSCHEMA \
    --target-authentication AzureActiveDirectory \
    --table TABLE_A \
    --page-start 14
```

### Copy Views

Copy all views from the source db to the target db:

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema MYSCHEMA \
    --source-user xxx \
    --source-password xxx \
    --target-server xyzserver.database.windows.net \
    --target-db azure-db \
    --target-schema OTHERSCHEMA \
    --target-authentication AzureActiveDirectory \
    --copy-view
```

Limit views to copy by enumerating the view names:

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema MYSCHEMA \
    --source-user xxx \
    --source-password xxx \
    --target-server xyzserver.database.windows.net \
    --target-db azure-db \
    --target-schema OTHERSCHEMA \
    --target-authentication AzureActiveDirectory \
    --copy-view \
    --view VIEW_ABC1 --view VIEW_ABC2 VIEW_ABC3
```

or using a regexp for view selection:

```bash
./mssql_copy_table.py \
    --source-server localhost \
    --source-db my-db \
    --source-schema MYSCHEMA \
    --source-user xxx \
    --source-password xxx \
    --target-server xyzserver.database.windows.net \
    --target-db azure-db \
    --target-schema OTHERSCHEMA \
    --target-authentication AzureActiveDirectory \
    --copy-view \
    --view-filter "VIEW_ABC\d"
```
