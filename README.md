# Sql Server Copy Tables and Views

This python script is able to copy one or more tables (selected rows) or views from one sql server database/schema to another.

If the tables already exist, it is able to truncate them or if they do not exist yet, if creates them and fills them with data.

The script is able to use username/password credentials or Azure Identity, so no credentials need to be passed to the script (just use ```az login``` before).

It also copies any indices that belong to the tables.

The script uses pages to bulk read and write data.

## Installation

The installation uses `uv` package manager.
See https://docs.astral.sh/uv/getting-started/installation/ for details.

### Install dependencies

```bash
uv sync
```

This creates a `.venv` virtual environment and installs all dependencies declared in `pyproject.toml` (`pyodbc`, `azure-identity`).

### SQL Server ODBC Drivers

See either here how to install them:

* Linux: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
* MacOS: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos

### UnixODBC

* You might also need unixodbc, if not already installed with the driver above:

MacOS:

```bash
brew install unixodbc
```

Linux/Debian:

```bash
sudo apt install unixodbc
```

## Running the scripts

There are three ways to run the scripts after `uv sync`:

### 1. Using installed commands (recommended)

`uv sync` installs the scripts as commands into `.venv/bin/`. Activate the virtual environment once, then call them by name:

```bash
# Linux/macOS
source .venv/bin/activate
mssql-copy-table --source-server ...
mssql-execute-sql --server ...
```

```ps1
# Windows
.venv\Scripts\Activate.ps1
mssql-copy-table --source-server ...
```

### 2. Using `uv run`

Run without activating the virtual environment — `uv run` picks it up automatically:

```bash
uv run mssql_copy_table.py --source-server ...
uv run mssql_execute_sql.py --server ...
```

### 3. Running from outside the project directory

Pass `--directory` to tell `uv run` where the project (and its `.venv`) lives:

```bash
uv run --directory /path/to/mssql_copy_table mssql_copy_table.py --source-server ...
uv run --directory /path/to/mssql_copy_table mssql_execute_sql.py --server ...
```

Alternatively, activate the venv by its full path and use the installed commands from anywhere:

```bash
source /path/to/mssql_copy_table/.venv/bin/activate
mssql-copy-table --source-server ...
```

### 4. Executing the script directly

The scripts have a `#!/usr/bin/env -S uv run --script` shebang, so they can be executed directly (Linux/macOS):

```bash
./mssql_copy_table.py --source-server ...
./mssql_execute_sql.py --server ...
```

## Help

Start the script with

```bash
uv run mssql_copy_table.py
# or
./mssql_copy_table.py
```

```bash
./mssql_copy_table.py --help
usage: mssql_copy_table.py [-h] [--source-driver SOURCE_DRIVER]
                           --source-server SOURCE_SERVER --source-db SOURCE_DB
                           [--source-schema SOURCE_SCHEMA]
                           [--source-authentication SOURCE_AUTHENTICATION]
                           [--source-user SOURCE_USER]
                           [--source-password SOURCE_PASSWORD]
                           [--source-list-tables]
                           [--target-driver TARGET_DRIVER]
                           --target-server TARGET_SERVER --target-db TARGET_DB
                           [--target-schema TARGET_SCHEMA]
                           [--target-authentication TARGET_AUTHENTICATION]
                           [--target-user TARGET_USER]
                           [--target-password TARGET_PASSWORD]
                           [--target-list-tables]
                           [--truncate-table | --no-truncate-table]
                           [--create-table | --no-create-table]
                           [--copy-indices | --no-copy-indices]
                           [--copy-foreign-keys | --no-copy-foreign-keys]
                           [--drop-indices | --no-drop-indices]
                           [--copy-data | --no-copy-data] [--dry-run]
                           [--compare-table | --no-compare-table]
                           [--compare-view | --no-compare-view]
                           [-t TABLES [TABLES ...]] [--all-tables]
                           [--table-filter TABLE_FILTER]
                           [--table-filter-exclude TABLE_FILTER_EXCLUDE]
                           [--page-size PAGE_SIZE] [--page-start PAGE_START]
                           [--auto-resume] [--where WHERE_CLAUSE]
                           [--delete-where | --no-delete-where]
                           [--join JOINS [JOINS ...]]
                           [--copy-view | --no-copy-view]
                           [--view VIEWS [VIEWS ...]]
                           [--view-filter VIEW_FILTER]
                           [--view-filter-exclude VIEW_FILTER_EXCLUDE]
                           [--copy-synonym | --no-copy-synonym]
                           [--synonym SYNONYMS [SYNONYMS ...]]
                           [--synonym-filter SYNONYM_FILTER]
                           [--synonym-filter-exclude SYNONYM_FILTER_EXCLUDE]
                           [--debug-sql]
                           [--progress-track-file PROGRESS_FILE_NAME]

Copy one or more tables from an sql server to another sql server

options:
  -h, --help            show this help message and exit
  --source-driver SOURCE_DRIVER
                        source database server driver (default: {ODBC Driver
                        18 for SQL Server})
  --source-server SOURCE_SERVER
                        source database server name
  --source-db SOURCE_DB
                        source database name
  --source-schema SOURCE_SCHEMA
                        source database schema name (default: dbo)
  --source-authentication SOURCE_AUTHENTICATION
                        source database authentication. Possible to use
                        AzureActiveDirectory (default: UsernamePassword)
  --source-user SOURCE_USER
                        source database username, if authentication is set to
                        UsernamePassword
  --source-password SOURCE_PASSWORD
                        source database password, if authentication is set to
                        UsernamePassword
  --source-list-tables  If set, a list of tables is printed, no data is
                        copied! (default: False)
  --target-driver TARGET_DRIVER
                        target database server driver (default: {ODBC Driver
                        18 for SQL Server})
  --target-server TARGET_SERVER
                        target database server name
  --target-db TARGET_DB
                        target database name
  --target-schema TARGET_SCHEMA
                        target database schema name (default: dbo)
  --target-authentication TARGET_AUTHENTICATION
                        target database authentication. Possible to use
                        AzureActiveDirectory (default: UsernamePassword)
  --target-user TARGET_USER
                        source database username, if authentication is set to
                        UsernamePassword
  --target-password TARGET_PASSWORD
                        source database password, if authentication is set to
                        UsernamePassword
  --target-list-tables  If set, a list of tables is printed, no data is
                        copied!
  --truncate-table, --no-truncate-table
                        If set, truncate the target table before inserting
                        rows from source table. If this option is set, the
                        tables are NOT recreated, even if --create-table is
                        used! (default: False)
  --create-table, --no-create-table
                        If set, drop (if exists) and (re)create the target
                        table before inserting rows from source table. All
                        columns, types and not-null and primary key
                        constraints will also be copied. Indices of the table
                        will also be recreated if not prevented by --no-copy-
                        indices flag (default: True)
  --copy-indices, --no-copy-indices
                        Create the indices for the target tables as they exist
                        on the source table (default: True)
  --copy-foreign-keys, --no-copy-foreign-keys
                        Create foreign key constraints on the target tables
                        after all tables are copied (default: True)
  --drop-indices, --no-drop-indices
                        Drop indices before copying data for performance
                        reasons. The indices are created after copying by
                        --copy-indices afterwards (default: True)
  --copy-data, --no-copy-data
                        Copy the data of the tables. Default True! Use --no-
                        copy-data if you want to creat the indices only.
                        (default: True)
  --dry-run             Do not modify target database, just print what would
                        happen. (default: False)
  --compare-table, --no-compare-table
                        If set, do not copy any data, but compare the source
                        and the target table(s) and print if there are any
                        differences in columns, indices or content rows.
                        (default: False)
  --compare-view, --no-compare-view
                        If set, do not copy any data, but compare the source
                        and the target view(s) and print if there are any
                        differences in columns. (default: False)
  -t, --table TABLES [TABLES ...]
                        Specify the tables you want to copy. Either repeat "-t
                        <name> -t <name2>" or by "-t <name> <name2>"
  --all-tables          Copy all tables in the schema from the source db to
                        the target db. (default: False)
  --table-filter TABLE_FILTER
                        Filter on table names using this regular expression
                        (regexp must match table names). Use with "--all-
                        tables" or one of the "list-tables" arguments.
                        (default: None)
  --table-filter-exclude TABLE_FILTER_EXCLUDE
                        Filter out table names using this regular expression
                        (regexp must match table names). Use with "--all-
                        tables" or one of the "list-tables" arguments.
                        (default: None)
  --page-size PAGE_SIZE
                        Page size of rows that are copied in one step.
                        Depending on the size of table, values between 50000
                        (default) and 500000 are working well (depending on
                        the number of rows, etc.). (default: 50000)
  --page-start, --start-page PAGE_START
                        Page to start with. Please note that the first page
                        number ist 1 to match the output during copying of the
                        data. The output of a page number indicates the page
                        is read. The "w" after the page number shows that the
                        pages was successfully written. Please also note that
                        this settings does not make much sense if you copy
                        more than one table! (default: 1)
  --auto-resume         Before copying a table, compare the row count already
                        present in the target table with the source table. If
                        they match exactly, the table is assumed to be fully
                        copied already and is skipped. If the target row count
                        is an exact multiple of "--page-size", copying
                        automatically resumes at the correct page instead of
                        starting over. (default: False)
  --where WHERE_CLAUSE  If set, this where clause is added to all queries
                        executed on the source data source. If you only want
                        to add some rows, use in combination with the params "
                        --no-create-table --no-drop-indices --no-copy-
                        indices". (default: None)
  --delete-where, --no-delete-where
                        Delete all rows in the target table using the given
                        where clause if a where clause is set with the "--
                        where" parameter. (default: False)
  --join JOINS [JOINS ...]
                        Add one or more joins to the selection of data
                        (probably only useful in combination with the --where
                        clause). The original table name is "source_table" to
                        use in the joins. Either use the parameter multiple
                        times or separate the joins with spaces.". (default:
                        None)
  --copy-view, --no-copy-view
                        Copy the views. By default all views are copied if not
                        limited by "--view <name>" "--view-filter <regepx>"!
                        (default: False)
  --view VIEWS [VIEWS ...]
                        Specify the views you want to copy. Either repeat "--
                        view <name> --view <name2>" or by "--view <name>
                        <name2>"
  --view-filter VIEW_FILTER
                        Filter view names using this regular expression
                        (regexp must match view names). (default: None)
  --view-filter-exclude VIEW_FILTER_EXCLUDE
                        Filter to exclude view names using this regular
                        expression (regexp must match view names). (default:
                        None)
  --copy-synonym, --no-copy-synonym
                        Copy the synonyms. By default all synonyms are copied
                        if not limited by "--synonym <name>" "--synonym-filter
                        <regexp>"! (default: False)
  --synonym SYNONYMS [SYNONYMS ...]
                        Specify the synonyms you want to copy. Either repeat "
                        --synonym <name> --synonym <name2>" or by "--synonym
                        <name> <name2>"
  --synonym-filter SYNONYM_FILTER
                        Filter synonym names using this regular expression
                        (regexp must match synonym names). (default: None)
  --synonym-filter-exclude SYNONYM_FILTER_EXCLUDE
                        Filter to exclude synonym names using this regular
                        expression (regexp must match synonym names).
                        (default: None)
  --debug-sql           If enabled, prints sql statements. (default: 0)
  --progress-track-file PROGRESS_FILE_NAME
                        If set, a file with the given name is used to remember
                        which tables/views it already processed sucessfully.
                        If the script is restarted, all tables/views are not
                        processed that were processed sucessfully before.".
                        (default: None)
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

Copy tables using a regular expression for the table names (include and exclude). Use azure databases with azure identity to login (```--target-authentication AzureActiveDirectory```). Also use a larger page size than the default 50000 rows:

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
    --table-filter-exclude "^TABLE_123$" \
    --page-size 100000 \
    --all-tables
```

If the copy process breaks, one can restart the copy and start from a given page using ```--page-start 123``` parameter.
The copy process prints which pages have been read and written, so one knows exactly how many rows were already copied and what is missing:

```
Copying table TABLE_A ... 921070 rows ... paging 19 pages each 50000 rows, page 1r(0.4s)1w(9.6s) 2r(0.3s)2w(4.1s) 3r(0.3s)3w(4.9s) 4r(0.3s)4w(4.4s) 5r(0.3s)5w(4.0s) 6r(0.3s)6w(3.8s) 7r(0.3s)7w(5.6s) 8r(0.4s)8w(5.0s) 9r(0.4s)9w(6.2s) 10r(0.4s)10w(4.8s) 11r(0.4s)11w(4.0s) 12r(0.4s)12w(7.1s) 13r(0.4s)13w(5.0s) 14r(0.4s)14w(in progress 15s) ETA 12:34:56 (0h 3m 10s)
```

This indicates that page 1 to 13 were read and written (```Nr```/```Nw``` including the seconds to read and write each page), and page 14 was read and is currently being written (the "in progress" ticker updates every 5 seconds, together with an ETA and estimated remaining duration computed from the completed pages). So if the process somehow dies at this moment, one could restart the copy process by using ```--page-start 14``` (the last page that wasn't confirmed as written).

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

### Auto-Resume an Interrupted Copy

Figuring out the correct ```--page-start``` by hand is tedious, especially when copying several tables at once. ```--auto-resume``` does this automatically for each table before it is copied:

* If the target table already has exactly the same number of rows as the source (query/where-clause/joins included), the table is assumed to already be fully copied and is skipped (a log message is printed).
* If the target table's row count is an exact multiple of ```--page-size```, copying automatically resumes at the correct page instead of starting over.

Just like ```--page-start```, whenever ```--auto-resume``` decides to resume (or skip) a table, that table is automatically NOT truncated, recreated, nor are its indices dropped/recreated - only the missing data is copied. This makes it safe to just re-run the exact same command after an interruption:

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
    --all-tables \
    --auto-resume
```

### Copy only some rows using a where clause

Copy only a selected set of rows using a where clause. To prevent a table and indices recreation (as only some rows should be added) use the additional params ```--no-create-table --no-drop-indices --no-copy-indices```.

The ```--delete-where``` ensures that the rows in the target table are deleted before the rows are copied from the source table using the where clause. Like this, the copy command can be executed repeatedly.

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
    --table User \
    --where "id >= 10000 and id < 20000" \
    --delete-where \
    --no-create-table --no-drop-indices --no-copy-indices
```

#### JOINing other tables

If the where clause needs other tables to determine which rows to read, one can use ```--join``` to join other tables and use columns from the joint tables in the where clause.
The alias of the table to be copied is always `source_table`, the alias of the join table can be freely chosen by you.

The following example will join the table `country` on the `country_id` with the `User` table that should be copied and limit the rows to copy to european users:

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
    --table User \
    --where "source_table.id >= 10000 and source_table.id < 20000 and country.CONTINENT = 'Europe'" \
    --delete-where \
    --join "COUNTRY country ON source_table.country_id = country.country_id"
    --no-create-table --no-drop-indices --no-copy-indices
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

### Compare Tables or Views / DB Schemata

Compare all/selected tables. This will show if there is a table missing, indices are missing or if the number of rows differs:

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
    --compare-table \
    --all-tables
```

Compare all/selected views. This will show if there is a view missing, column definitions are different or if the view definition is different:

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
    --compare-view \
    --view-filter "^VIEW_ABC\d*$"
```

### Track Progress

Using the progress tracker allows a command to be restarted without redoing all operations that were
done successfully before. So if you want to copy 10 tables and the network connection break after the 
fifth table, restaring the same operation using a progress track file will not copy the first four tables
but skip them and right start with the fifth table:

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
    --all-tables \
    --progress-track-file progress-dbo.track
```

The file ```progress-dbo.track``` will be created and every sucessfull copy step is logged there. On a restart
of the same command, the entries in the track file are checked if there were successfully executed before. In this 
case they will be skipped and continued with the next operation.

## Compare Table Content Row-by-Row

`mssql_compare_table.py` compares the actual row content of two tables that have the same columns/types but come
from different sources - e.g. the same table copied into different schemata, different databases, or even
different servers. It reports:

* rows that only exist in one of the two tables (based on a unique key), and
* rows that exist in both, but differ in one or more compared columns.

Rows are matched between the two tables using a key (one or more columns) - by default the primary key is
auto-detected (tried on table1 first, then table2), or you can set it explicitly with `--key-columns` if neither
table has one, or if you want to match rows on other criteria.

By default all columns common to both tables are compared; use `--compare-columns` to compare only specific
columns, and/or `--compare-columns-exclude` to leave out specific columns (e.g. an audit/timestamp column that is
expected to differ).

The script only reads data - it never modifies either table.

### Authentication and connections

Authentication works the same way as in `mssql_copy_table.py` / `mssql_execute_sql.py` (username/password or
`AzureActiveDirectory`, using `az login` beforehand).

If the two tables live in the same database, just specify the connection once (`--server`, `--db`,
`--authentication`, `--user`, `--password`) together with `--schema1`/`--schema2` and/or `--table1`/`--table2`. If
no `--server2`/`--db2`/... is given at all, a single connection is reused for both tables.

To compare tables in different databases or on different servers, add `--server2`, `--db2`, `--driver2`,
`--authentication2`, `--user2`, `--password2` for the second table - each independently defaults to the value of
the corresponding first-table option (except `--authentication2`, which defaults to `UsernamePassword` rather than
inheriting `AzureActiveDirectory`, since the second server is likely to need different credentials).

### Naming the tables to compare

* `--schema1`/`--table1` (aliases `--schema`/`--table`) are required.
* `--schema2` defaults to `--schema1` (compare the same schema across two databases/servers).
* `--table2` defaults to `--table1` (compare the same table name across two schemata/databases/servers).

This covers all combinations: same table name in different schemata, different table names in the same schema, or
any mix across different databases/servers.

### Basic example: same database, different schema

```bash
./mssql_compare_table.py \
    --server localhost \
    --db my-db \
    --user xxx \
    --password xxx \
    --schema1 AUT_DSL \
    --schema2 DSL \
    --table1 DH7OBJ \
    --compare-columns O_DV_KDNR
```

### Comparing across two different servers/databases

```bash
./mssql_compare_table.py \
    --server portal-int-cl1-rel-sqlserver.database.windows.net \
    --db portal-int-cl1-rel-liferay-db \
    --authentication AzureActiveDirectory \
    --server2 localhost \
    --db2 liferay-db \
    --user2 sa \
    --password2 xxx \
    --schema1 AUT_DSL \
    --schema2 DSL \
    --table1 DH7OBJ
```

### Output

By default a CSV is printed to stdout with columns `diff_type` (`only_in_table1`, `only_in_table2` or `different`),
`source` (identifying which table a row came from - only the server/db/schema/table parts that actually differ
between the two sides are shown) and the key/compared columns. Use `--output <file>` to write it to a file instead.

For a `different` row, two CSV rows are printed (one per table) so the differing values can be compared directly.
This flat format works, but with many differing columns it can be hard to see at a glance which cells actually
differ.

#### Side-by-side diff files for an editor compare view

Pass `--diff-dir <dir>` to additionally write two CSV files into that directory - one per table, named after the
parts of server/db/schema/table that differ between them (the table name is always included). Both files contain
one row per key value, sorted and aligned identically, with missing rows left blank. Open both files in an editor's
built-in compare view (VS Code: right-click a file -> "Select for Compare", then right-click the other -> "Compare
with Selected"; IntelliJ: select both files -> "Compare Files") to see row/cell differences highlighted directly -
much easier to read than the flat CSV.

```bash
./mssql_compare_table.py \
    --server localhost \
    --db my-db \
    --user xxx \
    --password xxx \
    --schema1 AUT_DSL \
    --schema2 DSL \
    --table1 DH7OBJ \
    --diff-dir ./diff-out
```

When `--diff-dir` is used without an explicit `--output`, the flat CSV (which would otherwise clutter stdout) is
skipped - only the diff files and a one-line summary are printed.

By default the diff files only contain rows that differ or are missing from one side. Use
`--diff-dir-include-unchanged` to also include matching rows, keeping the two files fully aligned for context.
Use `--diff-dir-exclude-missing` to go the other way and drop rows that only exist in one of the two tables,
keeping just the common rows that differ. `--max-diff-rows <n>` caps the number of rows reported per category
(only-in-table1, only-in-table2, differing), including in the diff files.

### Reducing noise in the comparison

* `--ignore-whitespace-start-end` ignores leading/trailing whitespace differences in string values.
* `--normalize-special-chars` replaces a fixed set of look-alike special characters (curly/low quotes such as
  `„`/`"`/`"` -> `"`, `'`/`'`/`‚` -> `'`, en/em dash -> `-`) before comparing, to ignore differences caused by
  values coming from different encodings/sources. Both flags also apply to the values written to `--diff-dir`
  files.

```bash
./mssql_compare_table.py \
    --server localhost \
    --db my-db \
    --user xxx \
    --password xxx \
    --schema1 AUT_DSL \
    --schema2 DSL \
    --table1 DH7OBJ \
    --ignore-whitespace-start-end \
    --normalize-special-chars
```

### Comparing only a subset of rows

Just like `mssql_copy_table.py`, `--where` and `--join` narrow down the rows read for comparison. The table being
read is aliased `source_table` in both the where clause and the joins:

```bash
./mssql_compare_table.py \
    --server localhost \
    --db my-db \
    --user xxx \
    --password xxx \
    --schema1 AUT_DSL \
    --schema2 DSL \
    --table1 DH7OBJ \
    --where "source_table.O_DV_KDNR >= 10000 and source_table.O_DV_KDNR < 20000"
```

## Reorder Table Columns

SQL Server has no built-in way to change the physical column order of a table. `mssql_reorder_columns.py` works around this
by generating a script that creates a new table with the desired column order, copies the data over, drops the original
table and renames the new one back to the original name. It also recreates primary key/unique constraints, check
constraints, foreign keys (both defined on the table and referencing it from other tables) and indexes.

The script only **prints** the generated SQL - it does not execute anything against the database. Review the output and
run it yourself (e.g. with `mssql_execute_sql.py` or any SQL client).

Note: triggers and permissions/grants on the table are not detected or recreated - add those back manually if needed.

### Example table

Given a table with a primary key, a default value, a unique index and a foreign key:

```sql
CREATE TABLE dbo.Country (
    country_id INT IDENTITY(1,1) NOT NULL,
    iso_code   CHAR(2) NOT NULL,
    CONSTRAINT PK_Country PRIMARY KEY CLUSTERED (country_id)
);

CREATE TABLE dbo.[User] (
    id          INT IDENTITY(1,1) NOT NULL,
    email       NVARCHAR(255) NOT NULL,
    first_name  NVARCHAR(100) NULL,
    last_name   NVARCHAR(100) NULL,
    country_id  INT NOT NULL,
    is_active   BIT NOT NULL DEFAULT 1,
    created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_User PRIMARY KEY CLUSTERED (id),
    CONSTRAINT UQ_User_Email UNIQUE NONCLUSTERED (email),
    CONSTRAINT FK_User_Country FOREIGN KEY (country_id) REFERENCES dbo.Country (country_id)
);

CREATE NONCLUSTERED INDEX IX_User_LastName_FirstName ON dbo.[User] (last_name, first_name);

INSERT INTO dbo.Country (iso_code) VALUES
    ('AT'),
    ('DE'),
    ('CH');

INSERT INTO dbo.[User] (email, first_name, last_name, country_id) VALUES
    ('alice@example.com', 'Alice', 'Adams', 1),
    ('bob@example.com',   'Bob',   'Brown', 2),
    ('carol@example.com', 'Carol', 'Clark', 3);
```

### Calling the script

To move `email` and `country_id` to the front of the column list (all other columns are kept and appended in their
original order):

```bash
./mssql_reorder_columns.py \
    --server localhost \
    --db my-db \
    --user xxx \
    --password xxx \
    --table dbo.User \
    email country_id id first_name last_name is_active created_at
```

This prints the current column order, a warning about any columns left out of `column_order` (they get appended at the
end), and the generated SQL, including steps to drop/recreate `FK_User_Country`, `PK_User`, `UQ_User_Email` and
`IX_User_LastName_FirstName` around the rebuild.

By default, info about the current table (columns with their defaults/identity/nullability, primary key/unique
constraints, indexes, check constraints and foreign keys in both directions) is printed (`--print-info`, default
`True`), the generated SQL is printed (`--print-sql`, default `True`), but nothing is executed against the database
(`--execute-sql`, default `False`). Each of the three can be toggled independently, e.g. `--no-print-info` to skip the
table info and only see the generated SQL, or add `--execute-sql` to run the generated statements directly:

```bash
./mssql_reorder_columns.py \
    --server localhost \
    --db my-db \
    --user xxx \
    --password xxx \
    --table dbo.User \
    --execute-sql \
    email country_id id first_name last_name is_active created_at
```
