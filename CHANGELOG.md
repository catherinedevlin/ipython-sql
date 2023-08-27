# CHANGELOG

## 0.10.1dev

* [Feature] Automatically connect if the `dsn_filename` (defaults to `~/.jupysql/connections.ini`) contains a `default` section
* [Feature] Add `%sqlcmd connect` to see existing connections and create new ones (#632)
* [Fix] Clearer error messages when failing to initialize a connection
* [Doc] Added section on installing database drivers
* [Fix] Improve error when passing a non-identifier to start a connection (#764)
* [Fix] Display a warning (instead of raising an error) if the `default` connection in the `.ini` file cannot start
* [Fix] Display a message instead of an error when `toml` isn't installed and `pyproject.toml` is found (#825)
* [Fix] Fix argument parsing error on Windows when it contains quotations (#425)

## 0.10.0 (2023-08-19)

* [API Change] `%config SqlMagic.feedback` now takes values `0` (disabled), `1` (normal), `2` (verbose)
* [API Change] When loading connections from a `.ini` file via `%sql --section section_name`, the section name is set as the connection alias
* [API Change] Starting connections from a `.ini` file via `%sql [section_name]` has been deprecated
* [API Change] `%config SqlMagic.dsn_filename` default value changed from `odbc.ini` to `~/.jupysql/connections.ini`
* [Feature] Add `--binwidth/-W` to ggplot histogram for specifying binwidth ([#784](https://github.com/ploomber/jupysql/issues/784))
* [Feature] Add `%sqlcmd profile` support for DBAPI connections ([#743](https://github.com/ploomber/jupysql/issues/743))
* [Fix] Perform `ROLLBACK` when SQLAlchemy raises `PendingRollbackError`
* [Fix] Perform `ROLLBACK` when `psycopg2` raises `current transaction is aborted, commands ignored until end of transaction block`
* [Fix] Perform `ROLLBACK` when `psycopg2` raises `server closed the connection unexpectedly` ([#677](https://github.com/ploomber/jupysql/issues/677))
* [Fix] Fix a bug that caused a cell with a CTE to fail if it referenced a table/view with the same name as an existing snippet ([#753](https://github.com/ploomber/jupysql/issues/753))
* [Fix] Shorter `displaylimit` footer
* [Fix] `ResultSet` footer only displayed when `feedback=2`
* [Fix] Current connection and switching connections message only displayed when `feedback>=1`
* [Fix] `--persist/--persist-replace` perform `ROLLBACK` automatically when needed
* [Fix] `ResultSet` footer (when `displaylimit` truncates results and when showing how to convert to a data frame) now appears in the `ResultSet` plain text representation ([#682](https://github.com/ploomber/jupysql/issues/682))
* [Fix] Improve error when calling `%sqlcmd` ([#761](https://github.com/ploomber/jupysql/issues/761))
* [Fix] Fix count statement's result not displayed when `displaylimit=None` ([#801](https://github.com/ploomber/jupysql/issues/801))
* [Fix] Fix an error that caused a connection error message to be turned into a `print` statement
* [Fix] Fix Twice message printing when switching to the current connection ([#772](https://github.com/ploomber/jupysql/issues/772))
* [Fix] Error when using %sqlplot in snowflake ([#697](https://github.com/ploomber/jupysql/issues/697))
* [Doc] Fixes documentation inaccuracy that said `:variable` was deprecated (we brought it back in `0.9.0`)

## 0.9.1 (2023-08-10)

* [Feature] Added `--breaks/-B` to ggplot histogram for specifying breaks ([#719](https://github.com/ploomber/jupysql/issues/719))
* [Feature] Adds Redshift support for `%sqlplot boxplot`
* [Fix] Fix boxplot for duckdb native ([#728](https://github.com/ploomber/jupysql/issues/728))
* [Fix] Fix error when using SQL Server with pyodbc that caused queries to fail due to multiple open result sets
* [Fix] Improves performance when converting DuckDB results to `pandas.DataFrame`
* [Fix] Fixes a bug when converting a CTE stored with `--save` into a `pandas.DataFrame` via `.DataFrame()`
* [Doc] Add Redshift tutorial

## 0.9.0 (2023-08-01)

* [Feature] Allow loading configuration value from a `pyproject.toml` file upon magic initialization ([#689](https://github.com/ploomber/jupysql/issues/689))
* [Feature] Adds `with_` to `{SQLAlchemyConnection, DBAPIConnection}.raw_execute` to resolve CTEs
* [Feature] allows parametrizing queries with `:variable` with `%config SqlMagic.named_parameters = True`
* [Fix] Fix error that was incorrectly converted into a print message
* [Fix] Modified histogram query to ensure histogram binning is done correctly ([#751](https://github.com/ploomber/jupysql/issues/751))
* [Fix] Fix bug that caused the `COMMIT` not to work when the SQLAlchemy driver did not support `set_isolation_level`
* [Fix] Fixed vertical color breaks in histograms ([#702](https://github.com/ploomber/jupysql/issues/702))
* [Fix] Showing feedback when switching connections ([#727](https://github.com/ploomber/jupysql/issues/727))
* [Fix] Fix error that caused some connections not to be closed when calling `--close/-x`
* [Fix] Fix bug that caused the query transpilation process to fail when passing multiple statements
* [Fix] Fixes error when creating tables and querying them in the same cell when using DuckDB + SQLAlchemy ([#674](https://github.com/ploomber/jupysql/issues/674))
* [Fix] Using native methods to convert to data frames from DuckDB when using native connections and SQLAlchemy
* [Fix] Fix error that caused literals like `':something'` to be interpreted as query parameters

## 0.8.0 (2023-07-18)

* [Feature] Modified `TableDescription` to add styling, generate messages and format the calculated outputs ([#459](https://github.com/ploomber/jupysql/issues/459))
* [Feature] Support flexible spacing `myvar=<<` operator ([#525](https://github.com/ploomber/jupysql/issues/525))
* [Feature] Added a line under `ResultSet` to distinguish it from data frame and error message when invalid operations are performed ([#468](https://github.com/ploomber/jupysql/issues/468))
* [Feature] Moved `%sqlrender` feature to `%sqlcmd snippets` ([#647](https://github.com/ploomber/jupysql/issues/647))
* [Feature] Added tables listing stored snippets when `%sqlcmd snippets` is called ([#648](https://github.com/ploomber/jupysql/issues/648))
* [Feature] Better performance when using DuckDB native connection and converting to `pandas.DataFrame` or `polars.DataFrame`
* [Fix] Fixed CI issue by updating `invalid_connection_string_duckdb` in `test_magic.py` ([#631](https://github.com/ploomber/jupysql/issues/631))
* [Fix] Refactored `ResultSet` to lazy loading ([#470](https://github.com/ploomber/jupysql/issues/470))
* [Fix] Removed `WITH` when a snippet does not have a dependency ([#657](https://github.com/ploomber/jupysql/issues/657))
* [Fix] Used display module when generating CTE ([#649](https://github.com/ploomber/jupysql/issues/649))
* [Fix] Adding `--with` back because of issues with sqlglot query parser ([#684](https://github.com/ploomber/jupysql/issues/684))
* [Fix] Improving `<<` parsing logic ([#610](https://github.com/ploomber/jupysql/issues/610))
* [Fix] Migrate user feedback to use display module ([#548](https://github.com/ploomber/jupysql/issues/548))
* [Doc] Modified integrations content to ensure they're all consistent ([#523](https://github.com/ploomber/jupysql/issues/523))
* [Doc] Document `--persist-replace` in API section ([#539](https://github.com/ploomber/jupysql/issues/539))
* [Doc] Re-organized sections. Adds section showing how to share notebooks via Ploomber Cloud

## 0.7.9 (2023-06-19)

* [Feature] Modified `histogram` command to support data with NULL values ([#176](https://github.com/ploomber/jupysql/issues/176))
* [Feature] Automated dependency inference when creating CTEs. `--with` is now deprecated and will display a warning. ([#166](https://github.com/ploomber/jupysql/issues/166))
* [Feature] Close all connections when Python shuts down ([#563](https://github.com/ploomber/jupysql/issues/563))
* [Fix] Fixed `ResultSet` class to display result table with proper style and added relevant example ([#54](https://github.com/ploomber/jupysql/issues/54))
* [Fix] Fixed `Set` method in `Connection` class to recognize same descriptor with different aliases  ([#532](https://github.com/ploomber/jupysql/issues/532))
* [Fix] Added bottom-padding to the buttons in table explorer. Now they are not hidden by the scrollbar ([#540](https://github.com/ploomber/jupysql/issues/540))
* [Fix] `psutil` is no longer a dependency for JupySQL ([#541](https://github.com/ploomber/jupysql/issues/541))
* [Fix] Validating arguments passed to `%%sql` ([#561](https://github.com/ploomber/jupysql/issues/561))
* [Doc] Added bar and pie examples in the plotting section ([#564](https://github.com/ploomber/jupysql/issues/564))
* [Doc] Added more details to the SQL parametrization user guide. ([#288](https://github.com/ploomber/jupysql/issues/288))
* [Doc] Snowflake integration guide ([#384](https://github.com/ploomber/jupysql/issues/384))
* [Doc] User guide on using JupySQL in `.py` scripts ([#449](https://github.com/ploomber/jupysql/issues/449))
* [Doc] Added `%magic?` to APIs and quickstart ([#97](https://github.com/ploomber/jupysql/issues/97))

## 0.7.8 (2023-06-01)

* [Feature] Add `%sqlplot bar` and `%sqlplot pie` ([#508](https://github.com/ploomber/jupysql/issues/508))

## 0.7.7 (2023-05-31)

* [Feature] Clearer message display when executing queries, listing connections and persisting data frames ([#432](https://github.com/ploomber/jupysql/issues/432))
* [Feature] `%sql --connections` now displays an HTML table in Jupyter and a text-based table in the terminal
* [Fix] Fix CTE generation when the snippets have trailing semicolons
* [Doc] Hiding connection string when passing `--alias` when opening a connection ([#432](https://github.com/ploomber/jupysql/issues/432))
* [Doc] Fix `api/magic-sql.md` since it incorrectly stated that listing functions was `--list`, but it's `--connections` ([#432](https://github.com/ploomber/jupysql/issues/432))
* [Doc] Added Howto documentation for enabling JupyterLab cell runtime display ([#448](https://github.com/ploomber/jupysql/issues/448))

## 0.7.6 (2023-05-29)

* [Feature] Add `%sqlcmd explore` to explore tables interactively ([#330](https://github.com/ploomber/jupysql/issues/330))

* [Feature] Support for printing capture variables using `=<<` syntax (by [@jorisroovers](https://github.com/jorisroovers))

* [Feature] Adds `--persist-replace` argument to replace existing tables when persisting data frames ([#440](https://github.com/ploomber/jupysql/issues/440))

* [Fix] Fix error when checking if custom connection was PEP 249 Compliant ([#517](https://github.com/ploomber/jupysql/issues/517))

* [Doc] documenting how to manage connections with `Connection` object ([#282](https://github.com/ploomber/jupysql/issues/282))

* [Feature] Github Codespace (Devcontainer) support for development (by [@jorisroovers](https://github.com/jorisroovers)) ([#484](https://github.com/ploomber/jupysql/issues/484))

* [Feature] Added bar plot and pie charts to %sqlplot ([#417](https://github.com/ploomber/jupysql/issues/417))

## 0.7.5 (2023-05-24)

* [Feature] Using native DuckDB `.df()` method when using `autopandas`
* [Feature] Better error messages when function used in plotting API unsupported by DB driver ([#159](https://github.com/ploomber/jupysql/issues/159))
* [Feature] Detailed error messages when syntax error in SQL query, postgres connection password missing or inaccessible, invalid DuckDB connection string ([#229](https://github.com/ploomber/jupysql/issues/229))
* [Fix] Fix the default value of %config SqlMagic.displaylimit to 10 ([#462](https://github.com/ploomber/jupysql/issues/462))
* [Doc] documenting `%sqlcmd tables`/`%sqlcmd columns`

## 0.7.4 (2023-04-28)

No changes

## 0.7.3 (2023-04-28)

Never deployed due to a CI error

* [Fix] Fixing ipython version to 8.12.0 on python 3.8
* [Fix] Fix `--alias` when passing an existing engine
* [Doc] Tutorial on querying excel files with pandas and jupysql ([#423](https://github.com/ploomber/jupysql/pull/423))

## 0.7.2 (2023-04-25)

* [Feature] Support for DB API 2.0 drivers ([#350](https://github.com/ploomber/jupysql/issues/350))
* [Feature] Improve boxplot performance ([#152](https://github.com/ploomber/jupysql/issues/152))
* [Feature] Add sticky first column styling to sqlcmd profile command
* [Fix] Updates errors so only the error message is displayed (and traceback is hidden) ([#407](https://github.com/ploomber/jupysql/issues/407))
* [Fix] Fixes `%sqlcmd plot` when `--table` or `--column` have spaces ([#409](https://github.com/ploomber/jupysql/issues/409))
* [Doc] Add QuestDB tutorial ([#350](https://github.com/ploomber/jupysql/issues/350))

## 0.7.1 (2023-04-19)

* [Feature] Upgrades SQLAlchemy version to 2
* [Fix] Fix `%sqlcmd columns` in MySQL and MariaDB
* [Fix] `%sqlcmd --test` improved, changes in logic and addition of user guide ([#275](https://github.com/ploomber/jupysql/issues/275))
* [Doc] Algolia search added ([#64](https://github.com/ploomber/jupysql/issues/64))
* [Doc] Updating connecting guide (by [@DaveOkpare](https://github.com/DaveOkpare)) ([#56](https://github.com/ploomber/jupysql/issues/56))

## 0.7.0 (2023-04-05)

JupySQL is now available via `conda install jupysql -c conda-forge`. Thanks, [@sterlinm](https://github.com/sterlinm)!

* [API Change] Deprecates old SQL parametrization: `$var`, `:var`, and `{var}` in favor of `{{var}}`
* [Feature] Adds `%sqlcmd profile` ([#66](https://github.com/ploomber/jupysql/issues/66))
* [Feature] Adds `%sqlcmd test` to run tests on tables
* [Feature] Adds `--interact` argument to `%%sql` to enable interactivity in parametrized SQL queries ([#293](https://github.com/ploomber/jupysql/issues/293))
* [Feature] Results parse HTTP URLs to make them clickable ([#230](https://github.com/ploomber/jupysql/issues/230))
* [Feature] Adds `ggplot` plotting API (histogram and boxplot)
* [Feature] Adds `%%config SqlMagic.polars_dataframe_kwargs = {...}` (by [@jorisroovers](https://github.com/jorisroovers))
* [Feature] Adding `sqlglot` to better support SQL dialects in some internal SQL queries
* [Fix] Clearer error when using bad table/schema name with `%sqlcmd` and `%sqlplot` ([#155](https://github.com/ploomber/jupysql/issues/155))
* [Fix] Fix `%sqlcmd` exception handling ([#262](https://github.com/ploomber/jupysql/issues/262))
* [Fix] `--save` + `--with` double quotes syntax error in MySQL ([#145](https://github.com/ploomber/jupysql/issues/145))
* [Fix] Clearer error when using `--with` with snippets that do not exist ([#257](https://github.com/ploomber/jupysql/issues/257))
* [Fix] Pytds now automatically compatible
* [Fix] Jupysql with autopolars crashes when schema cannot be inferred from the first 100 rows (by [@jorisroovers](https://github.com/jorisroovers)) ([#312](https://github.com/ploomber/jupysql/issues/312))
* [Fix] Fix problem where a `%name` in a query (even if commented) would be interpreted as a query parameter ([#362](https://github.com/ploomber/jupysql/issues/362))
* [Fix] Better support for MySQL and MariaDB (generating internal SQL queries with backticks instead of double quotes)
* [Doc] Tutorial on ETLs via Jupysql and Github actions
* [Doc] SQL keywords autocompletion
* [Doc] Included schema and dataspec into `%sqlrender` API reference

## 0.6.6 (2023-03-16)

* [Fix] Pinning SQLAlchemy 1.x

## 0.6.5 (2023-03-15)

* [Feature] Displaying warning when passing a identifier with hyphens to `--save` or `--with`
* [Fix] Addresses enable AUTOCOMMIT config issue in PostgreSQL ([#90](https://github.com/ploomber/jupysql/issues/90))
* [Doc] User guide on querying Github API with DuckDB and JupySQL

## 0.6.4 (2023-03-12)

**Note:** This release has been yanked due to an error when using it with SQLAlchemy 2

* [Fix] Adds support for SQL Alchemy 2.0
* [Doc] Summary section on jupysql vs ipython-sql

## 0.6.3 (2023-03-06)

* [Fix] Displaying variable substitution warning only when the variable to expand exists in the user's namespace

## 0.6.2 (2023-03-05)

* [Fix] Deprecation warning incorrectly displayed [#213](https://github.com/ploomber/jupysql/issues/213)

## 0.6.1 (2023-03-02)

* [Feature] Support new variable substitution using `{{variable}}` format ([#137](https://github.com/ploomber/jupysql/pull/137))
* [Fix] Adds support for newer versions of prettytable

## 0.6.0 (2023-02-27)

* [API Change] Drops support for old versions of IPython (removed imports from `IPython.utils.traitlets`)
* [Feature] Adds `%%config SqlMagic.autopolars = True` ([#138](https://github.com/ploomber/jupysql/issues/138))

## 0.5.6 (2023-02-16)

* [Feature] Shows missing driver package suggestion message ([#124](https://github.com/ploomber/jupysql/issues/124))

## 0.5.5 (2023-02-08)

* [Fix] Clearer error message on connection failure ([#120](https://github.com/ploomber/jupysql/issues/120))
* [Doc] Adds tutorial on querying JSON data

## 0.5.4 (2023-02-06)

* [Feature] Adds `%jupysql`/`%%jupysql` as alias for `%sql`/`%%sql`
* [Fix] Adds community link to `ValueError` and `TypeError`

## 0.5.3 (2023-01-31)

* [Feature] Adds `%sqlcmd tables` ([#76](https://github.com/ploomber/jupysql/issues/76))
* [Feature] Adds `%sqlcmd columns` ([#76](https://github.com/ploomber/jupysql/issues/76))
* [Fix] `setup.py` fix due to change in setuptools 67.0.0

## 0.5.2 (2023-01-03)

* Adds example for connecting to a SQLite database with spaces ([#35](https://github.com/ploomber/jupysql/issues/35))
* Documents how to securely pass credentials ([#40](https://github.com/ploomber/jupysql/issues/40))
* Adds `-a/--alias` option to name connections for easier management ([#59](https://github.com/ploomber/jupysql/issues/59))
* Adds `%sqlplot` for plotting histograms and boxplots
* Adds missing documentation for the Python API
* Several improvements to the `sql.plot` module
* Removes `six` as dependency (drops Python 2 support)

## 0.5.1 (2022-12-26)

* Allow to connect to databases with an existing `sqlalchemy.engine.Engine` object

## 0.5 (2022-12-24)

* `ResultSet.plot()`, `ResultSet.bar()`, and `ResultSet.pie()` return `matplotlib.Axes` objects

## 0.4.7 (2022-12-23)

* Assigns a variable without displaying an output message ([#13](https://github.com/ploomber/jupysql/issues/13))

## 0.4.6 (2022-08-30)

* Updates telemetry key

## 0.4.5 (2022-08-13)

* Adds anonymous telemetry

## 0.4.4 (2022-08-06)

* Adds `plot` module (boxplot and histogram)

## 0.4.3 (2022-08-04)

* Adds `--save`, `--with`, and `%sqlrender` for SQL composition ([#1](https://github.com/ploomber/jupysql/issues/1))

## 0.4.2 (2022-07-26)

*First version release by Ploomber*

* Adds `--no-index` option to `--persist` data frames without the index

## 0.4.1

* Fixed .rst file location in MANIFEST.in
* Parse SQL comments in first line
* Bugfixes for DSN, `--close`, others

## 0.4.0

* Changed most non-SQL commands to argparse arguments (thanks pik)
* User can specify a creator for connections (thanks pik)
* Bogus pseudo-SQL command `PERSIST` removed, replaced with `--persist` arg
* Turn off echo of connection information with `displaycon` in config
* Consistent support for {} variables (thanks Lucas)

## 0.3.9

* Restored Python 2 compatibility (thanks tokenmathguy)
* Fix truth value of DataFrame error (thanks michael-erasmus)
* `<<` operator (thanks xiaochuanyu)
* added README example (thanks tanhuil)
* bugfix in executing column_local_vars (thanks tebeka)
* pgspecial installation optional (thanks jstoebel and arjoe)
* conceal passwords in connection strings (thanks jstoebel)

## 0.3.8

* Stop warnings for deprecated use of IPython 3 traitlets in IPython 4 (thanks graphaelli; also stonebig, aebrahim, mccahill)
* README update for keeping connection info private, from eshilts

## 0.3.7.1

* Avoid "connection busy" error for SQL Server (thanks Andrés Celis)

## 0.3.7

* New `column_local_vars` config option submitted by darikg
* Avoid contaminating user namespace from locals (thanks alope107)

## 0.3.6

* Fixed issue number 30, commit failures for sqlite (thanks stonebig, jandot)

## 0.3.5

* Indentations visible in HTML cells
* COMMIT each SQL statement immediately - prevent locks

## 0.3.4

* PERSIST pseudo-SQL command added

## 0.3.3

* Python 3 compatibility restored
* DSN access supported (thanks Berton Earnshaw)

## 0.3.2

* `.csv(filename=None)` method added to result sets

## 0.3.1

* Reporting of number of rows affected configurable with `feedback`

* Local variables usable as SQL bind variables

## 0.3.0

*Release date: 13-Oct-2013*

* displaylimit config parameter
* reports number of rows affected by each query
* test suite working again
* dict-style access for result sets by primary key

## 0.2.3

*Release date: 20-Sep-2013*

* Contributions from Olivier Le Thanh Duong:

  - SQL errors reported without internal IPython error stack

  - Proper handling of configuration


* Added .DataFrame(), .pie(), .plot(), and .bar() methods to
  result sets

## 0.2.2.1

*Release date: 01-Aug-2013*

Deleted Plugin import left behind in 0.2.2

## 0.2.2

*Release date: 30-July-2013*

Converted from an IPython Plugin to an Extension for 1.0 compatibility

## 0.2.1

*Release date: 15-June-2013*

* Recognize socket connection strings

* Bugfix - issue 4 (remember existing connections by case)

## 0.2.0

*Release date: 30-May-2013*

* Accept bind variables (Thanks Mike Wilson!)

## 0.1.2

*Release date: 29-Mar-2013*

* Python 3 compatibility

* use prettyprint package

* allow multiple SQL per cell

## 0.1.1

*Release date: 29-Mar-2013*

* Release to PyPI

* Results returned as lists

* print(_) to get table form in text console

* set autolimit and text wrap in configuration

## 0.1

*Release date: 21-Mar-2013*

* Initial release
