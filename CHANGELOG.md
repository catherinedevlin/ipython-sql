# CHANGELOG

## 0.7.0dev
* [Feature] Adds `%sqlcmd profile` (#66)
* [API Change] Deprecates old SQL parametrization: `$var`, `:var`, and `{var}` in favor of `{{var}}`
* [Fix] `--save` + `--with` double quotes syntax error in MySQL ([#145](https://github.com/ploomber/jupysql/issues/145))
* [Feature] Adds sql magic test to list of possible magics to test datasets
* [Feature] Adds `--interact` argument to `%%sql` to enable interactivity in parametrized SQL queries (#293)
* [Feature] Results parse HTTP URLs to make them clickable (#230)
* [Feature] Adds `ggplot` plotting API (histogram and boxplot)
* [Fix] Clearer error when using `--with` with snippets that do not exist (#257)
* [Fix] Pytds now automatically compatible
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
