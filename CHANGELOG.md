# CHANGELOG

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

* Fixed issue [#30](https://github.com/ploomber/jupysql/issues/30), commit failures for sqlite (thanks stonebig, jandot)

## 0.3.5

* Indentations visible in HTML cells
* COMMIT each SQL statement immediately - prevent locks



## 0.3.4

* PERSIST pseudo-SQL command added


## 0.3.3

* Python 3 compatibility restored
* DSN access supported (thanks Berton Earnshaw)


## 0.3.2

* ``.csv(filename=None)`` method added to result sets


## 0.3.1

* Reporting of number of rows affected configurable with ``feedback``

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

