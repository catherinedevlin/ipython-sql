---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.4
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: Configure the %sql/%%sql magics in Jupyter
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# `%sql` Configuration

Query results are loaded as lists, so very large result sets may use up
your system's memory and/or hang your browser.  There is no autolimit
by default.  However, `autolimit` (if set) limits the size of the result
set (usually with a `LIMIT` clause in the SQL).  `displaylimit` is similar,
but the entire result set is still pulled into memory (for later analysis);
only the screen display is truncated.

+++

## Setup

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%sql sqlite://
```

```{code-cell} ipython3
%%sql
CREATE TABLE languages (name, rating, change);
INSERT INTO languages VALUES ('Python', 14.44, 2.48);
INSERT INTO languages VALUES ('C', 13.13, 1.50);
INSERT INTO languages VALUES ('Java', 11.59, 0.40);
INSERT INTO languages VALUES ('C++', 10.00, 1.98);
```

## Options

```{code-cell} ipython3
%config SqlMagic
```

```{note}
If you have autopandas set to true, the displaylimit option will not apply. You can set the pandas display limit by using the pandas `max_rows` option as described in the [pandas documentation](http://pandas.pydata.org/pandas-docs/version/0.18.1/options.html#frequently-used-options).
```

+++

## Changing configuration

```{code-cell} ipython3
%config SqlMagic.feedback = False
```

## `displaycon`

Default: `True`

Show connection string after execution.

```{code-cell} ipython3
%config SqlMagic.displaycon = True
%sql SELECT * FROM languages LIMIT 2
```

```{code-cell} ipython3
%config SqlMagic.displaycon = False
%sql SELECT * FROM languages LIMIT 2
```

## `autolimit`

Default: `0` (no limit)

Automatically limit the size of the returned result sets (e.g., add a `LIMIT` at the end of the query).

```{code-cell} ipython3
%config SqlMagic.autolimit = 0
%sql SELECT * FROM languages
```

```{code-cell} ipython3
%config SqlMagic.autolimit = 1
%sql SELECT * FROM languages
```

```{code-cell} ipython3
%config SqlMagic.autolimit = 0
```

## `displaylimit`

Default: `None` (no limit)

Automatically limit the number of rows displayed (full result set is still stored).

```{code-cell} ipython3
%config SqlMagic.displaylimit = None
%sql SELECT * FROM languages
```

```{code-cell} ipython3
%config SqlMagic.displaylimit = 1
res = %sql SELECT * FROM languages
res
```

One displayed, but all results fetched:

```{code-cell} ipython3
len(res)
```

## `autopandas`

Default: `False`

Return Pandas DataFrames instead of regular result sets.

```{code-cell} ipython3
%config SqlMagic.autopandas = False
res = %sql SELECT * FROM languages
type(res)
```

```{code-cell} ipython3
%config SqlMagic.autopandas = True
df = %sql SELECT * FROM languages
type(df)
```

## `autopolars`

Default: `False`

Return Polars DataFrames instead of regular result sets.

```{code-cell} ipython3
%config SqlMagic.autopolars = False
res = %sql SELECT * FROM languages
type(res)
```

```{code-cell} ipython3
%config SqlMagic.autopolars = True
df = %sql SELECT * FROM languages
type(df)
```

## `feedback`

Default: `True`

Print number of rows affected by DML.

```{code-cell} ipython3
%config SqlMagic.feedback = True
```

```{code-cell} ipython3
%%sql
CREATE TABLE points (x, y);
INSERT INTO points VALUES (0, 0);
INSERT INTO points VALUES (1, 1);
```

```{code-cell} ipython3
%config SqlMagic.feedback = False
```

```{code-cell} ipython3
%%sql
CREATE TABLE more_points (x, y);
INSERT INTO more_points VALUES (0, 0);
INSERT INTO more_points VALUES (1, 1);
```

## PostgreSQL features

`psql`-style "backslash" [meta-commands](https://www.postgresql.org/docs/9.6/static/app-psql.html#APP-PSQL-META-COMMANDS) commands (``\d``, ``\dt``, etc.)
are provided by [PGSpecial](https://pypi.python.org/pypi/pgspecial).  Example:

```python
%sql \d
```

```{code-cell} ipython3

```
