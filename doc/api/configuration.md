---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.15.1
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

If you are concerned about query performance, please use the `autolimit` config.

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
%config SqlMagic.feedback = 0
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

Default: `10`

Automatically limit the number of rows displayed (full result set is still stored).

(To display all rows: set to `0` or `None`)

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

## `dsn_filename`

```{versionchanged} 0.10.0
`dsn_filename` default changed from `odbc.ini` to `~/.jupysql/connections.ini`.
```

Default: `~/.jupysql/connections.ini`

File to load connections configuration from. For an example, see: [](../user-guide/connection-file.md)

+++

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

## `polars_dataframe_kwargs`

Default: `{}`

Polars [DataFrame constructor](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html) keyword arguments (e.g. infer_schema_length, nan_to_null, schema_overrides, etc)

```{code-cell} ipython3
# By default Polars will only look at the first 100 rows to infer schema
# Disable this limit by setting infer_schema_length to None
%config SqlMagic.polars_dataframe_kwargs = { "infer_schema_length": None}

# Create a table with 101 rows, last row has a string which will cause the
# column type to be inferred as a string (rather than crashing polars)
%sql CREATE TABLE points (x, y);
insert_stmt = ""
for _ in range(100):
    insert_stmt += "INSERT INTO points VALUES (1, 2);"
%sql {{insert_stmt}}
%sql INSERT INTO points VALUES (1, "foo");


%sql SELECT * FROM points
```

To unset:

```{code-cell} ipython3
%config SqlMagic.polars_dataframe_kwargs = {}
```

## `feedback`

```{versionchanged} 0.10
`feedback` takes values `0`, `1`, and `2` instead of `True`/`False`
```

Default: `1`

Control the quantity of messages displayed when performing certain operations. Each
value enables the ones from previous values plus new ones:

- `0`: Minimal feedback
- `1`: Normal feedback (default)
  - Connection name when switching
  - Connection name when running a query
  - Number of rows afffected by DML (e.g., `INSERT`, `UPDATE`, `DELETE`)
- `2`: All feedback
  - Footer to distinguish pandas/polars data frames from JupySQL's result sets

## `style`

DEFAULT: `DEFAULT`

Set the table printing style to any of prettytable's defined styles

```{code-cell} ipython3
%config SqlMagic.style = "MSWORD_FRIENDLY"
res = %sql SELECT * FROM languages LIMIT 2
print(res)
```

```{code-cell} ipython3
%config SqlMagic.style = "SINGLE_BORDER"
res = %sql SELECT * FROM languages LIMIT 2
print(res)
```

## `named_parameters`

```{versionadded} 0.9
```

Default: `False`

If True, it enables named parameters `:variable`. Learn more in the [tutorial.](../user-guide/template.md)

```{code-cell} ipython3
%config SqlMagic.named_parameters=True
```

```{code-cell} ipython3
rating = 12
```

```{code-cell} ipython3
%%sql
SELECT *
FROM languages
WHERE rating > :rating
```

## Loading from `pyproject.toml`

```{versionadded} 0.9
```

You can define configurations in a `pyproject.toml` file and automatically load the configurations when you run `%load_ext sql`. If the file is not found in the current or parent directories, default values will be used. A sample `pyproject.toml` could look like this:

```
[tool.jupysql.SqlMagic]
feedback = true
autopandas = true
```

Note that `pyproject.toml` is only for setting configurations. To store connection details, please use [`connections.ini`](../user-guide/connection-file.md) file.
