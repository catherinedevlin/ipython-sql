---
jupytext:
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
    description lang=en: List tables and columns from your database in Jupyter via JupySQL
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# List tables and columns

```{note}
This example uses `SQLite` but the same commands work for other databases.
```

With JupySQL, you can quickly explore what tables are available in your database and which columns each table has.

+++

## Setup

```{code-cell} ipython3
%load_ext sql
%sql sqlite://
```

Let's create some sample tables in the default schema:

```{code-cell} ipython3
:tags: [hide-output]

%%sql
CREATE TABLE coordinates (x INT, y INT)
```

```{code-cell} ipython3
:tags: [hide-output]

%%sql
CREATE TABLE people (name TEXT, birth_year INT)
```

## List tables

+++

Use `%sqlcmd tables` to print the tables for the current connection:

```{code-cell} ipython3
%sqlcmd tables
```

Pass `--schema/-s` to get tables in a different schema:

```python
%sqlcmd tables --schema schema
```

+++

## List columns

Use `%sqlcmd columns --table/-t` to get the columns for the given table.

```{code-cell} ipython3
%sqlcmd columns --table coordinates
```

```{code-cell} ipython3
%sqlcmd columns -t people
```

If the table isn't in the defautl schema, pass `--schema/-s`. Let's create a new table in a new schema:

```{code-cell} ipython3
:tags: [hide-output]

import sqlite3

with sqlite3.connect("my.db") as conn:
    conn.execute("CREATE TABLE numbers (n FLOAT)")
```

```{code-cell} ipython3
:tags: [hide-output]

%%sql
ATTACH DATABASE 'my.db' AS some_schema
```

Get the columns for the table in the newly created schema:

```{code-cell} ipython3
%sqlcmd columns --table numbers --schema some_schema
```

## Run Tests on Column

Use `%sqlcmd test` to run tests on your dataset.

For example, to see if all the values in the column birth_year are greater than 100:

```{code-cell} ipython3
%sqlcmd test --table people --column birth_year --greater 100
```

Four different comparator commands exist: `greater`, `greater-or-equal`, `less-than`, `less-than-or-equal`, and `no-nulls`.

Command will return True if all tests pass, otherwise an error with sample breaking cases will be printed out.
