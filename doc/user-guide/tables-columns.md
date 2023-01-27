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

Let's create some sample tables:

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

## List tables

+++

Use `%sqlcmd tables` to print the tables for the current connection:

```{code-cell} ipython3
%sqlcmd tables
```

## List columns

+++

User `%sqlcmd columns --table/-t` to get the columns for the given table:

```{code-cell} ipython3
%sqlcmd columns --table coordinates
```

```{code-cell} ipython3
%sqlcmd columns -t people
```

If the table isn't in the defautl schema, pass `--schema/-s`:

```{code-cell} ipython3
%sqlcmd columns --table numbers --schema some_schema
```
