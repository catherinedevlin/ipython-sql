---
jupytext:
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.5
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: Documentation for the %sqlcmd tables and %sqlcmd columns
      from JupySQL
    keywords: jupyter, sql, jupysql, tables, columns
    property=og:locale: en_US
---

# `%sqlcmd tables`/`%sqlcmd columns`

`%sqlcmd tables` returns the current table names saved in environments.

`%sqlcmd columns` returns the column information in a specified table.

## Load Data

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "penguins.csv",
    )
```

```{code-cell} ipython3
%%sql
SELECT * FROM penguins.csv LIMIT 3
```

Let's save the file penguins.csv as a table penguins.

```{code-cell} ipython3
:tags: [hide-output]

%%sql 
DROP TABLE IF EXISTS penguins;

CREATE TABLE penguins (
    species VARCHAR(255),
    island VARCHAR(255),
    bill_length_mm DECIMAL(5, 2),
    bill_depth_mm DECIMAL(5, 2),
    flipper_length_mm DECIMAL(5, 2),
    body_mass_g INTEGER,
    sex VARCHAR(255)
);

COPY penguins FROM 'penguins.csv' WITH (FORMAT CSV, HEADER TRUE);
```

## `%sqlcmd tables`

+++

Returns the current table names saved in environments.

```{code-cell} ipython3
%sqlcmd tables
```

Arguments:

`-s`/`--schema` Get all table names under this schema 

To show the usage of schema, let's put two tables under two schema.
In this code example, we create schema s1 and s2. We put **t1** under schema s1, **t2** under schema s2

```{code-cell} ipython3
:tags: [hide-output]

%%sql
CREATE SCHEMA IF NOT EXISTS s1;
CREATE SCHEMA IF NOT EXISTS s2;
CREATE TABLE s1.t1(id INTEGER PRIMARY KEY, other_id INTEGER);
CREATE TABLE s2.t2(id INTEGER PRIMARY KEY, j VARCHAR);
```

```{code-cell} ipython3
:tags: [hide-output]

%sqlcmd tables -s s1
```

As expected, the argument returns the table names under schema s1, which is t1.

+++

## `%sqlcmd columns`

+++

Arguments:

`-t/--table` (Required) Get the column features of a specified table. 

`-s/--schema` (Optional) Get the column features of a table under a schema

```{code-cell} ipython3
%sqlcmd columns -t penguins
```

```{code-cell} ipython3

%sqlcmd columns -s s1 -t t1
```
