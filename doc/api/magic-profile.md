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
    description lang=en: Documentation for the %sqlcmd profile from JupySQL
    keywords: jupyter, sql, jupysql, profile
    property=og:locale: en_US
---

# `%sqlcmd profile` 

`%sqlcmd profile` allows you to obtain summary statistics of a table quickly. The code used here is compatible with all major databases.

```{note}
You can view the documentation and command line arguments by running `%sqlcmd?`
```

Arguments:

`-t`/`--table` (Required) Get the profile of a table

`-s`/`--schema` (Optional) Get the profile of a table under a specified schema

`-o`/`--output` (Optional) Output the profile at a specified location (path name expected)

```{note}
This example requires duckdb-engine: `pip install duckdb-engine`
```

## Load CSV Data with DuckDB

Load the extension and connect to an in-memory DuckDB database:

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```

Load and download `penguins.csv` dataset , using DuckDB.

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
SELECT * FROM "penguins.csv" LIMIT 3
```

## Load Parquet Data with DuckDB

Load and download a sample dataset that contains historical taxi data from NYC, using DuckDB.

```{code-cell} ipython3
import os
from pathlib import Path
from urllib.request import urlretrieve

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
new_filename = "yellow_tripdata_2021.parquet"

if not Path(new_filename).is_file():
    urlretrieve(url, new_filename)
    # Rename the downloaded file to remove the month ("-" interferes with the SQL query)
    os.rename(new_filename, new_filename.replace("-01", ""))
```

```{code-cell} ipython3
%%sql
SELECT * FROM yellow_tripdata_2021.parquet LIMIT 3
```

# Profile 

Let us profile the `penguins.csv` data

```{code-cell} ipython3
%sqlcmd profile --table "penguins.csv"
```

Let us profile the `yellow_tripdata_2021.parquet` data

```{code-cell} ipython3
%sqlcmd profile --table "yellow_tripdata_2021.parquet"
```

# Saving report as HTML

To save the generated report as an HTML file, use the `--output/-o` attribute followed by the desired file name.

To save the profile of the `penguins.csv` data as an HTML file:

```{code-cell} ipython3
:tags: [hide-output]

%sqlcmd profile --table "penguins.csv" --output penguins-report.html
```

```{code-cell} ipython3
from IPython.display import HTML

HTML("penguins-report.html")
```

To save the profile of the `yellow_tripdata_2021.parquet` data as an HTML file:

```{code-cell} ipython3
:tags: [hide-output]

%sqlcmd profile --table "yellow_tripdata_2021.parquet" --output taxi-report.html
```

```{code-cell} ipython3
from IPython.display import HTML

HTML("taxi-report.html")
```

# Use schemas with DuckDB

To profile a specific table from various tables in different schemas, we can use the `--schema/-s` attribute.

Let's save the file penguins.csv as a table `penguins` under the schema `s1`.

```{code-cell} ipython3
%%sql 
DROP TABLE IF EXISTS penguins;
CREATE SCHEMA IF NOT EXISTS s1;
CREATE TABLE s1.penguins (
    species VARCHAR(255),
    island VARCHAR(255),
    bill_length_mm DECIMAL(5, 2),
    bill_depth_mm DECIMAL(5, 2),
    flipper_length_mm DECIMAL(5, 2),
    body_mass_g INTEGER,
    sex VARCHAR(255)
);
COPY s1.penguins FROM 'penguins.csv' WITH (FORMAT CSV, HEADER TRUE);
```

```{code-cell} ipython3
%sqlcmd profile --table penguins --schema s1 
```

# Use schemas with SQLite

```{code-cell} ipython3
%%sql duckdb:///
INSTALL 'sqlite_scanner';
LOAD 'sqlite_scanner';
```

```{code-cell} ipython3
import sqlite3

with sqlite3.connect("a.db") as conn:
    conn.execute("CREATE TABLE my_numbers (number FLOAT)")
    conn.execute("INSERT INTO my_numbers VALUES (1)")
    conn.execute("INSERT INTO my_numbers VALUES (2)")
    conn.execute("INSERT INTO my_numbers VALUES (3)")
```

```{code-cell} ipython3
%%sql
ATTACH DATABASE 'a.db' AS a_schema
```

```{code-cell} ipython3
import sqlite3

with sqlite3.connect("b.db") as conn:
    conn.execute("CREATE TABLE my_numbers (number FLOAT)")
    conn.execute("INSERT INTO my_numbers VALUES (11)")
    conn.execute("INSERT INTO my_numbers VALUES (22)")
    conn.execute("INSERT INTO my_numbers VALUES (33)")
```

```{code-cell} ipython3
%%sql
ATTACH DATABASE 'b.db' AS b_schema
```

Let’s profile `my_numbers` of `b_schema`

```{code-cell} ipython3
%sqlcmd profile --table my_numbers --schema b_schema
```