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
    description lang=en: "Quickstart for JupySQL: a package to run SQL in Jupyter"
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# Quick Start

JupySQL allows you to run SQL and plot large datasets in Jupyter via a `%sql`, `%%sql`, and `%sqlplot` magics. JupySQL is compatible with all major databases (e.g., PostgreSQL, MySQL, SQL Server), data warehouses (e.g., Snowflake, BigQuery, Redshift), and embedded engines (SQLite, and DuckDB).

It is a fork of `ipython-sql` with many bug fixes and a lot of great new features!

+++

## Installation

Run this on your terminal (we'll use DuckDB for this example):

```sh
pip install jupysql duckdb-engine
```

Or the following in a Jupyter notebook:

```{code-cell} ipython3
%pip install jupysql duckdb-engine --quiet
```

## Setup

Load the extension:

```{code-cell} ipython3
%load_ext sql
```

Let's download some sample `.csv` data:

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "penguins.csv",
    )
```

Start a DuckDB in-memory database:

```{code-cell} ipython3
%sql duckdb://
```

```{tip}
You can create as many connections as you want. Pass an `--alias {alias}` to easily
[switch them or close](howto.md#switch-connections) them.
```

## Querying

For short queries, you can write them in a single line via the `%sql` line magic:

```{code-cell} ipython3
%sql SELECT * FROM penguins.csv LIMIT 3
```

For longer queries, you can break them down into multiple lines using the `%%sql` cell magic:

```{code-cell} ipython3
%%sql
SELECT *
FROM penguins.csv
WHERE bill_length_mm > 40
LIMIT 3
```

## Saving queries

```{code-cell} ipython3
%%sql --save not-nulls --no-execute
SELECT *
FROM penguins.csv
WHERE bill_length_mm IS NOT NULL
AND bill_depth_mm IS NOT NULL
```

## Plotting

```{code-cell} ipython3
%sqlplot boxplot --column bill_length_mm bill_depth_mm --table not-nulls --with not-nulls
```

```{code-cell} ipython3
%sqlplot histogram --column bill_length_mm bill_depth_mm --table not-nulls --with not-nulls
```

## `pandas` integration

```{code-cell} ipython3
result = %sql SELECT * FROM penguins.csv
```

```{code-cell} ipython3
df = result.DataFrame()
```

```{code-cell} ipython3
df.head()
```
