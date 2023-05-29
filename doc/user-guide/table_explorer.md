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
    description lang=en: Templatize SQL queries in Jupyter via JupySQL
    keywords: jupyter, sql, jupysql, jinja
    property=og:locale: en_US
---

# Table Explorer


```{versionadded} 0.7.6
~~~
pip install jupysql --upgrade
~~~
```

In this guide, we demonstrate how to use JupySQL's table explorer to visualize SQL tables in HTML format and interact with them efficiently. By running SQL queries in the background instead of loading the data into memory, we minimize the resource consumption and processing time required for handling large datasets, making the interaction with the SQL tables faster and more streamlined.

```{note}
If you are using JupyterLab or Binder, please ensure that you have installed the latest version of the JupySQL plugin by running the following command: `pip install jupysql-plugin --upgrade`.
```

Let's start by preparing our dataset. We'll be using the [NYC taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Download the data

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

if not Path("yellow_tripdata_2021-01.parquet").is_file():
    urlretrieve(url, "yellow_tripdata_2021.parquet")
```

## Set connection

After our dataset is ready, we should set our connection.

For this demonstration, we'll be using the `DuckDB` connection.

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```

## Create the table

To create the table, use the `explore` attribute and specify the name of the table that was just downloaded.

```{code-cell} ipython3
:tags: []

%sqlcmd explore --table "yellow_tripdata_2021.parquet"
```


See interactive and live example on [Binder](https://binder.ploomber.io/v2/gh/ploomber/jupysql/master?urlpath=lab/tree/doc/user-guide/table_explorer.md).
