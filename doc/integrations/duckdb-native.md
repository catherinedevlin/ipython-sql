---
jupytext:
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.7
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: Use DuckDB from Jupyter using JupySQL
    keywords: jupyter, sql, jupysql, duckdb, plotting
    property=og:locale: en_US
---

# DuckDB (Native)

```{note}
JupySQL also supports DuckDB via SQLAlchemy, to learn more, see
[the tutorial](../integrations/duckdb.md). To learn the differences, [click here.](../tutorials/duckdb-native-sqlalchemy.md)
```

JupySQL integrates with DuckDB so you can run SQL queries in a Jupyter notebook. Jump into any section to learn more!

+++

## Pre-requisites for `.csv` file

```{code-cell} ipython3
%pip install jupysql duckdb --quiet
```

```{code-cell} ipython3
import duckdb

%load_ext sql
conn = duckdb.connect()
%sql conn --alias duckdb
```

### Load sample data

+++

Get a sample `.csv` file:

```{code-cell} ipython3
from urllib.request import urlretrieve

_ = urlretrieve(
    "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
    "penguins.csv",
)
```

### Query

+++

The data from the `.csv` file must first be registered as a table in order for the table to be listed.

```{code-cell} ipython3
%%sql
CREATE TABLE penguins AS SELECT * FROM penguins.csv
```

```{code-cell} ipython3
%%sql
SELECT *
FROM penguins.csv
LIMIT 3
```

```{code-cell} ipython3
%%sql
SELECT species, COUNT(*) AS count
FROM penguins.csv
GROUP BY species
ORDER BY count DESC
```

### Plotting

```{code-cell} ipython3
%%sql species_count <<
SELECT species, COUNT(*) AS count
FROM penguins.csv
GROUP BY species
ORDER BY count DESC
```

```{code-cell} ipython3
ax = species_count.bar()
# customize plot (this is a matplotlib Axes object)
_ = ax.set_title("Num of penguins by species")
```

## Pre-requisites for `.parquet` file

```{code-cell} ipython3
%pip install jupysql duckdb pyarrow --quiet
%load_ext sql
conn = duckdb.connect()
%sql conn --alias duckdb
```

### Load sample data

+++

Get a sample `.parquet` file:

```{code-cell} ipython3
from urllib.request import urlretrieve

_ = urlretrieve(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet",
    "yellow_tripdata_2021-01.parquet",
)
```

### Query

+++

Identically, to list the data from a `.parquet` file as a table, the data must first be registered as a table.

```{code-cell} ipython3
%%sql
CREATE TABLE tripdata AS SELECT * FROM "yellow_tripdata_2021-01.parquet"
```

```{code-cell} ipython3
%%sql
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
FROM "yellow_tripdata_2021-01.parquet"
LIMIT 3
```

```{code-cell} ipython3
%%sql
SELECT
    passenger_count, AVG(trip_distance) AS avg_trip_distance
FROM "yellow_tripdata_2021-01.parquet"
GROUP BY passenger_count
ORDER BY passenger_count ASC
```

### Plotting

```{code-cell} ipython3
%%sql avg_trip_distance <<
SELECT
    passenger_count, AVG(trip_distance) AS avg_trip_distance
FROM "yellow_tripdata_2021-01.parquet"
GROUP BY passenger_count
ORDER BY passenger_count ASC
```

```{code-cell} ipython3
ax = avg_trip_distance.plot()
# customize plot (this is a matplotlib Axes object)
_ = ax.set_title("Avg trip distance by num of passengers")
```

## Load sample data from a SQLite database

If you have a large SQlite database, you can use DuckDB to perform analytical queries it with much better performance.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
import urllib.request
from pathlib import Path

# download sample database
if not Path("my.db").is_file():
    url = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"  # noqa
    urllib.request.urlretrieve(url, "my.db")
```

We'll use `sqlite_scanner` extension to load a sample SQLite database into DuckDB:

```{code-cell} ipython3
import duckdb

conn = duckdb.connect()
%sql conn
```

```{code-cell} ipython3
%%sql
INSTALL 'sqlite_scanner';
LOAD 'sqlite_scanner';
CALL sqlite_attach('my.db');
```

```{code-cell} ipython3
%%sql
SELECT * FROM track LIMIT 5
```

## Plotting large datasets

```{versionadded} 0.5.2
```

This section demonstrates how we can efficiently plot large datasets with DuckDB and JupySQL without blowing up our machine's memory. `%sqlplot` performs all aggregations in DuckDB.

Let's install the required package:

```{code-cell} ipython3
%pip install jupysql duckdb pyarrow --quiet
```

Now, we download a sample data: NYC Taxi data split in 3 parquet files:

```{code-cell} ipython3
N_MONTHS = 3

# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
for i in range(1, N_MONTHS + 1):
    filename = f"yellow_tripdata_2021-{str(i).zfill(2)}.parquet"
    if not Path(filename).is_file():
        print(f"Downloading: {filename}")
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        urllib.request.urlretrieve(url, filename)
```

In total, this contains more then 4.6M observations:

```{code-cell} ipython3
%%sql
SELECT count(*) FROM 'yellow_tripdata_2021-*.parquet'
```

Let's use JupySQL to get a histogram of `trip_distance` across all 12 files:

```{code-cell} ipython3
%sqlplot histogram --table yellow_tripdata_2021-*.parquet --column trip_distance --bins 50
```

We have some outliers, let's find the 99th percentile:

```{code-cell} ipython3
%%sql
SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY trip_distance)
FROM 'yellow_tripdata_2021-*.parquet'
```

We now write a query to remove everything above that number:

```{code-cell} ipython3
%%sql --save no_outliers --no-execute
SELECT trip_distance
FROM 'yellow_tripdata_2021-*.parquet'
WHERE trip_distance < 18.93
```

```{code-cell} ipython3
%sqlplot histogram --table no_outliers --column trip_distance --bins 50
```

## Querying existing dataframes

```{code-cell} ipython3
import pandas as pd
import duckdb

conn = duckdb.connect()
df = pd.DataFrame({"x": range(10)})
```

```{code-cell} ipython3
%sql conn
```

```{code-cell} ipython3
%%sql
SELECT *
FROM df
WHERE x > 4
```
