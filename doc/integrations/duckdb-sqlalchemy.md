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

# DuckDB (SQLAlchemy)

```{important}
Beginning in version `0.8.0`, we're now recommending to use DuckDB
[native connections](../integrations/duckdb.md). We'll still support SQLALchemy
connections but new features will primarily ship to native connections.
[Read more here.](../tutorials/duckdb-native-sqlalchemy.md)
```

JupySQL integrates with DuckDB so you can run SQL queries in a Jupyter notebook. Jump into any section to learn more!

+++

## Pre-requisites for `.csv` file

```{code-cell} ipython3
%pip install jupysql duckdb duckdb-engine --quiet
%load_ext sql
%sql duckdb://
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

The cell above allows the data to now be listed as a table from the following code:

```{code-cell} ipython3
%sqlcmd tables
```

List columns in the penguins table:

```{code-cell} ipython3
%sqlcmd columns -t penguins
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
%pip install jupysql duckdb duckdb-engine pyarrow --quiet
%load_ext sql
%sql duckdb://
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

The data is now able to be listed as a table from the following code:

```{code-cell} ipython3
%sqlcmd tables
```

List columns in the tripdata table:

```{code-cell} ipython3
%sqlcmd columns -t tripdata
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

## Plotting large datasets

```{versionadded} 0.5.2
```

This section demonstrates how we can efficiently plot large datasets with DuckDB and JupySQL without blowing up our machine's memory. `%sqlplot` performs all aggregations in DuckDB.

Let's install the required package:

```{code-cell} ipython3
%pip install jupysql duckdb duckdb-engine pyarrow --quiet
```

Now, we download a sample data: NYC Taxi data split in 3 parquet files:

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

N_MONTHS = 3

# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
for i in range(1, N_MONTHS + 1):
    filename = f"yellow_tripdata_2021-{str(i).zfill(2)}.parquet"
    if not Path(filename).is_file():
        print(f"Downloading: {filename}")
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        urlretrieve(url, filename)
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

```{code-cell} ipython3
%sqlplot boxplot --table no_outliers --column trip_distance
```

## Querying existing dataframes

```{code-cell} ipython3
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("duckdb:///:memory:")
df = pd.DataFrame({"x": range(100)})
```

```{code-cell} ipython3
%sql engine
```

```{code-cell} ipython3
%%sql
SELECT *
FROM df
WHERE x > 95
```

## Passing parameters to connection

```{code-cell} ipython3
from sqlalchemy import create_engine

some_engine = create_engine(
    "duckdb:///:memory:",
    connect_args={
        "preload_extensions": [],
    },
)
```

```{code-cell} ipython3
%sql some_engine
```
