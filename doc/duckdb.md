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

# DuckDB integration


JupySQL integrates with DuckDB so you can run SQL queries in a Jupyter notebook. Jump into any section to learn more!

+++

## Querying a `.csv` file

### Installation and setup

```{code-cell} ipython3
%pip install jupysql duckdb duckdb-engine --quiet
%load_ext sql
%sql duckdb://
```

Get a sample `.csv.` file:

```{code-cell} ipython3
from urllib.request import urlretrieve

_ = urlretrieve("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
                "penguins.csv")
```

### Query

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

### Plot

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

## Querying a `.parquet` file

### Installation and setup

```{code-cell} ipython3
%pip install jupysql duckdb duckdb-engine pyarrow --quiet
%load_ext sql
%sql duckdb://
```

Download sample `.parquet` file:

```{code-cell} ipython3
from urllib.request import urlretrieve

_ = urlretrieve("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet",
                "yellow_tripdata_2021-01.parquet")
```

### Query

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

### Plot

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

## Reading from a SQLite database

If you have a large SQlite database, you can use DuckDB to perform analytical queries it with much better performance.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
import urllib.request
from pathlib import Path
from sqlite3 import connect

# download sample database
if not Path('my.db').is_file():
    url = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
    urllib.request.urlretrieve(url, 'my.db')
```

We'll use `sqlite_scanner` extension to load a sample SQLite database into DuckDB:

```{code-cell} ipython3
%%sql duckdb:///
INSTALL 'sqlite_scanner';
LOAD 'sqlite_scanner';
CALL sqlite_attach('my.db');
```

```{code-cell} ipython3
%%sql
SELECT * FROM track LIMIT 5
```

## Plotting large datasets

*New in version 0.4.4*

```{note}
This is a beta feature, please [join our community](https://ploomber.io/community) and let us know what plots we should add next!
```


This section demonstrates how we can efficiently plot large datasets with DuckDB and JupySQL without blowing up our machine's memory.

Let's install the required package:

```{code-cell} ipython3
%pip install jupysql duckdb duckdb-engine pyarrow --quiet
```

Now, we download a sample data: NYC Taxi data splitted in 3 parquet files:

```{code-cell} ipython3
N_MONTHS = 3

# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
for i in range(1, N_MONTHS + 1):
    filename = f'yellow_tripdata_2021-{str(i).zfill(2)}.parquet'
    if not Path(filename).is_file():
        print(f'Downloading: {filename}')
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
        urllib.request.urlretrieve(url, filename)
```

In total, this contains more then 4.6M observations:

```{code-cell} ipython3
%%sql
SELECT count(*) FROM 'yellow_tripdata_2021-*.parquet'
```

Now, let's keep track of how much  memory this Python session is using:

```{code-cell} ipython3
import psutil
import os

def memory_usage():
    """Print how much memory we're using
    """
    process = psutil.Process(os.getpid())
    total = process.memory_info().rss / 10 ** 9
    print(f'Using: {total:.1f} GB')
```

```{code-cell} ipython3
memory_usage()
```

```{code-cell} ipython3
from sql import plot
```

Let's use JupySQL to get a histogram of `trip_distance` across all 12 files:

```{code-cell} ipython3
plot.histogram('yellow_tripdata_2021-*.parquet', 'trip_distance', bins=50)
```

We have some outliers, let's find the 99th percentile:

```{code-cell} ipython3
%%sql
SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY trip_distance),
FROM 'yellow_tripdata_2021-*.parquet'
```

We now write a query to remove everything above that number:

```{code-cell} ipython3
%%sql --save no_outliers --no-execute
SELECT trip_distance
FROM 'yellow_tripdata_2021-*.parquet'
WHERE trip_distance < 18.93
```

Now we create a new histogram:

```{code-cell} ipython3
plot.histogram('no_outliers', 'trip_distance', bins=50, with_=['no_outliers'])
```

```{code-cell} ipython3
memory_usage()
```

We see that memory usage increase just a bit.

+++

### Benchmark: Using pandas

We now repeat the same process using pandas.

```{code-cell} ipython3
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet
```

Data loading:

```{code-cell} ipython3
tables = []

# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
for i in range(1, N_MONTHS):
    filename = f'yellow_tripdata_2021-{str(i).zfill(2)}.parquet'
    t = pyarrow.parquet.read_table(filename)
    tables.append(t)

table = pyarrow.concat_tables(tables)
df = pyarrow.concat_tables(tables).to_pandas()
```

First histogram:

```{code-cell} ipython3
_ = plt.hist(df.trip_distance, bins=50)
```

```{code-cell} ipython3
cutoff = df.trip_distance.quantile(.99)
cutoff
```

```{code-cell} ipython3
subset = df.trip_distance[df.trip_distance < cutoff]
```

```{code-cell} ipython3
_ = plt.hist(subset, bins=50)
```

```{code-cell} ipython3
memory_usage()
```

**We're using 1.6GB of memory just by loading the data with pandas!**

Try re-running the notebook with the full 12 months (change `N_MONTHS` to `12` in the earlier cell), and you'll see that memory usage blows up to 8GB.

Even deleting the dataframes does not completely free up the memory ([explanation here](https://stackoverflow.com/a/39377643/709975)):

```{code-cell} ipython3
del df, subset
```

```{code-cell} ipython3
memory_usage()
```
