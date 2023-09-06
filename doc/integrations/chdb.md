---
jupytext:
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
    description lang=en: Use chDB from Jupyter using JupySQL
    keywords: jupyter, sql, jupysql, chDB
    property=og:locale: en_US
---

# chDB

JupySQL integrates with chDB so you can run SQL queries in a Jupyter notebook. Jump into any section to learn more!

+++

## Pre-requisites for `.parquet` file

```{code-cell} ipython3
%pip install jupysql chdb pyarrow --quiet
```

```{code-cell} ipython3
from chdb import dbapi

conn = dbapi.connect()

%load_ext sql
%sql conn --alias chdb
```

### Get a sample `.parquet` file:

```{code-cell} ipython3
from urllib.request import urlretrieve

_ = urlretrieve(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet",
    "yellow_tripdata_2021-01.parquet",
)
```

### Query on S3/HTTP/File

+++

Query a local file

```{code-cell} ipython3
%%sql
SELECT
    passenger_count, AVG(trip_distance) AS avg_trip_distance
FROM file("yellow_tripdata_2021-01.parquet")
GROUP BY passenger_count
```

Run a file over HTTP

```{code-cell} ipython3
%%sql
SELECT
    RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
FROM url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet')
-- query on s3 --
--  FROM s3('xxxx')
GROUP BY
    RegionID
ORDER BY c
DESC LIMIT 10
```
