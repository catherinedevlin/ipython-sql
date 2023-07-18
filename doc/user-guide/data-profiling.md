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
---

# Data profiling


```{versionadded} 0.7
~~~
pip install jupysql --upgrade
~~~
```


When dealing with a new dataset, it's crucial for practitioners to have a comprehensive understanding of the data in a timely manner. This involves exploring and summarizing the dataset efficiently to extract valuable insights. However, this can be a time-consuming process. Fortunately, `%sqlcmd profile` offers an easy way to generate statistics and descriptive information, enabling practitioners to quickly gain a deeper understanding of the dataset.

Available statistics:

* The count of non empty values
* The number of unique values
* The top (most frequent) value
* The frequency of your top value
* The mean, standard deviation, min and max values
* The percentiles of your data: 25%, 50% and 75%.

## Examples

### DuckDB

In this example we'll demonstrate the process of profiling a sample dataset that contains historical taxi data from NYC, using DuckDB. However, the code used here is compatible with all major databases.

Download the data

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

if not Path("yellow_tripdata_2021-01.parquet").is_file():
    urlretrieve(url, "yellow_tripdata_2021-01.parquet")
```

Setup

```{note}
This example requires duckdb-engine: `pip install duckdb-engine`
```

Load the extension and connect to an in-memory DuckDB database:

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%sql duckdb://
```

```{code-cell} ipython3
%%sql
CREATE TABLE taxi_trips AS
SELECT * FROM 'yellow_tripdata_2021-01.parquet'
```

Profile table:

```{code-cell} ipython3
%sqlcmd profile --table taxi_trips
```

### Saving report as HTML

To save the generated report as an HTML file, use the `--output`/`-o` attribute followed by the desired file name

```{code-cell} ipython3
:tags: [hide-output]

%sqlcmd profile --table taxi_trips --output my-report.html
```

```{code-cell} ipython3
from IPython.display import HTML

HTML("my-report.html")
```

### Use schemas

To profile a specific table from various tables in different schemas, we can use the `--schema/-s` attribute.

```{code-cell} ipython3
:tags: [hide-output]

%%sql
CREATE SCHEMA some_schema
```

```{code-cell} ipython3
:tags: [hide-output]

%%sql
CREATE TABLE some_schema.trips AS
SELECT * FROM 'yellow_tripdata_2021-01.parquet'
```

Let's profile `my_numbers` of `b_schema`

```{code-cell} ipython3
%sqlcmd profile --table trips --schema some_schema
```
