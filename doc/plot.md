---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Plotting large datasets


```{tip}
[![](https://raw.githubusercontent.com/ploomber/ploomber/master/_static/open-in-jupyterlab.svg)](https://binder.ploomber.io/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fjupysql%26urlpath%3Dlab%252Ftree%252Fjupysql%252Fexamples%252Fplot.ipynb%26branch%3Dmaster)

Or try locally:

~~~
pip install k2s -U && k2s get ploomber/jupysql/master/examples/plot.ipynb
~~~

```


*New in version 0.4.4*

```{note}
This is a beta feature, please [join our community](https://ploomber.io/community) and let us know what plots we should add next!
```

Using libraries like `matplotlib` or `seaborn`, requires fetching all the data locally, which quickly can fill up the memory in your machine. JupySQL runs computations in the warehouse/database to drastically reduce memory usage and runtime.

+++

As an example, we are using a sales database from a record store. We’ll find the artists that have produced the largest number of Rock and Metal songs.

Let’s load some data:

```{code-cell} ipython3
import urllib.request
from pathlib import Path
from sqlite3 import connect

if not Path('my.db').is_file():
    url = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
    urllib.request.urlretrieve(url, 'my.db')
```

Now, let's initialize the extension so we only retrieve a few rows.

Please note that `jupysql` and `memory_profiler` need o be installed.

```{code-cell} ipython3
%load_ext autoreload
%autoreload 2

%load_ext sql
%load_ext memory_profiler
```

We'll be using a sample dataset that contains information on music tracks:

```{code-cell} ipython3
%%sql sqlite:///my.db
SELECT * FROM "Track" LIMIT 3
```

The `Track` table contains 3503 rows:

```{code-cell} ipython3
%%sql
SELECT COUNT(*) FROM "Track"
```

## Boxplot

```{note}
To use `plot.boxplot`, your SQL engine must support:

`percentile_disc(...) WITHIN GROUP (ORDER BY ...)`

[Snowflake](https://docs.snowflake.com/en/sql-reference/functions/percentile_disc.html),
[Postgres](https://www.postgresql.org/docs/9.4/functions-aggregate.html),
[DuckDB](https://duckdb.org/docs/sql/aggregates), and others support this.
```

```{code-cell} ipython3
from sql import plot
import matplotlib.pyplot as plt
plt.style.use('seaborn')
```

```{code-cell} ipython3
%%memit
plot.boxplot('Track', 'Milliseconds')
```

Note that the plot consumes only a few MiB of memory (increment), since most of the processing happens in the SQL engine. Furthermore, you'll also see big performance improvements if using a warehouse like Snowflake, Redshift or BigQuery, since they can process large amounts of data efficiently.

+++

## Histogram

```{code-cell} ipython3
%%memit
plot.histogram('Track', 'Milliseconds', bins=50)
```

## Benchmark

For comparison, let's see what happens if we compute locally:

```{code-cell} ipython3
from IPython import get_ipython

def fetch_data():
    """
    Only needed to enable %%memit, this is the same as doing
    res = %sql SELECT "Milliseconds" FROM "Track"
    """
    ip = get_ipython()
    return ip.run_line_magic('sql', 'SELECT "Milliseconds" FROM "Track"')
```

Fetching data consumes a lot of memory:

```{code-cell} ipython3
%%memit
res = fetch_data()
```

Plotting functions also increase memory usage:

```{code-cell} ipython3
%%memit
_ = plt.boxplot(res.DataFrame().Milliseconds)
```

```{code-cell} ipython3
%%memit
_ = plt.hist(res.DataFrame().Milliseconds, bins=50)
```

The memory consumption is a lot higher!
