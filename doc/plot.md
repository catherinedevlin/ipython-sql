---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Plotting large datasets

*New in version 0.4.4*

```{note}
This is a beta feature, please [join our community](https://ploomber.io/community) and let us know what plots we should add next!
```

Using libraries like `matplotlib` or `seaborn`, requires fetching all the data locally, which quickly can fill up the memory in your machine. JupySQL runs computations in the warehouse/database to drastically reduce memory usage and runtime.

```{code-cell} ipython3
%load_ext autoreload
%autoreload 2

%load_ext sql
%load_ext memory_profiler
```

We'll be using a sample dataset that contains information on music tracks:

```{code-cell} ipython3
%%sql
SELECT * FROM "TrackAll" LIMIT 2
```

The `TrackAll` table contains 2.9 million rows:

```{code-cell} ipython3
%%sql
SELECT COUNT(*) FROM "TrackAll"
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
```

```{code-cell} ipython3
%%memit
plot.boxplot('TrackAll', 'Milliseconds')
```

Note that the plot consumes only a few MiB of memory (increment), since most of the processing happens in the SQL engine. Furthermore, you'll also see big performance improvements if using a warehouse like Snowflake, Redshift or BigQuery, since they can process large amounts of data efficiently.

+++

## Histogram

```{code-cell} ipython3
%%memit
plot.histogram('TrackAll', 'Milliseconds', bins=50)
```

## Benchmark

For comparison, let's see what happens if we compute locally:

```{code-cell} ipython3
from IPython import get_ipython

def fetch_data():
    """
    Only needed to enable %%memit, this is the same as doing
    res = %sql SELECT "Milliseconds" FROM "TrackAll"
    """
    ip = get_ipython()
    return ip.run_line_magic('sql', 'SELECT "Milliseconds" FROM "TrackAll"')
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
