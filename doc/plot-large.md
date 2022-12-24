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

# Plotting large datasets


```{tip}
[![](https://raw.githubusercontent.com/ploomber/ploomber/master/_static/open-in-jupyterlab.svg)](https://binder.ploomber.io/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fjupysql%26urlpath%3Dlab%252Ftree%252Fjupysql%252Fexamples%252Fplot.ipynb%26branch%3Dmaster)

Or try locally:

~~~
pip install k2s -U && k2s get ploomber/jupysql/master/examples/plot.ipynb
~~~

```

```{dropdown} Required packages
~~~
pip install jupysql memory_profiler
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

Please note that `jupysql` and `memory_profiler` need to be installed. You can install them with `pip install jupysql memory_profiler` from your terminal or `!pip install jupysql memory_profiler` from this notebook.

```{code-cell} ipython3
%load_ext autoreload
%autoreload 2

%load_ext sql
%load_ext memory_profiler
```

We'll use `sqlite_scanner` extension to load a sample SQLite database into DuckDB:

```{code-cell} ipython3
%%sql duckdb:///
INSTALL 'sqlite_scanner';
LOAD 'sqlite_scanner';
CALL sqlite_attach('my.db');
```

We'll be using a sample dataset that contains information on music tracks:

```{code-cell} ipython3
%%sql
SELECT * FROM "Track" LIMIT 3
```

The `Track` table contains 3503 rows:

```{code-cell} ipython3
%%sql
SELECT COUNT(*) FROM "Track"
```

Since we want to test plotting capabilities with a large database, we are going to create a new table by appending the original databse multiple times.

For this, we will proceed to create a SQL template script that we will use  along Python to create a database generator. The SQL template will perform a union between the database with itself 500 times, since the [maximum number of terms in a compound SELECT statement is **500**](https://www.sqlite.org/limits.html). in any case, this will generate a table with ~1.7 million rows, as we will see below.

```{code-cell} ipython3
%%writefile large-table-template.sql
DROP TABLE IF EXISTS "TrackAll";

CREATE TABLE "TrackAll" AS
    {% for _ in range(500) %}
        SELECT * FROM "Track"
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
;
```

Now, the following Python script will use the SQL template script to generate a now script to create a big table from the database.

```{code-cell} ipython3
%%writefile large-table-gen.py
from pathlib import Path
from jinja2 import Template

t = Template(Path('large-table-template.sql').read_text())
Path('large-table.sql').write_text(t.render())
```

We can now proceed to execute the Python generator. The Python script can be run using the magic command `%run <file.py>`. After it generates the SQL script, we execute it to create the new table. To execute the SQL script, we use the `--file` flag from from [JupySQL's options](https://jupysql.readthedocs.io/en/latest/options.html) along the `%sql` magic command.

```{code-cell} ipython3
%run large-table-gen.py
%sql --file large-table.sql
```

As we can see, the new table contains **~1.7 million rows**.

+++

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
