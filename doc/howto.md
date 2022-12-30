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

# How-To

## Query CSV files with SQL

You can use `JupySQL` and `DuckDB` to query CSV files with SQL in a Jupyter notebook.

+++

### Installation

```{code-cell} ipython3
%pip install jupysql duckdb duckdb-engine --quiet
```

### Setup

Load JupySQL:

```{code-cell} ipython3
%load_ext sql
```

Create an in-memory DuckDB database:

```{code-cell} ipython3
%sql duckdb://
```

Download some sample data:

```{code-cell} ipython3
from urllib.request import urlretrieve

_ = urlretrieve("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv", "penguins.csv")
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

## Register SQLite UDF

To register a user-defined function (UDF) when using SQLite, you can use [SQLAlchemy's `@event.listens_for`](https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#user-defined-functions) and SQLite's [`create_function`](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function):

### Install JupySQL

```{code-cell} ipython3
%pip install jupysql --quiet
```

### Create engine and register function

```{code-cell} ipython3
from sqlalchemy import create_engine
from sqlalchemy import event

def mysum(x, y):
    return x + y

engine = create_engine("sqlite://")

@event.listens_for(engine, "connect")
def connect(conn, rec):
    conn.create_function(name="MYSUM", narg=2, func=mysum)
```

### Create connection with existing engine

```{versionadded} 0.5.1
Pass existing engines to `%sql`
```

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%sql engine
```

## Query

```{code-cell} ipython3
%%sql
SELECT MYSUM(1, 2)
```
