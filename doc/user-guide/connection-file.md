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
    description lang=en: Using a connection file
    keywords: jupyter, jupysql, sqlalchemy
    property=og:locale: en_US
---

# Using a connection file

Using a connection file is the recommended way to manage connections, it helps you to:

- Avoid storing your credentials in your notebook
- Manage multiple database connections
- Define them in a single place to use it in all your notebooks

```{code-cell} ipython3
%load_ext sql
```

By default, connections are read/stored in a `odbc.ini` file:

```{code-cell} ipython3
%config SqlMagic.dsn_filename
```

However, you can change this:

```{code-cell} ipython3
%config SqlMagic.dsn_filename = "my-connections.ini"
```

The `.ini` format defines sections and you can define key-value pairs within each section. For example:

```ini
[section_name]
key = value
```

Add a section and set the key-value pairs to add a new connection. When JupySQL loads them, it'll initialize a [`sqlalchemy.engine.URL`](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.engine.URL.create) object and then start the connection. Valid keys are:

- `drivername`: the name of the database backend
- `username`: the username
- `password`: database password
- `host`: name of the host
- `port`: the port number
- `database`: the database name
- `query`: a dictionary of string keys to be passed to the connection upon connect (learn more [here](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.engine.URL.create))

For example, to configure an in-memory DuckDB database:

```ini
[duck]
drivername = duckdb
```

```{code-cell} ipython3
from pathlib import Path

_ = Path("my-connections.ini").write_text(
    """
[duck]
drivername = duckdb
"""
)
```

To connect to a database defined in the connections file, use `--section` and pass the section name:

```{code-cell} ipython3
%sql --section duck
```

```{versionchanged} 0.10.0
The connection alias is automatically set when using `%sql --section`
```

Note that the alias is set to the section name:

```{code-cell} ipython3
%sql --connections
```

```{versionchanged} 0.10.0
Loading connections from the `.ini` (`%sql [section_name]`) file has been deprecated. Use `%sql --section section_name` instead.
```

```{code-cell} ipython3
from urllib.request import urlretrieve
from pathlib import Path

url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"

if not Path("penguins.csv").exists():
    urlretrieve(url, "penguins.csv")
```

```{code-cell} ipython3
%%sql
drop table if exists penguins;

create table penguins as
select * from penguins.csv
```

```{code-cell} ipython3
%%sql
select * from penguins
```

Let's now define another connection so we can show how we can manage multiple ones:

```{code-cell} ipython3
_ = Path("my-connections.ini").write_text(
    """
[duck]
drivername = duckdb

[second_duck]
drivername = duckdb
"""
)
```

Start a new connection from the `second_duck` section name:

```{code-cell} ipython3
%sql --section second_duck
```

```{code-cell} ipython3
%sql --connections
```

There are no tables since this is a new database:

```{code-cell} ipython3
%sqlcmd tables
```

If we switch to the first connection (by passing the alias), we'll see the table:

```{code-cell} ipython3
%sql duck
```

```{code-cell} ipython3
%sqlcmd tables
```

We can change back to the other connection:

```{code-cell} ipython3
%sql second_duck
```

```{code-cell} ipython3
%sqlcmd tables
```
