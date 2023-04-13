---
jupytext:
  formats: md:myst
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.5
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: Connect to a SQL database from a Jupyter notebook
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# Connecting to a database

Learn how to connect to various databases using JupySQL.

## Connect with a URL string

Connection strings follow the [SQLAlchemy URL format](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls). This is the fastest way to connect to your database and the recommended way if you're using SQLite or DuckDB.

Database URLs have the following format:

```
dialect+driver://username:password@host:port/database
```


```{important}
If you're using a database that requires a password, keep reading for more secure methods.
```

+++

## Building URL strings securely

To connect in a more secure way, you can dynamically build your URL string so your password isn't hardcoded:

```python
from getpass import getpass

password = getpass()
```

When you execute the cell above in a notebook, a text box will appear and whatever you type will be stored in the `password` variable.

```{code-cell} ipython3
:tags: [remove-cell]

# this cell is hidden in the docs, only used to simulate
# the getpass() call
password = "mysupersecretpassword"
```

Then, you can build your connection string:

```{code-cell} ipython3
db_url = f"postgresql://user:{password}@localhost/database"
```

Create an engine and connect:

```{code-cell} ipython3
:tags: [remove-cell]

# this cell is hidden in the docs, only used to fake
# the db_url
db_url = "duckdb://"
```

```{code-cell} ipython3
from sqlalchemy import create_engine

engine = create_engine(db_url)
```

```{code-cell} ipython3
:tags: [remove-output]

%load_ext sql
```

```{code-cell} ipython3
%sql engine
```

+++ {"user_expressions": []}

```{important}
Unlike `ipython-sql`, JupySQL doesn't allow expanding your database URL with the `$` character:

~~~python
# this doesn't work in JupySQL!
db_url = "dialect+driver://username:password@host:port/database"
%sql $db_url
~~~
```

+++ {"user_expressions": []}

## Securely storing your password

If you want to store your password securely (and don't get prompted whenever you start a connection), you can use [keyring](https://github.com/jaraco/keyring):

```{code-cell} ipython3
:tags: [remove-output]

%pip install keyring --quiet
```

+++ {"user_expressions": []}

Execute the following in your notebook:

```python
import keyring

keyring.set_password("my_database", "my_username", "my_password")
```

+++ {"user_expressions": []}

Then, delete the cell above (so your password isn't hardcoded!). Now, you can retrieve your password with:

```python
from sqlalchemy import create_engine
import keyring

password = keyring.get_password("my_database", "my_username")
```

```{code-cell} ipython3
:tags: [remove-cell]

# this cell is hidden in the docs, only used to fake
# the password variable
password = "password"
```

```{code-cell} ipython3
db_url = f"postgresql://user:{password}@localhost/database"
```

```{code-cell} ipython3
:tags: [remove-cell]

# this cell is hidden in the docs, only used to fake
# the db_url
db_url = "duckdb://"
```

+++ {"user_expressions": []}

Create an engine and connect:

```{code-cell} ipython3
engine = create_engine(db_url)
```

```{code-cell} ipython3
:tags: [remove-output]

%load_ext sql
```

```{code-cell} ipython3
%sql engine
```

```{tip}
If you have issues using `keyring`, send us a message on [Slack.](https://ploomber.io/community)
```

+++

## Passing custom arguments to a URL

+++

Connection arguments not whitelisted by SQLALchemy can be provided with `--connection_arguments`. See [SQLAlchemy Args](https://docs.sqlalchemy.org/en/13/core/engines.html#custom-dbapi-args).

Here's an example using SQLite:

```{code-cell} ipython3
:tags: [remove-output]

%load_ext sql
```

```{code-cell} ipython3
%sql --connection_arguments '{"timeout":10}' sqlite://
```

## Connecting via an environment variable

+++

Set the `DATABASE_URL` environment variable, and `%sql` will automatically load it. You can do this either by setting the environment variable from your terminal or in your notebook:

```python
from getpass import getpass
from os import environ

password = getpass()
environ["DATABASE_URL"] = f"postgresql://user:{password}@localhost/database"
```

```{code-cell} ipython3
:tags: [remove-cell]

# this cell is hidden in the docs, only used to fake
# the environment variable
from os import environ
environ["DATABASE_URL"] = "sqlite://"
```

```{code-cell} ipython3
:tags: [remove-output]

%load_ext sql
```

```{code-cell} ipython3
%sql
```

## Using an existing `sqlalchemy.engine.Engine`

You can use an existing `Engine` by passing the variable name to `%sql`.

```{code-cell} ipython3
import pandas as pd
from sqlalchemy.engine import create_engine
```

```{code-cell} ipython3
engine = create_engine("sqlite://")
```

```{code-cell} ipython3
df = pd.DataFrame({"x": range(5)})
df.to_sql("numbers", engine)
```

```{code-cell} ipython3
:tags: [remove-output]

%load_ext sql
```

```{code-cell} ipython3
%sql engine
```

```{code-cell} ipython3
%%sql
SELECT * FROM numbers
```

+++ {"user_expressions": []}

## Using a URL object

If your URL has too many fields, rt's easier to build it with the `URL` object from SQLAlchemy:

```{code-cell} ipython3
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

connection_url = URL.create(
    "mssql+pyodbc",
    username="sa",
    password="MyPassword!",
    host="localhost",
    port=1433,
    database="master",
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": "yes",
    },
)

print(connection_url)
```

+++ {"user_expressions": []}

Once you have the URL, you can create an engine and connect:

```python
engine = create_engine(connection_url)
%sql engine
```

+++ {"user_expressions": []}

## Configuration file (DSN)

You can store connection information in a `odbc.ini` configuration file:

```
[MY_DB] 
drivername=postgresql
host=my.remote.host 
port=5433 
database=mydatabase 
username=myuser 
password=1234
```

```{important}
Leave your configuration file out of your git repository by adding it to the `.gitignore` file!
```

To connect to the database:

```
%sql --section MY_DB
```

+++ {"user_expressions": []}

If your configuration file has a different name:

```{code-cell} ipython3
%config SqlMagic.dsn_filename='./dsn.ini'
```

## Tutorials

Vendor-specific details are available in our tutorials:

- [PostgreSQL](integrations/postgres-connect)
- [ClickHouse](integrations/clickhouse)
- [MariaDB](integrations/mariadb)
- [MindsDB](integrations/mindsdb)
- [MSSQL](integrations/mssql)
- [MySQL](integrations/mysql)
