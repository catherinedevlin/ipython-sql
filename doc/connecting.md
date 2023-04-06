---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.4
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: "Connect to a SQL database from a Jupyter notebook"
    keywords: "jupyter, sql, jupysql"
    property=og:locale: "en_US"
---

# Connecting to Databases with SQL Magic

Learn how to connect to various databases using SQL Magic in this tutorial. SQL Magic is a Jupyter Notebook extension that allows you to execute SQL queries directly in your notebook cells. We'll show you how to establish connections, connect securely, and use existing `sqlalchemy.engine.Engine` instances.

## Establishing a connection

Connection strings follow the SQLAlchemy URL standard. Here are some example connection strings for various databases:

Some example connection strings:

```
mysql+pymysql://scott:tiger@localhost/foo
oracle://scott:tiger@127.0.0.1:1521/sidname
sqlite://
sqlite:///foo.db
mssql+pyodbc://username:password@host/database?driver=SQL+Server+Native+Client+11.0
```

**Note:** `mysql` and `mysql+pymysql` connections (and perhaps others) don't read your client character set information from `.my.cnf.` You need to specify it in the connection string:

```
mysql+pymysql://scott:tiger@localhost/foo?charset=utf8
```

For an `impala` connection with [`impyla`](https://github.com/cloudera/impyla) for HiveServer2, you need to disable autocommit:

```
%config SqlMagic.autocommit=False
%sql impala://hserverhost:port/default?kerberos_service_name=hive&auth_mechanism=GSSAPI
```

Connection arguments not whitelisted by SQLALchemy can be provided as
a flag with (-a|--connection_arguments)the connection string as a JSON string. See [SQLAlchemy Args](https://docs.sqlalchemy.org/en/13/core/engines.html#custom-dbapi-args)


```
%sql --connection_arguments {"timeout":10,"mode":"ro"} sqlite:// SELECT * FROM work;
%sql -a '{"timeout":10, "mode":"ro"}' sqlite:// SELECT * from work;
```

+++

## Connecting to Specifit Databases

Check out our guide for connecting to a database:

- [PostgreSQL](integrations/postgres-connect)
- [ClickHouse](integrations/clickhouse)
- [MariaDB](integrations/mariadb)
- [MindsDB](integrations/mindsdb)
- [MSSQL](integrations/mssql)
- [MySQL](integrations/mysql)

+++

## Connecting securely

**It is highly recommended** that you do not pass plain credentials.

```{code-cell} ipython3
%load_ext sql
```

### Building connection strings with `getpass`

One option is to use `getpass`, type your password, build your connection string and pass it to `%sql`:

+++

```python
from getpass import getpass

password = getpass()
connection_string = f"postgresql://user:{password}@localhost/database"
%sql $connection_string
```

+++

### Using `DATABASE_URL`

+++

You can also set the `DATABASE_URL` environment variable, and `%sql` will automatically load it from there. You can do it either by setting the environment variable from your terminal or in your notebook:

```python
from getpass import getpass
from os import environ

password = getpass()
environ["DATABASE_URL"] = f"postgresql://user:{password}@localhost/database"
```

```python
# without any args, %sql reads from DATABASE_URL
%sql
```

+++

## Using DSN connections

Alternately, you can store connection info in a configuration file, under a section name chosen to  refer to your database.

For example, if `dsn.ini` contains:

```
[DB_CONFIG_1] 
drivername=postgres 
host=my.remote.host 
port=5433 
database=mydatabase 
username=myuser 
password=1234
```

then you can:

```
%config SqlMagic.dsn_filename='./dsn.ini'
%sql --section DB_CONFIG_1 
```

+++

## Using an existing `sqlalchemy.engine.Engine`

```{versionadded} 0.5.1
```

Use an existing `Engine` by passing the variable name to `%sql`.

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
%load_ext sql
```

```{code-cell} ipython3
%sql engine
```

```{code-cell} ipython3
%%sql
SELECT * FROM numbers
```
