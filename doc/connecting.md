---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
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

# Connecting

Connection strings are [SQLAlchemy URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls) standard.

Some example connection strings:

```
mysql+pymysql://scott:tiger@localhost/foo
oracle://scott:tiger@127.0.0.1:1521/sidname
sqlite://
sqlite:///foo.db
mssql+pyodbc://username:password@host/database?driver=SQL+Server+Native+Client+11.0
```

Note that `mysql` and `mysql+pymysql` connections (and perhaps others)
don't read your client character set information from .my.cnf.  You need
to specify it in the connection string::

```
mysql+pymysql://scott:tiger@localhost/foo?charset=utf8
```

Note that an `impala` connection with [`impyla`](https://github.com/cloudera/impyla) for HiveServer2 requires disabling autocommit::

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

## DSN connections

Alternately, you can store connection info in a configuration file, under a section name chosen to  refer to your database.

For example, if dsn.ini contains:

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

```{code-cell} ipython3

```
