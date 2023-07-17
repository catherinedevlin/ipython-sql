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
    description lang=en: JupySQL and DuckDB with SQLAlchemy vs native connection
    keywords: jupyter, jupysql, duckdb, sqlalchemy
    property=og:locale: en_US
---

# DuckDB (native vs SQLAlchemy)

```{admonition} TL;DR
If using DuckDB, use a native connection. Only use SQLAlchemy for legacy projects
```


Historically, `ipython-sql` has only supported databases via SQLAlchemy. Using SQLAlchemy introduces a big overhead, when converting results to data frames. We attempted to fix this by allowing users to open a connection with SQLAlchemy while still leveraging DuckDB's highly performant capabilities to convert results into data frames; however, we encountered many edge cases, and ultimately decided to deprecate this behavior.

In consequence, DuckDB connections made via SQLAlchemy suffer from the performance problem, but native connections do not. So we're now recommending users to connect to DuckDB via a native connection, this is possible since JupySQL introduced support for generic [DBAPI 2.0](https://peps.python.org/pep-0249/) drivers in version 0.7.2.

+++

## Performance comparison (pandas)

### DuckDB + SQLALchemy

```{code-cell} ipython3
import pandas as pd

df = pd.DataFrame({"x": range(1_000_000)})
```

```{code-cell} ipython3
%load_ext sql
%config SqlMagic.autopandas = True
%sql duckdb:// --alias duckdb-sqlalchemy
```

```{code-cell} ipython3
%%time
_ = %sql SELECT * FROM df
```

## DuckDB + native

```{code-cell} ipython3
import duckdb

conn = duckdb.connect()
%sql conn --alias duckdb-native
```

```{code-cell} ipython3
%%time
_ = %sql SELECT * FROM df
```

## Performance comparison (polars)

```{code-cell} ipython3
%config SqlMagic.autopolars = True
%sql duckdb-sqlalchemy
```

### DuckDB + SQLAlchemy

```{code-cell} ipython3
%%time
_ = %sql SELECT * FROM df
```

### DuckDB + native

```{code-cell} ipython3
%sql duckdb-native
```

```{code-cell} ipython3
%%time
_ = %sql SELECT * FROM df
```

## Limitations of using native connections

As of version 0.7.10, the only caveat is that `%sqlcmd` and `%sqlplot boxplot` won't work with a native connection.

```{code-cell} ipython3
---
editable: true
slideshow:
  slide_type: ''
tags: [raises-exception]
---
%sqlcmd
```

```{code-cell} ipython3
---
editable: true
slideshow:
  slide_type: ''
tags: [raises-exception]
---
%sqlplot boxplot --table df --column x
```

## Suppress warnings

When converting large datasets using SQLALchemy, you'll see a warning:

```{code-cell} ipython3
%sql duckdb-sqlalchemy
_ = %sql SELECT * FROM df
```

To suppress it, add this at the top of your notebook/script:

```{code-cell} ipython3
from sql.warnings import JupySQLDataFramePerformanceWarning
import warnings
warnings.filterwarnings("ignore", category=JupySQLDataFramePerformanceWarning)
```

```{code-cell} ipython3
%sql duckdb-sqlalchemy
_ = %sql SELECT * FROM df
```
