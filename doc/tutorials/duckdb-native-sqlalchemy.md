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

Beginning in 0.9, JupySQL supports DuckDB via a native connection and SQLAlchemy, both with comparable performance.

At the moment, the only difference is that some features are only available when using SQLAlchemy.

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

As of version 0.9.0, the only caveat is that `%sqlcmd` won't work with a native connection.

```{code-cell} ipython3
---
editable: true
slideshow:
  slide_type: ''
tags: [raises-exception]
---
%sqlcmd
```
