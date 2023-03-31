---
jupytext:
  notebook_metadata_filter: myst
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
    description lang=en: Documentation for the %sqlplot magic from JupySQL
    keywords: jupyter, sql, jupysql, plotting
    property=og:locale: en_US
---

# `%sqlplot`

```{versionadded} 0.5.2
```


```{note}
`%sqlplot` requires `matplotlib`: `pip install matplotlib` and this example requires
duckdb-engine: `pip install duckdb-engine`
```

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%sql duckdb://
```

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "penguins.csv",
    )
```

```{code-cell} ipython3
%%sql
SELECT * FROM "penguins.csv" LIMIT 3
```

## `%sqlplot boxplot`


```{note}
To use `%sqlplot boxplot`, your SQL engine must support:

`percentile_disc(...) WITHIN GROUP (ORDER BY ...)`

[Snowflake](https://docs.snowflake.com/en/sql-reference/functions/percentile_disc.html),
[Postgres](https://www.postgresql.org/docs/9.4/functions-aggregate.html),
[DuckDB](https://duckdb.org/docs/sql/aggregates), and others support this.
```

Shortcut: `%sqlplot box`

`-t`/`--table` Table to use (if using DuckDB: path to the file to query)

`-c`/`--column` Column(s) to plot. You might pass one than one value (e.g., `-c a b c`)

`-o`/`--orient` Boxplot orientation (`h` for horizontal, `v` for vertical)

`-w`/`--with` Use a previously saved query as input data

```{code-cell} ipython3
%sqlplot boxplot --table penguins.csv --column body_mass_g
```

### Transform data before plotting

```{code-cell} ipython3
%%sql
SELECT island, COUNT(*)
FROM penguins.csv
GROUP BY island
ORDER BY COUNT(*) DESC
```

```{code-cell} ipython3
%%sql --save biscoe --no-execute
SELECT *
FROM penguins.csv
WHERE island = 'Biscoe'
```

```{code-cell} ipython3
%sqlplot boxplot --table biscoe --column body_mass_g --with biscoe
```

### Horizontal boxplot

```{code-cell} ipython3
%sqlplot boxplot --table penguins.csv --column bill_length_mm --orient h
```

### Multiple columns

```{code-cell} ipython3
%sqlplot boxplot --table penguins.csv --column bill_length_mm bill_depth_mm flipper_length_mm
```

## `%sqlplot histogram`

Shortcut: `%sqlplot hist`

`-t`/`--table` Table to use (if using DuckDB: path to the file to query)

`-c`/`--column` Column to plot

`-b`/`--bins` (default: `50`) Number of bins

`-w`/`--with` Use a previously saved query as input data

+++

Histogram does not support NULL values, so let's remove them:

```{code-cell} ipython3
%%sql --save no_nulls --no-execute
SELECT *
FROM penguins.csv
WHERE body_mass_g IS NOT NULL
```

```{code-cell} ipython3
%sqlplot histogram --table no_nulls --column body_mass_g --with no_nulls
```

### Number of bins

```{code-cell} ipython3
%sqlplot histogram --table no_nulls --column body_mass_g --with no_nulls --bins 100
```

### Multiple columns

```{code-cell} ipython3
%sqlplot histogram --table no_nulls --column bill_length_mm bill_depth_mm --with no_nulls
```

## Customize plot

`%sqlplot` returns a `matplotlib.Axes` object.

```{code-cell} ipython3
ax = %sqlplot histogram --table no_nulls --column body_mass_g --with no_nulls
ax.set_title("Body mass (grams)")
_ = ax.grid()
```
