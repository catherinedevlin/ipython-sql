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
