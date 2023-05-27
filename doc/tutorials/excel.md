---
jupytext:
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
    description lang=en: Read Excel files using Jupysql and query on it
    keywords: jupyter, sql, jupysql, excel, xlsx
    property=og:locale: en_US
---

# Loading and Querying Excel Files

In this tutorial, we will be using small financial data stored in an Excel file containing over 700 records. The dataset is publicly available [here](https://go.microsoft.com/fwlink/?LinkID=521962). We will use the `read_excel` function from the pandas library to read the Excel file and store it in the database using the `%sql --persist` command of jupysql, which works across multiple databases. For additional compatibility between different databases and jupysql, please check out this [page](../integrations/compatibility.md).

```{note}
DuckDB doesn't support reading excel files. Their `excel` [extension](https://duckdb.org/docs/extensions/overview) provides excel like formatting.
```


```{note}
For this tutorial, we aim to showcase the versatility of jupysql as a framework by using `--persist`. However, DuckDB natively supports Pandas DataFrame and you do not need to use `--persist`. With DuckDB, complex queries such as aggregations and joins can run more efficiently on the DataFrame compared to Pandas native functions. You can refer to this [blog](https://duckdb.org/2021/05/14/sql-on-pandas.html) for a detailed comparison (Note: the comparison is based on Pandas v1.\*, not the recently released Pandas v2.\*, which uses PyArrow as a backend). 
```

Installing dependencies:

```{code-cell} ipython3
---
:tags: [hide-output]
---

%pip install jupysql duckdb duckdb-engine pandas openpyxl --quiet
```

Reading dataframe using `pandas.read_excel`:

```{code-cell} ipython3
import pandas as pd

df = pd.read_excel("https://go.microsoft.com/fwlink/?LinkID=521962")
```

Initializing jupysql and connecting to `duckdb` database

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```

Persisting the dataframe in duckdb database. It is stored in table named `df`.

```{code-cell} ipython3
# If you are using DuckDB, you can omit this cell
%sql --persist df
```

## Running some standard queries
- Selecting first 3 queries

```{code-cell} ipython3
%%sql 
SELECT *
FROM df
LIMIT 3
```

- Countries in the database

```{code-cell} ipython3
%%sql 
SELECT DISTINCT Country
FROM df
```

- Evaluating total profit country-wise and ordering them in desceding order according to profit.

```{code-cell} ipython3
%%sql
select Country, SUM(Profit) Total_Profit
from df
group by Country
order by Total_Profit DESC
```
