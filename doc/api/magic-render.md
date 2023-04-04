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
    description lang=en: Documentation for the %sqlrender magic from JupySQL
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# `%sqlrender`

```{versionadded} 0.4.3
```

`%sqlrender` helps you compose large SQL queries.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%sql sqlite:///
```

```{code-cell} ipython3
import pandas as pd

url = (
    "https://gist.githubusercontent.com/jaidevd"
    "/23aef12e9bf56c618c41/raw/c05e98672b8d52fa0"
    "cb94aad80f75eb78342e5d4/books.csv"
)
books = pd.read_csv(url)
```

```{code-cell} ipython3
%sql --persist books
```

```{code-cell} ipython3
%sql SELECT * FROM books LIMIT 5
```

## `%sqlrender`

`-w`/`--with` Use a previously saved query as input data

```{code-cell} ipython3
%%sql --save books_fav --no-execute
SELECT *
FROM books
WHERE genre = 'data_science'
```

```{code-cell} ipython3
%%sql --save books_fav_long --no-execute --with books_fav
SELECT * FROM books_fav
WHERE Height >= 240
```

```{code-cell} ipython3
query = %sqlrender books_fav_long --with books_fav_long
print(query)
```

```{code-cell} ipython3

```
