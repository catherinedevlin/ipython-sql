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
%%sql --save writers_fav --no-execute
SELECT *
FROM authors
WHERE genre = 'non-fiction'
```

```{code-cell} ipython3
%%sql --save writers_fav_modern --no-execute --with writers_fav
SELECT * FROM writers_fav
WHERE born >= 1970
```

```{code-cell} ipython3
query = %sqlrender writers_fav_modern --with writers_fav_modern
print(query)
```
