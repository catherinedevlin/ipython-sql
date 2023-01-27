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

# Exploring the database

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%sql sqlite://
```

```{code-cell} ipython3
%%sql
CREATE TABLE test (n INT, name TEXT)
```

```{code-cell} ipython3
%%sql
CREATE TABLE another (n INT, name TEXT)
```

```{code-cell} ipython3
from sql import inspect

inspect.get_table_names()
```

```{code-cell} ipython3
inspect.get_columns("another")
```
