---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.5
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Template

## Variable Expansion as `{{variable}}`

We support the variable expansion in the form of `{{variable}}`, this also allows the user to write the query as template with some dynamic variables

```{code-cell} ipython3
:tags: [remove-cell]

%load_ext sql
from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "penguins.csv",
    )
%sql duckdb://
```

Now, let's give a simple query template and define some variables where we will apply in the template:

```{code-cell} ipython3
dynamic_limit = 5
dynamic_column = "island, sex"
```

```{code-cell} ipython3
%sql SELECT {{dynamic_column}} FROM penguins.csv LIMIT {{dynamic_limit}}
```

Note that variables will be fetched from the local namespace into the SQL statement.

Please aware that we also support the `$variable` or `{variable_name}` way, but those will be deprecated in future version, [see more](https://jupysql.ploomber.io/en/latest/intro.html?highlight=variable#variable-substitution).

```{code-cell} ipython3

```
