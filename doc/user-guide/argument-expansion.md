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
    description lang=en: Variable substitution of arguments in Jupyter via JupySQL
    keywords: jupyter, sql, jupysql, jinja
    property=og:locale: en_US
---

# Parameterizing arguments

```{versionadded} 0.10.8
JupySQL uses Jinja templates for enabling parametrization of arguments. Arguments are parametrized with `{{variable}}`.
```


## Parametrization via `{{variable}}`

JupySQL supports variable expansion of arguments in the form of `{{variable}}`. This allows the user to specify arguments with placeholders that can be replaced by variables dynamically.

The benefits of using parametrized arguments is that they can be reused for different purposes.

Let's load some data and connect to the in-memory DuckDB instance:

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
%config SqlMagic.displaylimit = 3
```

```{code-cell} ipython3
filename = "penguins.csv"
```


```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        filename,
    )
```

Now let's create a snippet from the data by declaring a `table` variable and use it in the `--save` argument.

+++

### Create a snippet

```{code-cell} ipython3
table = "penguins_data"
```

```{code-cell} ipython3
%%sql --save {{table}}
SELECT *
FROM penguins.csv
```

```{code-cell} ipython3
snippet = %sqlcmd snippets {{table}}
print(snippet)
```


### Plot a histogram

Now, let's declare a variable `column` and plot a histogram on the data.

```{code-cell} ipython3
column = "body_mass_g"
```

```{code-cell} ipython3
%sqlplot boxplot --table {{table}} --column {{column}}
```

### Profile and Explore

We can use the `filename` variable to profile and explore the data as well:

```{code-cell} ipython3
%sqlcmd profile --table {{filename}}
```

```{code-cell} ipython3
%sqlcmd explore --table {{filename}}
```

### Run some tests

```{code-cell} ipython3
%sqlcmd test --table {{table}} --column {{column}} --greater 3500
```

