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
---

# Interactive SQL Queries

```{versionadded} 0.7
~~~
pip install jupysql --upgrade
~~~
```


Interactive command allows you to visualize and manipulate widget and interact with your SQL clause.
We will demonstrate how to create widgets and dynamically query the dataset.

```{note}
`%sql --interact` requires `ipywidgets`: `pip install ipywidgets`
```

## `%sql --interact {{widget_variable}}`

First, you need to define the variable as the form of basic data type or ipywidgets Widget.
Then pass the variable name into `--interact` argument

```{code-cell} ipython3
%load_ext sql
import ipywidgets as widgets

from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "penguins.csv",
    )
%sql duckdb://
```

## Basic Data Types

The simplest way is to declare a variable with basic data types (Numeric, Text, Boolean...), the [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/examples/Using%20Interact.html?highlight=interact#Basic-interact) will autogenerates UI controls for those variables

```{code-cell} ipython3
body_mass_min = 3500
%sql --interact body_mass_min SELECT * FROM penguins.csv WHERE body_mass_g > {{body_mass_min}} LIMIT 5
```

```{code-cell} ipython3
island = (  # Try to change Torgersen to Biscoe, Torgersen or Dream in the below textbox
    "Torgersen"
)
%sql --interact island SELECT * FROM penguins.csv WHERE island == '{{island}}' LIMIT 5
```

## `ipywidgets` Widget

You can use widgets to build fully interactive GUIs for your SQL clause.

See more for complete [Widget List](https://ipywidgets.readthedocs.io/en/stable/examples/Widget%20List.html)

+++

### IntSlider

```{code-cell} ipython3
body_mass_lower_bound = widgets.IntSlider(min=2500, max=3500, step=25, value=3100)

%sql --interact body_mass_lower_bound SELECT * FROM penguins.csv WHERE body_mass_g <= {{body_mass_lower_bound}} LIMIT 5
```

### FloatSlider

```{code-cell} ipython3
bill_length_mm_lower_bound = widgets.FloatSlider(
    min=35.0, max=45.0, step=0.1, value=40.0
)

%sql --interact bill_length_mm_lower_bound SELECT * FROM penguins.csv WHERE bill_length_mm <= {{bill_length_mm_lower_bound}} LIMIT 5
```

## Complete Example

To demonstrate the way to combine basic data type and ipywidgets into our interactive SQL Clause

```{code-cell} ipython3
body_mass_lower_bound = 3600
show_limit = (0, 50, 1)
sex_selection = widgets.RadioButtons(
    options=["MALE", "FEMALE"], description="Sex", disabled=False
)
species_selections = widgets.SelectMultiple(
    options=["Adelie", "Chinstrap", "Gentoo"],
    value=["Adelie", "Chinstrap"],
    # rows=10,
    description="Species",
    disabled=False,
)
```

```{code-cell} ipython3
%%sql --interact show_limit --interact body_mass_lower_bound --interact species_selections --interact sex_selection
SELECT * FROM penguins.csv 
WHERE species IN{{species_selections}} AND 
body_mass_g > {{body_mass_lower_bound}} AND 
sex == '{{sex_selection}}'
LIMIT {{show_limit}} 
```

```{code-cell} ipython3

```
