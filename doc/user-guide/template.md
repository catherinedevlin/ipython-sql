---
jupytext:
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.6
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: Templatize SQL queries in Jupyter via JupySQL
    keywords: jupyter, sql, jupysql, jinja
    property=og:locale: en_US
---

# Parameterizing SQL queries

```{versionchanged} 0.7
JupySQL uses Jinja templates for enabling SQL query parametrization. Queries are parametrized with `{{variable}}`.
```

```{note}
The legacy formats of parametrization, namely `{variable}`, `:variable`, and `$variable` of `ipython-sql` have been deprecated in the current and future versions, to prevent SQL parsing issues.
```


## Variable Expansion

JupySQL supports variable expansion in the form of `{{variable}}`. This allows the user to write a query with placeholders that can be replaced by variables dynamically.

The benefits of using parametrized SQL queries are:

* They can be reused with different values and for different purposes.
* Such queries can be prepared ahead of time and reused without having to create distinct SQL queries for each scenario.
* Parametrized queries can be used with dynamic data also.

Let's load some data and connect to the in-memory DuckDB instance:

```{code-cell} ipython3
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

Now, let's define a simple query template with placeholders, and substitute the placeholders with a couple of variables using variable expansion.

```{code-cell} ipython3
dynamic_limit = 5
dynamic_column = "island, sex"
```

```{code-cell} ipython3
%sql SELECT {{dynamic_column}} FROM penguins.csv LIMIT {{dynamic_limit}}
```

Note that the variables substituted in the SQL statement are fetched from the local namespace.

## Variable expansion using loops

Now, let's look at parametrizing queries using a for loop. First, we'll create a set of unique `sex` values. This is required since the dataset contains samples for which `sex` couldn't be determined (`null`).

```{code-cell} ipython3
sex = ("MALE", "FEMALE")
```

Then, we'll set a list of islands of interest, and for each island calculate the average `body_mass_g` of all penguins belonging to that island.

```{code-cell} ipython3
%%sql --save avg_body_mass
{% set islands = ["Torgersen", "Biscoe", "Dream"] %}
select
    sex,
    {% for island in islands %}
    avg(case when island = '{{island}}' then body_mass_g end) as {{island}}_body_mass_g,
    {% endfor %}
from penguins.csv
where sex in {{sex}}
group by sex 
```

Here's the final compiled query:

```{code-cell} ipython3
final = %sqlcmd snippets avg_body_mass
print(final)
```

## Macros + variable expansion

`Macros` is a construct analogous to functions that promote re-usability. We'll first define a macro for converting a value from `millimetre` to `centimetre`. And then use this macro in the query using variable expansion.

```{code-cell} ipython3
%%sql --save convert
{% macro mm_to_cm(column_name, precision=2) %}
    ({{ column_name }} / 10)::numeric(16, {{ precision }})
{% endmacro %}

select
  sex, island,
  {{ mm_to_cm('bill_length_mm') }} as bill_length_cm,
  {{ mm_to_cm('bill_depth_mm') }} as bill_length_cm,
from penguins.csv
```

Let's see the final rendered query:

```{code-cell} ipython3
final = %sqlcmd snippets convert
print(final)
```

```{code-cell} ipython3
%sqlcmd snippets -d convert
```

## Create tables in loop

We can also create multiple tables in a loop using parametrized queries. Let's segregate the dataset by `island`.

```{code-cell} ipython3
for island in ("Torgersen", "Biscoe", "Dream"):
    %sql CREATE TABLE {{island}} AS (SELECT * from penguins.csv WHERE island = '{{island}}')
```

```{code-cell} ipython3
%sqlcmd tables
```

Let's verify data in one of the tables:

```{code-cell} ipython3
%sql SELECT * FROM Torgersen;
```

[Click here](https://jupysql.ploomber.io/en/latest/intro.html?highlight=variable#variable-substitution) for more details.
