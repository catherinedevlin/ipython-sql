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


## Parametrization via `{{variable}}`

JupySQL supports variable expansion in the form of `{{variable}}`. This allows the user to write a query with placeholders that can be replaced by variables dynamically.

The benefits of using parametrized SQL queries are:

* They can be reused with different values and for different purposes.
* Such queries can be prepared ahead of time and reused without having to create distinct SQL queries for each scenario.
* Parametrized queries can be used with dynamic data also.

Let's load some data and connect to the in-memory DuckDB instance:

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
%config SqlMagic.displaylimit = 3
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

The simplest use case is to use a variable to determine which data to filter:

+++

### Data filtering

```{code-cell} ipython3
sex = "MALE"
```

```{code-cell} ipython3
%%sql
SELECT *
FROM penguins.csv
WHERE  sex = '{{sex}}'
```

Note that we have to add quotes around `{{sex}}`, since the literal is replaced.

+++

`{{variable}}` parameters are not limited to `WHERE` clauses, you can use them anywhere:

```{code-cell} ipython3
dynamic_limit = 5
dynamic_column = "island, sex"
```

```{code-cell} ipython3
%sql SELECT {{dynamic_column}} FROM penguins.csv LIMIT {{dynamic_limit}}
```

### SQL generation

```{note}
We use [jinja](https://jinja.palletsprojects.com/en/3.1.x/) to parametrize queries, to learn more about the syntax, check our their docs.
```

Since there are no restrictions on where you can use `{{variable}}` you can use it to dynamically generate SQL if you also use advanced control structures. 

Let's look at generating SQL queries using a `{% for %}` loop. First, we'll create a set of unique `sex` values. This is required since the dataset contains samples for which `sex` couldn't be determined (`null`).

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

### SQL generation with macros

If `{% for %}` lops are not enough, you can modularize your code generation even more with macros.

macros is a construct analogous to functions that promote re-usability. We'll first define a macro for converting a value from `millimetre` to `centimetre`. And then use this macro in the query using variable expansion.

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

### Using snippets

You can combine the snippets feature with `{{variable}}`:

```{code-cell} ipython3
species = "Adelie"
```

```{code-cell} ipython3
%%sql --save one_species --no-execute
SELECT * FROM penguins.csv
WHERE species = '{{species}}'
```

```{code-cell} ipython3
%%sql
SELECT *
FROM one_species
```

```{important}
When storing a snippet with `{{variable}}`, the values are replaced upon saving, so assigning a new value to `variable` won't have any effect.
```

```{code-cell} ipython3
species = "Gentoo"
```

```{code-cell} ipython3
%%sql
SELECT *
FROM one_species
```

### Combining Python and `{{variable}}`

You can combine Python code with the `%sql` magic to execute parametrized queries.

Let's see how we can create multiple tables, each one containing the penguins for a given `island`.

```{code-cell} ipython3
for island in ("Torgersen", "Biscoe", "Dream"):
    %sql CREATE TABLE {{island}} AS (SELECT * from penguins.csv WHERE island = '{{island}}')
```

```{code-cell} ipython3
%sqlcmd tables
```

Let's verify data in one of the tables:

```{code-cell} ipython3
%sql SELECT * FROM Dream;
```

```{code-cell} ipython3
%sql SELECT * FROM Torgersen;
```

## Parametrization via `:variable`

```{versionadded} 0.9
```

There is a second method to parametrize variables via `:variable`. This method has the following limitations

- Only available for SQLAlchemy connections
- Only works for data filtering parameters (`WHERE`, `IN`, `>=`, etc.)


To enable it:

```{code-cell} ipython3
%config SqlMagic.named_paramstyle = True
```

```{code-cell} ipython3
sex = "MALE"
```

```{code-cell} ipython3
%%sql
SELECT *
FROM penguins.csv
WHERE sex = :sex
```

Note that we don't have to quote `:sex`. When using `:variable`, if `variable` is a string, it'll automatically be quoted. 

Here's another example where we use the parameters for an `IN` and a `>=` clauses:

```{code-cell} ipython3
one = "Adelie"
another = "Chinstrap"
min_body_mass_g = 4500
```

```{code-cell} ipython3
%%sql
SELECT *
FROM penguins.csv
WHERE species IN (:one, :another)
AND body_mass_g >= :min_body_mass_g
```

Parametrizing other parts of the query like table names or column names won't work.

```{code-cell} ipython3
tablename = "penguins.csv"
```

```{code-cell} ipython3
:tags: [raises-exception]

%%sql
SELECT *
FROM :tablename
```

### Using snippets and `:variable`

Unlike `{{variable}`, `:variable` parameters are evaluated at execution time, meaning you can `--save` a query and the output will change depending on the value of `variable` when the query is executed:

```{code-cell} ipython3
sex = "MALE"
```

```{code-cell} ipython3
%%sql --save one_sex
SELECT *
FROM penguins.csv
WHERE sex = :sex
```

```{code-cell} ipython3
sex = "FEMALE"
```

```{code-cell} ipython3
%%sql
SELECT * FROM one_sex
```
