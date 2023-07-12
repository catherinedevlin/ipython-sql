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
myst:
  html_meta:
    description lang=en: Test columns from your database in Jupyter via JupySQL
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---


# Testing with sqlcmd

```{note}
This example uses `SQLite` but the same commands work for other databases.
```

```{code-cell} ipython3
%load_ext sql
%sql sqlite://
```

Let's create a sample table:

```{code-cell} ipython3
:tags: [hide-output]
%%sql sqlite://
CREATE TABLE writer (first_name, last_name, year_of_death);
INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
```


## Run Tests on Column

Use `%sqlcmd test` to run quantitative tests on your dataset.

For example, to see if all the values in the column birth_year are less than 2000, we can use:

```{code-cell} ipython3
%sqlcmd test --table writer --column year_of_death --less-than 2000
```

Because both William Shakespeare and Bertold Brecht died before the year 2000, this command will return True. 

However, if we were to run:

```{code-cell} ipython3
:tags: [raises-exception]
%sqlcmd test --table writer --column year_of_death --greater 1700
```

We see that a value that failed our test was William Shakespeare, as he died in 1616.

We can also pass several comparator arguments to test:

```{code-cell} ipython3
:tags: [raises-exception]
%sqlcmd test --table writer --column year_of_death --greater-or-equal 1616 --less-than-or-equal 1956
```

Here, because Shakespeare died in 1616 and Brecht in 1956, our test passes. 

However, if we search for a window between 1800 and 1900:

```{code-cell} ipython3
:tags: [raises-exception]
%sqlcmd test --table writer --column year_of_death --greater 1800 --less-than 1900
```

The test fails, returning both Shakespeare and Brecht.

Currently, 5 different comparator arguments are supported: `greater`, `greater-or-equal`, `less-than`, `less-than-or-equal`, and `no-nulls`. 

