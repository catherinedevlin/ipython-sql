---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Pandas integration

If you have installed [`pandas`](http://pandas.pydata.org/), you can use a result set's `.DataFrame()` method.

Let's create some sample data:

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE writer (first_name, last_name, year_of_death);
INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
```

Query the sample data and convert to `pandas.DataFrame`:

```{code-cell} ipython3
result = %sql SELECT * FROM writer WHERE year_of_death > 1900
dataframe = result.DataFrame()
dataframe
```

The `--persist` argument, with the name of a  DataFrame object in memory, 
will create a table name in the database from the named DataFrame.   Or use `--append` to add rows to an existing  table by that name.

```{code-cell} ipython3
%sql --persist dataframe
```

```{code-cell} ipython3
%sql SELECT * FROM dataframe;
```
