---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
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
    description lang=en: Convert outputs from SQL queries to pandas data frames using JupySQL
    keywords: jupyter, sql, jupysql, pandas
    property=og:locale: en_US
---

# Pandas

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

## Convert to `pandas.DataFrame`

+++

Query the sample data and convert to `pandas.DataFrame`:

```{code-cell} ipython3
result = %sql SELECT * FROM writer WHERE year_of_death > 1900
```

```{code-cell} ipython3
df = result.DataFrame()
type(df)
```

```{code-cell} ipython3
df
```

Or using the cell magic:

```{code-cell} ipython3
%%sql result <<
SELECT * FROM writer WHERE year_of_death > 1900
```

```{code-cell} ipython3
result.DataFrame()
```

## Convert automatically

```{code-cell} ipython3
%config SqlMagic.autopandas = True
df = %sql SELECT * FROM writer
type(df)
```

```{code-cell} ipython3
df
```

## Uploading a dataframe to the database

+++

The `--persist` argument, with the name of a  DataFrame object in memory, 
will create a table name in the database from the named DataFrame.   Or use `--append` to add rows to an existing  table by that name.

```{code-cell} ipython3
%sql --persist df
```

```{code-cell} ipython3
%sql SELECT * FROM df;
```

```{code-cell} ipython3

```
