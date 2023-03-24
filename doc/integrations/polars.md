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
    description lang=en: Convert outputs from SQL queries to polars data frames using JupySQL
    keywords: jupyter, sql, jupysql, polars
    property=og:locale: en_US
---

# Polars

If you have installed [`polars`](https://www.pola.rs/), you can use a result set's `.PolarsDataFrame()` method.

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

## Convert to `polars.DataFrame`

+++

Query the sample data and convert to `polars.DataFrame`:

```{code-cell} ipython3
result = %sql SELECT * FROM writer WHERE year_of_death > 1900
```

```{code-cell} ipython3
df = result.PolarsDataFrame()
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
result.PolarsDataFrame()
```

## Convert automatically

```{code-cell} ipython3
%config SqlMagic.autopolars = True
df = %sql SELECT * FROM writer
type(df)
```

```{code-cell} ipython3
df
```
