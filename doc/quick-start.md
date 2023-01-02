---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
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

# Quick Start

JupySQL allows you to run SQL in Jupyter/IPython via a `%sql` and `%%sql` magics. It is a fork of `ipython-sql` with bug fixes and a lot of great new features!

+++

## Installation

Run this on your terminal:

```sh
pip install jupysql
```

Or the following in a Jupyter notebook:

```{code-cell} ipython3
%pip install jupysql --quiet
```

## Setup

Load the extension:

```{code-cell} ipython3
%load_ext sql
```

Let's see an example using a SQLite database. Let's insert some sample data from the TIOBE index:

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE languages (name, rating, change);
INSERT INTO languages VALUES ('Python', 14.44, 2.48);
INSERT INTO languages VALUES ('C', 13.13, 1.50);
INSERT INTO languages VALUES ('Java', 11.59, 0.40);
INSERT INTO languages VALUES ('C++', 10.00, 1.98);
```

## Querying

```{code-cell} ipython3
%sql SELECT * FROM languages
```

## `pandas` integration

```{code-cell} ipython3
result = %sql SELECT * FROM languages
```

```{code-cell} ipython3
df = result.DataFrame()
```

```{code-cell} ipython3
df.head()
```

```{code-cell} ipython3
type(df)
```

## Connecting

To authenticate with your database, you can build your connection string:

```python
user = os.getenv('SOME_USER')
password = os.getenv('SOME_PASSWORD')
connection_string = f"postgresql://{user}:{password}@localhost/some_database"
```

Then, pass it to the `%sql` magic

```python
%sql $connection_string
```
