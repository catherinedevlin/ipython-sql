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
---

# API

``-l`` / ``--connections``
    List all active connections

``-x`` / ``--close <session-name>`` 
    Close named connection 

``-c`` / ``--creator <creator-function>``
    Specify creator function for new connection

``-s`` / ``--section <section-name>``
    Section of dsn_file to be used for generating a connection string

``-p`` / ``--persist``
    Create a table name in the database from the named DataFrame

``-n`` / ``--no-index``
    Do not persist data frame's index

``--append``
    Like ``--persist``, but appends to the table if it already exists 

``-a`` / ``--connection_arguments <"{connection arguments}">``
    Specify dictionary of connection arguments to pass to SQL driver

``-f`` / ``--file <path>``
    Run SQL from file at this path

```{code-cell} ipython3
:tags: [remove-input]

from pathlib import Path

files = [Path("db_one.db"), Path("db_two.db"), Path("my_data.csv")]

for f in files:
    if f.exists():
        f.unlink()
```

## Initialization

```{code-cell} ipython3
%load_ext sql
```

## Connect to database

```{code-cell} ipython3
%sql sqlite:///db_one.db
```

## List connections

Connect to another database to demonstrate `--list`'s usage:

```{code-cell} ipython3
%sql sqlite:///db_two.db
```

```{code-cell} ipython3
%sql --list
```

## Close connection

```{code-cell} ipython3
%sql --close sqlite:///db_one.db
```

## Create table

```{code-cell} ipython3
import pandas as pd

my_data = pd.DataFrame({"x": range(3), "y": range(3)})
```

```{code-cell} ipython3
%sql --persist my_data
```

```{code-cell} ipython3
%sql SELECT * FROM my_data
```

## Create table without `DataFrame` index

```{code-cell} ipython3
my_chars = pd.DataFrame({"char": ["a", "b", "c"]})
my_chars
```

```{code-cell} ipython3
%sql --persist my_chars --no-index
```

```{code-cell} ipython3
%sql SELECT * FROM my_chars
```

## Append to table

```{code-cell} ipython3
my_data = pd.DataFrame({"x": range(3, 6), "y": range(3, 6)})
```

```{code-cell} ipython3
%sql --append my_data
```

```{code-cell} ipython3
%sql SELECT * FROM my_data
```

## Query

```{code-cell} ipython3
%sql SELECT * FROM my_data LIMIT 2
```

```{code-cell} ipython3
%%sql
SELECT * FROM my_data LIMIT 2
```

## Compose large queries

```{code-cell} ipython3
%%sql --save larger_than_one --no-execute
SELECT x, y
FROM my_data
WHERE x > 1
```

```{code-cell} ipython3
%%sql --with larger_than_one
SELECT x, y
FROM larger_than_one
WHERE y < 5
```

## Convert result to `pandas.DataFrame`

```{code-cell} ipython3
result = %sql SELECT * FROM my_data
df = result.DataFrame()
print(type(df))
df.head()
```

## Store as CSV

```{code-cell} ipython3
result = %sql SELECT * FROM my_data
result.csv(filename="my_data.csv")
```

## Run query from file

```{code-cell} ipython3
from pathlib import Path

# generate sql file
Path("my-query.sql").write_text("""
SELECT *
FROM my_data
LIMIT 3
""")
```

```{code-cell} ipython3
%sql --file my-query.sql
```
