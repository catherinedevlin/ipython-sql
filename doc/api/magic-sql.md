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
myst:
  html_meta:
    description lang=en: Documentation for the %sql and %%sql magics from JupySQL
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# `%sql`/`%%sql`

``-l`` / ``--connections``
    List all active connections ([example](#list-connections))

``-x`` / ``--close <session-name/alias>``
    Close named connection ([example](#close-connection))

``-c`` / ``--creator <creator-function>``
    Specify creator function for new connection

``-s`` / ``--section <section-name>``
    Section of dsn_file to be used for generating a connection string

``-p`` / ``--persist``
    Create a table name in the database from the named DataFrame ([example](#create-table))

``--append``
    Like ``--persist``, but appends to the table if it already exists ([example](#append-to-table))

``-a`` / ``--connection_arguments <"{connection arguments}">``
    Specify dictionary of connection arguments to pass to SQL driver

``-f`` / ``--file <path>``
    Run SQL from file at this path ([example](#run-query-from-file))

```{versionadded} 0.4.2
```

``-n`` / ``--no-index``
    Do not persist data frame's index (used with `-p/--persist`) ([example](#create-table-without-dataframe-index))

```{versionadded} 0.4.3
```

``-S`` / ``--save <name>``
    Save this query for later use ([example](#compose-large-queries))

``-w`` / ``--with <name>``
    Use a previously saved query (used after `-S/--save`) ([example](#compose-large-queries))

```{versionadded} 0.5.2
```

``-A`` / ``--alias <alias>``
    Assign an alias when establishing a connection ([example](#connect-to-database))

```{code-cell} ipython3
:tags: [remove-input]

from pathlib import Path

files = [Path("db_one.db"), Path("db_two.db"), Path("db_three.db"), Path("my_data.csv")]

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

Assign an alias to the connection (**added 0.5.2**):

```{code-cell} ipython3
%sql sqlite:///db_two.db --alias db-two
```

```{code-cell} ipython3
%sql sqlite:///db_three.db --alias db-three
```

To make all subsequent queries to use certain connection, pass the connection name:

```{code-cell} ipython3
%sql db-two
```

```{code-cell} ipython3
%sql db-three
```

You can inspect which is the current active connection:

```{code-cell} ipython3
%sql --list
```

For more details on managing connections, see [Switch connections](../howto.md#switch-connections).

+++

## List connections

```{code-cell} ipython3
%sql --list
```

## Close connection

```{code-cell} ipython3
%sql --close sqlite:///db_one.db
```

Or pass an alias (**added in 0.5.2**):

```{code-cell} ipython3
%sql --close db-two
```

## Create table

```{code-cell} ipython3
%sql sqlite://
```

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

## Programmatic SQL queries

```{code-cell} ipython3
QUERY = """
SELECT *
FROM my_data
LIMIT 3
"""

%sql $QUERY
```

## Templated SQL queries

```{code-cell} ipython3
from string import Template

template = Template(
    """
SELECT *
FROM my_data
LIMIT $limit
"""
)

limit_one = template.substitute(limit=1)
limit_two = template.substitute(limit=2)
```

**Important:** Ensure you sanitize the input parameters; as malicious parameters will be able to run arbitrary SQL queries.

```{code-cell} ipython3
%sql $limit_one
```

```{code-cell} ipython3
%sql $limit_two
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
Path("my-query.sql").write_text(
    """
SELECT *
FROM my_data
LIMIT 3
"""
)
```

```{code-cell} ipython3
%sql --file my-query.sql
```
