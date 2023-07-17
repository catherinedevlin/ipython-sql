---
jupytext:
  notebook_metadata_filter: myst
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
myst:
  html_meta:
    description lang=en: Run SQL in a Jupyter notebook with JupySQL
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# Introduction

JupySQL allows you to run SQL in Jupyter/IPython via a `%sql` and `%%sql` magics.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE languages (name, rating, change);
INSERT INTO languages VALUES ('Python', 14.44, 2.48);
INSERT INTO languages VALUES ('C', 13.13, 1.50);
INSERT INTO languages VALUES ('Java', 11.59, 0.40);
INSERT INTO languages VALUES ('C++', 10.00, 1.98);
```

*Note: data from the TIOBE index*

```{code-cell} ipython3
%sql SELECT * FROM languages
```

```{code-cell} ipython3
result = _
print(result)
```

```{code-cell} ipython3
result.keys
```

```{code-cell} ipython3
result[0][0]
```

```{code-cell} ipython3
result[0].rating
```

After the first connection, connect info can be omitted::

```{code-cell} ipython3
%sql select count(*) from languages
```

Connections to multiple databases can be maintained.  You can switch connection using --alias
Suppose we create two database, named one and two. Then, assign alias to both connections so we can switch them by name:

```sql
%sql sqlite:///one.db --alias one
%sql sqlite:///two.db --alias two
```

```sql
%sql 
```

It will run query in "two" database since it's the latest one we connected to.

Pass the alias to make it the current connection:

```sql
%sql one
```

You can pass an alias and query in the same cell:

```sql
%sql two
SELECT * FROM two
```

However, this isn’t supported with the line magic (e.g., `%sql one SELECT * FROM one`).

+++

For secure access, you may dynamically access your credentials (e.g. from your system environment or `getpass.getpass`) to avoid storing your password in the notebook itself. Then, create the connection and pass it to the magic:

+++

```python
from sqlalchemy import create_engine

user = os.getenv('SOME_USER')
password = os.getenv('SOME_PASSWORD')

engine = create_engine(f"postgresql://{user}:{password}@localhost/some_database")
%sql engine
```

+++

You may use multiple SQL statements inside a single cell, but you will only see any query results from the last of them, so this really only makes sense for statements with no output

+++

```
%%sql sqlite://
CREATE TABLE writer (first_name, last_name, year_of_death);
INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
```

+++

As a convenience, dict-style access for result sets is supported, with the
leftmost column serving as key, for unique values.

+++

```
result = %sql select * from work
result['richard2']
```

+++

Results can also be retrieved as an iterator of dictionaries (``result.dicts()``)
or a single dictionary with a tuple of scalar values per key (``result.dict()``)

## Assignment

Ordinary IPython assignment works for single-line `%sql` queries:

```{code-cell} ipython3
lang = %sql SELECT * FROM languages
```

The `<<` operator captures query results in a local variable, and
can be used in multi-line ``%%sql``:

```{code-cell} ipython3
%%sql lang << SELECT *
FROM languages
```

The `myvar= <<` syntax captures query results in a local variable as well as
returning the results.

```{code-cell} ipython3
%%sql lang= << SELECT *
FROM languages
```

+++

## Considerations

Because jupysql accepts `--`-delimited options like `--persist`, but `--` 
is also the syntax to denote a SQL comment, the parser needs to make some assumptions.

- If you try to pass an unsupported argument, like `--lutefisk`, it will 
  be interpreted as a SQL comment and will not throw an unsupported argument 
  exception.
- If the SQL statement begins with a first-line comment that looks like one 
  of the accepted arguments - like `%sql --persist is great!` - it will be 
  parsed like an argument, not a comment.  Moving the comment to the second 
  line or later will avoid this.
