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

# JupySQL

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

Connections to multiple databases can be maintained.  You can refer to
an existing connection by username@database

```sql
%%sql will@shakes
select charname, speechcount from character
where  speechcount = (select max(speechcount)
from character);
```

```python
print(_)
```

+++

If no connect string is supplied, ``%sql`` will provide a list of existing connections;
however, if no connections have yet been made and the environment variable ``DATABASE_URL``
is available, that will be used.

For secure access, you may dynamically access your credentials (e.g. from your system environment or `getpass.getpass`) to avoid storing your password in the notebook itself. Use the `$` before any variable to access it in your `%sql` command.

+++

```
user = os.getenv('SOME_USER')
password = os.getenv('SOME_PASSWORD')
connection_string = "postgresql://{user}:{password}@localhost/some_database".format(user=user, password=password)
%sql $connection_string
```

+++

You may use multiple SQL statements inside a single cell, but you will
only see any query results from the last of them, so this really only
makes sense for statements with no output

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

## Variable substitution 

Bind variables (bind parameters) can be used in the "named" (:x) style.
The variable names used should be defined in the local namespace.

```{code-cell} ipython3
name = 'Python'
```

```{code-cell} ipython3
%sql select * from languages where name = :name
```

```{code-cell} ipython3
%sql select * from languages where name = '{name}';
```

Alternately, ``$variable_name`` or ``{variable_name}`` can be 
used to inject variables from the local namespace into the SQL 
statement before it is formed and passed to the SQL engine.
(Using ``$`` and ``{}`` together, as in ``${variable_name}``, 
is not supported.)

Bind variables are passed through to the SQL engine and can only 
be used to replace strings passed to SQL.  ``$`` and ``{}`` are 
substituted before passing to SQL and can be used to form SQL 
statements dynamically.

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
