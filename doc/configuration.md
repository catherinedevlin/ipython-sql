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

# Configuration

Query results are loaded as lists, so very large result sets may use up
your system's memory and/or hang your browser.  There is no autolimit
by default.  However, `autolimit` (if set) limits the size of the result
set (usually with a `LIMIT` clause in the SQL).  `displaylimit` is similar,
but the entire result set is still pulled into memory (for later analysis);
only the screen display is truncated.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%config SqlMagic
```

To change the configuration:

```{code-cell} ipython3
%config SqlMagic.feedback = False
```

Please note: if you have autopandas set to true, the displaylimit option will not apply. You can set the pandas display limit by using the pandas `max_rows` option as described in the [pandas documentation](http://pandas.pydata.org/pandas-docs/version/0.18.1/options.html#frequently-used-options).

+++

## PostgreSQL features

`psql`-style "backslash" [meta-commands](https://www.postgresql.org/docs/9.6/static/app-psql.html#APP-PSQL-META-COMMANDS) commands (``\d``, ``\dt``, etc.)
are provided by [PGSpecial](https://pypi.python.org/pypi/pgspecial).  Example:

```python
%sql \d
```
