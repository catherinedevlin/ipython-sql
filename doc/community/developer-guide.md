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
    description lang=en: JupySQL's developer guide
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# Developer guide

+++

## Unit testing

### Magics (e.g., `%sql`, `%%sql`, etc)

This guide will show you the basics of writing unit tests for JupySQL magics. Magics are commands that begin with `%` (line magics) and `%%` (cell magics).

In the unit testing suite, there are a few pytest fixtures that prepare the environment so you can get started:

- `ip_empty` - Empty IPython session
- `ip` - IPython session with some sample data

So a typical test will look like this:

```{code-cell} ipython3
def test_something(ip):
    ip.run_cell('%sql sqlite://')
    result = ip.run_cell("""%%sql
    SELECT * FROM test
    """)

    assert result.success
```

To see some sample tests, [click here.](https://github.com/ploomber/jupysql/blob/master/src/tests/test_magic.py)


The IPython sessions are created like this:

```{code-cell} ipython3
from IPython.core.interactiveshell import InteractiveShell
from sql.magic import SqlMagic

ip_session = InteractiveShell()
ip_session.register_magics(SqlMagic)
```

To run some code:

```{code-cell} ipython3
out = ip_session.run_cell("1 + 1")
```

To test the output:

```{code-cell} ipython3
assert out.result == 2
```

You can also check for execution success:

```{code-cell} ipython3
assert out.success
```

```{important}
Always check for success! Since `run_cell` won't raise an error if the code fails
```

```{code-cell} ipython3
try:
    ip_session.run_cell("1 / 0")
except Exception as e:
    print(f"Error: {e}")
else:
    print("No error")
```

Note that the `run_cell` only printed the error but did not raise an exception.

+++

#### Capturing errors

Let's see how to test that the code raises an expected error:

```{code-cell} ipython3
out = ip_session.run_cell("1 / 0")
```

```{code-cell} ipython3
# this returns the raised exception
out.error_in_exec
```

```{code-cell} ipython3
:tags: [raises-exception]

# this raises the error
out.raise_error()
```

You can then use pytest to check the error:

```{code-cell} ipython3
import pytest
```

```{code-cell} ipython3
with pytest.raises(ZeroDivisionError):
    out.raise_error()
```

To check the error message:

```{code-cell} ipython3
with pytest.raises(ZeroDivisionError) as excinfo:
    out.raise_error()
```

```{code-cell} ipython3
assert str(excinfo.value) == 'division by zero'
```
