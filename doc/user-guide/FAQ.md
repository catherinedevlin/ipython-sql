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
---

# Jupyter Magic

One may be unfamiliar with the commands prefixed with `%` used in this instruction. Here is a detailed description of this command and its usage. 

## Definition of Jupyter Magic

Magics are specific to and provided by the IPython kernel. Some common usage of magic functions are: running external code files, timing code execution, and loading IPython Extensions. 

Suppose execute.py is a python code file

```
%run execute.py
%timeit L = [n ** 2 for n in range(1000)] (Timing executions -- will return 1000 loops, best of 3: 325 µs per loop)
```

In our code above, we use **%load_ext** to load an IPython extension by its module name, `sql`, and then directly use the extension by using `%sql`.

```
load an IPython extension by its module name.
%load_ext sql 
```

## Line Magic VS Cell Magic

**Line magics**, which are denoted by a single % prefix and operate on a single line of input, and **cell magics**, which are denoted by a double %% prefix and operate on multiple lines of input. 

For example, for the code above, **%sql** is a line magic, and **%%sql** is a code magic. 

### Reference 
[IPython doc](https://ipython.readthedocs.io/en/stable/interactive/magics.html#cell-magics)

[Python Data Science Handbook](https://jakevdp.github.io/PythonDataScienceHandbook/01.03-magic-commands.html)
