---
jupytext:
  notebook_metadata_filter: myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.6
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
myst:
  html_meta:
    description lang=en: Documentation for %sqlcmd snippets from JupySQL
    keywords: jupyter, sql, jupysql, snippets
    property=og:locale: en_US
---

# `%sqlcmd snippets`

`%sqlcmd snippets` returns the query snippets saved using `--save`

## Load Data

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```

```{code-cell} ipython3
from pathlib import Path
from urllib.request import urlretrieve

if not Path("penguins.csv").is_file():
    urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "penguins.csv",
    )
```

```{code-cell} ipython3
%%sql
SELECT * FROM penguins.csv LIMIT 3
```

Let's save a couple of snippets.

```{code-cell} ipython3
:tags: [hide-output]

%%sql --save gentoo
SELECT * FROM penguins.csv where species == 'Gentoo'
```

```{code-cell} ipython3
:tags: [hide-output]

%%sql --save chinstrap
SELECT * FROM penguins.csv where species == 'Chinstrap'
```

## `%sqlcmd snippets`

+++

Returns all the snippets saved in the environment

```{code-cell} ipython3
%sqlcmd snippets
```

Arguments:

`{snippet_name}` Return a snippet.

`-d`/`--delete` Delete a snippet.

`-D`/`--delete-force` Force delete a snippet. This may be useful if there are other dependent snippets, and you still need to delete this snippet.

`-A`/`--delete-force-all` Force delete a snippet and all dependent snippets.

```{code-cell} ipython3
chinstrap_snippet = %sqlcmd snippets chinstrap
print(chinstrap_snippet)
```

This returns the stored snippet `chinstrap`.

Calling `%sqlcmd snippets {snippet_name}` also works on a snippet that is dependent on others. To demonstrate it, let's create a snippet dependent on the `chinstrap` snippet.

```{code-cell} ipython3
%%sql --save chinstrap_sub
SELECT * FROM chinstrap where island == 'Dream'
```

```{code-cell} ipython3
chinstrap_sub_snippet = %sqlcmd snippets chinstrap_sub
print(chinstrap_sub_snippet)
```

This returns the stored snippet `chinstrap_sub`.

Now, let's see how to delete a stored snippet.

```{code-cell} ipython3
%sqlcmd snippets -d gentoo
```

This deletes the stored snippet `gentoo`.

Now, let's see how to delete a stored snippet that other snippets are dependent on. Recall we have created `chinstrap_sub` which is dependent on `chinstrap`.

```{code-cell} ipython3
print(chinstrap_sub_snippet)
```

Trying to delete the `chinstrap` snippet will display an error message:

```{code-cell} ipython3
:tags: [raises-exception]

%sqlcmd snippets -d chinstrap
```

If you still wish to delete this snippet, you should use `force-delete` by running the below command:

```{code-cell} ipython3
%sqlcmd snippets -D chinstrap
```

Now, let's see how to delete a snippet and all other dependent snippets. We'll create a few snippets again.

```{code-cell} ipython3
:tags: [hide-output]

%%sql --save chinstrap
SELECT * FROM penguins.csv where species == 'Chinstrap'
```

```{code-cell} ipython3
:tags: [hide-output]

%%sql --save chinstrap_sub
SELECT * FROM chinstrap where island == 'Dream'
```

Now, force delete `chinstrap` and its dependent `chinstrap_sub`:

```{code-cell} ipython3
%sqlcmd snippets -A chinstrap
```
