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

# Dumping

Result sets come with a ``.csv(filename=None)`` method.  This generates
comma-separated text either as a return value (if ``filename`` is not
specified) or in a file of the given name.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE writer (first_name, last_name, year_of_death);
INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
```

```{code-cell} ipython3
result = %sql SELECT * FROM writer
result.csv(filename='writer.csv')
```

```{code-cell} ipython3
import pandas as pd

df = pd.read_csv('writer.csv')
df
```
