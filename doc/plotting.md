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

# Plotting

If you have installed ``matplotlib``, you can use a result set's
``.plot()``, ``.pie()``, and ``.bar()`` methods for quick plotting:

```{code-cell}
%load_ext sql
```

```{code-cell}
%%sql sqlite://
CREATE TABLE languages (name, rating, change);
INSERT INTO languages VALUES ('Python', 14.44, 2.48);
INSERT INTO languages VALUES ('C', 13.13, 1.50);
INSERT INTO languages VALUES ('Java', 11.59, 0.40);
INSERT INTO languages VALUES ('C++', 10.00, 1.98);
```

*Note: data from the TIOBE index*

```{code-cell}
rating = %sql SELECT name, rating FROM languages
rating.plot()
```

```{code-cell}
change = %sql SELECT name, change FROM languages
change.bar()
```
