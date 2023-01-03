---
jupytext:
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
---

# Plotting (legacy API)

```{note}
This is a legacy API that's kept for backwards compatibility.
```

+++

Ensure you have `matplotlib` installed:

```{code-cell} ipython3
%pip install matplotlib --quiet
```

```{code-cell} ipython3
%load_ext sql
```

Connect to an in-memory SQLite database.

```{code-cell} ipython3
%sql sqlite://
```

## Line

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE points (x, y);
INSERT INTO points VALUES (0, 0);
INSERT INTO points VALUES (1, 1.5);
INSERT INTO points VALUES (2, 3);
INSERT INTO points VALUES (3, 3);
```

```{code-cell} ipython3
points = %sql SELECT x, y FROM points
points.plot()
```

## Bar

+++

*Note: sample data from the TIOBE index.*

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE languages (name, rating, change);
INSERT INTO languages VALUES ('Python', 14.44, 2.48);
INSERT INTO languages VALUES ('C', 13.13, 1.50);
INSERT INTO languages VALUES ('Java', 11.59, 0.40);
INSERT INTO languages VALUES ('C++', 10.00, 1.98);
```

```{code-cell} ipython3
change = %sql SELECT name, change FROM languages
change.bar()
```

## Pie

Data from [Our World in Data.](https://ourworldindata.org/grapher/energy-consumption-by-source-and-country?time=latest)

```{code-cell} ipython3
%%sql sqlite://
CREATE TABLE energy_2021 (source, percentage);
INSERT INTO energy_2021 VALUES ('Oil', 31.26);
INSERT INTO energy_2021 VALUES ('Coal', 27.17);
INSERT INTO energy_2021 VALUES ('Gas', 24.66);
INSERT INTO energy_2021 VALUES ('Hydropower', 6.83);
INSERT INTO energy_2021 VALUES ('Nuclear', 4.3);
INSERT INTO energy_2021 VALUES ('Wind', 2.98);
INSERT INTO energy_2021 VALUES ('Solar', 1.65);
INSERT INTO energy_2021 VALUES ('Biofuels', 0.70);
INSERT INTO energy_2021 VALUES ('Other renewables', 0.47);
```

```{code-cell} ipython3
energy = %sql SELECT source, percentage FROM energy_2021
energy.pie()
```
