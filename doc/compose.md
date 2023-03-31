---
jupytext:
  notebook_metadata_filter: myst
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
    description lang=en: "Use JupySQL to organize large SQL queries in a Jupyter notebook"
    keywords: "jupyter, sql, jupysql"
    property=og:locale: "en_US"
---

# Organizing Large Queries


```{dropdown} Required packages
~~~
pip install jupysql matplotlib
~~~
```


*New in version 0.4.3*

```{note}
This is a beta feature, please [join our community](https://ploomber.io/community) and
let us know how we can improve it!
```

JupySQL allows you to break queries into multiple cells, simplifying the process of building large queries.

- **Simplify  and modularize your workflow:** JupySQL simplifies SQL queries and promotes code reusability by breaking down large queries into manageable chunks and enabling the creation of reusable query modules.
- **Seamless integration:** JupySQL flawlessly combines the power of SQL with the flexibility of Jupyter Notebooks, offering a one-stop solution for all your data analysis needs.
- **Cross-platform compatibility:** JupySQL supports popular databases like PostgreSQL, MySQL, SQLite, and more, ensuring you can work with any data source.

## Example: record store data

### Goal: 

Using Jupyter notebooks, make a query against an SQLite database table named 'Track' with Rock and Metal song information. Find and show the artists with the most Rock and Metal songs. Show your results in a bar chart.


#### Data download and initialization

Download the SQLite database file if it doesn't exist

```{code-cell} ipython3
import urllib.request
from pathlib import Path

if not Path("my.db").is_file():
    url = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"  # noqa
    urllib.request.urlretrieve(url, "my.db")
```

Initialize the SQL extension and set autolimit=3 to only retrieve a few rows

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%config SqlMagic.autolimit = 3
```

Query the track-level information from the Track table

```{code-cell} ipython3
%%sql sqlite:///my.db
SELECT * FROM Track
```

#### Data wrangling

Join the Track, Album, and Artist tables to get the artist name, and save the query as `tracks_with_info`

*Note: `--save` stores the query, not the data*

```{code-cell} ipython3
%%sql --save tracks_with_info
SELECT t.*, a.title AS album, ar.Name as artist
FROM Track t
JOIN Album a
USING (AlbumId)
JOIN Artist ar
USING (ArtistId)
```

Filter genres we are interested in (Rock and Metal) and save the query as `genres_fav`

```{code-cell} ipython3
%%sql --save genres_fav
SELECT * FROM Genre
WHERE Name
LIKE '%rock%'
OR Name LIKE '%metal%' 
```

Join the filtered genres and tracks, so we only get Rock and Metal tracks, and save the query as `track_fav`

Note that we are using `--with`; this will retrieve previously saved queries, and preprend them (using CTEs), then, we save the query in `track_fav` .

```{code-cell} ipython3
%%sql --with genres_fav --with tracks_with_info --save track_fav
SELECT t.*
FROM tracks_with_info t
JOIN genres_fav
ON t.GenreId = genres_fav.GenreId
```

Use the `track_fav` query to find artists with the most Rock and Metal tracks, and save the query as `top_artist`

```{code-cell} ipython3
%%sql --with track_fav --save top_artist
SELECT artist, COUNT(*) FROM track_fav
GROUP BY artist
ORDER BY COUNT(*) DESC
```

#### Data visualization

Once we have the desired results from the query `top_artist`, we can generate a visualization using the bar method

```{code-cell} ipython3
top_artist = %sql --with top_artist SELECT * FROM top_artist
top_artist.bar()
```

It looks like Iron Maiden had the highest number of rock and metal songs in the table.

We can render the full query with the `%sqlrender` magic:

```{code-cell} ipython3
final = %sqlrender top_artist
print(final)
```

We can verify the retrieved query returns the same result:

```{code-cell} ipython3
%%sql
{{final}}
```

## Summary

In the given example, we demonstrated JupySQL's usage as a tool for managing large SQL queries in Jupyter Notebooks. It effectively broke down a complex query into smaller, organized parts, simplifying the process of analyzing a record store's sales database. By using JupySQL, users can easily maintain and reuse their queries, enhancing the overall data analysis experience.