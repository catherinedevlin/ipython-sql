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
    description lang=en: Use JupySQL to organize large SQL queries in a Jupyter notebook
    keywords: jupyter, sql, jupysql
    property=og:locale: en_US
---

# Organizing Large Queries


```{dropdown} Required packages
~~~
pip install jupysql matplotlib
~~~
```


```{versionchanged} 0.7.10
```

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


We automatically extract the tables from the query and infer the dependencies from all the saved snippets.


```{code-cell} ipython3
%%sql --save track_fav
SELECT t.*
FROM tracks_with_info t
JOIN genres_fav
ON t.GenreId = genres_fav.GenreId
```

Now let's find artists with the most Rock and Metal tracks, and save the query as `top_artist`

```{code-cell} ipython3
%%sql --save top_artist
SELECT artist, COUNT(*) FROM track_fav
GROUP BY artist
ORDER BY COUNT(*) DESC
```


```{note}
A saved snippet will override an existing table with the same name during query formation. If you wish to delete a snippet please refer to [sqlcmd snippets API](api/magic-snippets.md).

```

#### Data visualization

Once we have the desired results from the query `top_artist`, we can generate a visualization using the bar method

```{code-cell} ipython3
top_artist = %sql SELECT * FROM top_artist
top_artist.bar()
```

It looks like Iron Maiden had the highest number of rock and metal songs in the table.

We can render the full query with the `%sqlcmd snippets {name}` magic:

```{code-cell} ipython3
final = %sqlcmd snippets top_artist
print(final)
```

We can verify the retrieved query returns the same result:

```{code-cell} ipython3
%%sql
{{final}}
```

#### `--with` argument

JupySQL also allows you to specify the snippet name explicitly by passing the `--with` argument. This is particularly useful when our parsing logic is unable to determine the table name due to dialect variations. For example, consider the below example:

```{code-cell} ipython3
%sql duckdb://
```

```{code-cell} ipython3
%%sql --save first_cte --no-execute
SELECT 1 AS column1, 2 AS column2
```

```{code-cell} ipython3
%%sql --save second_cte --no-execute
SELECT
  sum(column1),
  sum(column2) FILTER (column2 = 2)
FROM first_cte
```

```{code-cell} ipython3
:tags: [raises-exception]

%%sql
SELECT * FROM second_cte
```

Note that the query fails because the clause `FILTER (column2 = 2)` makes it difficult for the parser to extract the table name. While this syntax works on some dialects like `DuckDB`, the more common usage is to specify `WHERE` clause as well, like `FILTER (WHERE column2 = 2)`.

Now let's run the same query by specifying `--with` argument.

```{code-cell} ipython3
%%sql --with first_cte --save second_cte --no-execute
SELECT
  sum(column1),
  sum(column2) FILTER (column2 = 2)
FROM first_cte
```

```{code-cell} ipython3
%%sql
SELECT * FROM second_cte
```


## Summary

In the given example, we demonstrated JupySQL's usage as a tool for managing large SQL queries in Jupyter Notebooks. It effectively broke down a complex query into smaller, organized parts, simplifying the process of analyzing a record store's sales database. By using JupySQL, users can easily maintain and reuse their queries, enhancing the overall data analysis experience.
