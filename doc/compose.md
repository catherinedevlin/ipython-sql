---
jupytext:
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

# Composing large queries

*New in version 0.4.3*

```{note}
This is a beta feature, please [join our community](https://ploomber.io/community) and let us know how we can improve it!
```

JupySQL allows you to break queries into multiple cells, simplifying the process of building large queries.

As an example, we are using a sales database from a record store. We'll find the artists that have produced the largest number of Rock and Metal songs.

Let's load some data:

```{code-cell} ipython3
import urllib.request
from pathlib import Path
from sqlite3 import connect

if not Path('my.db').is_file():
    url = "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
    urllib.request.urlretrieve(url, 'my.db')
```

Initialize the extension and set `autolimit=3` so we only retrieve a few rows.

```{code-cell} ipython3
%load_ext sql
```

```{code-cell} ipython3
%config SqlMagic.autolimit = 3
```

Let's see the track-level information:

```{code-cell} ipython3
%%sql sqlite:///my.db
SELECT * FROM Track
```

Let's join track with album and artist to get the artist name and store the query using `--save tracks_with_info`.

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

Let's subset the genres we are interested in (Rock and Metal) and save the query.

```{code-cell} ipython3
%%sql --save genres_fav
SELECT * FROM Genre
WHERE Name
LIKE '%rock%'
OR Name LIKE '%metal%' 
```

Now, join genres and tracks, so we only get Rock and Metal tracks. 

Note that we are using `--with`; this will retrieve previously saved queries, and preprend them (using CTEs), then, we save the query in `track_fav` .

```{code-cell} ipython3
%%sql --with genres_fav --with tracks_with_info --save track_fav
SELECT t.*
FROM tracks_with_info t
JOIN genres_fav
ON t.GenreId = genres_fav.GenreId
```

We can now use `track_fav` (which contains Rock and Metal tracks). Let's find which artists have produced the most tracks (and save the query):

```{code-cell} ipython3
%%sql --with track_fav --save top_artist
SELECT artist, COUNT(*) FROM track_fav
GROUP BY artist
ORDER BY COUNT(*) DESC
```

Let's retrieve `top_artist` and plot the results:

```{code-cell} ipython3
top_artist = %sql --with top_artist SELECT * FROM top_artist
top_artist.bar()
```

We can render the full query with the `%sqlrender` magic:

```{code-cell} ipython3
final = %sqlrender top_artist
print(final)
```

We can verify the retrieved query returns the same result:

```{code-cell} ipython3
%%sql
$final
```
