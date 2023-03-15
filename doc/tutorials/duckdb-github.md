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
    description lang=en: Use JupySQL and DuckDB to query JSON files with SQL
    keywords: jupyter, sql, jupysql, json, duckdb
    property=og:locale: en_US
---

# Analyzing Github Data with JupySQL + DuckDB

JupySQL and DuckDB have many use cases. Here, let's query the Github REST API to run some analysis using these tools. 

```{code-cell} ipython3
:tags: [remove-cell]
from pathlib import Path

paths = ["jupyterdata.json", "jupyterdata.csv"]

for path in paths:
    path = Path(path)

    if path.exists():
        print(f"Deleting {path}")
        path.unlink()
```

```{code-cell} ipython3
:tags: [hide-output]
%pip install jupysql duckdb duckdb-engine rich --quiet
```

## Pulling from Github API

First, let's pull information on repositories relating to 'Jupyter' from the Github API. Some operations may require a token, but accessing them is very simple if you have a Github account. More information on authentication can be found [here](https://docs.github.com/en/rest/guides/getting-started-with-the-rest-api?apiVersion=2022-11-28#authenticating). Our query will pull any repository relating to Jupyter, sorted by most to least stars.

```{code-cell} ipython3
import requests
import json
from pathlib import Path

res = requests.get(
    'https://api.github.com/search/repositories?q=jupyter&sort=stars&order=desc',
)
```

We then parse the information pulled from the API into a JSON format that we can run analysis on with JupySQL. We also need to save it locally as a `.json` file. Let's make it easier by only dumping the 'items' array.

```{code-cell} ipython3
parsed = res.json()

_ = Path("jupyterdata.json").write_text(json.dumps(parsed['items'], indent=4))
```

## Querying JSON File

Let's get some information on our first result. Load the extension and start a DuckDB in-memory database:

```{code-cell} ipython3
%load_ext sql
%sql duckdb://
```
Looking at our .json file, we have information on thousands of repositories. To start, let's load information on our results.

```{code-cell} ipython3
:tags: [hide-output]
%%sql
SELECT *
FROM read_json_auto('jupyterdata.json')
```

However, this is a lot of information. After seeing what we're working with, let's pull the name of the repository, the author, the description, and the URL to make things cleaner. Let's also limit our results to the top 5 starred repos. 

```{code-cell} ipython3
%%sql
SELECT 
    name AS name,
    owner.login AS user,
    description AS description,
    html_url AS URL,
    stargazers_count AS stars
FROM read_json_auto('jupyterdata.json')
LIMIT 5
```

We can also load all of the pulled repositories that, say, have a certain range of stars:

```{code-cell} ipython3
%%sql
SELECT 
    name AS name,
    owner.login AS user,
    description AS description,
    html_url AS URL,
    stargazers_count AS stars
FROM read_json_auto('jupyterdata.json')
WHERE stargazers_count < 15000 AND stargazers_count > 10000 
```

And save it to a .csv file:

```{code-cell} ipython3
%%sql
COPY (
    SELECT
    name AS name,
    owner.login AS user,
    description AS description,
    html_url AS URL,
    stargazers_count AS stars
    FROM read_json_auto('jupyterdata.json')
    WHERE stargazers_count < 15000 AND stargazers_count > 10000 
)

TO 'jupyterdata.csv' (HEADER, DELIMITER ',');
```

```{code-cell} ipython3
%%sql
SELECT * FROM 'jupyterdata.csv'
```

There's no shortage of information that we can pull from this API, so this is just one example. Feel free to give it a try yourself— or explore using JupySQL with another API or `.json` file!

