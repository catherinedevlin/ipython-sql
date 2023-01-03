from pathlib import Path
import urllib.request

# this requires duckdb: pip install duckdb
import duckdb

from sql import plot


if not Path("iris.csv").is_file():
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
        "iris.csv",
    )

conn = duckdb.connect(database=":memory:")

plot.boxplot("iris.csv", "petal width", conn=conn, orient="h")
