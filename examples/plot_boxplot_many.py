from pathlib import Path
import urllib.request

from sql.connection import Connection

from sql import plot


if not Path("iris.csv").is_file():
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
        "iris.csv",
    )

conn = Connection.from_connect_str("duckdb://").session

plot.boxplot("iris.csv", ["petal width", "sepal width"], conn=conn)
