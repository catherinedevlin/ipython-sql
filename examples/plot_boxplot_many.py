from pathlib import Path
import urllib.request

from sqlalchemy import create_engine

from sql import plot


if not Path("iris.csv").is_file():
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
        "iris.csv",
    )

conn = create_engine("duckdb://")

plot.boxplot("iris.csv", ["petal width", "sepal width"], conn=conn)
