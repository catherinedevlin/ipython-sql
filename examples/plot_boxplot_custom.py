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

# returns matplotlib.Axes object
ax = plot.boxplot("iris.csv", "petal width", conn=conn)
ax.set_title("My custom title")
ax.grid()
