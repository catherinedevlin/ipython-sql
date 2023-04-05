import urllib.request

from sqlalchemy import create_engine

from sql import plot


urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
    "iris.csv",
)

conn = create_engine("duckdb://")

plot.histogram("iris.csv", ["petal width", "sepal width"], bins=50, conn=conn)
