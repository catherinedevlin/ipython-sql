from pathlib import Path
import urllib.request

from sqlalchemy import create_engine

from sql.connection import SQLAlchemyConnection
from sql import plot


if not Path("iris.csv").is_file():
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
        "iris.csv",
    )

conn = SQLAlchemyConnection(create_engine("duckdb://"))

plot.boxplot("iris.csv", "petal width", conn=conn)
