try:
    from pyspark.sql import DataFrame
    from pyspark.sql.connect.dataframe import DataFrame as CDataFrame
except ModuleNotFoundError:
    DataFrame = None
    CDataFrame = None

from sql import exceptions


def handle_spark_dataframe(dataframe, should_cache=False):
    """Execute a ResultSet sqlaproxy using pysark module."""
    if not DataFrame and not CDataFrame:
        raise exceptions.MissingPackageError("pysark not installed")

    return SparkResultProxy(dataframe, dataframe.columns, should_cache)


class SparkResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy from
    SqlAlchemy.
    """

    dataframe = None

    def __init__(self, dataframe, headers, should_cache):
        self.dataframe = dataframe
        self.fetchall = dataframe.collect
        self.rowcount = dataframe.count()
        self.keys = lambda: headers
        self.cursor = SparkCursor(headers)
        self.returns_rows = True
        if should_cache:
            self.dataframe.cache()

    def fetchmany(self, size):
        return self.dataframe.take(size)

    def fetchone(self):
        return self.dataframe.head()

    def close(self):
        self.dataframe.unpersist()


class SparkCursor(object):
    """Class to extend to give SqlAlchemy Cursor like behaviour"""

    description = None

    def __init__(self, headers) -> None:
        self.description = headers
