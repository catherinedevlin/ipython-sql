from sql.connection import DBAPIConnection
from sql.run.resultset import ResultSet

from sql import _testing


class Config:
    autopandas = None
    autopolars = None
    autocommit = True
    feedback = True
    polars_dataframe_kwargs = {}
    style = "DEFAULT"
    autolimit = 0
    displaylimit = 10


def test_resultset(setup_postgreSQL):
    import psycopg2

    config = _testing.DatabaseConfigHelper.get_database_config("postgreSQL")

    conn_raw = psycopg2.connect(
        database=config["database"],
        user=config["username"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
    )
    conn = DBAPIConnection(conn_raw)

    statement = "SELECT 'hello' AS greeting;"
    results = conn.raw_execute(statement)

    rs = ResultSet(results, Config, statement, conn)

    assert rs.keys == ["greeting"]
    assert rs._is_dbapi_results
