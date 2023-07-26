import sqlite3
from unittest.mock import Mock

from IPython.core.error import UsageError
import pandas
import polars
import pytest
from sqlalchemy import create_engine
import duckdb

from sql.connection import SQLAlchemyConnection, DBAPIConnection
from sql.run.run import (
    run_statements,
    is_postgres_or_redshift,
    select_df_type,
)
from sql.run.pgspecial import handle_postgres_special
from sql.run.resultset import ResultSet


@pytest.fixture
def mock_conns():
    conn = SQLAlchemyConnection(Mock())
    conn.connection_sqlalchemy.execution_options.side_effect = ValueError
    return conn


class Config:
    autopandas = None
    autopolars = None
    autocommit = True
    feedback = True
    polars_dataframe_kwargs = {}
    style = "DEFAULT"
    autolimit = 0
    displaylimit = 10


class ConfigPandas(Config):
    autopandas = True
    autopolars = False


class ConfigPolars(Config):
    autopandas = False
    autopolars = True


@pytest.fixture
def pytds_conns(mock_conns):
    mock_conns._dialect = "mssql+pytds"
    return mock_conns


@pytest.fixture
def mock_resultset():
    class ResultSet:
        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def DataFrame(cls):
            return pandas.DataFrame()

        @classmethod
        def PolarsDataFrame(cls):
            return polars.DataFrame()

    return ResultSet


@pytest.mark.parametrize(
    "dialect",
    [
        "postgres",
        "redshift",
    ],
)
def test_is_postgres_or_redshift(dialect):
    assert is_postgres_or_redshift(dialect) is True


def test_handle_postgres_special(mock_conns):
    with pytest.raises(UsageError) as excinfo:
        handle_postgres_special(mock_conns, "\\")

    assert "pgspecial not installed" in str(excinfo.value)


def test_select_df_type_is_pandas(mock_resultset):
    output = select_df_type(mock_resultset, ConfigPandas)
    assert isinstance(output, pandas.DataFrame)


def test_select_df_type_is_polars(mock_resultset):
    output = select_df_type(mock_resultset, ConfigPolars)
    assert isinstance(output, polars.DataFrame)


def test_sql_starts_with_begin(mock_conns):
    with pytest.raises(UsageError, match="does not support transactions") as excinfo:
        run_statements(mock_conns, "BEGIN", Config)

    assert excinfo.value.error_type == "RuntimeError"


def test_sql_is_empty(mock_conns):
    assert run_statements(mock_conns, "  ", Config) == "Connected: %s" % mock_conns.name


@pytest.mark.parametrize(
    "connection",
    [
        SQLAlchemyConnection(create_engine("duckdb://")),
        SQLAlchemyConnection(create_engine("sqlite://")),
        DBAPIConnection(duckdb.connect()),
        DBAPIConnection(sqlite3.connect("")),
    ],
    ids=[
        "duckdb-sqlalchemy",
        "sqlite-sqlalchemy",
        "duckdb",
        "sqlite",
    ],
)
@pytest.mark.parametrize(
    "config, expected_type",
    [
        [Config, ResultSet],
        [ConfigPandas, pandas.DataFrame],
        [ConfigPolars, polars.DataFrame],
    ],
)
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT 1; SELECT 2;",
    ],
    ids=["single", "multiple"],
)
def test_run(connection, config, expected_type, sql):
    out = run_statements(connection, sql, config)
    assert isinstance(out, expected_type)


def test_do_not_fail_if_sqlalchemy_autocommit_not_supported():
    conn = SQLAlchemyConnection(create_engine("sqlite://"))
    conn.connection_sqlalchemy.execution_options = Mock(
        side_effect=Exception("AUTOCOMMIT not supported!")
    )

    run_statements(conn, "SELECT 1", Config)

    # TODO: test .commit called or not depending on config!
