import logging
from unittest.mock import Mock

import pandas
import polars
import pytest

import warnings

from sql.connection import Connection
from sql.run import (
    run,
    handle_postgres_special,
    is_postgres_or_redshift,
    select_df_type,
    set_autocommit,
    interpret_rowcount,
)


@pytest.fixture
def mock_conns():
    Connection.name = str()
    Connection.dialect = "postgres"
    return Connection


@pytest.fixture
def mock_config():
    class Config:
        autopandas = None
        autopolars = None
        autocommit = True
        feedback = True

    return Config


@pytest.fixture
def pytds_conns(mock_conns):
    mock_conns.dialect = "mssql+pytds"
    return mock_conns


@pytest.fixture
def config_pandas(mock_config):
    mock_config.autopandas = True
    mock_config.autopolars = False

    return mock_config


@pytest.fixture
def config_polars(mock_config):
    mock_config.autopandas = False
    mock_config.autopolars = True

    return mock_config


@pytest.fixture
def mock_resultset():
    class ResultSet:
        def __init__(self, *args, **kwargs):
            ...

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
    with pytest.raises(ImportError):
        handle_postgres_special(mock_conns, "\\")


def test_set_autocommit(mock_conns, mock_config, caplog):
    caplog.set_level(logging.DEBUG)
    output = set_autocommit(mock_conns, mock_config)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
    assert "The database driver doesn't support such " in caplog.records[0].msg
    assert output is True


def test_pytds_autocommit(pytds_conns, mock_config):
    with warnings.catch_warnings(record=True) as w:
        output = set_autocommit(pytds_conns, mock_config)
        assert (
            str(w[-1].message)
            == "Autocommit is not supported for pytds, thus is automatically disabled"
        )
        assert output is False


def test_select_df_type_is_pandas(monkeypatch, config_pandas, mock_resultset):
    monkeypatch.setattr("sql.run.select_df_type", mock_resultset.DataFrame())
    output = select_df_type(mock_resultset, config_pandas)
    assert isinstance(output, pandas.DataFrame)


def test_select_df_type_is_polars(monkeypatch, config_polars, mock_resultset):
    monkeypatch.setattr("sql.run.select_df_type", mock_resultset.PolarsDataFrame())
    output = select_df_type(mock_resultset, config_polars)
    assert isinstance(output, polars.DataFrame)


def test_sql_starts_with_begin(mock_conns, mock_config):
    with pytest.raises(ValueError, match="does not support transactions"):
        run(mock_conns, "BEGIN", mock_config)


def test_sql_is_empty(mock_conns, mock_config):
    assert run(mock_conns, "  ", mock_config) == "Connected: %s" % mock_conns.name


def test_run(monkeypatch, mock_conns, mock_resultset, config_pandas):
    monkeypatch.setattr("sql.run.handle_postgres_special", Mock())
    monkeypatch.setattr("sql.run._commit", Mock())
    monkeypatch.setattr("sql.run.interpret_rowcount", Mock())
    monkeypatch.setattr("sql.run.ResultSet", mock_resultset)

    output = run(mock_conns, "\\", config_pandas)
    assert isinstance(output, type(mock_resultset.DataFrame()))


def test_interpret_rowcount():
    assert interpret_rowcount(-1) == "Done."
    assert interpret_rowcount(1) == "%d rows affected." % 1


def test__commit_is_called(
    monkeypatch,
    mock_conns,
    mock_config,
):
    mock__commit = Mock()
    monkeypatch.setattr("sql.run._commit", mock__commit)
    monkeypatch.setattr("sql.run.handle_postgres_special", Mock())
    monkeypatch.setattr("sql.run.interpret_rowcount", Mock())
    monkeypatch.setattr("sql.run.ResultSet", Mock())

    run(mock_conns, "\\", mock_config)

    mock__commit.assert_called()
