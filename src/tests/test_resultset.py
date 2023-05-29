from sqlalchemy import create_engine
from sql.connection import Connection
from pathlib import Path
from unittest.mock import Mock

import pytest
import pandas as pd
import polars as pl
import sqlalchemy

from sql.run import ResultSet
from sql import run as run_module


@pytest.fixture
def config():
    config = Mock()
    config.displaylimit = 5
    config.autolimit = 100
    return config


@pytest.fixture
def result():
    df = pd.DataFrame({"x": range(3)})  # noqa
    engine = sqlalchemy.create_engine("duckdb://")

    conn = engine.connect()
    result = conn.execute(sqlalchemy.text("select * from df"))
    yield result
    conn.close()


@pytest.fixture
def result_set(result, config):
    return ResultSet(result, config)


def test_resultset_getitem(result_set):
    assert result_set[0] == (0,)
    assert result_set[0:2] == [(0,), (1,)]


def test_resultset_dict(result_set):
    assert result_set.dict() == {"x": (0, 1, 2)}


def test_resultset_dicts(result_set):
    assert list(result_set.dicts()) == [{"x": 0}, {"x": 1}, {"x": 2}]


def test_resultset_dataframe(result_set, monkeypatch):
    monkeypatch.setattr(run_module.Connection, "current", Mock())

    assert result_set.DataFrame().equals(pd.DataFrame({"x": range(3)}))


def test_resultset_polars_dataframe(result_set, monkeypatch):
    assert result_set.PolarsDataFrame().frame_equal(pl.DataFrame({"x": range(3)}))


def test_resultset_csv(result_set, tmp_empty):
    result_set.csv("file.csv")

    assert Path("file.csv").read_text() == "x\n0\n1\n2\n"


def test_resultset_str(result_set):
    assert str(result_set) == "+---+\n| x |\n+---+\n| 0 |\n| 1 |\n| 2 |\n+---+"


def test_resultset_repr_html(result_set):
    assert result_set._repr_html_() == (
        "<table>\n    <thead>\n        <tr>\n            "
        "<th>x</th>\n        </tr>\n    </thead>\n    <tbody>\n        "
        "<tr>\n            <td>0</td>\n        </tr>\n        <tr>\n            "
        "<td>1</td>\n        </tr>\n        <tr>\n            <td>2</td>\n        "
        "</tr>\n    </tbody>\n</table>"
    )


def test_resultset_config_autolimit_dict(result, config):
    config.autolimit = 1

    assert ResultSet(result, config).dict() == {"x": (0,)}


def test_resultset_with_non_sqlalchemy_results(config):
    df = pd.DataFrame({"x": range(3)})  # noqa
    conn = Connection(engine=create_engine("duckdb://"))
    result = conn.execute("SELECT * FROM df")
    assert ResultSet(result, config) == [(0,), (1,), (2,)]


def test_none_pretty(config):
    conn = Connection(engine=create_engine("sqlite://"))
    result = conn.execute("create table some_table (name, age)")
    result_set = ResultSet(result, config)
    assert result_set.pretty is None
    assert "" == str(result_set)
