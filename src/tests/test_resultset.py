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

import re


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
    return ResultSet(result, config).fetch_results()


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
    resultset = ResultSet(result, config).fetch_results()
    assert resultset.dict() == {"x": (0,)}


def test_resultset_with_non_sqlalchemy_results(config):
    df = pd.DataFrame({"x": range(3)})  # noqa
    conn = Connection(engine=create_engine("duckdb://"))
    result = conn.execute("SELECT * FROM df")
    assert ResultSet(result, config).fetch_results() == [(0,), (1,), (2,)]


def test_none_pretty(config):
    conn = Connection(engine=create_engine("sqlite://"))
    result = conn.execute("create table some_table (name, age)")
    result_set = ResultSet(result, config)
    assert result_set.pretty is None
    assert "" == str(result_set)


def test_lazy_loading(result, config):
    resultset = ResultSet(result, config)
    assert len(resultset._results) == 0
    resultset.fetch_results()
    assert len(resultset._results) == 3


@pytest.mark.parametrize(
    "autolimit, expected",
    [
        (None, 3),
        (False, 3),
        (0, 3),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
)
def test_lazy_loading_autolimit(result, config, autolimit, expected):
    config.autolimit = autolimit
    resultset = ResultSet(result, config)
    assert len(resultset._results) == 0
    resultset.fetch_results()
    assert len(resultset._results) == expected


@pytest.mark.parametrize(
    "displaylimit, expected",
    [
        (0, 3),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
)
def test_lazy_loading_displaylimit(result, config, displaylimit, expected):
    config.displaylimit = displaylimit
    result_set = ResultSet(result, config)

    assert len(result_set._results) == 0
    result_set.fetch_results()
    html = result_set._repr_html_()
    row_count = _get_number_of_rows_in_html_table(html)
    assert row_count == expected


@pytest.mark.parametrize(
    "displaylimit, expected_display",
    [
        (0, 3),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
)
def test_lazy_loading_displaylimit_fetch_all(
    result, config, displaylimit, expected_display
):
    max_results_count = 3
    config.autolimit = False
    config.displaylimit = displaylimit
    result_set = ResultSet(result, config)

    # Initialize result_set without fetching results
    assert len(result_set._results) == 0

    # Fetch the min number of rows (based on configuration)
    result_set.fetch_results()

    html = result_set._repr_html_()
    row_count = _get_number_of_rows_in_html_table(html)
    expected_results = (
        max_results_count
        if expected_display + 1 >= max_results_count
        else expected_display + 1
    )

    assert len(result_set._results) == expected_results
    assert row_count == expected_display

    # Fetch the the rest results, but don't display them in the table
    result_set.fetch_results(fetch_all=True)

    html = result_set._repr_html_()
    row_count = _get_number_of_rows_in_html_table(html)

    assert len(result_set._results) == max_results_count
    assert row_count == expected_display


@pytest.mark.parametrize(
    "displaylimit, expected_display",
    [
        (0, 3),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
)
def test_lazy_loading_list(result, config, displaylimit, expected_display):
    max_results_count = 3
    config.autolimit = False
    config.displaylimit = displaylimit
    result_set = ResultSet(result, config)

    # Initialize result_set without fetching results
    assert len(result_set._results) == 0

    # Fetch the min number of rows (based on configuration)
    result_set.fetch_results()

    expected_results = (
        max_results_count
        if expected_display + 1 >= max_results_count
        else expected_display + 1
    )

    assert len(result_set._results) == expected_results
    assert len(list(result_set)) == max_results_count


@pytest.mark.parametrize(
    "autolimit, expected_results",
    [
        (0, 3),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
)
def test_lazy_loading_autolimit_list(result, config, autolimit, expected_results):
    config.autolimit = autolimit
    result_set = ResultSet(result, config)
    assert len(result_set._results) == 0

    result_set.fetch_results()

    assert len(result_set._results) == expected_results
    assert len(list(result_set)) == expected_results


def _get_number_of_rows_in_html_table(html):
    """
    Returns the number of <tr> tags within the <tbody> section
    """
    pattern = r"<tbody>(.*?)<\/tbody>"
    tbody_content = re.findall(pattern, html, re.DOTALL)[0]
    row_count = len(re.findall(r"<tr>", tbody_content))

    return row_count
