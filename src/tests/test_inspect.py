from pathlib import Path
from unittest.mock import Mock

from inspect import getsource
import pytest
from functools import partial

from IPython.core.error import UsageError
from prettytable import PrettyTable

from sql import inspect, connection


@pytest.fixture
def sample_db(ip_empty, tmp_empty):
    ip_empty.run_cell("%sql sqlite:///first.db --alias first")
    ip_empty.run_cell("%sql CREATE TABLE one (x INT, y TEXT)")
    ip_empty.run_cell("%sql CREATE TABLE another (i INT, j TEXT)")
    ip_empty.run_cell("%sql sqlite:///second.db --alias second")
    ip_empty.run_cell("%sql CREATE TABLE uno (x INT, y TEXT)")
    ip_empty.run_cell("%sql CREATE TABLE dos (i INT, j TEXT)")
    ip_empty.run_cell("%sql --close second")
    ip_empty.run_cell("%sql first")
    ip_empty.run_cell("%sql ATTACH DATABASE 'second.db' AS schema")

    yield

    ip_empty.run_cell("%sql --close first")
    Path("first.db").unlink()
    Path("second.db").unlink()


@pytest.mark.parametrize(
    "function",
    [
        inspect.get_table_names,
        partial(inspect.get_columns, name="some_name"),
        inspect.get_schema_names,
    ],
)
def test_no_active_session(function, monkeypatch):
    monkeypatch.setattr(connection.ConnectionManager, "current", None)

    with pytest.raises(UsageError, match="No active connection") as excinfo:
        function()

    assert excinfo.value.error_type == "RuntimeError"


@pytest.mark.parametrize(
    "first, second, schema",
    [
        ["one", "another", None],
        ["uno", "dos", "schema"],
    ],
)
def test_tables(sample_db, first, second, schema):
    tables = inspect.get_table_names(schema=schema)

    assert "Name" in repr(tables)
    assert first in repr(tables)
    assert second in repr(tables)

    assert "<table>" in tables._repr_html_()
    assert "Name" in tables._repr_html_()
    assert first in tables._repr_html_()
    assert second in tables._repr_html_()


@pytest.mark.parametrize(
    "name, first, second, schema",
    [
        ["one", "x", "y", None],
        ["another", "i", "j", None],
        ["uno", "x", "y", "schema"],
        ["dos", "i", "j", "schema"],
    ],
)
def test_get_column(sample_db, name, first, second, schema):
    columns = inspect.get_columns(name, schema=schema)

    assert "name" in repr(columns)
    assert first in repr(columns)
    assert second in repr(columns)

    assert "<table>" in columns._repr_html_()
    assert "name" in columns._repr_html_()
    assert first in columns._repr_html_()
    assert second in columns._repr_html_()


@pytest.mark.parametrize(
    "name, schema, error",
    [
        [
            "some_table",
            "schema",
            "There is no table with name 'some_table' in schema 'schema'",
        ],
        [
            "name",
            None,
            "There is no table with name 'name' in the default schema",
        ],
    ],
)
def test_nonexistent_table(sample_db, name, schema, error):
    with pytest.raises(UsageError) as excinfo:
        inspect.get_columns(name, schema)

    assert excinfo.value.error_type == "TableNotFoundError"
    assert error.lower() in str(excinfo.value).lower()


@pytest.mark.parametrize(
    "function",
    [
        inspect.get_table_names,
        inspect.get_columns,
    ],
)
def test_telemetry(function):
    assert "@telemetry.log_call" in getsource(function)


def test_get_schema_names(ip):
    ip.run_cell(
        """%%sql sqlite:///my.db
CREATE TABLE IF NOT EXISTS test_table (id INT)
"""
    )

    ip.run_cell(
        """%%sql
ATTACH DATABASE 'my.db' AS test_schema
"""
    )

    expected_schema_names = ["main", "test_schema"]
    schema_names = inspect.get_schema_names()
    for schema in schema_names:
        assert schema in expected_schema_names


@pytest.mark.parametrize(
    "get_columns, rows, field_names, name, schema",
    [
        [
            [
                {"column_a": "a", "column_b": "b"},
                # the second row does not have column_b
                {
                    "column_a": "a2",
                },
            ],
            [["a", "b"], ["a2", ""]],
            ["column_a", "column_b"],
            "test_table",
            None,
        ],
        [
            [
                {"column_a": "a", "column_b": "b"},
                # the second row does not have column_b
                {
                    "column_a": "a2",
                },
            ],
            [["a", "b"], ["a2", ""]],
            ["column_a", "column_b"],
            "another_table",
            "another_schema",
        ],
        [
            [
                {
                    "column_a": "a2",
                },
                # contains an extra column
                {"column_a": "a", "column_b": "b"},
            ],
            [["a2", ""], ["a", "b"]],
            ["column_a", "column_b"],
            "test_table",
            None,
        ],
        [
            [
                {"column_a": "a", "column_b": "b"},
                {"column_b": "b2", "column_a": "a2"},
            ],
            [["a", "b"], ["a2", "b2"]],
            ["column_a", "column_b"],
            "test_table",
            None,
        ],
        [
            [
                dict(),
                dict(),
            ],
            [[], []],
            [],
            "test_table",
            None,
        ],
        [
            None,
            [],
            [],
            "test_table",
            None,
        ],
    ],
    ids=[
        "missing-val-second-row",
        "missing-val-second-row-another-schema",
        "extra-val-second-row",
        "keeps-order",
        "empty-dictionaries",
        "none-return-value",
    ],
)
def test_columns_with_missing_values(
    tmp_empty, ip, monkeypatch, get_columns, rows, field_names, name, schema
):
    mock = Mock()
    mock.get_columns.return_value = get_columns

    monkeypatch.setattr(inspect, "_get_inspector", lambda _: mock)

    ip.run_cell(
        """%%sql sqlite:///another.db
CREATE TABLE IF NOT EXISTS another_table (id INT)
"""
    )

    ip.run_cell(
        """%%sql sqlite:///my.db
CREATE TABLE IF NOT EXISTS test_table (id INT)
"""
    )

    ip.run_cell(
        """%%sql
ATTACH DATABASE 'another.db' as 'another_schema';
"""
    )

    pt = PrettyTable(field_names=field_names)
    pt.add_rows(rows)

    assert str(inspect.get_columns(name=name, schema=schema)) == str(pt)
