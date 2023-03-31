from inspect import getsource
import sqlite3
import pytest
from functools import partial

from sql import inspect, connection


@pytest.fixture
def sample_db(tmp_empty):
    conn = connection.Connection.from_connect_str("sqlite://")

    conn.session.execute("CREATE TABLE one (x INT, y TEXT)")
    conn.session.execute("CREATE TABLE another (i INT, j TEXT)")

    conn_mydb = sqlite3.connect("my.db")
    conn_mydb.execute("CREATE TABLE uno (x INT, y TEXT)")
    conn_mydb.execute("CREATE TABLE dos (i INT, j TEXT)")
    conn_mydb.close()

    conn.session.execute("ATTACH DATABASE 'my.db' AS schema")


@pytest.mark.parametrize(
    "function",
    [
        inspect.get_table_names,
        partial(inspect.get_columns, name="some_name"),
        inspect.get_schema_names,
    ],
)
def test_no_active_session(function, monkeypatch):
    monkeypatch.setattr(connection.Connection, "current", None)
    with pytest.raises(RuntimeError, match="No active connection"):
        function()


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
def test_nonexistent_table(name, schema, error):
    with pytest.raises(ValueError) as excinfo:
        inspect.get_columns(name, schema)

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
