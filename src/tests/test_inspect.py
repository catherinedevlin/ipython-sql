import sqlite3
import pytest
from functools import partial


from sql import inspect, connection


@pytest.fixture
def sample_db(tmp_empty):
    conn = connection.Connection.from_connect_str("sqlite://")

    conn.session.execute("CREATE TABLE one (x INT, y TEXT)")
    conn.session.execute("CREATE TABLE another (i INT, j TEXT)")

    sqlite3.connect("my.db").close()

    conn.session.execute("ATTACH DATABASE 'my.db' AS schema")


@pytest.mark.parametrize(
    "function",
    [
        inspect.get_table_names,
        partial(inspect.get_columns, name="some_name"),
    ],
)
def test_no_active_session(function):
    with pytest.raises(RuntimeError, match="No active connection"):
        function()


def test_tables(sample_db):
    tables = inspect.get_table_names()

    assert "Name" in repr(tables)
    assert "one" in repr(tables)
    assert "another" in repr(tables)

    assert "<table>" in tables._repr_html_()
    assert "Name" in tables._repr_html_()
    assert "one" in tables._repr_html_()
    assert "another" in tables._repr_html_()


@pytest.mark.parametrize(
    "name, first, second",
    [
        ["one", "x", "y"],
        ["another", "i", "j"],
    ],
)
def test_get_column(sample_db, name, first, second):
    columns = inspect.get_columns(name)

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

    assert str(excinfo.value) == error
