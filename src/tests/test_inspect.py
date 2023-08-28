from pathlib import Path
from unittest.mock import Mock

from inspect import getsource
import pytest
from functools import partial

from IPython.core.error import UsageError
from prettytable import PrettyTable

from sql import inspect, connection


EXPECTED_SUGGESTIONS_MESSAGE = "Did you mean:"
EXPECTED_NO_TABLE_IN_SCHEMA = "There is no table with name {0!r} in schema {1!r}"
EXPECTED_NO_TABLE_IN_DEFAULT_SCHEMA = (
    "There is no table with name {0!r} in the default schema"
)


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
    "table, offset, n_rows, expected_rows, expected_columns",
    [
        ("number_table", 0, 0, [], ["x", "y"]),
        ("number_table", 5, 0, [], ["x", "y"]),
        ("number_table", 50, 0, [], ["x", "y"]),
        ("number_table", 50, 10, [], ["x", "y"]),
        (
            "number_table",
            2,
            10,
            [(2, 4), (0, 2), (-5, -1), (-2, -3), (-2, -3), (-4, 2), (2, -5), (4, 3)],
            ["x", "y"],
        ),
        (
            "number_table",
            2,
            100,
            [(2, 4), (0, 2), (-5, -1), (-2, -3), (-2, -3), (-4, 2), (2, -5), (4, 3)],
            ["x", "y"],
        ),
        ("number_table", 0, 2, [(4, -2), (-5, 0)], ["x", "y"]),
        ("number_table", 2, 2, [(2, 4), (0, 2)], ["x", "y"]),
        (
            "number_table",
            2,
            5,
            [(2, 4), (0, 2), (-5, -1), (-2, -3), (-2, -3)],
            ["x", "y"],
        ),
        ("empty_table", 2, 5, [], ["column", "another"]),
    ],
)
def test_fetch_sql_with_pagination_no_sort(
    ip, table, offset, n_rows, expected_rows, expected_columns
):
    rows, columns = inspect.fetch_sql_with_pagination(table, offset, n_rows)

    assert rows == expected_rows
    assert columns == expected_columns


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


@pytest.mark.parametrize(
    "table",
    ["no_such_table", ""],
)
def test_fetch_sql_with_pagination_no_table_error(ip, table):
    with pytest.raises(UsageError) as excinfo:
        inspect.fetch_sql_with_pagination(table, 0, 2)

    assert excinfo.value.error_type == "TableNotFoundError"


def test_fetch_sql_with_pagination_none_table(ip):
    with pytest.raises(UsageError) as excinfo:
        inspect.fetch_sql_with_pagination(None, 0, 2)

    assert excinfo.value.error_type == "UsageError"


@pytest.mark.parametrize(
    "table, offset, n_rows, sort_by, order_by, expected_rows, expected_columns",
    [
        ("number_table", 0, 0, "x", "DESC", [], ["x", "y"]),
        ("number_table", 5, 0, "x", "DESC", [], ["x", "y"]),
        ("number_table", 50, 0, "y", "ASC", [], ["x", "y"]),
        ("number_table", 50, 10, "y", "ASC", [], ["x", "y"]),
        ("number_table", 0, 2, "x", "DESC", [(4, -2), (4, 3)], ["x", "y"]),
        ("number_table", 0, 2, "x", "ASC", [(-5, 0), (-5, -1)], ["x", "y"]),
        ("empty_table", 2, 5, "column", "ASC", [], ["column", "another"]),
        ("number_table", 2, 2, "x", "ASC", [(-4, 2), (-2, -3)], ["x", "y"]),
        ("number_table", 2, 2, "x", "DESC", [(2, 4), (2, -5)], ["x", "y"]),
        (
            "number_table",
            2,
            10,
            "x",
            "DESC",
            [(2, 4), (2, -5), (0, 2), (-2, -3), (-2, -3), (-4, 2), (-5, 0), (-5, -1)],
            ["x", "y"],
        ),
        (
            "number_table",
            2,
            100,
            "x",
            "DESC",
            [(2, 4), (2, -5), (0, 2), (-2, -3), (-2, -3), (-4, 2), (-5, 0), (-5, -1)],
            ["x", "y"],
        ),
        (
            "number_table",
            2,
            5,
            "y",
            "ASC",
            [(-2, -3), (4, -2), (-5, -1), (-5, 0), (0, 2)],
            ["x", "y"],
        ),
    ],
)
def test_fetch_sql_with_pagination_with_sort(
    ip, table, offset, n_rows, sort_by, order_by, expected_rows, expected_columns
):
    rows, columns = inspect.fetch_sql_with_pagination(
        table, offset, n_rows, sort_by, order_by
    )

    assert rows == expected_rows
    assert columns == expected_columns


@pytest.mark.parametrize(
    "table, expected_result",
    [
        ("number_table", True),
        ("test", True),
        ("author", True),
        ("empty_table", True),
        ("numbers1", False),
        ("test1", False),
        ("author1", False),
        ("empty_table1", False),
        (None, False),
    ],
)
def test_is_table_exists_ignore_error(ip, table, expected_result):
    assert expected_result is inspect.is_table_exists(table, ignore_error=True)


@pytest.mark.parametrize(
    "table, expected_error, error_type",
    [
        ("number_table", False, "TableNotFoundError"),
        ("test", False, "TableNotFoundError"),
        ("author", False, "TableNotFoundError"),
        ("empty_table", False, "TableNotFoundError"),
        ("numbers1", True, "TableNotFoundError"),
        ("test1", True, "TableNotFoundError"),
        ("author1", True, "TableNotFoundError"),
        ("empty_table1", True, "TableNotFoundError"),
        (None, True, "UsageError"),
    ],
)
def test_is_table_exists(ip, table, expected_error, error_type):
    if expected_error:
        with pytest.raises(UsageError) as excinfo:
            inspect.is_table_exists(table)

        assert excinfo.value.error_type == error_type
    else:
        inspect.is_table_exists(table)


@pytest.mark.parametrize(
    "table, expected_error, expected_suggestions",
    [
        ("number_table", None, []),
        ("number_tale", UsageError, ["number_table"]),
        ("_table", UsageError, ["number_table", "empty_table"]),
        (None, UsageError, []),
    ],
)
def test_is_table_exists_with(ip, table, expected_error, expected_suggestions):
    with_ = ["temp"]

    ip.run_cell(
        f"""
        %%sql --save {with_[0]} --no-execute
        SELECT *
        FROM {table}
        WHERE x > 2
        """
    )
    if expected_error:
        with pytest.raises(expected_error) as error:
            inspect.is_table_exists(table)

        error_suggestions_arr = str(error.value).split(EXPECTED_SUGGESTIONS_MESSAGE)

        if len(expected_suggestions) > 0:
            assert len(error_suggestions_arr) > 1
            for suggestion in expected_suggestions:
                assert suggestion in error_suggestions_arr[1]
        else:
            assert len(error_suggestions_arr) == 1
    else:
        inspect.is_table_exists(table)


def test_get_list_of_existing_tables(ip):
    expected = ["author", "empty_table", "number_table", "test", "website"]
    list_of_tables = inspect._get_list_of_existing_tables()
    for table in expected:
        assert table in list_of_tables


@pytest.mark.parametrize(
    "table, query, suggestions",
    [
        ("tes", "%sqlcmd columns --table {}", ["test"]),
        ("_table", "%sqlcmd columns --table {}", ["empty_table", "number_table"]),
        ("no_similar_tables", "%sqlcmd columns --table {}", []),
        ("tes", "%sqlcmd profile --table {}", ["test"]),
        ("_table", "%sqlcmd profile --table {}", ["empty_table", "number_table"]),
        ("no_similar_tables", "%sqlcmd profile --table {}", []),
        ("tes", "%sqlplot histogram --table {} --column x", ["test"]),
        ("tes", "%sqlplot boxplot --table {} --column x", ["test"]),
    ],
)
def test_bad_table_error_message(ip, table, query, suggestions):
    query = query.format(table)

    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(query)

    expected_error_message = EXPECTED_NO_TABLE_IN_DEFAULT_SCHEMA.format(table)

    error_message = str(excinfo.value)
    assert str(expected_error_message).lower() in error_message.lower()

    error_suggestions_arr = error_message.split(EXPECTED_SUGGESTIONS_MESSAGE)

    if len(suggestions) > 0:
        assert len(error_suggestions_arr) > 1
        for suggestion in suggestions:
            assert suggestion in error_suggestions_arr[1]


@pytest.mark.parametrize(
    "table, schema, query, suggestions",
    [
        (
            "test_table",
            "invalid_name_no_match",
            "%sqlcmd columns --table {} --schema {}",
            [],
        ),
        (
            "test_table",
            "te_schema",
            "%sqlcmd columns --table {} --schema {}",
            ["test_schema"],
        ),
        (
            "invalid_name_no_match",
            "test_schema",
            "%sqlcmd columns --table {} --schema {}",
            [],
        ),
        (
            "test_tabl",
            "test_schema",
            "%sqlcmd columns --table {} --schema {}",
            ["test_table", "test"],
        ),
        (
            "invalid_name_no_match",
            "invalid_name_no_match",
            "%sqlcmd columns --table {} --schema {}",
            [],
        ),
        (
            "_table",
            "_schema",
            "%sqlcmd columns --table {} --schema {}",
            ["test_schema"],
        ),
    ],
)
def test_bad_table_error_message_with_schema(ip, query, suggestions, table, schema):
    query = query.format(table, schema)

    expected_error_message = EXPECTED_NO_TABLE_IN_SCHEMA.format(table, schema)

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

    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(query)

    error_message = str(excinfo.value)
    assert str(expected_error_message).lower() in error_message.lower()

    error_suggestions_arr = error_message.split(EXPECTED_SUGGESTIONS_MESSAGE)

    if len(suggestions) > 0:
        assert len(error_suggestions_arr) > 1
        for suggestion in suggestions:
            assert suggestion in error_suggestions_arr[1]
