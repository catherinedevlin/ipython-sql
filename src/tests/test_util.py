from datetime import datetime
import pytest
from sql import util
import json
from IPython.core.error import UsageError

ERROR_MESSAGE = "Table cannot be None"
EXPECTED_SUGGESTIONS_MESSAGE = "Did you mean :"
EXPECTED_NO_TABLE_IN_DEFAULT_SCHEMA = (
    "There is no table with name {0!r} in the default schema"
)
EXPECTED_NO_TABLE_IN_SCHEMA = "There is no table with name {0!r} in schema {1!r}"
EXPECTED_STORE_SUGGESTIONS = (
    "but there is a stored query.\nDid you miss passing --with {0}?"
)


@pytest.fixture
def ip_snippets(ip):
    ip.run_cell(
        """
%%sql --save a --no-execute
SELECT *
FROM number_table
"""
    )
    ip.run_cell(
        """
            %%sql --save b --no-execute
            SELECT *
            FROM a
            WHERE x > 5
            """
    )
    ip.run_cell(
        """
            %%sql --save c --no-execute
            SELECT *
            FROM a
            WHERE x < 5
            """
    )
    yield ip


@pytest.mark.parametrize(
    "store_table, query",
    [
        pytest.param(
            "a",
            "%sqlcmd columns --table {}",
            marks=pytest.mark.xfail(reason="this is not working yet, see #658"),
        ),
        pytest.param(
            "bbb",
            "%sqlcmd profile --table {}",
            marks=pytest.mark.xfail(reason="this is not working yet, see #658"),
        ),
        ("c_c", "%sqlplot histogram --table {} --column x"),
        ("d_d_d", "%sqlplot boxplot --table {} --column x"),
    ],
    ids=[
        "columns",
        "profile",
        "histogram",
        "boxplot",
    ],
)
def test_no_errors_with_stored_query(ip_empty, store_table, query):
    ip_empty.run_cell("%sql duckdb://")

    ip_empty.run_cell(
        """%%sql
CREATE TABLE numbers (
    x FLOAT
);

INSERT INTO numbers (x) VALUES (1), (2), (3);
"""
    )

    ip_empty.run_cell(
        f"""
        %%sql --save {store_table} --no-execute
        SELECT *
        FROM numbers
        """
    )

    out = ip_empty.run_cell(query.format(store_table, store_table))
    assert out.success


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
    assert expected_result is util.is_table_exists(table, ignore_error=True)


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
            util.is_table_exists(table)

        assert excinfo.value.error_type == error_type
    else:
        util.is_table_exists(table)


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
            util.is_table_exists(table)

        error_suggestions_arr = str(error.value).split(EXPECTED_SUGGESTIONS_MESSAGE)

        if len(expected_suggestions) > 0:
            assert len(error_suggestions_arr) > 1
            for suggestion in expected_suggestions:
                assert suggestion in error_suggestions_arr[1]
        else:
            assert len(error_suggestions_arr) == 1
    else:
        util.is_table_exists(table)


def test_get_list_of_existing_tables(ip):
    expected = ["author", "empty_table", "number_table", "test", "website"]
    list_of_tables = util._get_list_of_existing_tables()
    for table in expected:
        assert table in list_of_tables


@pytest.mark.parametrize(
    "src, ltypes, expected",
    [
        # 1-D flatten
        ([1, 2, 3], list, [1, 2, 3]),
        # 2-D flatten
        ([(1, 2), 3], None, [1, 2, 3]),
        ([(1, 2), 3], tuple, [1, 2, 3]),
        ([[[1, 2], 3]], list, [1, 2, 3]),
        (([[1, 2], 3]), None, [1, 2, 3]),
        (((1, 2), 3), tuple, (1, 2, 3)),
        (((1, 2), 3), None, (1, 2, 3)),
        (([1, 2], 3), None, (1, 2, 3)),
        (([1, 2], 3), list, (1, 2, 3)),
        # 3-D flatten
        (([[1, 2]], 3), list, (1, 2, 3)),
        (([[1, 2]], 3), None, (1, 2, 3)),
    ],
)
def test_flatten(src, ltypes, expected):
    if ltypes:
        assert util.flatten(src, ltypes) == expected
    else:
        assert util.flatten(src) == expected


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
    rows, columns = util.fetch_sql_with_pagination(table, offset, n_rows)

    assert rows == expected_rows
    assert columns == expected_columns


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
    rows, columns = util.fetch_sql_with_pagination(
        table, offset, n_rows, sort_by, order_by
    )

    assert rows == expected_rows
    assert columns == expected_columns


@pytest.mark.parametrize(
    "table",
    ["no_such_table", ""],
)
def test_fetch_sql_with_pagination_no_table_error(ip, table):
    with pytest.raises(UsageError) as excinfo:
        util.fetch_sql_with_pagination(table, 0, 2)

    assert excinfo.value.error_type == "TableNotFoundError"


def test_fetch_sql_with_pagination_none_table(ip):
    with pytest.raises(UsageError) as excinfo:
        util.fetch_sql_with_pagination(None, 0, 2)

    assert excinfo.value.error_type == "UsageError"


date_format = "%Y-%m-%d %H:%M:%S"


@pytest.mark.parametrize(
    "rows, columns, expected_json",
    [
        ([(1, 2), (3, 4)], ["x", "y"], [{"x": 1, "y": 2}, {"x": 3, "y": 4}]),
        ([(1,), (3,)], ["x"], [{"x": 1}, {"x": 3}]),
        (
            [
                ("a", datetime.strptime("2021-01-01 00:30:10", date_format)),
                ("b", datetime.strptime("2021-02-01 00:30:10", date_format)),
            ],
            ["id", "datetime"],
            [
                {
                    "datetime": "2021-01-01 00:30:10",
                    "id": "a",
                },
                {
                    "datetime": "2021-02-01 00:30:10",
                    "id": "b",
                },
            ],
        ),
        (
            [(None, "a1", "b1"), (None, "a2", "b2")],
            ["x", "y", "z"],
            [
                {
                    "x": "None",
                    "y": "a1",
                    "z": "b1",
                },
                {
                    "x": "None",
                    "y": "a2",
                    "z": "b2",
                },
            ],
        ),
    ],
)
def test_parse_sql_results_to_json(ip, capsys, rows, columns, expected_json):
    j = util.parse_sql_results_to_json(rows, columns)
    j = json.loads(j)
    with capsys.disabled():
        assert str(j) == str(expected_json)


def test_get_all_keys(ip_snippets):
    keys = util.get_all_keys()
    assert "a" in keys
    assert "b" in keys
    assert "c" in keys


def test_get_key_dependents(ip_snippets):
    keys = util.get_key_dependents("a")
    assert "b" in keys
    assert "c" in keys


def test_del_saved_key(ip_snippets):
    keys = util.del_saved_key("c")
    assert "a" in keys
    assert "b" in keys


def test_del_saved_key_error(ip_snippets):
    with pytest.raises(UsageError) as excinfo:
        util.del_saved_key("non_existent_key")
    assert "No such saved snippet found : non_existent_key" in str(excinfo.value)
