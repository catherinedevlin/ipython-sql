import pytest
from sql import util

ERROR_MESSAGE = "Table cannot be None"
EXPECTED_SUGGESTIONS_MESSAGE = "Did you mean :"
EXPECTED_NO_TABLE_IN_DEFAULT_SCHEMA = (
    "There is no table with name {0!r} in the default schema"
)
EXPECTED_NO_TABLE_IN_SCHEMA = "There is no table with name {0!r} in schema {1!r}"
EXPECTED_STORE_SUGGESTIONS = (
    "but there is a stored query.\nDid you miss passing --with {0}?"
)


@pytest.mark.parametrize(
    "store_table, query",
    [
        ("a", "%sqlcmd columns --table {}"),
        ("bbb", "%sqlcmd profile --table {}"),
        ("c_c", "%sqlplot histogram --table {} --column x"),
        ("d_d_d", "%sqlplot boxplot --table {} --column x"),
    ],
)
def test_missing_with(ip, store_table, query):
    ip.run_cell(
        f"""
        %%sql --save {store_table} --no-execute
        SELECT *
        FROM number_table
        """
    ).result

    query = query.format(store_table)
    out = ip.run_cell(query)

    expected_store_message = EXPECTED_STORE_SUGGESTIONS.format(store_table)

    error_message = str(out.error_in_exec)
    assert isinstance(out.error_in_exec, ValueError)
    assert str(expected_store_message).lower() in error_message.lower()


@pytest.mark.parametrize(
    "store_table, query",
    [
        ("a", "%sqlcmd columns --table {} --with {}"),
        ("bbb", "%sqlcmd profile --table {} --with {}"),
        ("c_c", "%sqlplot histogram --table {} --with {} --column x"),
        ("d_d_d", "%sqlplot boxplot --table {} --with {} --column x"),
    ],
)
def test_no_errors_with_stored_query(ip, store_table, query):
    ip.run_cell(
        f"""
        %%sql --save {store_table} --no-execute
        SELECT *
        FROM number_table
        """
    ).result

    query = query.format(store_table, store_table)
    out = ip.run_cell(query)

    expected_store_message = EXPECTED_STORE_SUGGESTIONS.format(store_table)
    error_message = str(out.error_in_exec)
    assert not isinstance(out.error_in_exec, ValueError)
    assert str(expected_store_message).lower() not in error_message.lower()


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
    out = ip.run_cell(query)

    expected_error_message = EXPECTED_NO_TABLE_IN_DEFAULT_SCHEMA.format(table)

    error_message = str(out.error_in_exec)
    assert isinstance(out.error_in_exec, ValueError)
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

    out = ip.run_cell(query)

    error_message = str(out.error_in_exec)
    assert isinstance(out.error_in_exec, ValueError)
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
    "table, expected_error",
    [
        ("number_table", None),
        ("test", None),
        ("author", None),
        ("empty_table", None),
        ("numbers1", ValueError),
        ("test1", ValueError),
        ("author1", ValueError),
        ("empty_table1", ValueError),
        (None, ValueError),
    ],
)
def test_is_table_exists(ip, table, expected_error):
    if expected_error:
        with pytest.raises(ValueError):
            util.is_table_exists(table)
    else:
        util.is_table_exists(table)


@pytest.mark.parametrize(
    "table, expected_error, expected_suggestions",
    [
        ("number_table", None, []),
        ("number_tale", ValueError, ["number_table"]),
        ("_table", ValueError, ["number_table", "empty_table"]),
        (None, ValueError, []),
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
            util.is_table_exists(table, with_=with_)

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
