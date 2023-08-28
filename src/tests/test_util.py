from datetime import datetime
import pytest
from sql import util
import json

ERROR_MESSAGE = "Table cannot be None"
EXPECTED_STORE_SUGGESTIONS = (
    "but there is a stored query.\nDid you miss passing --with {0}?"
)


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


@pytest.mark.parametrize(
    "string, substrings, expected",
    [
        ["some-string", ["some", "another"], True],
        ["some-string", ["another", "word"], False],
    ],
)
def test_is_sqlalchemy_error(string, substrings, expected):
    result = util.if_substring_exists(string, substrings)
    assert result == expected
