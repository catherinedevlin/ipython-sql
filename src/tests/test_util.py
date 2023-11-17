from datetime import datetime
from IPython.core.error import UsageError
import pytest
from sql import util
import json
from sql.magic import SqlMagic
from sql.magic_cmd import SqlCmdMagic
from sql.magic_plot import SqlPlotMagic

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


@pytest.mark.parametrize(
    "args, aliases",
    [
        # for creator/c
        (
            ["--creator", "--creator"],
            [],
        ),
        (
            ["-c", "-c"],
            [],
        ),
        (
            ["--creator", "-c"],
            [("c", "creator")],
        ),
        # for persist/p
        (
            ["--persist", "--persist"],
            [],
        ),
        (
            ["-p", "-p"],
            [],
        ),
        (
            ["--persist", "-p"],
            [("p", "persist")],
        ),
        # for no-index/n
        (
            ["--persist", "--no-index", "--no-index"],
            [],
        ),
        (
            ["--persist", "-n", "-n"],
            [],
        ),
        (
            ["--persist", "--no-index", "-n"],
            [("n", "no-index")],
        ),
        # for file/f
        (
            ["--file", "--file"],
            [],
        ),
        (
            ["-f", "-f"],
            [],
        ),
        (
            ["--file", "-f"],
            [("f", "file")],
        ),
        # for save/S
        (
            ["--save", "--save"],
            [],
        ),
        (
            ["-S", "-S"],
            [],
        ),
        (
            ["--save", "-S"],
            [("S", "save")],
        ),
        # for alias/A
        (
            ["--alias", "--alias"],
            [],
        ),
        (
            ["-A", "-A"],
            [],
        ),
        (
            ["--alias", "-A"],
            [("A", "alias")],
        ),
        # for connections/l
        (
            ["--connections", "--connections"],
            [],
        ),
        (
            ["-l", "-l"],
            [],
        ),
        (
            ["--connections", "-l"],
            [("l", "connections")],
        ),
        # for close/x
        (
            ["--close", "--close"],
            [],
        ),
        (
            ["-x", "-x"],
            [],
        ),
        (
            ["--close", "-x"],
            [("x", "close")],
        ),
        # for mixed
        (
            ["--creator", "--creator", "-c", "--persist", "--file", "-f", "-c"],
            [("c", "creator"), ("f", "file")],
        ),
    ],
)
def test_check_duplicate_arguments_raises_usageerror_for_sql_magic(
    check_duplicate_message_factory,
    args,
    aliases,
):
    with pytest.raises(UsageError) as excinfo:
        util.check_duplicate_arguments(
            SqlMagic.execute,
            "sql",
            args,
            ["-w", "--with", "--append", "--interact"],
        )
    assert check_duplicate_message_factory("sql", args, aliases) in str(excinfo.value)


@pytest.mark.parametrize(
    "args, aliases",
    [
        # for table/t
        (
            ["--table", "--table", "--column"],
            [],
        ),
        (
            ["-t", "-t", "--column"],
            [],
        ),
        (
            ["--table", "-t", "--column"],
            [("t", "table")],
        ),
        # for column/c
        (
            ["--table", "--column", "--column"],
            [],
        ),
        (
            ["--table", "-c", "-c"],
            [],
        ),
        (
            ["--table", "--column", "-c"],
            [("c", "column")],
        ),
        # for bins/b
        (
            ["--table", "--column", "--bins", "--bins"],
            [],
        ),
        (
            ["--table", "--column", "-b", "-b"],
            [],
        ),
        (
            ["--table", "--column", "--bins", "-b"],
            [("b", "bins")],
        ),
        # for breaks/B
        (
            ["--table", "--column", "--breaks", "--breaks"],
            [],
        ),
        (
            ["--table", "--column", "-B", "-B"],
            [],
        ),
        (
            ["--table", "--column", "--breaks", "-B"],
            [("B", "breaks")],
        ),
        # for binwidth/W
        (
            ["--table", "--column", "--binwidth", "--binwidth"],
            [],
        ),
        (
            ["--table", "--column", "-W", "-W"],
            [],
        ),
        (
            ["--table", "--column", "--binwidth", "-W"],
            [("W", "binwidth")],
        ),
        # for orient/o
        (
            ["--table", "--column", "--orient", "--orient"],
            [],
        ),
        (
            ["--table", "--column", "-o", "-o"],
            [],
        ),
        (
            ["--table", "--column", "--orient", "-o"],
            [("o", "orient")],
        ),
        # for show-numbers/S
        (
            ["--table", "--column", "--show-numbers", "--show-numbers"],
            [],
        ),
        (
            ["--table", "--column", "-S", "-S"],
            [],
        ),
        (
            ["--table", "--column", "--show-numbers", "-S"],
            [("S", "show-numbers")],
        ),
        # for mixed
        (
            [
                "--table",
                "--column",
                "--column",
                "-w",
                "--with",
                "--show-numbers",
                "--show-numbers",
                "--binwidth",
                "--orient",
                "-o",
                "--breaks",
                "-B",
            ],
            [("w", "with"), ("o", "orient"), ("B", "breaks")],
        ),
    ],
)
def test_check_duplicate_arguments_raises_usageerror_for_sqlplot(
    check_duplicate_message_factory,
    args,
    aliases,
):
    with pytest.raises(UsageError) as excinfo:
        util.check_duplicate_arguments(
            SqlPlotMagic.execute,
            "sqlplot",
            args,
            ["-w", "--with"],
        )

    assert check_duplicate_message_factory("sqlplot", args, aliases) in str(
        excinfo.value
    )


DISALLOWED_ALIASES = {
    "sqlcmd": {
        "-t": "--table",
        "-s": "--schema",
        "-o": "--output",
    },
}


@pytest.mark.parametrize(
    "args, aliases",
    [
        # for schema/s
        (
            ["--schema", "--schema"],
            [],
        ),
        (
            ["-s", "-s"],
            [],
        ),
        (
            ["--schema", "-s"],
            [("s", "schema")],
        ),
        # for table/t
        (
            ["--table", "--table"],
            [],
        ),
        (
            ["-t", "-t"],
            [],
        ),
        (
            ["--table", "-t"],
            [("t", "table")],
        ),
        # for mixed
        (
            ["--table", "-t", "-s", "-s", "--schema"],
            [("t", "table"), ("s", "schema")],
        ),
    ],
)
def test_check_duplicate_arguments_raises_usageerror_for_sqlcmd(
    check_duplicate_message_factory,
    args,
    aliases,
):
    with pytest.raises(UsageError) as excinfo:
        util.check_duplicate_arguments(
            SqlCmdMagic.execute,
            "sqlcmd",
            args,
            [],
            DISALLOWED_ALIASES["sqlcmd"],
        )
    assert check_duplicate_message_factory("sqlcmd", args, aliases) in str(
        excinfo.value
    )


ALLOWED_DUPLICATES = {
    "sql": ["-w", "--with", "--append", "--interact"],
    "sqlplot": ["-w", "--with"],
    "sqlcmd": [],
}


@pytest.mark.parametrize(
    "magic_execute, args, cmd_from",
    [
        (SqlMagic.execute, ["--creator"], "sql"),
        (SqlMagic.execute, ["-c"], "sql"),
        (SqlMagic.execute, ["--persist"], "sql"),
        (SqlMagic.execute, ["-p"], "sql"),
        (SqlMagic.execute, ["--persist", "--no-index"], "sql"),
        (SqlMagic.execute, ["--persist", "-n"], "sql"),
        (SqlMagic.execute, ["--file"], "sql"),
        (SqlMagic.execute, ["-f"], "sql"),
        (SqlMagic.execute, ["--save"], "sql"),
        (SqlMagic.execute, ["-S"], "sql"),
        (SqlMagic.execute, ["--alias"], "sql"),
        (SqlMagic.execute, ["-A"], "sql"),
        (SqlMagic.execute, ["--connections"], "sql"),
        (SqlMagic.execute, ["-l"], "sql"),
        (SqlMagic.execute, ["--close"], "sql"),
        (SqlMagic.execute, ["-x"], "sql"),
        (SqlPlotMagic.execute, ["--table", "--column"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "-c"], "sqlplot"),
        (SqlPlotMagic.execute, ["-t", "--column"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "--breaks"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "-B"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "--bins"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "-b"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "--binwidth"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "-W"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "--orient"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "-o"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "--show-numbers"], "sqlplot"),
        (SqlPlotMagic.execute, ["--table", "--column", "-S"], "sqlplot"),
        (SqlCmdMagic.execute, ["--table"], "sqlcmd"),
        (SqlCmdMagic.execute, ["-t"], "sqlcmd"),
        (SqlCmdMagic.execute, ["--table", "--schema"], "sqlcmd"),
        (SqlCmdMagic.execute, ["--table", "-s"], "sqlcmd"),
    ],
)
def test_check_duplicate_arguments_does_not_raise_usageerror(
    magic_execute, args, cmd_from
):
    assert util.check_duplicate_arguments(
        magic_execute, cmd_from, args, ALLOWED_DUPLICATES[cmd_from]
    )
