from pathlib import Path

from sql.command import SQLCommand

import pytest


@pytest.mark.parametrize(
    "line, cell, parsed_sql, parsed_connection",
    [
        ("something --no-execute", "", "something\n", ""),
        ("sqlite://", "", "", "sqlite://"),
        ("SELECT * FROM TABLE", "", "SELECT * FROM TABLE\n", ""),
        ("SELECT * FROM", "TABLE", "SELECT * FROM\nTABLE", ""),
    ],
    ids=[
        "arg-with-option",
        "connection-string",
        "sql-query",
        "sql-query-in-line-and-cell",  # NOTE: I'm unsure under which circumstances this happens # noqa
    ],
)
def test_parsed(ip, line, cell, parsed_sql, parsed_connection):
    sql_line = ip.magics_manager.lsmagic()["line"]["sql"].__self__

    cmd = SQLCommand(sql_line, line, cell)

    assert cmd.parsed == {
        "connection": parsed_connection,
        "result_var": None,
        "sql": parsed_sql,
        "sql_original": parsed_sql,
    }


def test_parsed_sql_when_using_with(ip):
    ip.run_cell_magic(
        "sql",
        "--save author_one",
        """
        SELECT * FROM author LIMIT 1
        """,
    )

    line = "--with author_one"
    cell = "SELECT * FROM author_one"
    sql_line = ip.magics_manager.lsmagic()["line"]["sql"].__self__

    cmd = SQLCommand(sql_line, line, cell)

    sql = (
        "WITH author_one AS (\n    \n\n        "
        "SELECT * FROM author LIMIT 1\n        \n)"
        "\n\nSELECT * FROM author_one"
    )

    assert cmd.parsed == {
        "connection": "",
        "result_var": None,
        "sql": sql,
        "sql_original": "\nSELECT * FROM author_one",
    }


def test_parsed_sql_when_using_file(ip, tmp_empty):
    Path("query.sql").write_text("SELECT * FROM author")

    sql_line = ip.magics_manager.lsmagic()["line"]["sql"].__self__

    cmd = SQLCommand(sql_line, "--file query.sql", "")

    assert cmd.parsed == {
        "connection": "",
        "result_var": None,
        "sql": "SELECT * FROM author\n\n",
        "sql_original": "SELECT * FROM author\n\n",
    }


def test_args(ip):
    ip.run_cell_magic(
        "sql",
        "--save author_one",
        """
        SELECT * FROM author LIMIT 1
        """,
    )

    line = "--with author_one"
    cell = ""
    sql_line = ip.magics_manager.lsmagic()["line"]["sql"].__self__

    cmd = SQLCommand(sql_line, line, cell)

    assert cmd.args.__dict__ == {
        "line": "",
        "connections": False,
        "close": None,
        "creator": None,
        "section": None,
        "persist": False,
        "no_index": False,
        "append": False,
        "connection_arguments": None,
        "file": None,
        "save": None,
        "with_": ["author_one"],
        "no_execute": False,
    }

    # assert cmd.command_text == "something\n"
