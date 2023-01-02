from pathlib import Path

import pytest
from sqlalchemy import create_engine

from sql.command import SQLCommand


@pytest.fixture
def sql_magic(ip):
    return ip.magics_manager.lsmagic()["line"]["sql"].__self__


@pytest.mark.parametrize(
    "line, cell, parsed_sql, parsed_connection, parsed_result_var",
    [
        ("something --no-execute", "", "something\n", "", None),
        ("sqlite://", "", "", "sqlite://", None),
        ("SELECT * FROM TABLE", "", "SELECT * FROM TABLE\n", "", None),
        ("SELECT * FROM", "TABLE", "SELECT * FROM\nTABLE", "", None),
        ("my_var << SELECT * FROM table", "", "SELECT * FROM table\n", "", "my_var"),
        ("my_var << SELECT *", "FROM table", "SELECT *\nFROM table", "", "my_var"),
        ("[db]", "", "", "sqlite://", None),
    ],
    ids=[
        "arg-with-option",
        "connection-string",
        "sql-query",
        "sql-query-in-line-and-cell",
        "parsed-var-single-line",
        "parsed-var-multi-line",
        "config",
    ],
)
def test_parsed(
    ip,
    sql_magic,
    line,
    cell,
    parsed_sql,
    parsed_connection,
    parsed_result_var,
    tmp_empty,
):
    # needed for the last test case
    Path("odbc.ini").write_text(
        """
[db]
drivername = sqlite
"""
    )

    cmd = SQLCommand(sql_magic, ip.user_ns, line, cell)

    assert cmd.parsed == {
        "connection": parsed_connection,
        "result_var": parsed_result_var,
        "sql": parsed_sql,
        "sql_original": parsed_sql,
    }

    assert cmd.connection == parsed_connection
    assert cmd.sql == parsed_sql
    assert cmd.sql_original == parsed_sql


def test_parsed_sql_when_using_with(ip, sql_magic):
    ip.run_cell_magic(
        "sql",
        "--save author_one",
        """
        SELECT * FROM author LIMIT 1
        """,
    )

    cmd = SQLCommand(
        sql_magic, ip.user_ns, line="--with author_one", cell="SELECT * FROM author_one"
    )

    sql = (
        "WITH author_one AS (\n    \n\n        "
        "SELECT * FROM author LIMIT 1\n        \n)"
        "\n\nSELECT * FROM author_one"
    )

    sql_original = "\nSELECT * FROM author_one"

    assert cmd.parsed == {
        "connection": "",
        "result_var": None,
        "sql": sql,
        "sql_original": sql_original,
    }

    assert cmd.connection == ""
    assert cmd.sql == sql
    assert cmd.sql_original == sql_original


def test_parsed_sql_when_using_file(ip, sql_magic, tmp_empty):
    Path("query.sql").write_text("SELECT * FROM author")
    cmd = SQLCommand(sql_magic, ip.user_ns, "--file query.sql", "")

    assert cmd.parsed == {
        "connection": "",
        "result_var": None,
        "sql": "SELECT * FROM author\n\n",
        "sql_original": "SELECT * FROM author\n\n",
    }

    assert cmd.connection == ""
    assert cmd.sql == "SELECT * FROM author\n\n"
    assert cmd.sql_original == "SELECT * FROM author\n\n"


def test_args(ip, sql_magic):
    ip.run_cell_magic(
        "sql",
        "--save author_one",
        """
        SELECT * FROM author LIMIT 1
        """,
    )

    cmd = SQLCommand(sql_magic, ip.user_ns, line="--with author_one", cell="")

    assert cmd.args.__dict__ == {
        "alias": None,
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


@pytest.mark.parametrize(
    "line",
    [
        "my_engine",
        " my_engine",
        "my_engine ",
    ],
)
def test_parse_sql_when_passing_engine(ip, sql_magic, tmp_empty, line):
    engine = create_engine("sqlite:///my.db")
    ip.user_global_ns["my_engine"] = engine

    cmd = SQLCommand(sql_magic, ip.user_ns, line, cell="SELECT * FROM author")

    sql_expected = "\nSELECT * FROM author"

    assert cmd.parsed == {
        "connection": engine,
        "result_var": None,
        "sql": sql_expected,
        "sql_original": sql_expected,
    }

    assert cmd.connection is engine
    assert cmd.sql == sql_expected
    assert cmd.sql_original == sql_expected


def test_variable_substitution_cell_magic(ip, sql_magic):
    ip.user_global_ns["username"] = "some-user"

    cmd = SQLCommand(
        sql_magic,
        ip.user_ns,
        line="",
        cell="GRANT CONNECT ON DATABASE postgres TO $username;",
    )

    assert cmd.parsed["sql"] == "\nGRANT CONNECT ON DATABASE postgres TO some-user;"
