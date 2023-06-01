from pathlib import Path
from IPython.core.error import UsageError

import pytest
from sqlalchemy import create_engine

from sql.command import SQLCommand


@pytest.fixture
def sql_magic(ip):
    return ip.magics_manager.lsmagic()["line"]["sql"].__self__


@pytest.mark.parametrize(
    (
        "line, cell, parsed_sql, parsed_connection, parsed_result_var,"
        "parsed_return_result_var"
    ),
    [
        ("something --no-execute", "", "something", "", None, False),
        ("sqlite://", "", "", "sqlite://", None, False),
        ("SELECT * FROM TABLE", "", "SELECT * FROM TABLE", "", None, False),
        ("SELECT * FROM", "TABLE", "SELECT * FROM\nTABLE", "", None, False),
        (
            "my_var << SELECT * FROM table",
            "",
            "SELECT * FROM table",
            "",
            "my_var",
            False,
        ),
        (
            "my_var << SELECT *",
            "FROM table",
            "SELECT *\nFROM table",
            "",
            "my_var",
            False,
        ),
        (
            "my_var= << SELECT * FROM table",
            "",
            "SELECT * FROM table",
            "",
            "my_var",
            True,
        ),
        ("[db]", "", "", "sqlite://", None, False),
        ("--persist df", "", "df", "", None, False),
    ],
    ids=[
        "arg-with-option",
        "connection-string",
        "sql-query",
        "sql-query-in-line-and-cell",
        "parsed-var-single-line",
        "parsed-var-multi-line",
        "parsed-return-var-single-line",
        "config",
        "persist-dataframe",
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
    parsed_return_result_var,
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
        "return_result_var": parsed_return_result_var,
        "sql": parsed_sql,
        "sql_original": parsed_sql,
    }

    assert cmd.connection == parsed_connection
    assert cmd.sql == parsed_sql
    assert cmd.sql_original == parsed_sql


def test_parsed_sql_when_using_file(ip, sql_magic, tmp_empty):
    Path("query.sql").write_text("SELECT * FROM author")
    cmd = SQLCommand(sql_magic, ip.user_ns, "--file query.sql", "")

    assert cmd.parsed == {
        "connection": "",
        "result_var": None,
        "return_result_var": False,
        "sql": "SELECT * FROM author\n",
        "sql_original": "SELECT * FROM author\n",
    }

    assert cmd.connection == ""
    assert cmd.sql == "SELECT * FROM author\n"
    assert cmd.sql_original == "SELECT * FROM author\n"


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
        "persist_replace": False,
        "no_index": False,
        "append": False,
        "connection_arguments": None,
        "file": None,
        "interact": None,
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
        "return_result_var": False,
        "sql": sql_expected,
        "sql_original": sql_expected,
    }

    assert cmd.connection is engine
    assert cmd.sql == sql_expected
    assert cmd.sql_original == sql_expected


def test_variable_substitution_double_curly_cell_magic(ip, sql_magic):
    ip.user_global_ns["username"] = "some-user"

    cmd = SQLCommand(
        sql_magic,
        ip.user_ns,
        line="",
        cell="GRANT CONNECT ON DATABASE postgres TO {{username}};",
    )

    assert cmd.parsed["sql"] == "\nGRANT CONNECT ON DATABASE postgres TO some-user;"


def test_variable_substitution_double_curly_line_magic(ip, sql_magic):
    ip.user_global_ns["limit_number"] = 5
    ip.user_global_ns["column_name"] = "first_name"
    cmd = SQLCommand(
        sql_magic,
        ip.user_ns,
        line="SELECT {{column_name}} FROM author LIMIT {{limit_number}};",
        cell="",
    )

    assert cmd.parsed["sql"] == "SELECT first_name FROM author LIMIT 5;"


def test_with_contains_dash_show_warning_message(ip, sql_magic, capsys):
    with pytest.raises(UsageError) as error:
        ip.run_cell_magic(
            "sql",
            "--save author-sub",
            "SELECT last_name FROM author WHERE year_of_death > 1900",
        )

    assert error.value.error_type == "UsageError"
    assert "Using hyphens (-) in save argument isn't allowed" in str(error.value)
