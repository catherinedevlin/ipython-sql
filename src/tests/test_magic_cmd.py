import sqlite3

import pytest
from IPython.core.error import UsageError


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlcmd stuff",
            UsageError,
            "%sqlcmd has no command: 'stuff'. Valid commands are: 'tables', 'columns'",
        ],
        [
            "%sqlcmd columns",
            UsageError,
            "the following arguments are required: -t/--table",
        ],
    ],
)
def test_error(tmp_empty, ip, cell, error_type, error_message):
    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(out.error_in_exec) == error_message


def test_tables(ip):
    out = ip.run_cell("%sqlcmd tables").result._repr_html_()
    assert "author" in out
    assert "empty_table" in out
    assert "test" in out


def test_tables_with_schema(ip, tmp_empty):
    with sqlite3.connect("my.db") as conn:
        conn.execute("CREATE TABLE numbers (some_number FLOAT)")

    ip.run_cell(
        """%%sql
ATTACH DATABASE 'my.db' AS some_schema
"""
    )

    out = ip.run_cell("%sqlcmd tables --schema some_schema").result._repr_html_()

    assert "numbers" in out


def test_columns(ip):
    out = ip.run_cell("%sqlcmd columns -t author").result._repr_html_()
    assert "first_name" in out
    assert "last_name" in out
    assert "year_of_death" in out


def test_columns_with_schema(ip, tmp_empty):
    with sqlite3.connect("my.db") as conn:
        conn.execute("CREATE TABLE numbers (some_number FLOAT)")

    ip.run_cell(
        """%%sql
ATTACH DATABASE 'my.db' AS some_schema
"""
    )

    out = ip.run_cell(
        "%sqlcmd columns --table numbers --schema some_schema"
    ).result._repr_html_()

    assert "some_number" in out
