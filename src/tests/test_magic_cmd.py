import sqlite3

import pytest
from IPython.core.error import UsageError
from pathlib import Path


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlcmd",
            UsageError,
            "Missing argument for %sqlcmd. "
            "Valid commands are: tables, columns, test, profile",
        ],
        [
            "%sqlcmd ",
            UsageError,
            "Missing argument for %sqlcmd. "
            "Valid commands are: tables, columns, test, profile",
        ],
        [
            "%sqlcmd  ",
            UsageError,
            "Missing argument for %sqlcmd. "
            "Valid commands are: tables, columns, test, profile",
        ],
        [
            "%sqlcmd   ",
            UsageError,
            "Missing argument for %sqlcmd. "
            "Valid commands are: tables, columns, test, profile",
        ],
        [
            "%sqlcmd stuff",
            UsageError,
            "%sqlcmd has no command: 'stuff'. "
            "Valid commands are: tables, columns, test, profile",
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


def test_table_profile(ip, tmp_empty):
    ip.run_cell(
        """
    %%sql sqlite://
    CREATE TABLE numbers (rating float, price float, number int, word varchar(50));
    INSERT INTO numbers VALUES (14.44, 2.48, 82, 'a');
    INSERT INTO numbers VALUES (13.13, 1.50, 93, 'b');
    INSERT INTO numbers VALUES (12.59, 0.20, 98, 'a');
    INSERT INTO numbers VALUES (11.54, 0.41, 89, 'a');
    INSERT INTO numbers VALUES (10.532, 0.1, 88, 'c');
    INSERT INTO numbers VALUES (11.5, 0.2, 84, '   ');
    INSERT INTO numbers VALUES (11.1, 0.3, 90, 'a');
    INSERT INTO numbers VALUES (12.9, 0.31, 86, '');
    """
    )

    expected = {
        "count": [8, 8, 8, 8],
        "mean": [12.2165, "6.875e-01", 88.75, 0.0],
        "min": [10.532, 0.1, 82, ""],
        "max": [14.44, 2.48, 98, "c"],
        "unique": [8, 7, 8, 5],
        "freq": [1, 2, 1, 4],
        "top": [14.44, 0.2, 98, "a"],
    }

    out = ip.run_cell("%sqlcmd profile -t numbers").result

    stats_table = out._table

    assert len(stats_table.rows) == len(expected)

    for row in stats_table:
        criteria = row.get_string(fields=[" "], border=False).strip()

        rating = row.get_string(fields=["rating"], border=False, header=False).strip()

        price = row.get_string(fields=["price"], border=False, header=False).strip()

        number = row.get_string(fields=["number"], border=False, header=False).strip()

        word = row.get_string(fields=["word"], border=False, header=False).strip()

        assert criteria in expected
        assert rating == str(expected[criteria][0])
        assert price == str(expected[criteria][1])
        assert number == str(expected[criteria][2])
        assert word == str(expected[criteria][3])


def test_table_schema_profile(ip, tmp_empty):
    with sqlite3.connect("a.db") as conn:
        conn.execute(("CREATE TABLE t (n FLOAT)"))
        conn.execute(("INSERT INTO t VALUES (1)"))
        conn.execute(("INSERT INTO t VALUES (2)"))
        conn.execute(("INSERT INTO t VALUES (3)"))

    with sqlite3.connect("b.db") as conn:
        conn.execute(("CREATE TABLE t (n FLOAT)"))
        conn.execute(("INSERT INTO t VALUES (11)"))
        conn.execute(("INSERT INTO t VALUES (22)"))
        conn.execute(("INSERT INTO t VALUES (33)"))

    ip.run_cell(
        """
    %%sql sqlite://
    ATTACH DATABASE 'a.db' AS a_schema;
    ATTACH DATABASE 'b.db' AS b_schema;
    """
    )

    expected = {
        "count": [3],
        "mean": [22.0],
        "min": [11.0],
        "max": [33.0],
        "std": [11.0],
        "unique": [3],
        "freq": [1],
        "top": [33.0],
    }

    out = ip.run_cell("%sqlcmd profile -t t --schema b_schema").result

    stats_table = out._table

    for row in stats_table:
        criteria = row.get_string(fields=[" "], border=False).strip()

        cell = row.get_string(fields=["n"], border=False, header=False).strip()

        if criteria in expected:
            assert cell == str(expected[criteria][0])


def test_table_profile_store(ip, tmp_empty):
    ip.run_cell(
        """
    %%sql sqlite://
    CREATE TABLE test_store (rating, price, number, symbol);
    INSERT INTO test_store VALUES (14.44, 2.48, 82, 'a');
    INSERT INTO test_store VALUES (13.13, 1.50, 93, 'b');
    INSERT INTO test_store VALUES (12.59, 0.20, 98, 'a');
    INSERT INTO test_store VALUES (11.54, 0.41, 89, 'a');
    """
    )

    ip.run_cell("%sqlcmd profile -t test_store --output test_report.html")

    report = Path("test_report.html")
    assert report.is_file()


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        ["%sqlcmd test -t test_numbers", UsageError, "Please use a valid comparator."],
        [
            "%sqlcmd test --t test_numbers --greater 12",
            UsageError,
            "Please pass a column to test.",
        ],
        [
            "%sqlcmd test --table test_numbers --column something --greater 100",
            UsageError,
            "Referenced column 'something' not found!",
        ],
    ],
)
def test_test_error(ip, cell, error_type, error_message):
    ip.run_cell(
        """
    %%sql sqlite://
    CREATE TABLE test_numbers (value);
    INSERT INTO test_numbers VALUES (14);
    INSERT INTO test_numbers VALUES (13);
    INSERT INTO test_numbers VALUES (12);
    INSERT INTO test_numbers VALUES (11);
    """
    )

    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(out.error_in_exec) == error_message
