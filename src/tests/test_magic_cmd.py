import sys

import pytest
from IPython.core.error import UsageError
from pathlib import Path

from sqlalchemy import create_engine
from sql.connection import Connection
from sql.store import store


VALID_COMMANDS_MESSAGE = (
    "Valid commands are: tables, " "columns, test, profile, explore, snippets"
)


@pytest.fixture
def ip_snippets(ip):
    for key in list(store):
        del store[key]
    ip.run_cell("%sql sqlite://")
    ip.run_cell(
        """
        %%sql --save high_price --no-execute
SELECT *
FROM "test_store"
WHERE price >= 1.50
"""
    )
    ip.run_cell(
        """
        %%sql --save high_price_a --no-execute
SELECT *
FROM "high_price"
WHERE symbol == 'a'
"""
    )
    ip.run_cell(
        """
        %%sql --save high_price_b --no-execute
SELECT *
FROM "high_price"
WHERE symbol == 'b'
"""
    )
    yield ip


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlcmd",
            UsageError,
            "Missing argument for %sqlcmd. " f"{VALID_COMMANDS_MESSAGE}",
        ],
        [
            "%sqlcmd ",
            UsageError,
            "Missing argument for %sqlcmd. " f"{VALID_COMMANDS_MESSAGE}",
        ],
        [
            "%sqlcmd  ",
            UsageError,
            "Missing argument for %sqlcmd. " f"{VALID_COMMANDS_MESSAGE}",
        ],
        [
            "%sqlcmd   ",
            UsageError,
            "Missing argument for %sqlcmd. " f"{VALID_COMMANDS_MESSAGE}",
        ],
        [
            "%sqlcmd stuff",
            UsageError,
            "%sqlcmd has no command: 'stuff'. " f"{VALID_COMMANDS_MESSAGE}",
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
    conn = Connection(engine=create_engine("sqlite:///my.db"))
    conn.execute("CREATE TABLE numbers (some_number FLOAT)")

    ip.run_cell(
        """%%sql
ATTACH DATABASE 'my.db' AS some_schema
"""
    )

    out = ip.run_cell("%sqlcmd tables --schema some_schema").result._repr_html_()

    assert "numbers" in out


@pytest.mark.xfail(
    sys.platform == "win32",
    reason="problem in IPython.core.magic_arguments.parse_argstring",
)
@pytest.mark.parametrize(
    "cmd, cols",
    [
        ["%sqlcmd columns -t author", ["first_name", "last_name", "year_of_death"]],
        [
            "%sqlcmd columns -t 'table with spaces'",
            ["first", "second"],
        ],
        [
            '%sqlcmd columns -t "table with spaces"',
            ["first", "second"],
        ],
    ],
)
def test_columns(ip, cmd, cols):
    out = ip.run_cell(cmd).result._repr_html_()
    assert all(col in out for col in cols)


def test_columns_with_schema(ip, tmp_empty):
    conn = Connection(engine=create_engine("sqlite:///my.db"))
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

    # Test sticky column style was injected
    assert "position: sticky;" in out._table_html


def test_table_schema_profile(ip, tmp_empty):
    ip.run_cell("%sql sqlite:///a.db")
    ip.run_cell("%sql CREATE TABLE t (n FLOAT)")
    ip.run_cell("%sql INSERT INTO t VALUES (1)")
    ip.run_cell("%sql INSERT INTO t VALUES (2)")
    ip.run_cell("%sql INSERT INTO t VALUES (3)")
    ip.run_cell("%sql --close sqlite:///a.db")

    ip.run_cell("%sql sqlite:///b.db")
    ip.run_cell("%sql CREATE TABLE t (n FLOAT)")
    ip.run_cell("%sql INSERT INTO t VALUES (11)")
    ip.run_cell("%sql INSERT INTO t VALUES (22)")
    ip.run_cell("%sql INSERT INTO t VALUES (33)")
    ip.run_cell("%sql --close sqlite:///b.db")

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


def test_snippet(ip_snippets):
    out = ip_snippets.run_cell("%sqlcmd snippets").result
    assert "high_price, high_price_a, high_price_b" in out


@pytest.mark.parametrize("arg", ["--delete", "-d"])
def test_delete_saved_key(ip_snippets, arg):
    out = ip_snippets.run_cell(f"%sqlcmd snippets {arg} high_price_a").result
    assert "high_price_a has been deleted.\n" in out
    stored_snippets = out[out.find("Stored snippets") + len("Stored snippets: ") :]
    assert "high_price, high_price_b" in stored_snippets
    assert "high_price_a" not in stored_snippets


@pytest.mark.parametrize("arg", ["--delete-force", "-D"])
def test_force_delete(ip_snippets, arg):
    out = ip_snippets.run_cell(f"%sqlcmd snippets {arg} high_price").result
    assert (
        "high_price has been deleted.\nhigh_price_a, "
        "high_price_b depend on high_price\n" in out
    )
    stored_snippets = out[out.find("Stored snippets") + len("Stored snippets: ") :]
    assert "high_price_a, high_price_b" in stored_snippets
    assert "high_price," not in stored_snippets


@pytest.mark.parametrize("arg", ["--delete-force-all", "-A"])
def test_force_delete_all(ip_snippets, arg):
    out = ip_snippets.run_cell(f"%sqlcmd snippets {arg} high_price").result
    assert "high_price_a, high_price_b, high_price has been deleted" in out
    assert "There are no stored snippets" in out


@pytest.mark.parametrize("arg", ["--delete-force-all", "-A"])
def test_force_delete_all_child_query(ip_snippets, arg):
    ip_snippets.run_cell(
        """
        %%sql --save high_price_b_child --no-execute
SELECT *
FROM "high_price_b"
WHERE symbol == 'b'
LIMIT 3
"""
    )
    out = ip_snippets.run_cell(f"%sqlcmd snippets {arg} high_price_b").result
    assert "high_price_b_child, high_price_b has been deleted" in out
    stored_snippets = out[out.find("Stored snippets") + len("Stored snippets: ") :]
    assert "high_price, high_price_a" in stored_snippets
    assert "high_price_b," not in stored_snippets
    assert "high_price_b_child" not in stored_snippets


@pytest.mark.parametrize("arg", ["--delete", "-d"])
def test_delete_snippet_error(ip_snippets, arg):
    out = ip_snippets.run_cell(f"%sqlcmd snippets {arg} high_price")
    assert isinstance(out.error_in_exec, UsageError)
    assert (
        str(out.error_in_exec) == "The following tables are dependent on high_price: "
        "high_price_a, high_price_b.\nPass --delete-force to only "
        "delete high_price.\nPass --delete-force-all to delete "
        "high_price_a, high_price_b and high_price"
    )


@pytest.mark.parametrize(
    "arg", ["--delete", "-d", "--delete-force-all", "-A", "--delete-force", "-D"]
)
def test_delete_invalid_snippet(arg, ip_snippets):
    out = ip_snippets.run_cell(f"%sqlcmd snippets {arg} non_existent_snippet")
    assert isinstance(out.error_in_exec, UsageError)
    assert (
        str(out.error_in_exec) == "No such saved snippet found "
        ": non_existent_snippet"
    )
