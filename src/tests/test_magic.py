import sqlite3
import platform
from pathlib import Path
import os.path
import re
import sys
import tempfile
from textwrap import dedent
from unittest.mock import patch

import polars as pl
import pytest
from sqlalchemy import create_engine
from IPython.core.error import UsageError
from sql.connection import Connection
from sql.magic import SqlMagic
from sql.run import ResultSet
from sql import magic

from conftest import runsql


def test_memory_db(ip):
    assert runsql(ip, "SELECT * FROM test;")[0][0] == 1
    assert runsql(ip, "SELECT * FROM test;")[1][1] == "bar"


def test_html(ip):
    result = runsql(ip, "SELECT * FROM test;")
    assert "<td>foo</td>" in result._repr_html_().lower()


def test_print(ip):
    result = runsql(ip, "SELECT * FROM test;")
    assert re.search(r"1\s+\|\s+foo", str(result))


def test_plain_style(ip):
    ip.run_line_magic("config", "SqlMagic.style = 'PLAIN_COLUMNS'")
    result = runsql(ip, "SELECT * FROM test;")
    assert re.search(r"1\s+\|\s+foo", str(result))


@pytest.mark.skip
def test_multi_sql(ip):
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite://
        SELECT last_name FROM author;
        """,
    )
    assert "Shakespeare" in str(result) and "Brecht" in str(result)


def test_result_var(ip, capsys):
    ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite://
        x <<
        SELECT last_name FROM author;
        """,
    )
    result = ip.user_global_ns["x"]
    out, _ = capsys.readouterr()

    assert "Shakespeare" in str(result) and "Brecht" in str(result)
    assert "Returning data to local variable" not in out


def test_result_var_link(ip, capsys):
    ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite://
        x <<
        SELECT link FROM website;
        """,
    )
    result = ip.user_global_ns["x"]
    out, _ = capsys.readouterr()
    assert (
        "<a href=https://en.wikipedia.org/wiki/Bertolt_Brecht>"
        "https://en.wikipedia.org/wiki/Bertolt_Brecht</a>" in result._repr_html_()
        and "<a href=https://en.wikipedia.org/wiki/William_Shakespeare>"
        "https://en.wikipedia.org/wiki/William_Shakespeare</a>" in result._repr_html_()
    )
    assert "Returning data to local variable" not in out
    assert "<a href=google_link>google_link</a>" not in result._repr_html_()


def test_result_var_multiline_shovel(ip):
    ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite:// x << SELECT last_name
        FROM author;
        """,
    )
    result = ip.user_global_ns["x"]
    assert "Shakespeare" in str(result) and "Brecht" in str(result)


def test_access_results_by_keys(ip):
    assert runsql(ip, "SELECT * FROM author;")["William"] == (
        "William",
        "Shakespeare",
        1616,
    )


def test_duplicate_column_names_accepted(ip):
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite://
        SELECT last_name, last_name FROM author;
        """,
    )
    assert ("Brecht", "Brecht") in result


def test_persist_missing_pandas(ip, monkeypatch):
    monkeypatch.setattr(magic, "DataFrame", None)

    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.run_cell("results_dframe = results.DataFrame()")
    result = ip.run_cell("%sql --persist sqlite:// results_dframe")
    assert "pip install pandas" in str(result.error_in_exec)


def test_persist(ip):
    runsql(ip, "")
    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.run_cell("results_dframe = results.DataFrame()")
    ip.run_cell("%sql --persist sqlite:// results_dframe")
    persisted = runsql(ip, "SELECT * FROM results_dframe")
    assert persisted == [(0, 1, "foo"), (1, 2, "bar")]


def test_persist_no_index(ip):
    runsql(ip, "")
    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.run_cell("results_no_index = results.DataFrame()")
    ip.run_cell("%sql --persist sqlite:// results_no_index --no-index")
    persisted = runsql(ip, "SELECT * FROM results_no_index")
    assert persisted == [(1, "foo"), (2, "bar")]


def test_persist_invalid_identifier(ip):
    result = ip.run_cell("%sql --persist sqlite:// not an identifier")
    assert "not a valid identifier" in str(result.error_in_exec)


def test_persist_undefined_variable(ip):
    result = ip.run_cell("%sql --persist sqlite:// not_a_variable")
    assert "it's undefined" in str(result.error_in_exec)


def test_append(ip):
    runsql(ip, "")
    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.run_cell("results_dframe_append = results.DataFrame()")
    ip.run_cell("%sql --persist sqlite:// results_dframe_append")
    persisted = runsql(ip, "SELECT COUNT(*) FROM results_dframe_append")
    ip.run_cell("%sql --append sqlite:// results_dframe_append")
    appended = runsql(ip, "SELECT COUNT(*) FROM results_dframe_append")
    assert appended[0][0] == persisted[0][0] * 2


def test_persist_nonexistent_raises(ip):
    runsql(ip, "")
    result = ip.run_cell("%sql --persist sqlite:// no_such_dataframe")
    assert result.error_in_exec


def test_persist_non_frame_raises(ip):
    ip.run_cell("not_a_dataframe = 22")
    runsql(ip, "")
    result = ip.run_cell("%sql --persist sqlite:// not_a_dataframe")
    assert isinstance(result.error_in_exec, UsageError)
    assert (
        "is not a Pandas DataFrame or Series".lower()
        in str(result.error_in_exec).lower()
    )


def test_persist_bare(ip):
    result = ip.run_cell("%sql --persist sqlite://")
    assert result.error_in_exec


def test_connection_args_enforce_json(ip):
    result = ip.run_cell('%sql --connection_arguments {"badlyformed":true')
    assert result.error_in_exec


@pytest.mark.skipif(platform.system() == "Windows", reason="failing on windows")
def test_connection_args_in_connection(ip):
    ip.run_cell('%sql --connection_arguments {"timeout":10} sqlite:///:memory:')
    result = ip.run_cell("%sql --connections")
    assert "timeout" in result.result["sqlite:///:memory:"].connect_args


@pytest.mark.skipif(platform.system() == "Windows", reason="failing on windows")
def test_connection_args_single_quotes(ip):
    ip.run_cell("%sql --connection_arguments '{\"timeout\": 10}' sqlite:///:memory:")
    result = ip.run_cell("%sql --connections")
    assert "timeout" in result.result["sqlite:///:memory:"].connect_args


@pytest.mark.skipif(platform.system() == "Windows", reason="failing on windows")
def test_connection_args_double_quotes(ip):
    ip.run_cell('%sql --connection_arguments "{\\"timeout\\": 10}" sqlite:///:memory:')
    result = ip.run_cell("%sql --connections")
    assert "timeout" in result.result["sqlite:///:memory:"].connect_args


# TODO: support
# @with_setup(_setup_author, _teardown_author)
# def test_persist_with_connection_info():
#     ip.run_cell("results = %sql SELECT * FROM author;")
#     ip.run_line_magic('sql', 'sqlite:// PERSIST results.DataFrame()')
#     persisted = ip.run_line_magic('sql', 'SELECT * FROM results')
#     assert 'Shakespeare' in str(persisted)


@pytest.mark.parametrize("value", ["None", "0"])
def test_displaylimit_disabled(ip, value):
    ip.run_line_magic("config", "SqlMagic.autolimit = None")

    ip.run_line_magic("config", f"SqlMagic.displaylimit = {value}")
    result = runsql(ip, "SELECT * FROM author;")

    assert "Brecht" in result._repr_html_()
    assert "Shakespeare" in result._repr_html_()


def test_displaylimit(ip):
    ip.run_line_magic("config", "SqlMagic.autolimit = None")

    ip.run_line_magic("config", "SqlMagic.displaylimit = 1")
    result = runsql(ip, "SELECT * FROM author ORDER BY first_name;")

    assert "Brecht" in result._repr_html_()
    assert "Shakespeare" not in result._repr_html_()


def test_column_local_vars(ip):
    ip.run_line_magic("config", "SqlMagic.column_local_vars = True")
    result = runsql(ip, "SELECT * FROM author;")
    assert result is None
    assert "William" in ip.user_global_ns["first_name"]
    assert "Shakespeare" in ip.user_global_ns["last_name"]
    assert len(ip.user_global_ns["first_name"]) == 2
    ip.run_line_magic("config", "SqlMagic.column_local_vars = False")


def test_userns_not_changed(ip):
    ip.run_cell(
        dedent(
            """
    def function():
        local_var = 'local_val'
        %sql sqlite:// INSERT INTO test VALUES (2, 'bar');
    function()"""
        )
    )
    assert "local_var" not in ip.user_ns


def test_bind_vars(ip):
    ip.user_global_ns["x"] = 22
    result = runsql(ip, "SELECT {{x}}")
    assert result[0][0] == 22


def test_autopandas(ip):
    ip.run_line_magic("config", "SqlMagic.autopandas = True")
    dframe = runsql(ip, "SELECT * FROM test;")
    assert not dframe.empty
    assert dframe.ndim == 2
    assert dframe.name[0] == "foo"


def test_autopolars(ip):
    ip.run_line_magic("config", "SqlMagic.autopolars = True")
    dframe = runsql(ip, "SELECT * FROM test;")

    assert type(dframe) == pl.DataFrame
    assert not dframe.is_empty()
    assert len(dframe.shape) == 2
    assert dframe["name"][0] == "foo"


def test_autopolars_infer_schema_length(ip):
    """Test for `SqlMagic.polars_dataframe_kwargs = {"infer_schema_length": None}`
    Without this config, polars will raise an exception when it cannot infer the
    correct schema from the first 100 rows.
    """
    # Create a table with 100 rows with a NULL value and one row with a non-NULL value
    ip.run_line_magic("config", "SqlMagic.autopolars = True")
    sql = ["CREATE TABLE test_autopolars_infer_schema (n INT, name TEXT)"]
    for i in range(100):
        sql.append(f"INSERT INTO test_autopolars_infer_schema VALUES ({i}, NULL)")
    sql.append("INSERT INTO test_autopolars_infer_schema VALUES (100, 'foo')")
    runsql(ip, sql)

    # By default, this dataset should raise a ComputeError
    with pytest.raises(pl.exceptions.ComputeError):
        runsql(ip, "SELECT * FROM test_autopolars_infer_schema;")

    # To avoid this error, pass the `infer_schema_length` argument to polars.DataFrame
    line_magic = 'SqlMagic.polars_dataframe_kwargs = {"infer_schema_length": None}'
    ip.run_line_magic("config", line_magic)
    dframe = runsql(ip, "SELECT * FROM test_autopolars_infer_schema;")
    assert dframe.schema == {"n": pl.Int64, "name": pl.Utf8}

    # Assert that if we unset the dataframe kwargs, the error is raised again
    ip.run_line_magic("config", "SqlMagic.polars_dataframe_kwargs = {}")
    with pytest.raises(pl.exceptions.ComputeError):
        runsql(ip, "SELECT * FROM test_autopolars_infer_schema;")

    runsql(ip, "DROP TABLE test_autopolars_infer_schema")


def test_mutex_autopolars_autopandas(ip):
    dframe = runsql(ip, "SELECT * FROM test;")
    assert type(dframe) == ResultSet

    ip.run_line_magic("config", "SqlMagic.autopolars = True")
    dframe = runsql(ip, "SELECT * FROM test;")
    assert type(dframe) == pl.DataFrame

    import pandas as pd

    ip.run_line_magic("config", "SqlMagic.autopandas = True")
    dframe = runsql(ip, "SELECT * FROM test;")
    assert type(dframe) == pd.DataFrame

    # Test that re-enabling autopolars works
    ip.run_line_magic("config", "SqlMagic.autopolars = True")
    dframe = runsql(ip, "SELECT * FROM test;")
    assert type(dframe) == pl.DataFrame

    # Disabling autopolars at this point should result in the default behavior
    ip.run_line_magic("config", "SqlMagic.autopolars = False")
    dframe = runsql(ip, "SELECT * FROM test;")
    assert type(dframe) == ResultSet


def test_csv(ip):
    ip.run_line_magic("config", "SqlMagic.autopandas = False")  # uh-oh
    result = runsql(ip, "SELECT * FROM test;")
    result = result.csv()
    for row in result.splitlines():
        assert row.count(",") == 1
    assert len(result.splitlines()) == 3


def test_csv_to_file(ip):
    ip.run_line_magic("config", "SqlMagic.autopandas = False")  # uh-oh
    result = runsql(ip, "SELECT * FROM test;")
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, "test.csv")
        output = result.csv(fname)
        assert os.path.exists(output.file_path)
        with open(output.file_path) as csvfile:
            content = csvfile.read()
            for row in content.splitlines():
                assert row.count(",") == 1
            assert len(content.splitlines()) == 3


def test_sql_from_file(ip):
    ip.run_line_magic("config", "SqlMagic.autopandas = False")
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, "test.sql")
        with open(fname, "w") as tempf:
            tempf.write("SELECT * FROM test;")
        result = ip.run_cell("%sql --file " + fname)
        assert result.result == [(1, "foo"), (2, "bar")]


def test_sql_from_nonexistent_file(ip):
    ip.run_line_magic("config", "SqlMagic.autopandas = False")
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, "nonexistent.sql")
        result = ip.run_cell("%sql --file " + fname)
        assert isinstance(result.error_in_exec, FileNotFoundError)


def test_dict(ip):
    result = runsql(ip, "SELECT * FROM author;")
    result = result.dict()
    assert isinstance(result, dict)
    assert "first_name" in result
    assert "last_name" in result
    assert "year_of_death" in result
    assert len(result["last_name"]) == 2


def test_dicts(ip):
    result = runsql(ip, "SELECT * FROM author;")
    for row in result.dicts():
        assert isinstance(row, dict)
        assert "first_name" in row
        assert "last_name" in row
        assert "year_of_death" in row


def test_bracket_var_substitution(ip):
    ip.user_global_ns["col"] = "first_name"
    assert runsql(ip, "SELECT * FROM author" " WHERE {{col}} = 'William' ")[0] == (
        "William",
        "Shakespeare",
        1616,
    )

    ip.user_global_ns["col"] = "last_name"
    result = runsql(ip, "SELECT * FROM author" " WHERE {{col}} = 'William' ")
    assert not result


# the next two tests had the same name, so I added a _2 to the second one
def test_multiline_bracket_var_substitution(ip):
    ip.user_global_ns["col"] = "first_name"
    assert runsql(ip, "SELECT * FROM author\n" " WHERE {{col}} = 'William' ")[0] == (
        "William",
        "Shakespeare",
        1616,
    )

    ip.user_global_ns["col"] = "last_name"
    result = runsql(ip, "SELECT * FROM author" " WHERE {{col}} = 'William' ")
    assert not result


def test_multiline_bracket_var_substitution_2(ip):
    ip.user_global_ns["col"] = "first_name"
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite:// SELECT * FROM author
        WHERE {{col}} = 'William'
        """,
    )
    assert ("William", "Shakespeare", 1616) in result

    ip.user_global_ns["col"] = "last_name"
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite:// SELECT * FROM author
        WHERE {{col}} = 'William'
        """,
    )
    assert not result


def test_json_in_select(ip):
    # Variable expansion does not work within json, but
    # at least the two usages of curly braces do not collide
    ip.user_global_ns["person"] = "prince"
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite://
        SELECT
          '{"greeting": "Farewell sweet {person}"}'
        AS json
        """,
    )

    assert result == [('{"greeting": "Farewell sweet {person}"}',)]


def test_closed_connections_are_no_longer_listed(ip):
    connections = runsql(ip, "%sql -l")
    connection_name = list(connections)[0]
    runsql(ip, f"%sql -x {connection_name}")
    connections_afterward = runsql(ip, "%sql -l")
    assert connection_name not in connections_afterward


def test_close_connection(ip, tmp_empty):
    # open two connections
    ip.run_cell("%sql sqlite:///one.db")
    ip.run_cell("%sql sqlite:///two.db")

    # close them
    ip.run_cell("%sql -x sqlite:///one.db")
    ip.run_cell("%sql --close sqlite:///two.db")

    assert "sqlite:///one.db" not in Connection.connections
    assert "sqlite:///two.db" not in Connection.connections


def test_alias(clean_conns, ip_empty, tmp_empty):
    ip_empty.run_cell("%sql sqlite:///one.db --alias one")
    assert {"one"} == set(Connection.connections)


def test_alias_existing_engine(clean_conns, ip_empty, tmp_empty):
    ip_empty.user_global_ns["first"] = create_engine("sqlite:///first.db")
    ip_empty.run_cell("%sql first --alias one")
    assert {"one"} == set(Connection.connections)


def test_alias_custom_connection(clean_conns, ip_empty, tmp_empty):
    ip_empty.user_global_ns["first"] = sqlite3.connect(":memory:")
    ip_empty.run_cell("%sql first --alias one")
    assert {"one"} == set(Connection.connections)


def test_close_connection_with_alias(ip, tmp_empty):
    # open two connections
    ip.run_cell("%sql sqlite:///one.db --alias one")
    ip.run_cell("%sql sqlite:///two.db --alias two")

    # close them
    ip.run_cell("%sql -x one")
    ip.run_cell("%sql --close two")

    assert "sqlite:///one.db" not in Connection.connections
    assert "sqlite:///two.db" not in Connection.connections
    assert "one" not in Connection.connections
    assert "two" not in Connection.connections


def test_close_connection_with_existing_engine_and_alias(ip, tmp_empty):
    ip.user_global_ns["first"] = create_engine("sqlite:///first.db")
    ip.user_global_ns["second"] = create_engine("sqlite:///second.db")

    # open two connections
    ip.run_cell("%sql first --alias one")
    ip.run_cell("%sql second --alias two")

    # close them
    ip.run_cell("%sql -x one")
    ip.run_cell("%sql --close two")

    assert "sqlite:///first.db" not in Connection.connections
    assert "sqlite:///second.db" not in Connection.connections
    assert "first" not in Connection.connections
    assert "second" not in Connection.connections


def test_close_connection_with_custom_connection_and_alias(ip, tmp_empty):
    ip.user_global_ns["first"] = sqlite3.connect("first.db")
    ip.user_global_ns["second"] = sqlite3.connect("second.db")

    # open two connections
    ip.run_cell("%sql first --alias one")
    ip.run_cell("%sql second --alias two")

    # close them
    ip.run_cell("%sql -x one")
    ip.run_cell("%sql --close two")

    assert "sqlite:///first.db" not in Connection.connections
    assert "sqlite:///second.db" not in Connection.connections
    assert "first" not in Connection.connections
    assert "second" not in Connection.connections


def test_column_names_visible(ip, tmp_empty):
    res = ip.run_line_magic("sql", "SELECT * FROM empty_table")

    assert "<th>column</th>" in res._repr_html_()
    assert "<th>another</th>" in res._repr_html_()


@pytest.mark.xfail(reason="known parse @ parser.py error")
def test_sqlite_path_with_spaces(ip, tmp_empty):
    ip.run_cell("%sql sqlite:///some database.db")

    assert Path("some database.db").is_file()


def test_pass_existing_engine(ip, tmp_empty):
    ip.user_global_ns["my_engine"] = create_engine("sqlite:///my.db")
    ip.run_line_magic("sql", "  my_engine ")

    runsql(
        ip,
        [
            "CREATE TABLE some_data (n INT, name TEXT)",
            "INSERT INTO some_data VALUES (10, 'foo')",
            "INSERT INTO some_data VALUES (20, 'bar')",
        ],
    )

    result = ip.run_line_magic("sql", "SELECT * FROM some_data")

    assert result == [(10, "foo"), (20, "bar")]


# theres some weird shared state with this one, moving it to the end
def test_autolimit(ip):
    # test table has two rows
    ip.run_line_magic("config", "SqlMagic.autolimit = 0")
    result = runsql(ip, "SELECT * FROM test;")
    assert len(result) == 2

    # test table has two rows
    ip.run_line_magic("config", "SqlMagic.autolimit = None")
    result = runsql(ip, "SELECT * FROM test;")
    assert len(result) == 2

    # test setting autolimit to 1
    ip.run_line_magic("config", "SqlMagic.autolimit = 1")
    result = runsql(ip, "SELECT * FROM test;")
    assert len(result) == 1


invalid_connection_string = """
No active connection.

To fix it:

Pass a valid connection string:
    Example: %sql postgresql://username:password@hostname/dbname

OR

Set the environment variable $DATABASE_URL

For technical support: https://ploomber.io/community
Documentation: https://jupysql.ploomber.io/en/latest/connecting.html
"""


def test_error_on_invalid_connection_string(ip_empty, clean_conns):
    result = ip_empty.run_cell("%sql some invalid connection string")

    assert invalid_connection_string.strip() == str(result.error_in_exec)
    assert isinstance(result.error_in_exec, UsageError)


invalid_connection_string_format = """\
Can't load plugin: sqlalchemy.dialects:something

To fix it, make sure you are using correct driver name:
Ref: https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls

For technical support: https://ploomber.io/community
Documentation: https://jupysql.ploomber.io/en/latest/connecting.html
"""  # noqa


def test_error_on_invalid_connection_string_format(ip_empty, clean_conns):
    result = ip_empty.run_cell("%sql something://")

    assert invalid_connection_string_format.strip() == str(result.error_in_exec)
    assert isinstance(result.error_in_exec, UsageError)


invalid_connection_string_existing_conns = """
Can't load plugin: sqlalchemy.dialects:something

To fix it, make sure you are using correct driver name:
Ref: https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls

For technical support: https://ploomber.io/community
Documentation: https://jupysql.ploomber.io/en/latest/connecting.html
"""  # noqa


def test_error_on_invalid_connection_string_with_existing_conns(ip_empty, clean_conns):
    ip_empty.run_cell("%sql sqlite://")
    result = ip_empty.run_cell("%sql something://")

    assert invalid_connection_string_existing_conns.strip() == str(result.error_in_exec)
    assert isinstance(result.error_in_exec, UsageError)


invalid_connection_string_with_possible_typo = """
Can't load plugin: sqlalchemy.dialects:sqlit

Perhaps you meant to use driver the dialect: "sqlite"

For technical support: https://ploomber.io/community
Documentation: https://jupysql.ploomber.io/en/latest/connecting.html
"""  # noqa


def test_error_on_invalid_connection_string_with_possible_typo(ip_empty, clean_conns):
    ip_empty.run_cell("%sql sqlite://")
    result = ip_empty.run_cell("%sql sqlit://")

    assert invalid_connection_string_with_possible_typo.strip() == str(
        result.error_in_exec
    )
    assert isinstance(result.error_in_exec, UsageError)


def test_jupysql_alias():
    assert SqlMagic.magics == {
        "line": {"jupysql": "execute", "sql": "execute"},
        "cell": {"jupysql": "execute", "sql": "execute"},
    }


@pytest.mark.xfail(reason="will be fixed once we deprecate the $name parametrization")
def test_columns_with_dollar_sign(ip_empty):
    ip_empty.run_cell("%sql sqlite://")
    result = ip_empty.run_cell(
        """
    %sql SELECT $2 FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three'))"""
    )

    html = result.result._repr_html_()

    assert "$2" in html


def test_save_with(ip):
    # First Query
    ip.run_cell(
        "%sql --save shakespeare SELECT * FROM author WHERE last_name = 'Shakespeare'"
    )
    # Second Query
    ip.run_cell(
        "%sql --with shakespeare --save shake_born_in_1616 SELECT * FROM "
        "shakespeare WHERE year_of_death = 1616"
    )

    # Third Query
    ip.run_cell(
        "%sql --save shake_born_in_1616_limit_10 --with shake_born_in_1616"
        " SELECT * FROM shake_born_in_1616 LIMIT 10"
    )

    second_out = ip.run_cell(
        "%sql --with shake_born_in_1616 SELECT * FROM shake_born_in_1616"
    )
    third_out = ip.run_cell(
        "%sql --with shake_born_in_1616_limit_10"
        " SELECT * FROM shake_born_in_1616_limit_10"
    )
    assert second_out.result == [("William", "Shakespeare", 1616)]
    assert third_out.result == [("William", "Shakespeare", 1616)]


@pytest.mark.parametrize(
    "prep_cell_1, prep_cell_2, prep_cell_3, with_cell_1,"
    " with_cell_2, with_cell_1_excepted, with_cell_2_excepted",
    [
        [
            "%sql --save everything SELECT * FROM number_table",
            "%sql --with everything --no-execute --save positive_x"
            " SELECT * FROM everything WHERE x > 0",
            "%sql --with positive_x --no-execute --save "
            "positive_x_and_y SELECT * FROM positive_x WHERE y > 0",
            "%sql --with positive_x SELECT * FROM positive_x",
            "%sql --with positive_x_and_y SELECT * FROM positive_x_and_y",
            [(4, -2), (2, 4), (2, -5), (4, 3)],
            [(2, 4), (4, 3)],
        ],
        [
            "%sql --save everything SELECT * FROM number_table",
            "%sql --with everything --no-execute --save odd_x "
            "SELECT * FROM everything WHERE x % 2 != 0",
            "%sql --with odd_x --no-execute --save odd_x_and_y "
            "SELECT * FROM odd_x WHERE y % 2 != 0",
            "%sql --with odd_x SELECT * FROM odd_x",
            "%sql --with odd_x_and_y SELECT * FROM odd_x_and_y",
            [(-5, 0), (-5, -1)],
            [(-5, -1)],
        ],
    ],
)
def test_save_with_number_table(
    ip,
    prep_cell_1,
    prep_cell_2,
    prep_cell_3,
    with_cell_1,
    with_cell_2,
    with_cell_1_excepted,
    with_cell_2_excepted,
):
    ip.run_cell(prep_cell_1)
    ip.run_cell(prep_cell_2)
    ip.run_cell(prep_cell_3)
    ip.run_cell(prep_cell_1)

    with_cell_1_out = ip.run_cell(with_cell_1).result
    with_cell_2_out = ip.run_cell(with_cell_2).result
    assert with_cell_1_excepted == with_cell_1_out
    assert with_cell_2_excepted == with_cell_2_out


def test_save_with_non_existing_with(ip):
    out = ip.run_cell(
        "%sql --with non_existing_sub_query " "SELECT * FROM non_existing_sub_query"
    )
    assert isinstance(out.error_in_exec, UsageError)


def test_save_with_non_existing_table(ip, capsys):
    ip.run_cell("%sql --save my_query SELECT * FROM non_existing_table")
    out, _ = capsys.readouterr()
    assert "(sqlite3.OperationalError) no such table: non_existing_table" in out


def test_save_with_bad_query_save(ip, capsys):
    ip.run_cell("%sql --save my_query SELECT * non_existing_table")
    ip.run_cell("%sql --with my_query SELECT * FROM my_query")
    out, _ = capsys.readouterr()
    assert '(sqlite3.OperationalError) near "non_existing_table": syntax error' in out


def test_interact_basic_data_types(ip, capsys):
    ip.user_global_ns["my_variable"] = 5
    ip.run_cell(
        "%sql --interact my_variable SELECT * FROM author LIMIT {{my_variable}}"
    )
    out, _ = capsys.readouterr()

    assert (
        "Interactive mode, please interact with below widget(s)"
        " to control the variable" in out
    )


@pytest.fixture
def mockValueWidget(monkeypatch):
    with patch("ipywidgets.widgets.IntSlider") as MockClass:
        instance = MockClass.return_value
        yield instance


def test_interact_basic_widgets(ip, mockValueWidget, capsys):
    print("mock", mockValueWidget.value)
    ip.user_global_ns["my_widget"] = mockValueWidget

    ip.run_cell(
        "%sql --interact my_widget SELECT * FROM number_table LIMIT {{my_widget}}"
    )
    out, _ = capsys.readouterr()
    assert (
        "Interactive mode, please interact with below widget(s)"
        " to control the variable" in out
    )


def test_interact_and_missing_ipywidgets_installed(ip):
    with patch.dict(sys.modules):
        sys.modules["ipywidgets"] = None
        ip.user_global_ns["my_variable"] = 5
        out = ip.run_cell(
            "%sql --interact my_variable SELECT * FROM author LIMIT {{my_variable}}"
        )
        assert isinstance(out.error_in_exec, ModuleNotFoundError)
