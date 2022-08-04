import os.path
import re
import tempfile
from textwrap import dedent

import pytest

from conftest import runsql


def test_memory_db(ip):
    assert runsql(ip, "SELECT * FROM test;")[0][0] == 1
    assert runsql(ip, "SELECT * FROM test;")[1]["name"] == "bar"


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


def test_result_var(ip):
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
    assert "Shakespeare" in str(result) and "Brecht" in str(result)


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
        u"William",
        u"Shakespeare",
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
    assert (u"Brecht", u"Brecht") in result


def test_persist(ip):
    runsql(ip, "")
    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.run_cell("results_dframe = results.DataFrame()")
    ip.run_cell("%sql --persist sqlite:// results_dframe")
    persisted = runsql(ip, "SELECT * FROM results_dframe")
    assert persisted == [(0, 1, 'foo'), (1, 2, 'bar')]


def test_persist_no_index(ip):
    runsql(ip, "")
    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.run_cell("results_no_index = results.DataFrame()")
    ip.run_cell("%sql --persist sqlite:// results_no_index --no-index")
    persisted = runsql(ip, "SELECT * FROM results_no_index")
    assert persisted == [(1, 'foo'), (2, 'bar')]


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
    assert result.error_in_exec


def test_persist_bare(ip):
    result = ip.run_cell("%sql --persist sqlite://")
    assert result.error_in_exec


def test_persist_frame_at_its_creation(ip):
    ip.run_cell("results = %sql SELECT * FROM author;")
    ip.run_cell("%sql --persist sqlite:// results.DataFrame()")
    persisted = runsql(ip, "SELECT * FROM results")
    assert "Shakespeare" in str(persisted)


def test_connection_args_enforce_json(ip):
    result = ip.run_cell('%sql --connection_arguments {"badlyformed":true')
    assert result.error_in_exec


def test_connection_args_in_connection(ip):
    ip.run_cell(
        '%sql --connection_arguments {"timeout":10} sqlite:///:memory:')
    result = ip.run_cell("%sql --connections")
    assert "timeout" in result.result["sqlite:///:memory:"].connect_args


def test_connection_args_single_quotes(ip):
    ip.run_cell(
        "%sql --connection_arguments '{\"timeout\": 10}' sqlite:///:memory:")
    result = ip.run_cell("%sql --connections")
    assert "timeout" in result.result["sqlite:///:memory:"].connect_args


def test_connection_args_double_quotes(ip):
    ip.run_cell(
        '%sql --connection_arguments "{\\"timeout\\": 10}" sqlite:///:memory:')
    result = ip.run_cell("%sql --connections")
    assert "timeout" in result.result["sqlite:///:memory:"].connect_args


# TODO: support
# @with_setup(_setup_author, _teardown_author)
# def test_persist_with_connection_info():
#     ip.run_cell("results = %sql SELECT * FROM author;")
#     ip.run_line_magic('sql', 'sqlite:// PERSIST results.DataFrame()')
#     persisted = ip.run_line_magic('sql', 'SELECT * FROM results')
#     assert 'Shakespeare' in str(persisted)


def test_displaylimit(ip):
    ip.run_line_magic("config", "SqlMagic.autolimit = None")
    ip.run_line_magic("config", "SqlMagic.displaylimit = None")
    result = runsql(
        ip,
        "SELECT * FROM (VALUES ('apple'), ('banana'), ('cherry')) AS Result ORDER BY 1;",
    )
    assert "apple" in result._repr_html_()
    assert "banana" in result._repr_html_()
    assert "cherry" in result._repr_html_()
    ip.run_line_magic("config", "SqlMagic.displaylimit = 1")
    result = runsql(
        ip,
        "SELECT * FROM (VALUES ('apple'), ('banana'), ('cherry')) AS Result ORDER BY 1;",
    )
    assert "apple" in result._repr_html_()
    assert "cherry" not in result._repr_html_()


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
        dedent("""
    def function():
        local_var = 'local_val'
        %sql sqlite:// INSERT INTO test VALUES (2, 'bar');
    function()"""))
    assert "local_var" not in ip.user_ns


def test_bind_vars(ip):
    ip.user_global_ns["x"] = 22
    result = runsql(ip, "SELECT :x")
    assert result[0][0] == 22


def test_autopandas(ip):
    ip.run_line_magic("config", "SqlMagic.autopandas = True")
    dframe = runsql(ip, "SELECT * FROM test;")
    assert not dframe.empty
    assert dframe.ndim == 2
    assert dframe.name[0] == "foo"


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
    assert runsql(ip, "SELECT * FROM author"
                  " WHERE {col} = 'William' ")[0] == (
                      u"William",
                      u"Shakespeare",
                      1616,
                  )

    ip.user_global_ns["col"] = "last_name"
    result = runsql(ip, "SELECT * FROM author"
                    " WHERE {col} = 'William' ")
    assert not result


def test_multiline_bracket_var_substitution(ip):

    ip.user_global_ns["col"] = "first_name"
    assert runsql(ip, "SELECT * FROM author\n"
                  " WHERE {col} = 'William' ")[0] == (
                      u"William",
                      u"Shakespeare",
                      1616,
                  )

    ip.user_global_ns["col"] = "last_name"
    result = runsql(ip, "SELECT * FROM author"
                    " WHERE {col} = 'William' ")
    assert not result


def test_multiline_bracket_var_substitution(ip):
    ip.user_global_ns["col"] = "first_name"
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite:// SELECT * FROM author
        WHERE {col} = 'William'
        """,
    )
    assert (u"William", u"Shakespeare", 1616) in result

    ip.user_global_ns["col"] = "last_name"
    result = ip.run_cell_magic(
        "sql",
        "",
        """
        sqlite:// SELECT * FROM author
        WHERE {col} = 'William'
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
    assert ('{"greeting": "Farewell sweet {person}"}', )


def test_close_connection(ip):
    connections = runsql(ip, "%sql -l")
    connection_name = list(connections)[0]
    runsql(ip, f"%sql -x {connection_name}")
    connections_afterward = runsql(ip, "%sql -l")
    assert connection_name not in connections_afterward


# theres some weird shared state with this one, moving it to the end
def test_autolimit(ip):
    ip.run_line_magic("config", "SqlMagic.autolimit = 0")
    result = runsql(ip, "SELECT * FROM test;")
    assert len(result) == 2
    ip.run_line_magic("config", "SqlMagic.autolimit = 1")
    result = runsql(ip, "SELECT * FROM test;")
    assert len(result) == 1
