import os
from pathlib import Path


import pytest

from sql.parse import (
    connection_str_from_dsn_section,
    parse,
    without_sql_comment,
    magic_args,
    escape_string_literals_with_colon_prefix,
    escape_string_slicing_notation,
    find_named_parameters,
    _connection_string,
    ConnectionsFile,
)


default_connect_args = {"options": "-csearch_path=test"}

PATH_TO_DSN_FILE = "src/tests/test_dsn_config.ini"


class DummyConfig:
    dsn_filename = Path("src/tests/test_dsn_config.ini")


def test_parse_no_sql():
    assert parse("will:longliveliz@localhost/shakes", PATH_TO_DSN_FILE) == {
        "connection": "will:longliveliz@localhost/shakes",
        "sql": "",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_with_sql():
    assert parse(
        "postgresql://will:longliveliz@localhost/shakes SELECT * FROM work",
        PATH_TO_DSN_FILE,
    ) == {
        "connection": "postgresql://will:longliveliz@localhost/shakes",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_sql_only():
    assert parse("SELECT * FROM work", PATH_TO_DSN_FILE) == {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_postgresql_socket_connection():
    assert parse("postgresql:///shakes SELECT * FROM work", PATH_TO_DSN_FILE) == {
        "connection": "postgresql:///shakes",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_expand_environment_variables_in_connection():
    os.environ["DATABASE_URL"] = "postgresql:///shakes"
    assert parse("$DATABASE_URL SELECT * FROM work", PATH_TO_DSN_FILE) == {
        "connection": "postgresql:///shakes",
        "sql": "SELECT * FROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_shovel_operator():
    assert parse("dest << SELECT * FROM work", PATH_TO_DSN_FILE) == {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": False,
    }


@pytest.mark.parametrize(
    "input_string",
    [
        "dest= << SELECT * FROM work",
        "dest = << SELECT * FROM work",
        "dest =<< SELECT * FROM work",
        "dest =        << SELECT * FROM work",
        "dest      =<< SELECT * FROM work",
        "dest =          << SELECT * FROM work",
        "dest=<< SELECT * FROM work",
        "dest=<<SELECT * FROM work",
        "dest    =<<SELECT * FROM work",
        "dest    =<<    SELECT * FROM work",
        "dest=   <<    SELECT * FROM work",
    ],
)
def test_parse_return_shovel_operator_with_equal(input_string, ip):
    result_var = {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": True,
    }
    assert parse(input_string, PATH_TO_DSN_FILE) == result_var


@pytest.mark.parametrize(
    "input_string",
    [
        "dest<< SELECT * FROM work",
        "dest<<SELECT * FROM work",
        "dest    <<SELECT * FROM work",
        "dest    <<    SELECT * FROM work",
        "dest <<SELECT * FROM work",
        "dest << SELECT * FROM work",
    ],
)
def test_parse_return_shovel_operator_without_equal(input_string, ip):
    result_var = {
        "connection": "",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": False,
    }
    assert parse(input_string, PATH_TO_DSN_FILE) == result_var


def test_parse_connect_plus_shovel():
    assert parse("sqlite:// dest << SELECT * FROM work", PATH_TO_DSN_FILE) == {
        "connection": "sqlite://",
        "sql": "SELECT * FROM work",
        "result_var": "dest",
        "return_result_var": False,
    }


def test_parse_early_newlines():
    assert parse("--comment\nSELECT *\n--comment\nFROM work", PATH_TO_DSN_FILE) == {
        "connection": "",
        "sql": "--comment\nSELECT *\n--comment\nFROM work",
        "result_var": None,
        "return_result_var": False,
    }


def test_parse_connect_shovel_over_newlines():
    assert parse("\nsqlite://\ndest\n<<\nSELECT *\nFROM work", PATH_TO_DSN_FILE) == {
        "connection": "sqlite://",
        "sql": "\nSELECT *\nFROM work",
        "result_var": "dest",
        "return_result_var": False,
    }


@pytest.mark.parametrize(
    "section, expected",
    [
        (
            "DB_CONFIG_1",
            "postgres://goesto11:seentheelephant@my.remote.host:5432/pgmain",
        ),
        (
            "DB_CONFIG_2",
            "mysql://thefin:fishputsfishonthetable@127.0.0.1/dolfin",
        ),
    ],
)
def test_connection_from_dsn_section(section, expected):
    result = connection_str_from_dsn_section(section=section, config=DummyConfig)
    assert result == expected


@pytest.mark.parametrize(
    "input_, expected",
    [
        ("", ""),
        (
            "drivername://user:pass@host:port/db",
            "drivername://user:pass@host:port/db",
        ),
        ("drivername://", "drivername://"),
        (
            "[DB_CONFIG_1]",
            "postgres://goesto11:seentheelephant@my.remote.host:5432/pgmain",
        ),
        ("DB_CONFIG_1", ""),
        ("not-a-url", ""),
    ],
    ids=[
        "empty",
        "full",
        "drivername",
        "section",
        "not-a-section",
        "not-a-url",
    ],
)
def test_connection_string(input_, expected):
    assert _connection_string(input_, "src/tests/test_dsn_config.ini") == expected


class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class ParserStub:
    opstrs = [
        [],
        ["-l", "--connections"],
        ["-x", "--close"],
        ["-c", "--creator"],
        ["-s", "--section"],
        ["-p", "--persist"],
        ["--append"],
        ["-a", "--connection_arguments"],
        ["-f", "--file"],
    ]
    _actions = [Bunch(option_strings=o) for o in opstrs]


parser_stub = ParserStub()


def test_without_sql_comment_plain():
    line = "SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == line


def test_without_sql_comment_with_arg():
    line = "--file moo.txt --persist SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == line


def test_without_sql_comment_with_comment():
    line = "SELECT * FROM author -- uff da"
    expected = "SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_with_arg_and_comment():
    line = "--file moo.txt --persist SELECT * FROM author -- uff da"
    expected = "--file moo.txt --persist SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_unspaced_comment():
    line = "SELECT * FROM author --uff da"
    expected = "SELECT * FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_dashes_in_string():
    line = "SELECT '--very --confusing' FROM author -- uff da"
    expected = "SELECT '--very --confusing' FROM author"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_comment_with_arg_and_leading_comment():
    line = "--file moo.txt --persist --comment, not arg"
    expected = "--file moo.txt --persist"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def test_without_sql_persist():
    line = "--persist my_table --uff da"
    expected = "--persist my_table"
    assert without_sql_comment(parser=parser_stub, line=line) == expected


def complete_with_defaults(mapping):
    defaults = {
        "alias": None,
        "line": ["some-argument"],
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
        "with_": None,
        "no_execute": False,
    }

    return {**defaults, **mapping}


@pytest.mark.parametrize(
    "line, expected",
    [
        (
            "some-argument",
            {"line": ["some-argument"]},
        ),
        (
            "a b c",
            {"line": ["a", "b", "c"]},
        ),
        (
            "a b c --file query.sql",
            {"line": ["a", "b", "c"], "file": "query.sql"},
        ),
    ],
)
def test_magic_args(ip, line, expected):
    sql_line = ip.magics_manager.lsmagic()["line"]["sql"]

    args = magic_args(sql_line, line)

    assert args.__dict__ == complete_with_defaults(expected)


@pytest.mark.parametrize(
    "query, expected_escaped, expected_found",
    [
        ("SELECT * FROM table where x > :x", "SELECT * FROM table where x > :x", []),
        (
            "SELECT * FROM table where x > ':x'",
            "SELECT * FROM table where x > '\\:x'",
            ["x"],
        ),
        (
            'SELECT * FROM table where x > ":y"',
            'SELECT * FROM table where x > "\\:y"',
            ["y"],
        ),
        (
            "SELECT * FROM table where x > '':something''",
            "SELECT * FROM table where x > ''\\:something''",
            ["something"],
        ),
        (
            'SELECT * FROM table where x > "":var""',
            'SELECT * FROM table where x > ""\\:var""',
            ["var"],
        ),
    ],
    ids=[
        "no-escape",
        "single-quote",
        "double-quote",
        "double-single-quote",
        "double-double-quote",
    ],
)
def test_escape_string_literals_with_colon_prefix(
    query, expected_escaped, expected_found
):
    escaped, found = escape_string_literals_with_colon_prefix(query)
    assert escaped == expected_escaped
    assert found == expected_found


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM penguins WHERE species = :species AND mass = ':mass'",
            ["species"],
        ),
        (
            'SELECT * FROM penguins WHERE species = :species AND mass = ":mass"',
            ["species"],
        ),
        (
            "SELECT * FROM penguins WHERE species = :species AND mass = :mass",
            ["species", "mass"],
        ),
    ],
)
def test_find_named_parameters(query, expected):
    assert find_named_parameters(query) == expected


@pytest.mark.parametrize(
    "content, expected",
    [
        (
            """
[duck]
drivername = duckdb
""",
            None,
        ),
        (
            """
[default]
drivername = duckdb
""",
            "duckdb://",
        ),
        (
            """
[default]
drivername = postgresql
host = localhost
port = 5432
username = user
password = pass
database = db
""",
            "postgresql://user:pass@localhost:5432/db",
        ),
    ],
    ids=[
        "no-default",
        "default",
        "default-postgres",
    ],
)
def test_connections_file_get_default_connection_url(tmp_empty, content, expected):
    Path("conns.ini").write_text(content)

    cf = ConnectionsFile(path_to_file="conns.ini")
    assert cf.get_default_connection_url() == expected


@pytest.mark.parametrize(
    "query_jupysql, expected_duckdb",
    [
        (
            "select 'hello'[:2]",
            "he",
        ),
        (
            "select 'hello'[2:]",
            "ello",
        ),
        (
            "select 'hello'[2:4]",
            "ell",
        ),
        (
            "select 'hello'[:-1]",
            "hell",
        ),
    ],
)
def test_slicing_jupysql_matches_duckdb_expected(
    ip_empty, query_jupysql, expected_duckdb
):
    ip_empty.run_cell("%load_ext sql")
    ip_empty.run_cell("%sql duckdb://")
    raw_result = ip_empty.run_line_magic("sql", query_jupysql)
    result_jupysql = list(raw_result.dict().values())[0][0]
    assert result_jupysql == expected_duckdb


@pytest.mark.parametrize(
    "query, expected_escaped, expected_found",
    [
        (
            "SELECT 'hello'",
            "SELECT 'hello'",
            [],
        ),
        (
            "SELECT 'hello'[:]",
            "SELECT 'hello'[:]",
            [],
        ),
        (
            "SELECT 'hello'[:2]",
            "SELECT 'hello'[\\:2]",
            ["2"],
        ),
        (
            "SELECT 'hello'[1:5]",
            "SELECT 'hello'[1\\:5]",
            ["5"],
        ),
        (
            "SELECT 'hello'[1:99]",
            "SELECT 'hello'[1\\:99]",
            ["99"],
        ),
        (
            "SELECT 'hello'[:123456789]",
            "SELECT 'hello'[\\:123456789]",
            ["123456789"],
        ),
    ],
    ids=[
        "no-slicing",
        "slicing-empty",
        "end-index-only",
        "begin-end-index",
        "end-index-two-digit",
        "end-index-many-digit",
    ],
)
def test_escape_string_slicing_notation(query, expected_escaped, expected_found):
    escaped, found = escape_string_slicing_notation(query)
    assert escaped == expected_escaped
    assert found == expected_found
