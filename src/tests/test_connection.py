import os
import sys
from unittest.mock import ANY, Mock, patch
import pytest


from IPython.core.error import UsageError
import duckdb
import sqlglot
import sqlalchemy
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ResourceClosedError

import sql.connection
from sql.connection import (
    SQLAlchemyConnection,
    DBAPIConnection,
    ConnectionManager,
    is_pep249_compliant,
    default_alias_for_engine,
    ResultSetCollection,
)


@pytest.fixture
def cleanup():
    yield
    ConnectionManager.connections = {}


@pytest.fixture
def mock_database(monkeypatch, cleanup):
    monkeypatch.setitem(sys.modules, "some_driver", Mock())
    monkeypatch.setattr(Engine, "connect", Mock())
    monkeypatch.setattr(sqlalchemy, "create_engine", Mock())


@pytest.fixture
def mock_postgres(monkeypatch, cleanup):
    monkeypatch.setitem(sys.modules, "psycopg2", Mock())
    monkeypatch.setattr(Engine, "connect", Mock())


def test_password_isnt_displayed(mock_postgres):
    ConnectionManager.from_connect_str("postgresql://user:topsecret@somedomain.com/db")

    table = ConnectionManager.connections_table()

    assert "topsecret" not in str(table)
    assert "topsecret" not in table._repr_html_()


def test_connection_name(mock_postgres):
    conn = ConnectionManager.from_connect_str(
        "postgresql://user:topsecret@somedomain.com/db"
    )

    assert conn.name == "user@db"


def test_alias(cleanup):
    ConnectionManager.from_connect_str("sqlite://", alias="some-alias")

    assert list(ConnectionManager.connections) == ["some-alias"]


def test_get_database_information():
    engine = create_engine("sqlite://")
    conn = SQLAlchemyConnection(engine=engine)

    assert conn._get_database_information() == {
        "dialect": "sqlite",
        "driver": "pysqlite",
        "server_version_info": ANY,
    }


def test_get_sqlglot_dialect_no_curr_connection(mock_database, monkeypatch):
    conn = SQLAlchemyConnection(engine=sqlalchemy.create_engine("someurl://"))
    monkeypatch.setattr(conn, "_get_database_information", lambda: {"dialect": None})
    assert conn._get_sqlglot_dialect() is None


@pytest.mark.parametrize(
    "sqlalchemy_connection_info, expected_sqlglot_dialect",
    [
        (
            {
                "dialect": "duckdb",
                "driver": "duckdb_engine",
                "server_version_info": [8, 0],
            },
            "duckdb",
        ),
        (
            {
                "dialect": "mysql",
                "driver": "pymysql",
                "server_version_info": [10, 10, 3, 10, 3],
            },
            "mysql",
        ),
        # sqlalchemy and sqlglot have different dialect name, test the mapping dict
        (
            {
                "dialect": "sqlalchemy_mock_dialect_name",
                "driver": "sqlalchemy_mock_driver_name",
                "server_version_info": [0],
            },
            "sqlglot_mock_dialect",
        ),
        (
            {
                "dialect": "only_support_in_sqlalchemy_dialect",
                "driver": "sqlalchemy_mock_driver_name",
                "server_version_info": [0],
            },
            "only_support_in_sqlalchemy_dialect",
        ),
    ],
)
def test_get_sqlglot_dialect(
    monkeypatch, sqlalchemy_connection_info, expected_sqlglot_dialect, mock_database
):
    """To test if we can get the dialect name in sqlglot package scope

    Args:
        monkeypatch (fixture): A convenient fixture for monkey-patching
        sqlalchemy_connection_info (dict): The metadata about the current dialect
        expected_sqlglot_dialect (str): Expected sqlglot dialect name
    """
    conn = SQLAlchemyConnection(engine=sqlalchemy.create_engine("someurl://"))

    monkeypatch.setattr(
        conn,
        "_get_database_information",
        lambda: sqlalchemy_connection_info,
    )
    monkeypatch.setattr(
        sql.connection.connection,
        "DIALECT_NAME_SQLALCHEMY_TO_SQLGLOT_MAPPING",
        {"sqlalchemy_mock_dialect_name": "sqlglot_mock_dialect"},
    )
    assert conn._get_sqlglot_dialect() == expected_sqlglot_dialect


@pytest.mark.parametrize(
    "cur_dialect, expected_support_backtick",
    [
        ("mysql", True),
        ("sqlite", True),
        ("postgres", False),
    ],
)
def test_is_use_backtick_template(
    mock_database, cur_dialect, expected_support_backtick, monkeypatch
):
    """To test if we can get the backtick supportive information from different dialects

    Args:
        monkeypatch (fixture): A convenient fixture for monkey-patching
        cur_dialect (bool): Patched dialect name
        expected_support_backtick (bool): Excepted boolean value to indicate
        if the dialect supports backtick identifier
    """
    # conn = Connection(engine=create_engine(sqlalchemy_url))
    conn = SQLAlchemyConnection(engine=sqlalchemy.create_engine("someurl://"))
    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: cur_dialect)
    assert conn.is_use_backtick_template() == expected_support_backtick


def test_is_use_backtick_template_sqlglot_missing_dialect_ValueError(
    mock_database, monkeypatch
):
    """Since accessing missing dialect will raise ValueError from sqlglot, we assume
    that's not support case
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "something_weird_dialect")
    assert conn.is_use_backtick_template() is False


def test_is_use_backtick_template_sqlglot_missing_tokenizer_AttributeError(
    mock_database, monkeypatch
):
    """Since accessing the dialect without Tokenizer Class will raise AttributeError
    from sqlglot, we assume that's not support case
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "mysql")
    monkeypatch.setattr(sqlglot.Dialect.get_or_raise("mysql"), "Tokenizer", None)

    assert conn.is_use_backtick_template() is False


def test_is_use_backtick_template_sqlglot_missing_identifiers_TypeError(
    mock_database, monkeypatch
):
    """Since accessing the IDENTIFIERS list of the dialect's Tokenizer Class
    will raise TypeError from sqlglot, we assume that's not support case
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "mysql")
    monkeypatch.setattr(
        sqlglot.Dialect.get_or_raise("mysql").Tokenizer, "IDENTIFIERS", None
    )
    assert conn.is_use_backtick_template() is False


def test_is_use_backtick_template_sqlglot_empty_identifiers(mock_database, monkeypatch):
    """Since looking up the "`" symbol in IDENTIFIERS list of the dialect's
    Tokenizer Class will raise TypeError from sqlglot, we assume that's not support case
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "mysql")
    monkeypatch.setattr(
        sqlglot.Dialect.get_or_raise("mysql").Tokenizer, "IDENTIFIERS", []
    )
    assert conn.is_use_backtick_template() is False


# Mock the missing package
# Ref: https://stackoverflow.com/a/28361013
def test_missing_duckdb_dependencies(cleanup, monkeypatch):
    with patch.dict(sys.modules):
        sys.modules["duckdb"] = None
        sys.modules["duckdb_engine"] = None

        with pytest.raises(UsageError) as excinfo:
            ConnectionManager.from_connect_str("duckdb://")

        assert excinfo.value.error_type == "MissingPackageError"
        assert "try to install package: duckdb-engine" + str(excinfo.value)


@pytest.mark.parametrize(
    "missing_pkg, except_missing_pkg_suggestion, connect_str",
    [
        # MySQL + MariaDB
        ["pymysql", "pymysql", "mysql+pymysql://"],
        ["mysqlclient", "mysqlclient", "mysql+mysqldb://"],
        ["mariadb", "mariadb", "mariadb+mariadbconnector://"],
        ["mysql-connector-python", "mysql-connector-python", "mysql+mysqlconnector://"],
        ["asyncmy", "asyncmy", "mysql+asyncmy://"],
        ["aiomysql", "aiomysql", "mysql+aiomysql://"],
        ["cymysql", "cymysql", "mysql+cymysql://"],
        ["pyodbc", "pyodbc", "mysql+pyodbc://"],
        # PostgreSQL
        ["psycopg2", "psycopg2", "postgresql+psycopg2://"],
        ["psycopg", "psycopg", "postgresql+psycopg://"],
        ["pg8000", "pg8000", "postgresql+pg8000://"],
        ["asyncpg", "asyncpg", "postgresql+asyncpg://"],
        ["psycopg2cffi", "psycopg2cffi", "postgresql+psycopg2cffi://"],
        # Oracle
        ["cx_oracle", "cx_oracle", "oracle+cx_oracle://"],
        ["oracledb", "oracledb", "oracle+oracledb://"],
        # MSSQL
        ["pyodbc", "pyodbc", "mssql+pyodbc://"],
        ["pymssql", "pymssql", "mssql+pymssql://"],
    ],
)
def test_missing_driver(
    missing_pkg, except_missing_pkg_suggestion, connect_str, monkeypatch
):
    with patch.dict(sys.modules):
        sys.modules[missing_pkg] = None
        with pytest.raises(UsageError) as excinfo:
            ConnectionManager.from_connect_str(connect_str)

        assert excinfo.value.error_type == "MissingPackageError"
        assert "try to install package: " + missing_pkg in str(excinfo.value)


def test_get_connections():
    SQLAlchemyConnection(engine=create_engine("sqlite://"))
    SQLAlchemyConnection(engine=create_engine("duckdb://"))

    assert ConnectionManager._get_connections() == [
        {
            "url": "duckdb://",
            "current": True,
            "alias": "duckdb://",
            "key": "duckdb://",
            "connection": ANY,
        },
        {
            "url": "sqlite://",
            "current": False,
            "alias": "sqlite://",
            "key": "sqlite://",
            "connection": ANY,
        },
    ]


def test_display_current_connection(capsys):
    SQLAlchemyConnection(engine=create_engine("duckdb://"))
    ConnectionManager.display_current_connection()

    captured = capsys.readouterr()
    assert captured.out == "Running query in 'duckdb://'\n"


def test_connections_table():
    SQLAlchemyConnection(engine=create_engine("sqlite://"))
    SQLAlchemyConnection(engine=create_engine("duckdb://"))

    connections = ConnectionManager.connections_table()
    assert connections._headers == ["current", "url", "alias"]
    assert connections._rows == [
        ["*", "duckdb://", "duckdb://"],
        ["", "sqlite://", "sqlite://"],
    ]


def test_properties(mock_postgres):
    conn = ConnectionManager.from_connect_str(
        "postgresql://user:topsecret@somedomain.com/db"
    )

    assert "topsecret" not in conn.url
    assert "***" in conn.url
    assert conn.name == "user@db"
    assert conn.dialect
    assert conn.connection_sqlalchemy
    assert conn.connection_sqlalchemy is conn._connection


@pytest.mark.parametrize(
    "conn, expected",
    [
        [sqlite3.connect(""), True],
        [duckdb.connect(""), True],
        [create_engine("sqlite://"), False],
        [object(), False],
        ["not_a_valid_connection", False],
        [0, False],
    ],
    ids=[
        "sqlite3-connection",
        "duckdb-connection",
        "sqlalchemy-engine",
        "dummy-object",
        "string",
        "int",
    ],
)
def test_is_pep249_compliant(conn, expected):
    assert is_pep249_compliant(conn) is expected


def test_close_all(ip_empty, monkeypatch):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql sqlite://")

    connections_copy = ConnectionManager.connections.copy()

    ConnectionManager.close_all()

    with pytest.raises(ResourceClosedError):
        connections_copy["sqlite://"].execute("").fetchall()

    with pytest.raises(ResourceClosedError):
        connections_copy["duckdb://"].execute("").fetchall()

    assert not ConnectionManager.connections


@pytest.mark.parametrize(
    "old_alias, new_alias",
    [
        (None, "duck1"),
        ("duck1", "duck2"),
        (None, None),
    ],
)
def test_new_connection_with_alias(ip_empty, old_alias, new_alias):
    """Test if a new connection with the same url but a
    new alias is registered for different cases of old alias
    """
    ip_empty.run_cell(f"%sql duckdb:// --alias {old_alias}")
    ip_empty.run_cell(f"%sql duckdb:// --alias {new_alias}")
    table = ip_empty.run_cell("sql --connections").result
    if old_alias is None and new_alias is None:
        assert new_alias not in table
    else:
        connection = table[new_alias]
        assert connection
        assert connection.url == "duckdb://"
        assert connection == ConnectionManager.current


@pytest.mark.parametrize(
    "url, expected",
    [
        [
            "postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase",
            "scott@mydatabase",
        ],
        ["duckdb://tmp/my.db", "duckdb://tmp/my.db"],
        ["duckdb:///my.db", "duckdb:///my.db"],
    ],
)
def test_default_alias_for_engine(url, expected, monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg2", Mock())

    engine = create_engine(url)
    assert default_alias_for_engine(engine) == expected


@pytest.mark.parametrize(
    "url",
    [
        "duckdb://",
        "sqlite://",
    ],
)
def test_create_connection_from_url(monkeypatch, url):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    conn = ConnectionManager.set(url, displaycon=False)

    assert connections == {url: conn}
    assert ConnectionManager.current == conn


@pytest.mark.parametrize(
    "url",
    [
        "duckdb://",
        "sqlite://",
    ],
)
def test_set_existing_connection(monkeypatch, url):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    ConnectionManager.set(url, displaycon=False)
    conn = ConnectionManager.set(url, displaycon=False)

    assert connections == {url: conn}
    assert ConnectionManager.current == conn


@pytest.mark.parametrize(
    "url",
    [
        "duckdb://",
        "sqlite://",
    ],
)
def test_set_engine(monkeypatch, url):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    engine = create_engine(url)

    conn = ConnectionManager.set(engine, displaycon=False)

    assert connections == {url: conn}
    assert ConnectionManager.current == conn


@pytest.mark.parametrize(
    "callable_, key",
    [
        [sqlite3.connect, "Connection"],
        [duckdb.connect, "DuckDBPyConnection"],
    ],
)
def test_set_dbapi(monkeypatch, callable_, key):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    conn = ConnectionManager.set(callable_(""), displaycon=False)

    assert connections == {key: conn}
    assert ConnectionManager.current == conn


def test_set_with_alias(monkeypatch):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    conn = ConnectionManager.set("sqlite://", displaycon=False, alias="some-sqlite-db")

    assert connections == {"some-sqlite-db": conn}
    assert ConnectionManager.current == conn


def test_set_and_load_with_alias(monkeypatch):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    ConnectionManager.set("sqlite://", displaycon=False, alias="some-sqlite-db")
    conn = ConnectionManager.set("some-sqlite-db", displaycon=False)

    assert connections == {"some-sqlite-db": conn}
    assert ConnectionManager.current == conn


def test_set_same_url_different_alias(monkeypatch):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    some = ConnectionManager.set("sqlite://", displaycon=False, alias="some-sqlite-db")
    another = ConnectionManager.set(
        "sqlite://", displaycon=False, alias="another-sqlite-db"
    )
    conn = ConnectionManager.set("some-sqlite-db", displaycon=False)

    assert connections == {"some-sqlite-db": some, "another-sqlite-db": another}
    assert ConnectionManager.current == conn
    assert some is conn


# NOTE: not sure what the use case for this one is but adding it since the logic
# is implemented this way
def test_same_alias(monkeypatch):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    conn = ConnectionManager.set("sqlite://", displaycon=False, alias="mydb")
    second = ConnectionManager.set("mydb", displaycon=False, alias="mydb")

    assert connections == {"mydb": conn}
    assert ConnectionManager.current == conn
    assert second is conn


def test_set_no_descriptor_and_no_active_connection(monkeypatch):
    connections = {}
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    with pytest.raises(UsageError) as excinfo:
        ConnectionManager.set(descriptor=None, displaycon=False, alias=None)

    assert "No active connection." in str(excinfo.value)


def test_set_no_descriptor_database_url(monkeypatch):
    connections = {}
    monkeypatch.setitem(os.environ, "DATABASE_URL", "sqlite://")
    monkeypatch.setattr(ConnectionManager, "connections", connections)

    conn = ConnectionManager.set(descriptor=None, displaycon=False)

    assert connections == {"sqlite://": conn}
    assert ConnectionManager.current == conn


def test_feedback_when_switching_connection_with_alias(ip_empty, tmp_empty, capsys):
    ip_empty.run_cell("%load_ext sql")
    ip_empty.run_cell("%sql duckdb:// --alias one")
    ip_empty.run_cell("%sql duckdb:// --alias two")
    ip_empty.run_cell("%sql one")

    captured = capsys.readouterr()
    assert "Switching to connection one" == captured.out.replace("\n", "")


def test_feedback_when_switching_connection_without_alias(ip_empty, tmp_empty, capsys):
    ip_empty.run_cell("%load_ext sql")
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql duckdb:// --alias one")
    ip_empty.run_cell("%sql duckdb:// --alias two")
    ip_empty.run_cell("%sql duckdb://")

    captured = capsys.readouterr()
    assert "Switching to connection duckdb://" == captured.out.replace("\n", "")


@pytest.fixture
def conn_sqlalchemy_duckdb():
    conn = SQLAlchemyConnection(engine=create_engine("duckdb://"))
    yield conn
    conn.close()


@pytest.fixture
def conn_dbapi_duckdb():
    conn = DBAPIConnection(duckdb.connect())
    yield conn
    conn.close()


@pytest.fixture
def mock_sqlalchemy_raw_execute(conn_sqlalchemy_duckdb, monkeypatch):
    mock = Mock()
    monkeypatch.setattr(conn_sqlalchemy_duckdb, "_connection_sqlalchemy", mock)
    # mock the dialect to pretend we're using tsql
    monkeypatch.setattr(conn_sqlalchemy_duckdb, "_get_sqlglot_dialect", lambda: "tsql")

    yield mock.execute, conn_sqlalchemy_duckdb


@pytest.fixture
def mock_dbapi_raw_execute(monkeypatch, conn_dbapi_duckdb):
    mock = Mock()
    monkeypatch.setattr(conn_dbapi_duckdb, "_connection", mock)
    # mock the dialect to pretend we're using tsql
    monkeypatch.setattr(conn_dbapi_duckdb, "_get_sqlglot_dialect", lambda: "tsql")

    yield mock.cursor().execute, conn_dbapi_duckdb


@pytest.mark.parametrize(
    "fixture_name",
    [
        "mock_sqlalchemy_raw_execute",
        "mock_dbapi_raw_execute",
    ],
)
def test_raw_execute_doesnt_transpile_sql_query(fixture_name, request):
    mock_execute, conn = request.getfixturevalue(fixture_name)

    conn.raw_execute("CREATE TABLE foo (bar INT)")
    conn.raw_execute("INSERT INTO foo VALUES (42), (43)")
    conn.raw_execute("SELECT * FROM foo LIMIT 1")

    calls = [
        str(call[0][0])
        for call in mock_execute.call_args_list
        # if running on sqlalchemy 1.x, the commit call is done via .execute,
        # ignore them
        if str(call[0][0]) != "commit"
    ]

    expected_number_of_calls = 3
    expected_calls = [
        "CREATE TABLE foo (bar INT)",
        "INSERT INTO foo VALUES (42), (43)",
        "SELECT * FROM foo LIMIT 1",
    ]

    assert len(calls) == expected_number_of_calls
    assert calls == expected_calls


@pytest.fixture
def mock_sqlalchemy_execute(monkeypatch):
    conn = SQLAlchemyConnection(engine=create_engine("duckdb://"))

    mock = Mock()
    monkeypatch.setattr(conn._connection, "execute", mock)
    # mock the dialect to pretend we're using tsql
    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "tsql")

    yield mock, conn


@pytest.fixture
def mock_dbapi_execute(monkeypatch):
    conn = DBAPIConnection(duckdb.connect())

    mock = Mock()
    monkeypatch.setattr(conn, "_connection", mock)
    # mock the dialect to pretend we're using tsql
    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "tsql")

    yield mock.cursor().execute, conn


@pytest.mark.parametrize(
    "fixture_name",
    [
        "mock_sqlalchemy_execute",
        "mock_dbapi_execute",
    ],
    ids=[
        "sqlalchemy",
        "dbapi",
    ],
)
def test_execute_transpiles_sql_query(fixture_name, request):
    mock_execute, conn = request.getfixturevalue(fixture_name)

    conn.execute("CREATE TABLE foo (bar INT)")
    conn.execute("INSERT INTO foo VALUES (42), (43)")
    conn.execute("SELECT * FROM foo LIMIT 1")

    calls = [
        str(call[0][0])
        for call in mock_execute.call_args_list
        # if running on sqlalchemy 1.x, the commit call is done via .execute,
        # ignore them
        if str(call[0][0]) != "commit"
    ]

    expected_number_of_calls = 3
    expected_calls = [
        "CREATE TABLE foo (bar INTEGER)",
        "INSERT INTO foo VALUES (42), (43)",
        # since we're transpiling, we should see TSQL code
        "SELECT TOP 1 * FROM foo",
    ]

    assert len(calls) == expected_number_of_calls
    assert calls == expected_calls


@pytest.mark.parametrize(
    "fixture_name",
    [
        "conn_sqlalchemy_duckdb",
        "conn_dbapi_duckdb",
    ],
)
@pytest.mark.parametrize("execute_method", ["execute", "raw_execute"])
def test_error_if_trying_to_execute_multiple_statements(
    monkeypatch, execute_method, fixture_name, request
):
    conn = request.getfixturevalue(fixture_name)

    with pytest.raises(NotImplementedError) as excinfo:
        method = getattr(conn, execute_method)
        method(
            """
CREATE TABLE foo (bar INT);
INSERT INTO foo VALUES (42), (43);
SELECT * FROM foo LIMIT 1;
"""
        )

    assert str(excinfo.value) == "Only one statement is supported."


@pytest.mark.parametrize(
    "fixture_name",
    [
        "conn_sqlalchemy_duckdb",
        "conn_dbapi_duckdb",
    ],
)
@pytest.mark.parametrize(
    "query_input,query_output",
    [
        (
            """
SELECT * FROM foo LIMIT 1;
""",
            "SELECT TOP 1 * FROM foo",
        ),
        (
            """
CREATE TABLE foo (bar INT);
INSERT INTO foo VALUES (42), (43);
SELECT * FROM foo LIMIT 1;
""",
            (
                "CREATE TABLE foo (bar INTEGER);\n"
                "INSERT INTO foo VALUES (42), (43);\n"
                "SELECT TOP 1 * FROM foo"
            ),
        ),
    ],
    ids=[
        "one_statement",
        "multiple_statements",
    ],
)
def test_transpile_query(monkeypatch, fixture_name, request, query_input, query_output):
    conn = request.getfixturevalue(fixture_name)
    monkeypatch.setattr(conn, "_get_sqlglot_dialect", lambda: "tsql")

    transpiled = conn._transpile_query(query_input)

    assert transpiled == query_output


def test_transpile_query_doesnt_transpile_if_it_doesnt_need_to(monkeypatch):
    conn = SQLAlchemyConnection(engine=create_engine("duckdb://"))

    query_input = """
    SELECT
    percentile_disc([0.25, 0.50, 0.75]) WITHIN GROUP  (ORDER BY "column")
AS percentiles
    FROM "table"
"""

    transpiled = conn._transpile_query(query_input)

    assert transpiled == query_input


def test_result_set_collection_append():
    collection = ResultSetCollection()
    collection.append(1)
    collection.append(2)

    assert collection._result_sets == [1, 2]


def test_result_set_collection_iterate():
    collection = ResultSetCollection()
    collection.append(1)
    collection.append(2)

    assert list(collection) == [1, 2]


def test_result_set_collection_is_last():
    collection = ResultSetCollection()
    first, second = object(), object()
    collection.append(first)

    assert len(collection) == 1
    assert collection.is_last(first)

    collection.append(second)

    assert len(collection) == 2
    assert not collection.is_last(first)
    assert collection.is_last(second)

    collection.append(first)

    assert len(collection) == 2
    assert collection.is_last(first)
    assert not collection.is_last(second)
