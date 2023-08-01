import uuid
from unittest.mock import ANY, Mock, call
from functools import partial


import sqlalchemy
from sqlalchemy import create_engine
import pytest


from sql.connection import SQLAlchemyConnection, DBAPIConnection, ConnectionManager
from sql import _testing
from sql.connection import connection


# TODO: refactor the fixtures so each test can use its own database
# and we don't have to worry about unique table names
def gen_name(prefix="table"):
    return f"{prefix}_{str(uuid.uuid4())[:8]}"


@pytest.mark.parametrize(
    "dynamic_db, Constructor, alias, dialect",
    [
        [
            "setup_postgreSQL",
            SQLAlchemyConnection,
            "postgresql://ploomber_app:***@localhost:5432/db",
            "postgresql",
        ],
        [
            "setup_duckDB_native",
            DBAPIConnection,
            "DuckDBPyConnection",
            "duckdb",
        ],
        [
            "setup_duckDB",
            SQLAlchemyConnection,
            "duckdb:////tmp/db-duckdb",
            "duckdb",
        ],
        [
            "setup_postgreSQL",
            partial(SQLAlchemyConnection, alias="some-postgres"),
            "some-postgres",
            "postgresql",
        ],
        [
            "setup_duckDB_native",
            partial(DBAPIConnection, alias="some-duckdb"),
            "some-duckdb",
            "duckdb",
        ],
        # TODO: add test for DBAPIConnection where we cannot detect the dialect
    ],
)
def test_connection_properties(dynamic_db, request, Constructor, alias, dialect):
    dynamic_db = request.getfixturevalue(dynamic_db)

    conn = Constructor(dynamic_db)

    assert conn.alias == alias
    assert conn.dialect == dialect


@pytest.mark.parametrize(
    "dynamic_db, Constructor, expected",
    [
        [
            "setup_postgreSQL",
            SQLAlchemyConnection,
            "postgresql://ploomber_app:***@localhost:5432/db",
        ],
        [
            "setup_duckDB",
            SQLAlchemyConnection,
            "duckdb:////tmp/db-duckdb",
        ],
        [
            "setup_duckDB_native",
            DBAPIConnection,
            "DuckDBPyConnection",
        ],
        [
            "setup_duckDB",
            partial(SQLAlchemyConnection, alias="some-alias"),
            "some-alias",
        ],
        [
            "setup_duckDB_native",
            partial(DBAPIConnection, alias="another-alias"),
            "another-alias",
        ],
    ],
)
def test_connection_identifiers(
    dynamic_db, request, monkeypatch, Constructor, expected
):
    dynamic_db = request.getfixturevalue(dynamic_db)

    Constructor(dynamic_db)

    assert len(ConnectionManager.connections) == 1
    assert set(ConnectionManager.connections) == {expected}


@pytest.mark.parametrize(
    "dynamic_db, Constructor, expected",
    [
        [
            "setup_postgreSQL",
            SQLAlchemyConnection,
            {
                "dialect": "postgresql",
                "driver": "psycopg2",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_duckDB",
            SQLAlchemyConnection,
            {
                "dialect": "duckdb",
                "driver": "duckdb_engine",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_duckDB_native",
            DBAPIConnection,
            {
                "dialect": "duckdb",
                "driver": "DuckDBPyConnection",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_SQLite",
            SQLAlchemyConnection,
            {
                "dialect": "sqlite",
                "driver": "pysqlite",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_mySQL",
            SQLAlchemyConnection,
            {
                "dialect": "mysql",
                "driver": "pymysql",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_mariaDB",
            SQLAlchemyConnection,
            {
                "dialect": "mysql",
                "driver": "pymysql",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_MSSQL",
            SQLAlchemyConnection,
            {
                "dialect": "mssql",
                "driver": "pyodbc",
                "server_version_info": ANY,
            },
        ],
        [
            "setup_Snowflake",
            SQLAlchemyConnection,
            {
                "dialect": "snowflake",
                "driver": "snowflake",
                "server_version_info": ANY,
            },
        ],
        # TODO: add oracle (cannot run it locally yet)
    ],
    ids=[
        "postgresql-sqlalchemy",
        "duckdb-sqlalchemy",
        "duckdb-dbapi",
        "sqlite-sqlalchemy",
        "mysql-sqlalchemy",
        "mariadb-sqlalchemy",
        "mssql-sqlalchemy",
        "snowflake-sqlalchemy",
    ],
)
def test_get_database_information(dynamic_db, request, Constructor, expected):
    conn = Constructor(request.getfixturevalue(dynamic_db))
    assert conn._get_database_information() == expected


@pytest.mark.parametrize(
    "dynamic_db, dialect",
    [
        ("ip_with_duckDB_native", "duckdb"),
        ("ip_with_sqlite_native_empty", None),
    ],
)
def test_dbapi_connection_sets_right_dialect(dynamic_db, dialect, request):
    request.getfixturevalue(dynamic_db)

    assert ConnectionManager.current.is_dbapi_connection
    assert ConnectionManager.current.dialect == dialect


def test_duckdb_autocommit_on_with_manual_commit(tmp_empty, monkeypatch):
    class Config:
        autocommit = True

    engine = create_engine("duckdb:///my.db")

    conn = SQLAlchemyConnection(engine=engine, config=Config)
    conn_mock_commit = Mock(wraps=conn._connection.commit)
    monkeypatch.setattr(conn._connection, "commit", conn_mock_commit)

    conn.raw_execute(
        """
CREATE TABLE numbers (
    x INTEGER
);
"""
    )
    conn.raw_execute(
        """
    INSERT INTO numbers VALUES (1), (2), (3);
    """
    )

    # if commit is working, the table should be readable from another connection
    another = SQLAlchemyConnection(
        engine=create_engine("duckdb:///my.db"), config=Config
    )
    another_mock_commit = Mock(wraps=another._connection.commit)
    monkeypatch.setattr(another._connection, "commit", another_mock_commit)

    results = another.raw_execute("SELECT * FROM numbers")

    assert list(results) == [(1,), (2,), (3,)]
    conn_mock_commit.assert_has_calls([call(), call()])
    # due to https://github.com/Mause/duckdb_engine/issues/734, we should not
    # call commit on SELECT statements
    another_mock_commit.assert_not_called()


def test_postgres_autocommit_on_with_manual_commit(setup_postgreSQL, monkeypatch):
    url = _testing.DatabaseConfigHelper.get_database_url("postgreSQL")

    class Config:
        autocommit = True

    monkeypatch.setattr(
        connection, "set_sqlalchemy_isolation_level", Mock(return_value=False)
    )

    engine = create_engine(url)

    conn = SQLAlchemyConnection(engine=engine, config=Config)
    conn_mock_commit = Mock(wraps=conn._connection.commit)
    monkeypatch.setattr(conn._connection, "commit", conn_mock_commit)

    conn.raw_execute(
        """
CREATE TABLE numbers (
    x INTEGER
);
"""
    )
    conn.raw_execute(
        """
    INSERT INTO numbers VALUES (1), (2), (3);
    """
    )

    # if commit is working, the table should be readable from another connection
    another = SQLAlchemyConnection(engine=create_engine(url), config=Config)
    another_mock_commit = Mock(wraps=another._connection.commit)
    monkeypatch.setattr(another._connection, "commit", another_mock_commit)

    results = another.raw_execute("SELECT * FROM numbers")

    assert list(results) == [(1,), (2,), (3,)]
    conn_mock_commit.assert_has_calls([call(), call()])
    # due to https://github.com/Mause/duckdb_engine/issues/734, we are not calling
    # commit on SELECT statements for DuckDB, but for other databases we do
    another_mock_commit.assert_has_calls([call()])


def test_duckdb_autocommit_off(tmp_empty):
    class Config:
        autocommit = False

    engine = create_engine("duckdb:///my.db")
    conn = SQLAlchemyConnection(engine=engine, config=Config)
    conn.raw_execute(
        """
CREATE TABLE numbers (
    x INTEGER
);
"""
    )
    conn.raw_execute(
        """
    INSERT INTO numbers VALUES (1), (2), (3);
    """
    )

    # since autocommit is off, the table should not be readable from another connection
    another = SQLAlchemyConnection(
        engine=create_engine("duckdb:///my.db"), config=Config
    )

    with pytest.raises(sqlalchemy.exc.ProgrammingError) as excinfo:
        another.raw_execute("SELECT * FROM numbers")

    assert "Catalog Error: Table with name numbers does not exist!" in str(
        excinfo.value
    )


# TODO: if we set autocommit to False, then we should not be able to create a table
# we need to add a test. Currently, it's failing with
# "CREATE DATABASE cannot run inside a transaction block" so looks like even with
# autocommit off, we are still in a transaction block (perhaps it's a psycopg2 thing?)
def test_autocommit_on_with_sqlalchemy_that_supports_isolation_level(setup_postgreSQL):
    """Test case when we use sqlalchemy to set the isolation level for autocommit"""

    class Config:
        autocommit = True

    url = _testing.DatabaseConfigHelper.get_database_url("postgreSQL")

    conn_one = SQLAlchemyConnection(create_engine(url), config=Config)
    conn_two = SQLAlchemyConnection(create_engine(url), config=Config)

    # mock commit to ensure it's not called
    conn_one._connection.commit = Mock(
        side_effect=ValueError(
            "commit should not be called manually if the "
            "driver supports isolation level"
        )
    )

    db = gen_name(prefix="db")
    name = gen_name(prefix="table")

    # this will fail if we don't use the isolation level feature because if we use
    # manual commit, then we'll get the "CREATE DATABASE cannot run inside a
    # transaction block" error
    conn_one.raw_execute(f"CREATE DATABASE {db}")

    conn_one.raw_execute(f"CREATE TABLE {name} (id int)")
    conn_two.raw_execute(f"SELECT * FROM {name}")

    assert conn_one._connection._execution_options == {"isolation_level": "AUTOCOMMIT"}


@pytest.mark.parametrize("autocommit_value", [True, False])
def test_mssql_with_pytds(setup_MSSQL, autocommit_value):
    """
    In https://github.com/ploomber/jupysql/issues/15, we determined that turning off
    autocommit would fix the issue but I was unable to reproduce the problem,
    this is working fine.
    """

    class Config:
        autocommit = autocommit_value

    url = _testing.DatabaseConfigHelper.get_database_url("mssql_pytds")

    conn_one = SQLAlchemyConnection(create_engine(url), config=Config)

    name = gen_name(prefix="table")
    conn_one.raw_execute(f"CREATE TABLE {name} (id int)")
    conn_one.raw_execute(f"INSERT INTO {name} VALUES (1), (2), (3)")
    results = conn_one.raw_execute(f"SELECT * FROM {name}").fetchall()

    conn_one.close()

    assert url.startswith("mssql+pytds")
    assert [(1,), (2,), (3,)] == results
