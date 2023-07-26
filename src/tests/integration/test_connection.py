from unittest.mock import ANY
from functools import partial


import pytest


from sql.connection import SQLAlchemyConnection, DBAPIConnection, ConnectionManager


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
