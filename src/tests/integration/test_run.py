import uuid
from functools import partial

import pytest
from sqlalchemy import create_engine
import sqlalchemy

from sql.connection import SQLAlchemyConnection, DBAPIConnection
from sql.run.run import run_statements
from sql import _testing


SQLALCHEMY_VERSION = int(sqlalchemy.__version__.split(".")[0])


@pytest.fixture
def psycopg2_factory():
    import psycopg2

    config = _testing.DatabaseConfigHelper.get_database_config("postgreSQL")

    return partial(
        psycopg2.connect,
        database=config["database"],
        user=config["username"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
    )


class ConfigAutocommit:
    autopandas = None
    autopolars = None
    autocommit = True
    feedback = True
    polars_dataframe_kwargs = {}
    style = "DEFAULT"
    autolimit = 0
    displaylimit = 10


class ConfigNoAutocommit(ConfigAutocommit):
    autocommit = False


# TODO: refactor the fixtures so each test can use its own database
# and we don't have to worry about unique table names
def gen_name():
    return f"table_{str(uuid.uuid4())[:8]}"


@pytest.mark.skipif(
    SQLALCHEMY_VERSION == 1, reason="this is failing with sqlalchemy 1.x"
)
def test_duckdb_sqlalchemy_doesnt_commit_by_default(tmp_empty):
    """
    This test checks that duckdb doesn't commit by default so we're sure that the
    commit behavior comes from our code
    """
    url = "duckdb:///my.db"

    conn_one = create_engine(url).connect()
    conn_two = create_engine(url).connect()

    name = gen_name()
    conn_one.execute(sqlalchemy.text(f"CREATE TABLE {name} (id int)"))

    with pytest.raises(sqlalchemy.exc.ProgrammingError) as excinfo:
        conn_two.execute(sqlalchemy.text(f"SELECT * FROM {name}"))

    assert f"Table with name {name} does not exist!" in str(excinfo.value)


def test_postgres_dbapi_doesnt_commit_by_default(setup_postgreSQL, psycopg2_factory):
    """
    This test checks that postgres doesn't commit by default so we're sure that the
    commit behavior comes from our code
    """
    import psycopg2

    conn_one = psycopg2_factory()
    conn_two = psycopg2_factory()

    name = gen_name()

    with conn_one.cursor() as c:
        c.execute(f"CREATE TABLE {name} (id int)")

    with pytest.raises(psycopg2.errors.UndefinedTable):
        with conn_two.cursor() as c:
            c.execute(f"SELECT * FROM {name}")


# TODO: duckdb-engine does not support isolation level so we need to test with
# a database that does (but first verify that this is the case)


@pytest.mark.skipif(
    SQLALCHEMY_VERSION == 1, reason="this is failing with sqlalchemy 1.x"
)
def test_autocommit_off_with_sqlalchemy_connection(tmp_empty):
    url = "duckdb:///my.db"

    engine_one = create_engine(url)
    engine_two = create_engine(url)

    conn_one = SQLAlchemyConnection(engine_one, config=ConfigNoAutocommit)
    conn_two = SQLAlchemyConnection(engine_two, config=ConfigNoAutocommit)

    name = gen_name()

    run_statements(conn_one, f"CREATE TABLE {name} (id int)", ConfigNoAutocommit)

    with pytest.raises(sqlalchemy.exc.ProgrammingError) as excinfo:
        run_statements(conn_two, f"SELECT * FROM {name}", ConfigNoAutocommit)

    assert f"Table with name {name} does not exist!" in str(excinfo.value)


def test_autocommit_with_sqlalchemy_connection_manual_commit(tmp_empty):
    """Test case when we manually call .commit() on the connection"""
    url = "duckdb:///my.db"

    engine_one = create_engine(url)
    engine_two = create_engine(url)

    conn_one = SQLAlchemyConnection(engine_one)
    conn_two = SQLAlchemyConnection(engine_two)

    name = gen_name()

    run_statements(conn_one, f"CREATE TABLE {name} (id int)", ConfigAutocommit)
    run_statements(conn_two, f"SELECT * FROM {name}", ConfigAutocommit)


def test_autocommit_with_sqlalchemy_that_supports_isolation_level(setup_postgreSQL):
    """Test case when we use sqlalchemy to set the isolation level for autocommit"""
    url = _testing.DatabaseConfigHelper.get_database_url("postgreSQL")

    conn_one = SQLAlchemyConnection(create_engine(url))
    conn_two = SQLAlchemyConnection(create_engine(url))

    name = gen_name()

    run_statements(conn_one, f"CREATE TABLE {name} (id int)", ConfigAutocommit)
    run_statements(conn_two, f"SELECT * FROM {name}", ConfigAutocommit)


# TODO: add create table test to generic operations
def test_autocommit_off_with_dbapi_connection(setup_postgreSQL, psycopg2_factory):
    import psycopg2

    conn_raw_one = psycopg2_factory()
    conn_raw_two = psycopg2_factory()
    conn_one = DBAPIConnection(conn_raw_one, config=ConfigNoAutocommit)
    conn_two = DBAPIConnection(conn_raw_two, config=ConfigNoAutocommit)

    name = gen_name()

    run_statements(conn_one, f"CREATE TABLE {name} (id int)", ConfigNoAutocommit)

    with pytest.raises(psycopg2.errors.UndefinedTable):
        run_statements(conn_two, f"SELECT * FROM {name}", ConfigNoAutocommit)


def test_autocommit_with_dbapi_connection(setup_postgreSQL, psycopg2_factory):
    conn_raw_one = psycopg2_factory()
    conn_raw_two = psycopg2_factory()

    conn_one = DBAPIConnection(conn_raw_one, config=ConfigAutocommit)
    conn_two = DBAPIConnection(conn_raw_two, config=ConfigAutocommit)

    name = gen_name()

    run_statements(conn_one, f"CREATE TABLE {name} (id int)", ConfigAutocommit)
    run_statements(conn_two, f"SELECT * FROM {name}", ConfigAutocommit)


# TODO: do not commit if on blacklist
