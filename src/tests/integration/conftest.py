import os
from pathlib import Path
import shutil
import pandas as pd
import pytest
from sqlalchemy import MetaData, Table, create_engine
import uuid
import duckdb

from sql import _testing
from sql import connection


@pytest.fixture(scope="function", autouse=True)
def isolate_connections(monkeypatch):
    """
    Fixture to ensure connections are isolated between tests, preventing tests
    from accidentally closing connections created by other tests.
    """
    connections = {}
    monkeypatch.setattr(connection.ConnectionManager, "connections", connections)
    monkeypatch.setattr(connection.ConnectionManager, "current", None)
    yield

    # FIXME: cannot close connections because some of them are shared across tests
    # e.g., setup_duckdb, we need to isolate them and then we'll be able to close them
    # here
    # connection.ConnectionManager.close_all()


@pytest.fixture
def get_database_config_helper():
    return _testing.DatabaseConfigHelper


@pytest.fixture(autouse=True)
def run_around_tests(tmpdir_factory):
    """
    Create the temporary folder to keep some static database storage files & destroy
    later
    """
    # Create tmp folder
    my_tmpdir = tmpdir_factory.mktemp(_testing.DatabaseConfigHelper.get_tmp_dir())
    yield my_tmpdir
    # Destroy tmp folder
    shutil.rmtree(str(my_tmpdir))


@pytest.fixture(scope="session")
def test_table_name_dict():
    return {
        "taxi": f"taxi_{str(uuid.uuid4())[:6]}",
        "numbers": f"numbers_{str(uuid.uuid4())[:6]}",
        "plot_something": f"plot_something_{str(uuid.uuid4())[:6]}",
        "new_table_from_df": f"new_table_from_df_{str(uuid.uuid4())[:6]}",
    }


def drop_table(engine, table_name):
    tbl = Table(table_name, MetaData(), autoload_with=engine)
    tbl.drop(engine, checkfirst=False)


def load_taxi_data(engine, table_name, index=True):
    table_name = table_name
    df = pd.DataFrame(
        {"taxi_driver_name": ["Eric Ken", "John Smith", "Kevin Kelly"] * 15}
    )
    df.to_sql(
        name=table_name, con=engine, chunksize=1000, if_exists="replace", index=index
    )


def load_plot_data(engine, table_name, index=True):
    df = pd.DataFrame({"x": range(0, 5), "y": range(5, 10)})
    df.to_sql(
        name=table_name, con=engine, chunksize=1000, if_exists="replace", index=index
    )


def load_numeric_data(engine, table_name, index=True):
    df = pd.DataFrame({"numbers_elements": [1, 2, 3] * 20})
    df.to_sql(
        name=table_name, con=engine, chunksize=1000, if_exists="replace", index=index
    )


def load_generic_testing_data(engine, test_table_name_dict, index=True):
    load_taxi_data(engine, table_name=test_table_name_dict["taxi"], index=index)
    load_plot_data(
        engine, table_name=test_table_name_dict["plot_something"], index=index
    )
    load_numeric_data(engine, table_name=test_table_name_dict["numbers"], index=index)


def tear_down_generic_testing_data(engine, test_table_name_dict):
    drop_table(engine, table_name=test_table_name_dict["taxi"])
    drop_table(engine, table_name=test_table_name_dict["plot_something"])
    drop_table(engine, table_name=test_table_name_dict["numbers"])


@pytest.fixture(scope="session")
def setup_postgreSQL(test_table_name_dict):
    with _testing.postgres():
        engine = create_engine(
            _testing.DatabaseConfigHelper.get_database_url("postgreSQL")
        )
        # Load pre-defined datasets
        load_generic_testing_data(engine, test_table_name_dict)
        yield engine
        tear_down_generic_testing_data(engine, test_table_name_dict)
        engine.dispose()


@pytest.fixture
def ip_with_postgreSQL(ip_empty, setup_postgreSQL):
    configKey = "postgreSQL"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

    # Select database engine
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + alias
    )
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture
def postgreSQL_config_incorrect_pwd(ip_empty, setup_postgreSQL):
    configKey = "postgreSQL"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]
    url = _testing.DatabaseConfigHelper.get_database_url(configKey)
    url = url.replace(":ploomber_app_password", "")
    return alias, url


@pytest.fixture(scope="session")
def setup_mySQL(test_table_name_dict):
    with _testing.mysql():
        engine = create_engine(
            _testing.DatabaseConfigHelper.get_database_url("mySQL"),
        )
        # Load pre-defined datasets
        load_generic_testing_data(engine, test_table_name_dict)
        yield engine
        tear_down_generic_testing_data(engine, test_table_name_dict)
        engine.dispose()


@pytest.fixture
def ip_with_mySQL(ip_empty, setup_mySQL):
    configKey = "mySQL"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

    # Select database engine
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + alias
    )
    yield ip_empty
    connection.ConnectionManager.close_all()


@pytest.fixture(scope="session")
def setup_mariaDB(test_table_name_dict):
    with _testing.mariadb():
        engine = create_engine(
            _testing.DatabaseConfigHelper.get_database_url("mariaDB")
        )
        # Load pre-defined datasets
        load_generic_testing_data(engine, test_table_name_dict)
        yield engine
        tear_down_generic_testing_data(engine, test_table_name_dict)
        engine.dispose()


@pytest.fixture
def ip_with_mariaDB(ip_empty, setup_mariaDB):
    configKey = "mariaDB"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

    # Select database engine
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + alias
    )
    yield ip_empty
    connection.ConnectionManager.close_all()


@pytest.fixture(scope="session")
def setup_SQLite(test_table_name_dict):
    config = _testing.DatabaseConfigHelper.get_database_config("SQLite")

    if Path(config["database"]).exists():
        Path(config["database"]).unlink()

    engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("SQLite"))
    # Load pre-defined datasets
    load_generic_testing_data(engine, test_table_name_dict)
    yield engine

    tear_down_generic_testing_data(engine, test_table_name_dict)
    engine.dispose()


@pytest.fixture
def ip_with_SQLite(ip_empty, setup_SQLite):
    configKey = "SQLite"
    config = _testing.DatabaseConfigHelper.get_database_config(configKey)
    alias = config["alias"]

    # Select database engine, use different sqlite database endpoint
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + alias
    )
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)

    connection.ConnectionManager.current.close()


@pytest.fixture(scope="session")
def setup_duckDB_native(test_table_name_dict):
    engine = duckdb.connect(database=":memory:", read_only=False)
    return engine


def load_generic_testing_data_duckdb_native(ip, test_table_name_dict):
    ip.run_cell("import pandas as pd")
    ip.run_cell(
        f"""{test_table_name_dict['taxi']} = pd.DataFrame({{'taxi_driver_name':
          ["Eric Ken", "John Smith", "Kevin Kelly"] * 15}} )"""
    )
    ip.run_cell(
        f"""{test_table_name_dict['plot_something']} = pd.DataFrame(
            {{"x": range(0, 5), "y": range(5, 10)}} )"""
    )
    ip.run_cell(
        f"""{test_table_name_dict['numbers']} = pd.DataFrame(
            {{"numbers_elements": [1, 2, 3] * 20}} )"""
    )
    return ip


def teardown_generic_testing_data_duckdb_native(ip, test_table_name_dict):
    ip.run_cell(f"del {test_table_name_dict['taxi']}")
    ip.run_cell(f"del {test_table_name_dict['plot_something']}")
    ip.run_cell(f"del {test_table_name_dict['numbers']}")
    return ip


@pytest.fixture
def ip_with_duckDB_native(ip_empty, setup_duckDB_native, test_table_name_dict):
    configKey = "duckDB"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

    engine = setup_duckDB_native
    ip_empty.push({"conn": engine})

    ip_empty.run_cell("%sql conn" + " --alias " + alias)
    ip_empty = load_generic_testing_data_duckdb_native(ip_empty, test_table_name_dict)
    yield ip_empty

    ip_empty = teardown_generic_testing_data_duckdb_native(
        ip_empty, test_table_name_dict
    )
    ip_empty.run_cell("%sql --close " + alias)


@pytest.fixture(scope="session")
def setup_duckDB(test_table_name_dict):
    config = _testing.DatabaseConfigHelper.get_database_config("duckDB")

    if Path(config["database"]).exists():
        Path(config["database"]).unlink()

    engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("duckDB"))
    # Load pre-defined datasets
    load_generic_testing_data(engine, test_table_name_dict)
    yield engine
    tear_down_generic_testing_data(engine, test_table_name_dict)
    engine.dispose()


@pytest.fixture
def ip_with_duckDB(ip_empty, setup_duckDB):
    configKey = "duckDB"
    config = _testing.DatabaseConfigHelper.get_database_config(configKey)
    alias = config["alias"]

    # Select database engine, use different sqlite database endpoint
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + alias
    )
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture
def ip_with_duckdb_native_empty(tmp_empty, ip_empty_testing):
    ip_empty_testing.run_cell("import duckdb; conn = duckdb.connect('my.db')")
    ip_empty_testing.run_cell("%sql conn --alias duck")
    yield ip_empty_testing
    ip_empty_testing.run_cell("conn.close()")


@pytest.fixture
def ip_with_duckdb_sqlalchemy_empty(tmp_empty, ip_empty_testing):
    ip_empty_testing.run_cell("%sql duckdb:///my.db --alias duckdb")
    yield ip_empty_testing
    ip_empty_testing.run_cell("%sql --close duckdb")


@pytest.fixture
def ip_with_sqlite_native_empty(tmp_empty, ip_empty_testing):
    ip_empty_testing.run_cell("import sqlite3; conn = sqlite3.connect('')")
    ip_empty_testing.run_cell("%sql conn --alias sqlite")
    yield ip_empty_testing
    ip_empty_testing.run_cell("conn.close()")


@pytest.fixture(scope="session")
def setup_MSSQL(test_table_name_dict):
    with _testing.mssql():
        engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("MSSQL"))
        # Load pre-defined datasets
        load_generic_testing_data(engine, test_table_name_dict)
        yield engine
        tear_down_generic_testing_data(engine, test_table_name_dict)
        engine.dispose()


@pytest.fixture
def ip_with_MSSQL(ip_empty, setup_MSSQL):
    configKey = "MSSQL"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

    # Select database engine
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + alias
    )
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_Snowflake(test_table_name_dict):
    username = os.getenv("SF_USERNAME")
    password = os.getenv("SF_PASSWORD")

    if username is None:
        raise ValueError("SF_USERNAME is required to run snowflake integration tests")

    if password is None:
        raise ValueError("SF_PASSWORD is required to run snowflake integration tests")

    engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("Snowflake"))
    engine.connect()
    # Load pre-defined datasets
    load_generic_testing_data(engine, test_table_name_dict, index=False)
    yield engine
    tear_down_generic_testing_data(engine, test_table_name_dict)
    engine.dispose()


@pytest.fixture
def ip_with_Snowflake(ip_empty, setup_Snowflake, pytestconfig):
    configKey = "Snowflake"
    config = _testing.DatabaseConfigHelper.get_database_config(configKey)
    # Select database engine
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + config["alias"]
    )
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + config["alias"])


@pytest.fixture(scope="session")
def setup_oracle(test_table_name_dict):
    with _testing.oracle():
        engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("oracle"))
        engine.connect()
        # Load pre-defined datasets
        load_generic_testing_data(engine, test_table_name_dict, index=False)
        yield engine
        tear_down_generic_testing_data(engine, test_table_name_dict)
        engine.dispose()


@pytest.fixture
def ip_with_oracle(ip_empty, setup_oracle, pytestconfig):
    configKey = "oracle"
    config = _testing.DatabaseConfigHelper.get_database_config(configKey)
    # Select database engine
    ip_empty.run_cell(
        "%sql "
        + _testing.DatabaseConfigHelper.get_database_url(configKey)
        + " --alias "
        + config["alias"]
    )
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + config["alias"])
