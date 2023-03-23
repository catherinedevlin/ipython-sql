import os
import shutil
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sql import _testing

is_on_github = False
if "GITHUB_ACTIONS" in os.environ:
    is_on_github = True


@pytest.fixture
def get_database_config_helper():
    return _testing.DatabaseConfigHelper


"""
Create the temporary folder to keep some static database storage files & destory later
"""


@pytest.fixture(autouse=True)
def run_around_tests(tmpdir_factory):
    # Create tmp folder
    my_tmpdir = tmpdir_factory.mktemp(_testing.DatabaseConfigHelper.get_tmp_dir())
    yield my_tmpdir
    # Destory tmp folder
    shutil.rmtree(str(my_tmpdir))


def load_taxi_data(engine):
    table_name = "taxi"
    df = pd.DataFrame(
        {"taxi_driver_name": ["Eric Ken", "John Smith", "Kevin Kelly"] * 15}
    )
    df.to_sql(name=table_name, con=engine, chunksize=1000, if_exists="replace")


def load_plot_data(engine):
    table_name = "plot_something"
    df = pd.DataFrame({"x": range(0, 5), "y": range(5, 10)})
    df.to_sql(name=table_name, con=engine, chunksize=1000, if_exists="replace")


def load_numeric_data(engine):
    table_name = "numbers"
    df = pd.DataFrame({"numbers_elements": [1, 2, 3]})
    df.to_sql(name=table_name, con=engine, chunksize=100_000, if_exists="replace")


@pytest.fixture(scope="session")
def setup_postgreSQL():
    with _testing.postgres(is_bypass_init=is_on_github):
        engine = create_engine(
            _testing.DatabaseConfigHelper.get_database_url("postgreSQL")
        )
        # Load pre-defined datasets
        load_taxi_data(engine)
        load_plot_data(engine)
        load_numeric_data(engine)
        yield engine
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


@pytest.fixture(scope="session")
def setup_mySQL():
    with _testing.mysql(is_bypass_init=is_on_github):
        engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("mySQL"))
        # Load pre-defined datasets
        load_taxi_data(engine)
        load_plot_data(engine)
        load_numeric_data(engine)
        yield engine
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
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_mariaDB():
    with _testing.mariadb(is_bypass_init=is_on_github):
        engine = create_engine(
            _testing.DatabaseConfigHelper.get_database_url("mariaDB")
        )
        # Load pre-defined datasets
        load_taxi_data(engine)
        load_plot_data(engine)
        load_numeric_data(engine)
        yield engine
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
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_SQLite():
    engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("SQLite"))
    # Load pre-defined datasets
    load_taxi_data(engine)
    load_plot_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_SQLite(ip_empty, setup_SQLite):
    configKey = "SQLite"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

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


@pytest.fixture(scope="session")
def setup_duckDB():
    engine = create_engine(_testing.DatabaseConfigHelper.get_database_url("duckDB"))
    # Load pre-defined datasets
    load_taxi_data(engine)
    load_plot_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_duckDB(ip_empty, setup_duckDB):
    configKey = "duckDB"
    alias = _testing.DatabaseConfigHelper.get_database_config(configKey)["alias"]

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
