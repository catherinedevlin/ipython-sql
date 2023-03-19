import shutil
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

TMP_DIR = "tmp"
databaseConfig = {
    "postgreSQL": {
        "drivername": "postgresql",
        "username": "ploomber_app",
        "password": "ploomber_app_password",
        "database": "db",
        "host": "localhost",
        "port": "5432",
        "alias": "postgreSQLTest",
    },
    "mySQL": {
        "drivername": "mysql+pymysql",
        "username": "ploomber_app",
        "password": "ploomber_app_password",
        "database": "db",
        "host": "localhost",
        "port": "33306",
        "alias": "mySQLTest",
    },
    "mariaDB": {
        "drivername": "mysql+pymysql",
        "username": "ploomber_app",
        "password": "ploomber_app_password",
        "database": "db",
        "host": "localhost",
        "port": "33309",
        "alias": "mySQLTest",
    },
    "SQLite": {
        "drivername": "sqlite",
        "username": None,
        "password": None,
        "database": "/{}/db-sqlite".format(TMP_DIR),
        "host": None,
        "port": None,
        "alias": "SQLiteTest",
    },
    "duckDB": {
        "drivername": "duckdb",
        "username": None,
        "password": None,
        "database": "/{}/db-duckdb".format(TMP_DIR),
        "host": None,
        "port": None,
        "alias": "duckDBTest",
    },
}


class DatabaseConfigHelper:
    @staticmethod
    def get_database_config(database):
        return databaseConfig[database]

    @staticmethod
    def get_database_url(database):
        return _get_database_url(database)


@pytest.fixture
def get_database_config_helper():
    return DatabaseConfigHelper


"""
Create the temporary folder to keep some static database storage files & destory later
"""


@pytest.fixture(autouse=True)
def run_around_tests(tmpdir_factory):
    # Create tmp folder
    my_tmpdir = tmpdir_factory.mktemp(TMP_DIR)
    yield my_tmpdir
    # Destory tmp folder
    shutil.rmtree(str(my_tmpdir))


# SQLAlchmey URL: https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls
def _get_database_url(database):
    return URL.create(
        drivername=databaseConfig[database]["drivername"],
        username=databaseConfig[database]["username"],
        password=databaseConfig[database]["password"],
        host=databaseConfig[database]["host"],
        port=databaseConfig[database]["port"],
        database=databaseConfig[database]["database"],
    ).render_as_string(hide_password=False)


def load_taxi_data(engine):
    table_name = "taxi"
    df = pd.DataFrame(
        {"taxi_driver_name": ["Eric Ken", "John Smith", "Kevin Kelly"] * 15}
    )
    df.to_sql(name=table_name, con=engine, chunksize=100_000, if_exists="replace")


def load_numeric_data(engine):
    table_name = "numbers"
    df = pd.DataFrame({"numbers_elements": [1, 2, 3]})
    df.to_sql(name=table_name, con=engine, chunksize=100_000, if_exists="replace")


@pytest.fixture(scope="session")
def setup_postgreSQL():
    engine = create_engine(_get_database_url("postgreSQL"))
    # Load taxi_data
    load_taxi_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_postgreSQL(ip_empty, setup_postgreSQL):
    configKey = "postgreSQL"
    alias = databaseConfig[configKey]["alias"]

    # Select database engine
    ip_empty.run_cell("%sql " + _get_database_url(configKey) + " --alias " + alias)
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_mySQL():
    engine = create_engine(_get_database_url("mySQL"))
    # Load taxi_data
    load_taxi_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_mySQL(ip_empty, setup_mySQL):
    configKey = "mySQL"
    alias = databaseConfig[configKey]["alias"]

    # Select database engine
    ip_empty.run_cell("%sql " + _get_database_url(configKey) + " --alias " + alias)
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_mariaDB():
    engine = create_engine(_get_database_url("mariaDB"), pool_recycle=1800)
    # Load taxi_data
    load_taxi_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_mariaDB(ip_empty, setup_mariaDB):
    configKey = "mariaDB"
    alias = databaseConfig[configKey]["alias"]

    # Select database engine
    ip_empty.run_cell("%sql " + _get_database_url(configKey) + " --alias " + alias)
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_SQLite():
    engine = create_engine(_get_database_url("SQLite"))
    # Load taxi_data
    load_taxi_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_SQLite(ip_empty, setup_SQLite):
    configKey = "SQLite"
    alias = databaseConfig[configKey]["alias"]

    # Select database engine, use different sqlite database endpoint
    ip_empty.run_cell("%sql " + _get_database_url(configKey) + " --alias " + alias)
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)


@pytest.fixture(scope="session")
def setup_duckDB():
    engine = create_engine(_get_database_url("duckDB"))
    # Load taxi_data
    load_taxi_data(engine)
    load_numeric_data(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def ip_with_duckDB(ip_empty, setup_duckDB):
    configKey = "duckDB"
    alias = databaseConfig[configKey]["alias"]

    # Select database engine, use different sqlite database endpoint
    ip_empty.run_cell("%sql " + _get_database_url(configKey) + " --alias " + alias)
    yield ip_empty
    # Disconnect database
    ip_empty.run_cell("%sql -x " + alias)
