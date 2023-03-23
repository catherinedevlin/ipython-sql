from dockerctx import new_container
from contextlib import contextmanager
import sys
import time
import docker
from docker import errors
from sqlalchemy.engine import URL

TMP_DIR = "tmp"


class DatabaseConfigHelper:
    @staticmethod
    def get_database_config(database):
        return databaseConfig[database]

    @staticmethod
    def get_database_url(database):
        return _get_database_url(database)

    @staticmethod
    def get_tmp_dir():
        return TMP_DIR


databaseConfig = {
    "postgreSQL": {
        "drivername": "postgresql",
        "username": "ploomber_app",
        "password": "ploomber_app_password",
        "database": "db",
        "host": "localhost",
        "port": "5432",
        "alias": "postgreSQLTest",
        "docker_ct": {
            "name": "postgres",
            "image": "postgres",
            "ports": {5432: 5432},
        },
    },
    "mySQL": {
        "drivername": "mysql+pymysql",
        "username": "ploomber_app",
        "password": "ploomber_app_password",
        "root_password": "ploomber_app_root_password",
        "database": "db",
        "host": "localhost",
        "port": "33306",
        "alias": "mySQLTest",
        "docker_ct": {
            "name": "mysql",
            "image": "mysql",
            "ports": {3306: 33306},
        },
    },
    "mariaDB": {
        "drivername": "mysql+pymysql",
        "username": "ploomber_app",
        "password": "ploomber_app_password",
        "root_password": "ploomber_app_root_password",
        "database": "db",
        "host": "localhost",
        "port": "33309",
        "alias": "mariaDBTest",
        "docker_ct": {
            "name": "mariadb",
            "image": "mariadb",
            "ports": {3306: 33309},
        },
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


client = docker.from_env()


def database_ready(
    database,
    timeout=20,
    poll_freq=0.2,
):
    """Wait until a postgres instance is ready to receive connections.

    .. note::

        This requires psycopg2 to be installed.

    :type host: str
    :type port: int
    :type timeout: float
    :type poll_freq: float
    """
    import sqlalchemy

    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            eng = sqlalchemy.create_engine(_get_database_url(database)).connect()
            eng.close()
            return True
        except Exception:
            pass
        time.sleep(poll_freq)

    return False


@contextmanager
def postgres(is_bypass_init=False):
    if is_bypass_init:
        yield None
        return
    db_config = DatabaseConfigHelper.get_database_config("postgreSQL")
    try:
        client = docker.from_env(version="auto")
        container = client.containers.get(db_config["docker_ct"]["name"])
        yield container
    except errors.NotFound:
        print("Creating new container: postgreSQL")
        with new_container(
            new_container_name=db_config["docker_ct"]["name"],
            image_name=db_config["docker_ct"]["image"],
            ports=db_config["docker_ct"]["ports"],
            environment={
                "POSTGRES_DB": db_config["database"],
                "POSTGRES_USER": db_config["username"],
                "POSTGRES_PASSWORD": db_config["password"],
            },
            ready_test=lambda: database_ready(database="postgreSQL"),
            healthcheck={
                "test": "pg_isready",
                "interval": 10000000000,
                "timeout": 5000000000,
                "retries": 5,
            },
        ) as container:
            yield container


@contextmanager
def mysql(is_bypass_init=False):
    if is_bypass_init:
        yield None
        return
    db_config = DatabaseConfigHelper.get_database_config("mySQL")
    try:
        client = docker.from_env(version="auto")
        container = client.containers.get(db_config["docker_ct"]["name"])
        yield container
    except errors.NotFound:
        print("Creating new container: mysql")
        with new_container(
            new_container_name=db_config["docker_ct"]["name"],
            image_name=db_config["docker_ct"]["image"],
            ports=db_config["docker_ct"]["ports"],
            environment={
                "MYSQL_DATABASE": db_config["database"],
                "MYSQL_USER": db_config["username"],
                "MYSQL_PASSWORD": db_config["password"],
                "MYSQL_ROOT_PASSWORD": db_config["root_password"],
            },
            command="mysqld --default-authentication-plugin=mysql_native_password",
            ready_test=lambda: database_ready(database="mySQL"),
            healthcheck={
                "test": [
                    "CMD",
                    "mysqladmin",
                    "ping",
                    "-h",
                    "localhost",
                    "--user=root",
                    "--password=ploomber_app_root_password",
                ],
                "timeout": 5000000000,
            },
        ) as container:
            yield container


@contextmanager
def mariadb(is_bypass_init=False):
    if is_bypass_init:
        yield None
        return
    db_config = DatabaseConfigHelper.get_database_config("mariaDB")
    try:
        client = docker.from_env(version="auto")
        curr = client.containers.get(db_config["docker_ct"]["name"])
        yield curr
    except errors.NotFound:
        print("Creating new container: mariaDB")
        with new_container(
            new_container_name=db_config["docker_ct"]["name"],
            image_name=db_config["docker_ct"]["image"],
            ports=db_config["docker_ct"]["ports"],
            environment={
                "MYSQL_DATABASE": db_config["database"],
                "MYSQL_USER": db_config["username"],
                "MYSQL_PASSWORD": db_config["password"],
                "MYSQL_ROOT_PASSWORD": db_config["root_password"],
            },
            command="mysqld --default-authentication-plugin=mysql_native_password",
            ready_test=lambda: database_ready(database="mariaDB"),
            healthcheck={
                "test": [
                    "CMD",
                    "mysqladmin",
                    "ping",
                    "-h",
                    "localhost",
                    "--user=root",
                    "--password=ploomber_app_root_password",
                ],
                "timeout": 5000000000,
            },
        ) as container:
            yield container


def main():
    print("Starting test containers...")

    with postgres(), mysql(), mariadb():
        print("Press CTRL+C to exit")
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            print("Exit, containers will be killed")
            sys.exit()


if __name__ == "__main__":
    main()
