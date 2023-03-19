import shutil
import pytest
import warnings
from sql.telemetry import telemetry
from unittest.mock import ANY, Mock


@pytest.fixture(autouse=True)
def run_around_tests(tmpdir_factory):
    # Create tmp folder
    my_tmpdir = tmpdir_factory.mktemp("tmp")
    yield my_tmpdir
    # Destory tmp folder
    shutil.rmtree(str(my_tmpdir))


@pytest.fixture
def mock_log_api(monkeypatch):
    mock_log_api = Mock()
    monkeypatch.setattr(telemetry, "log_api", mock_log_api)
    yield mock_log_api


# Query
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted",
    [
        ("ip_with_postgreSQL", 3),
        ("ip_with_mySQL", 3),
        ("ip_with_mariaDB", 3),
        ("ip_with_SQLite", 3),
        ("ip_with_duckDB", 3),
    ],
)
def test_query_count(ip_with_dynamic_db, excepted, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    out = ip_with_dynamic_db.run_line_magic("sql", "SELECT * FROM taxi LIMIT 3")
    assert len(out) == excepted


# Create
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted",
    [
        ("ip_with_postgreSQL", 15),
        ("ip_with_mySQL", 15),
        ("ip_with_mariaDB", 15),
        ("ip_with_SQLite", 15),
        ("ip_with_duckDB", 15),
    ],
)
def test_create_table_with_indexed_df(ip_with_dynamic_db, excepted, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    # Clean up
    ip_with_dynamic_db.run_cell("%sql DROP TABLE new_table_from_df")
    # Prepare DF
    ip_with_dynamic_db.run_cell("results = %sql SELECT * FROM taxi LIMIT 15")
    ip_with_dynamic_db.run_cell("new_table_from_df = results.DataFrame()")
    # Create table from DF
    persist_out = ip_with_dynamic_db.run_cell("%sql --persist new_table_from_df")
    query_out = ip_with_dynamic_db.run_cell("%sql SELECT * FROM new_table_from_df")
    assert persist_out.error_in_exec is None and query_out.error_in_exec is None
    assert len(query_out.result) == excepted


# Connection
def get_connection_count(ip_with_dynamic_db):
    out = ip_with_dynamic_db.run_line_magic("sql", "-l")
    print("Current connections:", out)
    connections_count = len(out)
    return connections_count


# Test - Number of active connection
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted",
    [
        ("ip_with_postgreSQL", 1),
        ("ip_with_mySQL", 1),
        ("ip_with_mariaDB", 1),
        ("ip_with_SQLite", 1),
        ("ip_with_duckDB", 1),
    ],
)
def test_active_connection_number(ip_with_dynamic_db, excepted, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    assert get_connection_count(ip_with_dynamic_db) == excepted


@pytest.mark.parametrize(
    "ip_with_dynamic_db, config_key",
    [
        ("ip_with_postgreSQL", "postgreSQL"),
        ("ip_with_mySQL", "mySQL"),
        ("ip_with_mariaDB", "mariaDB"),
        ("ip_with_SQLite", "SQLite"),
        ("ip_with_duckDB", "duckDB"),
    ],
)
def test_close_and_connect(
    ip_with_dynamic_db, config_key, request, get_database_config_helper
):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    conn_alias = get_database_config_helper.get_database_config(config_key)["alias"]
    database_url = get_database_config_helper.get_database_url(config_key)
    # Disconnect
    ip_with_dynamic_db.run_cell("%sql -x " + conn_alias)
    assert get_connection_count(ip_with_dynamic_db) == 0
    # Connect, also check there is no error on re-connecting
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        ip_with_dynamic_db.run_cell("%sql " + database_url + " --alias " + conn_alias)

    assert get_connection_count(ip_with_dynamic_db) == 1


# Telemetry
# Test - Number of active connection
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted_dialect, excepted_driver",
    [
        ("ip_with_postgreSQL", "postgresql", "psycopg2"),
        ("ip_with_mySQL", "mysql", "pymysql"),
        ("ip_with_mariaDB", "mysql", "pymysql"),
        ("ip_with_SQLite", "sqlite", "pysqlite"),
        ("ip_with_duckDB", "duckdb", "duckdb_engine"),
    ],
)
def test_telemetry_execute_command_has_connection_info(
    ip_with_dynamic_db, excepted_dialect, excepted_driver, mock_log_api, request
):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    mock_log_api.assert_called_with(
        action="jupysql-execute-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": {
                "dialect": excepted_dialect,
                "driver": excepted_driver,
                "server_version_info": ANY,
            },
        },
    )


@pytest.mark.parametrize(
    "ip_with_dynamic_db",
    [
        ("ip_with_postgreSQL"),
        ("ip_with_mySQL"),
        ("ip_with_mariaDB"),
        ("ip_with_SQLite"),
        ("ip_with_duckDB"),
    ],
)
def test_sql_cmd_magic_uno(ip_with_dynamic_db, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    result = ip_with_dynamic_db.run_cell(
        "%sqlcmd test --table numbers --column numbers_elements"
        " --less-than 5 --greater 1"
    ).result

    assert len(result) == 2
    assert "less_than" in result.keys()
    assert "greater" in result.keys()


@pytest.mark.parametrize(
    "ip_with_dynamic_db",
    [
        ("ip_with_postgreSQL"),
        ("ip_with_mySQL"),
        ("ip_with_mariaDB"),
        ("ip_with_SQLite"),
        ("ip_with_duckDB"),
    ],
)
def test_sql_cmd_magic_dos(ip_with_dynamic_db, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    result = ip_with_dynamic_db.run_cell(
        "%sqlcmd test --table numbers --column numbers_elements" " --greater-or-equal 3"
    ).result

    assert len(result) == 1
    assert "greater_or_equal" in result.keys()
    assert list(result["greater_or_equal"]) == [2, 3]
