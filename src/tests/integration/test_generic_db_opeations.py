import shutil
import pytest


@pytest.fixture(autouse=True)
def run_around_tests(tmpdir_factory):
    # Create tmp folder
    my_tmpdir = tmpdir_factory.mktemp("tmp")
    yield my_tmpdir
    # Destory tmp folder
    shutil.rmtree(str(my_tmpdir))


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
    # Connect
    ip_with_dynamic_db.run_cell("%sql " + database_url + " --alias " + conn_alias)
    assert get_connection_count(ip_with_dynamic_db) == 1
