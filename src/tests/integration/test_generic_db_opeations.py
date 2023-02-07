import pytest
# flake8: noqa
from fixtures.database import *


# Query
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted", [("ip_with_postgreSQL", 3), ("ip_with_mySQL", 3)]
)
def test_query_count(ip_with_dynamic_db, excepted, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    out = ip_with_dynamic_db.run_line_magic("sql", "SELECT * FROM taxi LIMIT 3")
    print("count out: ", len(out))
    assert len(out) == excepted


# Create
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted", [("ip_with_postgreSQL", 15), ("ip_with_mySQL", 15)]
)
def test_create_table_with_indexed_df(ip_with_dynamic_db, excepted, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    ip_with_dynamic_db.run_cell("results = %sql SELECT * FROM taxi LIMIT 15")
    ip_with_dynamic_db.run_cell("new_table_from_df = results.DataFrame()")
    ip_with_dynamic_db.run_cell("%sql --persist sqlite:// new_table_from_df")
    out = ip_with_dynamic_db.run_cell("%sql SELECT * FROM new_table_from_df")

    assert len(out.result) == excepted


# Connection
def get_connection_count(ip_with_dynamic_db):
    out = ip_with_dynamic_db.run_line_magic("sql", "-l")
    print("Current connections:", out)
    connections_count = len(out)
    return connections_count


# Test - Number of active connection
@pytest.mark.parametrize(
    "ip_with_dynamic_db, excepted", [("ip_with_postgreSQL", 1), ("ip_with_mySQL", 1)]
)
def test_active_connection_number(ip_with_dynamic_db, excepted, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    assert get_connection_count(ip_with_dynamic_db) == excepted


@pytest.mark.parametrize(
    "ip_with_dynamic_db, config_key",
    [("ip_with_postgreSQL", "postgreSQL"), ("ip_with_mySQL", "mySQL")],
)
def test_close_and_connect(ip_with_dynamic_db, config_key, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    conn_alias = databaseConfig[config_key]["alias"]
    # Disconnect
    ip_with_dynamic_db.run_cell("%sql -x " + conn_alias)
    assert get_connection_count(ip_with_dynamic_db) == 0
    # Connect
    ip_with_dynamic_db.run_cell(
        "%sql " + get_database_url(config_key) + " --alias " + conn_alias
    )
    assert get_connection_count(ip_with_dynamic_db) == 1
