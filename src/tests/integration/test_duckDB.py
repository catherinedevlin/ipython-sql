import logging
import pytest


@pytest.mark.parametrize(
    "ip, exp",
    [
        (
            "ip_with_duckDB",
            "'duckdb.DuckDBPyConnection' object has no attribute "
            "'set_isolation_level'\n",
        ),
        (
            "ip_with_duckDB_native",
            "'CustomSession' object has no attribute '_has_events'",
        ),
    ],
)
def test_auto_commit_mode_on(ip, exp, caplog, request):
    ip = request.getfixturevalue(ip)
    with caplog.at_level(logging.DEBUG):
        ip.run_cell("%config SqlMagic.autocommit=True")
        ip.run_cell("%sql CREATE TABLE weather4 (city VARCHAR,);")
    assert caplog.record_tuples[0][0] == "root"
    assert caplog.record_tuples[0][1] == logging.DEBUG
    assert (
        "The database driver doesn't support such AUTOCOMMIT"
        in caplog.record_tuples[0][2]
    )
    assert exp in caplog.record_tuples[0][2]


@pytest.mark.parametrize(
    "ip",
    [
        ("ip_with_duckDB"),
        ("ip_with_duckDB_native"),
    ],
)
def test_auto_commit_mode_off(ip, caplog, request):
    ip = request.getfixturevalue(ip)
    with caplog.at_level(logging.DEBUG):
        ip.run_cell("%config SqlMagic.autocommit=False")
        ip.run_cell("%sql CREATE TABLE weather (city VARCHAR,);")
    # Check there is no message gets printed
    assert caplog.record_tuples == []
    # Check the tables is created
    tables_out = ip.run_cell("%sql SHOW TABLES;").result
    assert any("weather" == table[0] for table in tables_out)
