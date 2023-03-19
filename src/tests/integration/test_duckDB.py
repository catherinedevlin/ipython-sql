import logging


def test_auto_commit_mode_on(ip_with_duckDB, caplog):
    with caplog.at_level(logging.DEBUG):
        ip_with_duckDB.run_cell("%config SqlMagic.autocommit=True")
        ip_with_duckDB.run_cell("%sql CREATE TABLE weather4 (city VARCHAR,);")
    assert caplog.record_tuples == [
        (
            "root",
            logging.DEBUG,
            "The database driver doesn't support such AUTOCOMMIT "
            "execution option\nPerhaps you can try running a manual "
            "COMMIT command\nMessage from the database driver\n\t"
            "Exception:  'duckdb.DuckDBPyConnection' object has no attribute"
            " 'set_isolation_level'\n",
        )
    ]


def test_auto_commit_mode_off(ip_with_duckDB, caplog):
    with caplog.at_level(logging.DEBUG):
        ip_with_duckDB.run_cell("%config SqlMagic.autocommit=False")
        ip_with_duckDB.run_cell("%sql CREATE TABLE weather (city VARCHAR,);")
    # Check there is no message gets printed
    assert caplog.record_tuples == []
    # Check the tables is created
    tables_out = ip_with_duckDB.run_cell("%sql SHOW TABLES;").result
    assert any("weather" == table[0] for table in tables_out)
