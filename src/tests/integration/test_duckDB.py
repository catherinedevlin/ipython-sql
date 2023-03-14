def test_auto_commit_mode_on(ip_with_duckDB, capsys):
    ip_with_duckDB.run_cell("%config SqlMagic.autocommit=True")
    ip_with_duckDB.run_cell("%sql CREATE TABLE weather (city VARCHAR,);")
    out, _ = capsys.readouterr()
    assert "The database driver doesn't support such AUTOCOMMIT execution option" in out


def test_auto_commit_mode_off(ip_with_duckDB, capsys):
    ip_with_duckDB.run_cell("%config SqlMagic.autocommit=False")
    ip_with_duckDB.run_cell("%sql CREATE TABLE weather (city VARCHAR,);")
    out, _ = capsys.readouterr()
    # Check there is no message gets printed
    assert (
        "The database driver doesn't support such AUTOCOMMIT execution option"
        not in out
    )

    # Check the tables is created
    out = ip_with_duckDB.run_cell("%sql SHOW TABLES;").result
    assert any('weather' == table[0] for table in out)
