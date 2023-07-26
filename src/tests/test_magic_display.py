def test_connection_string_displayed(ip_empty, capsys):
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql show tables")

    captured = capsys.readouterr()
    assert "Running query in 'duckdb://'" in captured.out


def test_dbapi_connection_display(ip_empty, capsys, tmp_empty):
    ip_empty.run_cell("import duckdb")
    ip_empty.run_cell("custom = duckdb.connect('anotherdb')")
    ip_empty.run_cell("%sql custom")
    ip_empty.run_cell("%sql show tables")

    captured = capsys.readouterr()
    assert "Running query in 'DuckDBPyConnection'" in captured.out


def test_connection_string_hidden_when_passing_alias(ip_empty, capsys):
    ip_empty.run_cell("%sql duckdb:// --alias myduckdbconn")
    ip_empty.run_cell("%sql show tables")

    captured = capsys.readouterr()
    assert "duckdb://" not in captured.out
    assert "Running query in 'myduckdbconn'" in captured.out


def test_display_message_when_persisting_data_frames(ip_empty, capsys):
    ip_empty.run_cell("import pandas as pd; df = pd.DataFrame({'x': range(5)})")
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql --persist df")

    captured = capsys.readouterr()
    assert "\nSuccess! Persisted df to the database.\n" in captured.out


def test_listing_connections(ip_empty, tmp_empty):
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql sqlite://")
    ip_empty.run_cell("%sql sqlite:///my.db --alias somedb")
    ip_empty.run_cell("from sqlalchemy import create_engine")
    ip_empty.run_cell("engine = create_engine('duckdb:///somedb')")
    ip_empty.run_cell("%sql engine --alias someduckdb")
    ip_empty.run_cell("import duckdb")
    ip_empty.run_cell("custom = duckdb.connect('anotherdb')")
    ip_empty.run_cell("%sql custom")

    connections_table = ip_empty.run_cell("%sql --connections").result
    txt = str(connections_table)

    assert connections_table._repr_html_()
    assert "DuckDBPyConnection" in txt
    assert "sqlite:///my.db" in txt
    assert "duckdb:///somedb" in txt
    assert "sqlite://" in txt
    assert "somedb" in txt
    assert "someduckdb" in txt
