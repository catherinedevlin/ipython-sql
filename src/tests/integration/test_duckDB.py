from unittest.mock import Mock
import pytest

import polars as pl
import pandas as pd


from sql.warnings import JupySQLDataFramePerformanceWarning


@pytest.mark.parametrize(
    "method, expected_type, expected_native_method",
    [
        ("DataFrame", pd.DataFrame, "df"),
        ("PolarsDataFrame", pl.DataFrame, "pl"),
    ],
)
def test_native_connection_converts_to_data_frames_natively(
    monkeypatch,
    ip_with_duckdb_native_empty,
    method,
    expected_type,
    expected_native_method,
):
    ip_with_duckdb_native_empty.run_cell(
        "%sql CREATE TABLE weather (city VARCHAR, temp_lo INT);"
    )
    ip_with_duckdb_native_empty.run_cell(
        "%sql INSERT INTO weather VALUES ('San Francisco', 46);"
    )
    ip_with_duckdb_native_empty.run_cell("%sql INSERT INTO weather VALUES ('NYC', 20);")
    ip_with_duckdb_native_empty.run_cell("results = %sql SELECT * FROM weather")

    results = ip_with_duckdb_native_empty.run_cell("results").result

    mock = Mock(wraps=results.sqlaproxy)
    monkeypatch.setattr(results, "_sqlaproxy", mock)

    out = ip_with_duckdb_native_empty.run_cell(f"results.{method}()")

    mock.execute.assert_called_once_with("SELECT * FROM weather")
    getattr(mock, expected_native_method).assert_called_once_with()
    assert isinstance(out.result, expected_type)
    assert out.result.shape == (2, 2)


@pytest.mark.parametrize(
    "conversion_cell, expected_type",
    [
        ("%config SqlMagic.autopandas = True", pd.DataFrame),
        ("%config SqlMagic.autopolars = True", pl.DataFrame),
    ],
    ids=[
        "autopandas_on",
        "autopolars_on",
    ],
)
def test_convert_to_dataframe_automatically(
    ip_with_duckdb_native_empty,
    conversion_cell,
    expected_type,
):
    ip_with_duckdb_native_empty.run_cell(conversion_cell)
    ip_with_duckdb_native_empty.run_cell(
        "%sql CREATE TABLE weather (city VARCHAR, temp_lo INT);"
    )
    ip_with_duckdb_native_empty.run_cell(
        "%sql INSERT INTO weather VALUES ('San Francisco', 46);"
    )
    ip_with_duckdb_native_empty.run_cell("%sql INSERT INTO weather VALUES ('NYC', 20);")
    df = ip_with_duckdb_native_empty.run_cell("%sql SELECT * FROM weather").result
    assert isinstance(df, expected_type)
    assert df.shape == (2, 2)


@pytest.mark.parametrize(
    "config",
    [
        "%config SqlMagic.autopandas = True",
        "%config SqlMagic.autopandas = False",
    ],
    ids=[
        "autopandas_on",
        "autopandas_off",
    ],
)
@pytest.mark.parametrize(
    "sql, tables",
    [
        ["%sql SELECT * FROM weather; SELECT * FROM weather;", ["weather"]],
        [
            "%sql CREATE TABLE names (name VARCHAR,); SELECT * FROM weather;",
            ["weather", "names"],
        ],
        [
            (
                "%sql CREATE TABLE names (city VARCHAR,);"
                "CREATE TABLE more_names (city VARCHAR,);"
                "INSERT INTO names VALUES ('NYC');"
                "SELECT * FROM names UNION ALL SELECT * FROM more_names;"
            ),
            ["weather", "names", "more_names"],
        ],
    ],
    ids=[
        "multiple_selects",
        "multiple_statements",
        "multiple_tables_created",
    ],
)
@pytest.mark.parametrize(
    "ip",
    [
        "ip_with_duckdb_native_empty",
        "ip_with_duckdb_sqlalchemy_empty",
    ],
)
def test_multiple_statements(ip, config, sql, tables, request):
    ip_ = request.getfixturevalue(ip)
    ip_.run_cell(config)

    ip_.run_cell("%sql CREATE TABLE weather (city VARCHAR,);")
    ip_.run_cell("%sql INSERT INTO weather VALUES ('NYC');")
    ip_.run_cell("%sql SELECT * FROM weather;")

    out = ip_.run_cell(sql)

    if config == "%config SqlMagic.autopandas = True":
        assert out.result.to_dict() == {"city": {0: "NYC"}}
    else:
        assert out.result.dict() == {"city": ("NYC",)}

    if ip == "ip_with_duckdb_sqlalchemy_empty":
        out_tables = ip_.run_cell("%sqlcmd tables")
        assert set(tables) == set(r[0] for r in out_tables.result._table.rows)


@pytest.mark.parametrize(
    "ip",
    [
        "ip_with_duckdb_native_empty",
        "ip_with_duckdb_sqlalchemy_empty",
    ],
)
def test_empty_data_frame_if_last_statement_is_not_select(ip, request):
    ip = request.getfixturevalue(ip)
    ip.run_cell("%config SqlMagic.autopandas=True")
    out = ip.run_cell("%sql CREATE TABLE a (c VARCHAR,); CREATE TABLE b (c VARCHAR,);")
    assert len(out.result) == 0


@pytest.mark.parametrize(
    "sql",
    [
        (
            "%sql CREATE TABLE a (x INT,); CREATE TABLE b (x INT,); "
            "INSERT INTO a VALUES (1,); INSERT INTO b VALUES(2,); "
            "SELECT * FROM a UNION ALL SELECT * FROM b;"
        ),
        """\
%%sql
CREATE TABLE a (x INT,);
CREATE TABLE b (x INT,);
INSERT INTO a VALUES (1,);
INSERT INTO b VALUES(2,);
SELECT * FROM a UNION ALL SELECT * FROM b;
""",
    ],
)
@pytest.mark.parametrize(
    "ip",
    [
        "ip_with_duckdb_native_empty",
        "ip_with_duckdb_sqlalchemy_empty",
    ],
)
def test_commits_all_statements(ip, sql, request):
    ip = request.getfixturevalue(ip)
    out = ip.run_cell(sql)
    assert out.error_in_exec is None
    assert out.result.dict() == {"x": (1, 2)}


@pytest.mark.parametrize("method", ["DataFrame", "PolarsDataFrame"])
def test_warn_when_using_sqlalchemy_and_converting_to_dataframe(ip_empty, method):
    ip_empty.run_cell("%sql duckdb://")
    df = pd.DataFrame(range(1000))  # noqa

    data = ip_empty.run_cell("%sql SELECT * FROM df;").result

    with pytest.warns(JupySQLDataFramePerformanceWarning) as record:
        getattr(data, method)()

    assert len(record) == 1
