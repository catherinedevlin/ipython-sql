import shutil
from matplotlib import pyplot as plt
import pytest
import warnings
from sql.telemetry import telemetry
from unittest.mock import ANY, Mock

import math

ALL_DATABASES = [
    "ip_with_postgreSQL",
    "ip_with_mySQL",
    "ip_with_mariaDB",
    "ip_with_SQLite",
    "ip_with_duckDB",
    "ip_with_MSSQL",
    "ip_with_Snowflake",
    "ip_with_oracle",
]


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
    "ip_with_dynamic_db, expected",
    [
        ("ip_with_postgreSQL", 3),
        ("ip_with_mySQL", 3),
        ("ip_with_mariaDB", 3),
        ("ip_with_SQLite", 3),
        ("ip_with_duckDB", 3),
        ("ip_with_Snowflake", 3),
    ],
)
def test_query_count(ip_with_dynamic_db, expected, request, test_table_name_dict):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    out = ip_with_dynamic_db.run_line_magic(
        "sql", f"SELECT * FROM {test_table_name_dict['taxi']} LIMIT 3"
    )

    # Test query with --with & --save
    ip_with_dynamic_db.run_cell(
        f"%sql --save taxi_subset --no-execute SELECT * FROM\
          {test_table_name_dict['taxi']} LIMIT 3"
    )
    out_query_with_save_arg = ip_with_dynamic_db.run_cell(
        "%sql --with taxi_subset SELECT * FROM taxi_subset"
    )

    assert len(out) == expected
    assert len(out_query_with_save_arg.result) == expected


# Create
@pytest.mark.parametrize(
    "ip_with_dynamic_db, expected, limit",
    [
        ("ip_with_postgreSQL", 15, 15),
        ("ip_with_mySQL", 15, 15),
        ("ip_with_mariaDB", 15, 15),
        ("ip_with_SQLite", 15, 15),
        ("ip_with_duckDB", 15, 15),
        # Snowflake doesn't support index, skip that
    ],
)
def test_create_table_with_indexed_df(
    ip_with_dynamic_db, expected, limit, request, test_table_name_dict
):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    # Clean up

    ip_with_dynamic_db.run_cell(
        f"%sql DROP TABLE {test_table_name_dict['new_table_from_df']}"
    )
    # Prepare DF
    ip_with_dynamic_db.run_cell(
        f"results = %sql SELECT * FROM {test_table_name_dict['taxi']}\
          LIMIT {limit}"
    )
    # Prepare expected df
    expected_df = ip_with_dynamic_db.run_cell(
        f"%sql SELECT * FROM {test_table_name_dict['taxi']}\
          LIMIT {limit}"
    )
    ip_with_dynamic_db.run_cell(
        f"{test_table_name_dict['new_table_from_df']} = results.DataFrame()"
    )
    # Create table from DF
    persist_out = ip_with_dynamic_db.run_cell(
        f"%sql --persist {test_table_name_dict['new_table_from_df']}"
    )
    out_df = ip_with_dynamic_db.run_cell(
        f"%sql SELECT * FROM {test_table_name_dict['new_table_from_df']}"
    )
    assert persist_out.error_in_exec is None and out_df.error_in_exec is None
    assert len(out_df.result) == expected
    assert expected_df.result.DataFrame().equals(
        out_df.result.DataFrame().loc[:, out_df.result.DataFrame().columns != "level_0"]
    )


# Connection
def get_connection_count(ip_with_dynamic_db):
    out = ip_with_dynamic_db.run_line_magic("sql", "-l")
    print("Current connections:", out)
    connections_count = len(out)
    return connections_count


# Test - Number of active connection
@pytest.mark.parametrize(
    "ip_with_dynamic_db, expected",
    [
        ("ip_with_postgreSQL", 1),
        ("ip_with_mySQL", 1),
        ("ip_with_mariaDB", 1),
        ("ip_with_SQLite", 1),
        ("ip_with_duckDB", 1),
        ("ip_with_MSSQL", 1),
        ("ip_with_Snowflake", 1),
    ],
)
def test_active_connection_number(ip_with_dynamic_db, expected, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    assert get_connection_count(ip_with_dynamic_db) == expected


@pytest.mark.parametrize(
    "ip_with_dynamic_db, config_key",
    [
        ("ip_with_postgreSQL", "postgreSQL"),
        ("ip_with_mySQL", "mySQL"),
        ("ip_with_mariaDB", "mariaDB"),
        ("ip_with_SQLite", "SQLite"),
        ("ip_with_duckDB", "duckDB"),
        ("ip_with_MSSQL", "MSSQL"),
        ("ip_with_Snowflake", "Snowflake"),
        ("ip_with_oracle", "oracle"),
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
    "ip_with_dynamic_db, expected_dialect, expected_driver",
    [
        ("ip_with_postgreSQL", "postgresql", "psycopg2"),
        ("ip_with_mySQL", "mysql", "pymysql"),
        ("ip_with_mariaDB", "mysql", "pymysql"),
        ("ip_with_SQLite", "sqlite", "pysqlite"),
        ("ip_with_duckDB", "duckdb", "duckdb_engine"),
        ("ip_with_MSSQL", "mssql", "pyodbc"),
        ("ip_with_Snowflake", "snowflake", "snowflake"),
        ("ip_with_oracle", "oracle", "oracledb"),
    ],
)
def test_telemetry_execute_command_has_connection_info(
    ip_with_dynamic_db, expected_dialect, expected_driver, mock_log_api, request
):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    mock_log_api.assert_called_with(
        action="jupysql-execute-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": {
                "dialect": expected_dialect,
                "driver": expected_driver,
                "server_version_info": ANY,
            },
        },
    )


@pytest.mark.parametrize(
    "cell",
    [
        (
            "%sqlplot histogram --with plot_something_subset --table\
              plot_something_subset --column x"
        ),
        (
            "%sqlplot hist --with plot_something_subset --table\
              plot_something_subset --column x"
        ),
        (
            "%sqlplot histogram --with plot_something_subset --table\
              plot_something_subset --column x --bins 10"
        ),
    ],
    ids=[
        "histogram",
        "hist",
        "histogram-bins",
    ],
)
@pytest.mark.parametrize(
    "ip_with_dynamic_db",
    [
        ("ip_with_postgreSQL"),
        ("ip_with_mySQL"),
        ("ip_with_mariaDB"),
        ("ip_with_SQLite"),
        ("ip_with_duckDB"),
        pytest.param(
            "ip_with_MSSQL",
            marks=pytest.mark.xfail(reason="sqlglot does not support SQL server"),
        ),
        pytest.param(
            "ip_with_Snowflake",
            marks=pytest.mark.xfail(
                reason="Something wrong with sqlplot histogram in snowflake"
            ),
        ),
        ("ip_with_oracle"),
    ],
)
def test_sqlplot_histogram(ip_with_dynamic_db, cell, request, test_table_name_dict):
    # clean current Axes
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    plt.cla()

    ip_with_dynamic_db.run_cell(
        f"%sql --save plot_something_subset\
         --no-execute SELECT * from {test_table_name_dict['plot_something']} LIMIT 3"
    )
    out = ip_with_dynamic_db.run_cell(cell)

    assert type(out.result).__name__ in {"Axes", "AxesSubplot"}


BOX_PLOT_FAIL_REASON = (
    "Known issue, the SQL engine must support percentile_disc() SQL clause"
)


@pytest.mark.parametrize(
    "cell",
    [
        "%sqlplot boxplot --with plot_something_subset \
        --table plot_something_subset --column x",
        "%sqlplot box --with plot_something_subset \
        --table plot_something_subset --column x",
        "%sqlplot boxplot --with plot_something_subset \
        --table plot_something_subset --column x --orient h",
        "%sqlplot boxplot --with plot_something_subset \
        --table plot_something_subset --column x",
    ],
    ids=[
        "boxplot",
        "box",
        "boxplot-with-horizontal",
        "boxplot-with",
    ],
)
@pytest.mark.parametrize(
    "ip_with_dynamic_db",
    [
        pytest.param("ip_with_postgreSQL"),
        pytest.param("ip_with_duckDB"),
        pytest.param(
            "ip_with_mySQL", marks=pytest.mark.xfail(reason=BOX_PLOT_FAIL_REASON)
        ),
        pytest.param(
            "ip_with_mariaDB", marks=pytest.mark.xfail(reason=BOX_PLOT_FAIL_REASON)
        ),
        pytest.param(
            "ip_with_SQLite", marks=pytest.mark.xfail(reason=BOX_PLOT_FAIL_REASON)
        ),
        pytest.param(
            "ip_with_Snowflake",
            marks=pytest.mark.xfail(
                reason="Something wrong with sqlplot boxplot in snowflake"
            ),
        ),
    ],
)
def test_sqlplot_boxplot(ip_with_dynamic_db, cell, request, test_table_name_dict):
    # clean current Axes
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    plt.cla()
    ip_with_dynamic_db.run_cell(
        f"%sql --save plot_something_subset --no-execute\
          SELECT * from {test_table_name_dict['plot_something']} LIMIT 3"
    )

    out = ip_with_dynamic_db.run_cell(cell)

    assert type(out.result).__name__ in {"Axes", "AxesSubplot"}


@pytest.mark.parametrize(
    "ip_with_dynamic_db",
    [
        ("ip_with_postgreSQL"),
        ("ip_with_mySQL"),
        ("ip_with_mariaDB"),
        ("ip_with_SQLite"),
        ("ip_with_duckDB"),
        ("ip_with_MSSQL"),
        ("ip_with_Snowflake"),
        ("ip_with_oracle"),
    ],
)
def test_sql_cmd_magic_uno(ip_with_dynamic_db, request, capsys):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    ip_with_dynamic_db.run_cell(
        """
    %%sql sqlite://
    CREATE TABLE test_numbers (value);
    INSERT INTO test_numbers VALUES (0);
    INSERT INTO test_numbers VALUES (4);
    INSERT INTO test_numbers VALUES (5);
    INSERT INTO test_numbers VALUES (6);
    """
    )

    ip_with_dynamic_db.run_cell(
        "%sqlcmd test --table test_numbers --column value" " --less-than 5 --greater 1"
    )

    _out = capsys.readouterr()

    assert "less_than" in _out.out
    assert "greater" in _out.out


@pytest.mark.parametrize(
    "ip_with_dynamic_db",
    [
        ("ip_with_postgreSQL"),
        ("ip_with_mySQL"),
        ("ip_with_mariaDB"),
        ("ip_with_SQLite"),
        ("ip_with_duckDB"),
        ("ip_with_MSSQL"),
        pytest.param(
            "ip_with_Snowflake",
            marks=pytest.mark.xfail(
                reason="Something wrong with test_sql_cmd_magic_dos in snowflake"
            ),
        ),
        ("ip_with_oracle"),
    ],
)
def test_sql_cmd_magic_dos(ip_with_dynamic_db, request, capsys):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    ip_with_dynamic_db.run_cell(
        """
    %%sql sqlite://
    CREATE TABLE test_numbers (value);
    INSERT INTO test_numbers VALUES (0);
    INSERT INTO test_numbers VALUES (4);
    INSERT INTO test_numbers VALUES (5);
    INSERT INTO test_numbers VALUES (6);
    """
    )

    ip_with_dynamic_db.run_cell(
        "%sqlcmd test --table test_numbers --column value --greater-or-equal 3"
    )

    _out = capsys.readouterr()

    assert "greater_or_equal" in _out.out
    assert "0" in _out.out


@pytest.mark.parametrize(
    "ip_with_dynamic_db, table, table_columns, expected",
    [
        (
            "ip_with_postgreSQL",
            "taxi",
            ["index", "taxi_driver_name"],
            {
                "count": [45, 45],
                "mean": [22.0, math.nan],
                "min": [0, "Eric Ken"],
                "max": [44, "Kevin Kelly"],
                "unique": [45, 3],
                "freq": [1, 15],
                "top": [0, "Eric Ken"],
                "std": ["1.299e+01", math.nan],
                "25%": [11.0, math.nan],
                "50%": [22.0, math.nan],
                "75%": [33.0, math.nan],
            },
        ),
        pytest.param(
            "ip_with_mySQL",
            "taxi",
            ["taxi_driver_name"],
            {
                "count": [45],
                "mean": [0.0],
                "min": ["Eric Ken"],
                "max": ["Kevin Kelly"],
                "unique": [3],
                "freq": [15],
                "top": ["Kevin Kelly"],
            },
            marks=pytest.mark.xfail(
                reason="Need to get column names from table with a different query"
            ),
        ),
        pytest.param(
            "ip_with_mariaDB",
            "taxi",
            ["taxi_driver_name"],
            {
                "count": [45],
                "mean": [0.0],
                "min": ["Eric Ken"],
                "max": ["Kevin Kelly"],
                "unique": [3],
                "freq": [15],
                "top": ["Kevin Kelly"],
            },
            marks=pytest.mark.xfail(
                reason="Need to get column names from table with a different query"
            ),
        ),
        (
            "ip_with_SQLite",
            "taxi",
            ["taxi_driver_name"],
            {
                "count": [45],
                "mean": [0.0],
                "min": ["Eric Ken"],
                "max": ["Kevin Kelly"],
                "unique": [3],
                "freq": [15],
                "top": ["Kevin Kelly"],
            },
        ),
        (
            "ip_with_duckDB",
            "taxi",
            ["index", "taxi_driver_name"],
            {
                "count": [45, 45],
                "mean": [22.0, math.nan],
                "min": [0, "Eric Ken"],
                "max": [44, "Kevin Kelly"],
                "unique": [45, 3],
                "freq": [1, 15],
                "top": [0, "Eric Ken"],
                "std": ["1.299e+01", math.nan],
                "25%": [11.0, math.nan],
                "50%": [22.0, math.nan],
                "75%": [33.0, math.nan],
            },
        ),
        (
            "ip_with_MSSQL",
            "taxi",
            ["taxi_driver_name"],
            {"unique": [3], "min": ["Eric Ken"], "max": ["Kevin Kelly"], "count": [45]},
        ),
        pytest.param(
            "ip_with_Snowflake",
            "taxi",
            ["taxi_driver_name"],
            {},
            marks=pytest.mark.xfail(
                reason="Something wrong with test_profile_query in snowflake"
            ),
        ),
        pytest.param(
            "ip_with_oracle",
            "taxi",
            ["taxi_driver_name"],
            {},
            marks=pytest.mark.xfail(
                reason="Something wrong with test_profile_query in snowflake"
            ),
        ),
    ],
)
def test_profile_query(
    request, ip_with_dynamic_db, table, table_columns, expected, test_table_name_dict
):
    pytest.skip("Skip on unclosed session issue")
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)

    out = ip_with_dynamic_db.run_cell(
        f"""
        %sqlcmd profile --table "{test_table_name_dict[table]}"
        """
    ).result

    stats_table = out._table

    assert len(stats_table.rows) == len(expected)

    for row in stats_table:
        criteria = row.get_string(fields=[" "], border=False).strip()

        for i, column in enumerate(table_columns):
            cell_value = row.get_string(
                fields=[column], border=False, header=False
            ).strip()

            assert criteria in expected
            assert cell_value == str(expected[criteria][i])


@pytest.mark.parametrize(
    "table",
    [
        "numbers",
    ],
)
@pytest.mark.parametrize("ip_with_dynamic_db", ALL_DATABASES)
def test_sqlcmd_tables_columns(
    ip_with_dynamic_db, table, request, test_table_name_dict
):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    out = ip_with_dynamic_db.run_cell(
        f"%sqlcmd columns --table {test_table_name_dict[table]}"
    )
    assert out.result


@pytest.mark.parametrize("ip_with_dynamic_db", ALL_DATABASES)
def test_sqlcmd_tables(ip_with_dynamic_db, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    out = ip_with_dynamic_db.run_cell("%sqlcmd tables")
    assert out.result


@pytest.mark.parametrize(
    "cell",
    [
        "%%sql\nSELECT * FROM numbers WHERE 0=1",
        "%%sql --with subset\nSELECT * FROM subset WHERE 0=1",
        "%%sql\nSELECT *\n-- %one $another\nFROM numbers WHERE 0=1",
    ],
    ids=[
        "simple-query",
        "cte",
        "interpolation-like-comment",
    ],
)
@pytest.mark.parametrize("ip_with_dynamic_db", ALL_DATABASES)
def test_sql_query(ip_with_dynamic_db, cell, request):
    ip_with_dynamic_db = request.getfixturevalue(ip_with_dynamic_db)
    ip_with_dynamic_db.run_cell(
        """%%sql --save subset --no-execute
SELECT * FROM numbers WHERE 1=0
"""
    )
    out = ip_with_dynamic_db.run_cell(cell)
    assert out.error_in_exec is None
