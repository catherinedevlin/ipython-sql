from pathlib import Path
from unittest.mock import ANY, Mock
import pytest
import urllib.request
from sql.telemetry import telemetry
from sql import plot
from sql.connection import SQLAlchemyConnection
from sqlalchemy import create_engine

# Ref: https://pytest.org/en/7.2.x/how-to/tmp_path.html#
# Utilize tmp directory to store downloaded csv


@pytest.fixture
def simple_file_path_iris(tmpdir):
    file_path_str = str(tmpdir.join("iris.csv"))

    if not Path(file_path_str).is_file():
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
            file_path_str,
        )

    yield file_path_str


@pytest.fixture
def simple_file_path_penguins(tmpdir):
    file_path_str = str(tmpdir.join("penguins.csv"))

    if not Path(file_path_str).is_file():
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com"
            "/mwaskom/seaborn-data/master/penguins.csv",
            file_path_str,
        )

    yield file_path_str


@pytest.fixture
def simple_db_conn():
    engine = create_engine("duckdb://")
    return SQLAlchemyConnection(engine=engine)


@pytest.fixture
def mock_log_api(monkeypatch):
    mock_log_api = Mock()
    monkeypatch.setattr(telemetry, "log_api", mock_log_api)
    yield mock_log_api


excepted_duckdb_connection_info = {
    "dialect": "duckdb",
    "driver": "duckdb_engine",
    "server_version_info": ANY,
}

excepted_sqlite_connection_info = {
    "dialect": "sqlite",
    "driver": "pysqlite",
    "server_version_info": ANY,
}


def test_boxplot_telemetry_execution(
    mock_log_api, simple_db_conn, simple_file_path_iris, ip
):
    ip.run_cell("%sql duckdb://")
    plot.boxplot(simple_file_path_iris, "petal width", conn=simple_db_conn, orient="h")
    mock_log_api.assert_called_with(
        action="jupysql-boxplot-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": excepted_duckdb_connection_info,
        },
    )


def test_histogram_telemetry_execution(
    mock_log_api, simple_db_conn, simple_file_path_iris, ip
):
    ip.run_cell("%sql duckdb://")
    plot.histogram(simple_file_path_iris, "petal width", bins=50, conn=simple_db_conn)

    mock_log_api.assert_called_with(
        action="jupysql-histogram-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": excepted_duckdb_connection_info,
        },
    )


def test_data_frame_telemetry_execution(mock_log_api, ip, simple_file_path_iris):
    # Simulate the cell query & get the DataFrame
    ip.run_cell("%sql duckdb://")
    ip.run_cell(
        "result = %sql SELECT * FROM read_csv_auto('" + simple_file_path_iris + "')"
    )
    ip.run_cell("result.DataFrame()")
    mock_log_api.assert_called_with(
        action="jupysql-data-frame-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": excepted_duckdb_connection_info,
        },
    )


def test_sqlcmd_snippets_query_telemetry_execution(
    mock_log_api, ip, simple_file_path_iris
):
    # Simulate the sqlcmd snippets query
    ip.run_cell("%sql duckdb://")
    ip.run_cell(
        "%sql --save class_setosa --no-execute "
        "SELECT * FROM read_csv_auto('"
        + simple_file_path_iris
        + "')"
        + " WHERE class='Iris-setosa'"
    )
    ip.run_cell("%sqlcmd snippets class_setosa")

    mock_log_api.assert_called_with(
        action="jupysql-execute-success", total_runtime=ANY, metadata=ANY
    )


def test_execute_telemetry_execution(mock_log_api, ip):
    ip.run_cell("%sql duckdb://")

    mock_log_api.assert_called_with(
        action="jupysql-execute-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": excepted_duckdb_connection_info,
        },
    )


def test_switch_connection_with_correct_telemetry_connection_info(mock_log_api, ip):
    ip.run_cell("%sql duckdb://")

    mock_log_api.assert_called_with(
        action="jupysql-execute-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": excepted_duckdb_connection_info,
        },
    )

    ip.run_cell("%sql sqlite://")

    mock_log_api.assert_called_with(
        action="jupysql-execute-success",
        total_runtime=ANY,
        metadata={
            "argv": ANY,
            "connection_info": excepted_sqlite_connection_info,
        },
    )
