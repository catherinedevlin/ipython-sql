from pathlib import Path
from unittest.mock import ANY, Mock
import pytest
import urllib.request
import duckdb
from sql.telemetry import telemetry
from sql import plot

# Ref: https://pytest.org/en/7.2.x/how-to/tmp_path.html#
# Utilize tmp directory to store downloaded csv


@pytest.fixture
def simple_file_path(tmpdir):
    file_path_str = str(tmpdir.join("iris.csv"))

    if not Path(file_path_str).is_file():
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com/plotly/datasets/master/iris-data.csv",
            file_path_str,
        )

    yield file_path_str


@pytest.fixture
def simple_db_conn():
    conn = duckdb.connect(database=":memory:")
    return conn


@pytest.fixture
def mock_log_api(monkeypatch):
    mock_log_api = Mock()
    monkeypatch.setattr(telemetry, "log_api", mock_log_api)
    yield mock_log_api


def test_boxplot_telemetry_execution(mock_log_api, simple_db_conn, simple_file_path):
    plot.boxplot(simple_file_path, "petal width", conn=simple_db_conn, orient="h")

    mock_log_api.assert_called_with(
        action="jupysql-boxplot-success", total_runtime=ANY, metadata=ANY
    )


def test_histogram_telemetry_execution(mock_log_api, simple_db_conn, simple_file_path):
    # Test the injected log_api gets called
    plot.histogram(simple_file_path, "petal width", bins=50, conn=simple_db_conn)

    mock_log_api.assert_called_with(
        action="jupysql-histogram-success", total_runtime=ANY, metadata=ANY
    )


def test_data_frame_telemetry_execution(mock_log_api, ip, simple_file_path):
    # Simulate the cell query & get the DataFrame
    ip.run_cell("%sql duckdb://")
    ip.run_cell("result = %sql SELECT * FROM read_csv_auto('" + simple_file_path + "')")
    ip.run_cell("result.DataFrame()")
    mock_log_api.assert_called_with(
        action="jupysql-data-frame-success", total_runtime=ANY, metadata=ANY
    )


def test_sqlrender_telemetry_execution(mock_log_api, ip, simple_file_path):
    # Simulate the sqlrender query
    ip.run_cell("%sql duckdb://")
    ip.run_cell(
        "%sql --save class_setosa --no-execute \
            SELECT * FROM read_csv_auto('"
        + simple_file_path
        + "' WHERE class='Iris-setosa'"
    )
    ip.run_cell("%sqlrender class_setosa")

    mock_log_api.assert_called_with(
        action="jupysql-sqlrender-success", total_runtime=ANY, metadata=ANY
    )


def test_execute_telemetry_execution(mock_log_api, ip):
    ip.run_cell("%sql duckdb://")

    mock_log_api.assert_called_with(
        action="jupysql-execute-success", total_runtime=ANY, metadata=ANY
    )
