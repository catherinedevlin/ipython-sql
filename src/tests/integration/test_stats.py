import pytest

from sql.stats import _summary_stats
from sql.connection import SQLAlchemyConnection


@pytest.mark.parametrize(
    "fixture_name",
    [
        "setup_duckDB",
        "setup_MSSQL",
        "setup_postgreSQL",
        "setup_redshift",
    ],
)
def test_summary_stats(fixture_name, request, test_table_name_dict):
    engine = request.getfixturevalue(fixture_name)
    conn = SQLAlchemyConnection(engine)
    table = test_table_name_dict["plot_something"]
    column = "x"

    assert _summary_stats(conn, table, column) == {
        "q1": 1.0,
        "med": 2.0,
        "q3": 3.0,
        "mean": 2.0,
        "N": 5.0,
    }
