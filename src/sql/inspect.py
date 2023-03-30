from sqlalchemy import inspect
from prettytable import PrettyTable
from ploomber_core.exceptions import modify_exceptions
from sql.connection import Connection
from sql.telemetry import telemetry
import sql.run
import math
from sql import util


def _get_inspector(conn):
    if conn:
        return inspect(conn)

    if not Connection.current:
        raise RuntimeError("No active connection")
    else:
        return inspect(Connection.current.session)


class DatabaseInspection:
    def __repr__(self) -> str:
        return self._table_txt

    def _repr_html_(self) -> str:
        return self._table_html


class Tables(DatabaseInspection):
    """
    Displays the tables in a database
    """

    def __init__(self, schema=None, conn=None) -> None:
        inspector = _get_inspector(conn)

        self._table = PrettyTable()
        self._table.field_names = ["Name"]

        for row in inspector.get_table_names(schema=schema):
            self._table.add_row([row])

        self._table_html = self._table.get_html_string()
        self._table_txt = self._table.get_string()


@modify_exceptions
class Columns(DatabaseInspection):
    """
    Represents the columns in a database table
    """

    def __init__(self, name, schema, conn=None) -> None:
        util.is_table_exists(name, schema)

        inspector = _get_inspector(conn)

        columns = inspector.get_columns(name, schema)

        self._table = PrettyTable()
        self._table.field_names = list(columns[0].keys())

        for row in columns:
            self._table.add_row(list(row.values()))

        self._table_html = self._table.get_html_string()
        self._table_txt = self._table.get_string()


@modify_exceptions
class TableDescription(DatabaseInspection):
    """
    Generates descriptive statistics.

    Descriptive statistics are:

    Count - Number of all not None values

    Mean - Mean of the values

    Max - Maximum of the values in the object.

    Min - Minimum of the values in the object.

    STD - Standard deviation of the observations

    25h, 50h and 75h percentiles

    Unique - Number of not None unique values

    Top - The most frequent value

    Freq - Frequency of the top value

    """

    def __init__(self, table_name, schema=None) -> None:
        util.is_table_exists(table_name, schema)

        if schema:
            table_name = f"{schema}.{table_name}"

        columns = sql.run.raw_run(
            Connection.current, f"SELECT * FROM {table_name} WHERE 1=0"
        ).keys()

        table_stats = dict({})
        columns_to_include_in_report = set()

        for column in columns:
            table_stats[column] = dict()

            # Note: index is reserved word in sqlite
            try:
                result_col_freq_values = sql.run.raw_run(
                    Connection.current,
                    f"""SELECT DISTINCT {column} as top,
                    COUNT({column}) as frequency FROM {table_name}
                    GROUP BY {column} ORDER BY Count({column}) Desc""",
                ).fetchall()

                table_stats[column]["freq"] = result_col_freq_values[0][1]
                table_stats[column]["top"] = result_col_freq_values[0][0]

                columns_to_include_in_report.update(["freq", "top"])

            except Exception:
                pass

            try:
                # get all non None values, min, max and avg.
                result_value_values = sql.run.raw_run(
                    Connection.current,
                    f"""
                    SELECT MIN({column}) AS min,
                    MAX({column}) AS max,
                    COUNT(DISTINCT {column}) AS unique_count,
                    COUNT({column}) AS count
                    FROM {table_name}
                    WHERE {column} IS NOT NULL
                    """,
                ).fetchall()

                table_stats[column]["min"] = result_value_values[0][0]
                table_stats[column]["max"] = result_value_values[0][1]
                table_stats[column]["unique"] = result_value_values[0][2]
                table_stats[column]["count"] = result_value_values[0][3]

                columns_to_include_in_report.update(["count", "unique", "min", "max"])

            except Exception:
                pass

            try:
                results_avg = sql.run.raw_run(
                    Connection.current,
                    f"""
                                SELECT AVG({column}) AS avg
                                FROM {table_name}
                                WHERE {column} IS NOT NULL
                                """,
                ).fetchall()

                table_stats[column]["mean"] = float(results_avg[0][0])
                columns_to_include_in_report.update(["mean"])

            except Exception:
                table_stats[column]["mean"] = math.nan

            # These keys are numeric and work only on duckdb
            special_numeric_keys = ["std", "25%", "50%", "75%"]

            try:
                # Note: stddev_pop and PERCENTILE_DISC will work only on DuckDB
                result = sql.run.raw_run(
                    Connection.current,
                    f"""
                    SELECT
                        stddev_pop({column}) as key_std,
                        percentile_disc(0.25) WITHIN GROUP
                        (ORDER BY {column}) as key_25,
                        percentile_disc(0.50) WITHIN GROUP
                        (ORDER BY {column}) as key_50,
                        percentile_disc(0.75) WITHIN GROUP
                        (ORDER BY {column}) as key_75
                    FROM {table_name}
                    """,
                ).fetchall()

                for i, key in enumerate(special_numeric_keys):
                    # r_key = f'key_{key.replace("%", "")}'
                    table_stats[column][key] = float(result[0][i])

                columns_to_include_in_report.update(special_numeric_keys)

            except TypeError:
                # for non numeric values
                for key in special_numeric_keys:
                    table_stats[column][key] = math.nan

            except Exception as e:
                # We tried to apply numeric function on
                # non numeric value, i.e: DateTime
                if "duckdb.BinderException" or "add explicit type casts" in str(e):
                    for key in special_numeric_keys:
                        table_stats[column][key] = math.nan

                # Failed to run sql command/func (e.g stddev_pop).
                # We ignore the cell stats for such case.
                pass

        self._table = PrettyTable()
        self._table.field_names = [" "] + list(table_stats.keys())

        rows = list(columns_to_include_in_report)
        rows.sort(reverse=True)
        for row in rows:
            values = [row]
            for column in table_stats:
                if row in table_stats[column]:
                    value = table_stats[column][row]
                else:
                    value = ""
                value = util.convert_to_scientific(value)
                values.append(value)

            self._table.add_row(values)

        self._table_html = self._table.get_html_string()
        self._table_txt = self._table.get_string()


@telemetry.log_call()
def get_table_names(schema=None):
    """Get table names for a given connection"""
    return Tables(schema)


@telemetry.log_call()
def get_columns(name, schema=None):
    """Get column names for a given connection"""
    return Columns(name, schema)


@telemetry.log_call()
def get_table_statistics(name, schema=None):
    """Get table statistics for a given connection.

    For all data types the results will include `count`, `mean`, `std`, `min`
    `max`, `25`, `50` and `75` percentiles. It will also include `unique`, `top`
    and `freq` statistics.
    """
    return TableDescription(name, schema=schema)


def get_schema_names(conn=None):
    """Get list of schema names for a given connection"""
    inspector = _get_inspector(conn)
    return inspector.get_schema_names()
