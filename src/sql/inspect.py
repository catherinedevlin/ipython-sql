from sqlalchemy import inspect
from prettytable import PrettyTable
from ploomber_core.exceptions import modify_exceptions
from sql.connection import ConnectionManager
from sql.telemetry import telemetry
from sql import exceptions
import math
from sql import util
from sql.store import get_all_keys
from IPython.core.display import HTML
import uuid


def _get_inspector(conn):
    if conn:
        return inspect(conn)

    if not ConnectionManager.current:
        raise exceptions.RuntimeError("No active connection")
    else:
        return inspect(ConnectionManager.current.connection_sqlalchemy)


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


def _add_missing_keys(keys, mapping):
    """
    Return a copy of `mapping` with all the missing `keys`, setting the
    value as an empty string
    """
    return {key: mapping.get(key, "") for key in keys}


# we're assuming there's one row that contains all keys, I tested this and worked fine
# my initial implementation just took all keys that appeared in "rows" but then order
# isn't preserved, which is important for user experience
def _get_row_with_most_keys(rows):
    """
    Get the row with the maximum length from the nested lists in `rows`
    """
    if not rows:
        return list()

    max_idx, max_ = None, 0

    for idx, row in enumerate(rows):
        if len(row) > max_:
            max_idx = idx
            max_ = len(row)

    if max_idx is None:
        return list()

    return list(rows[max_idx])


def _is_numeric(value):
    """Check if a column has numeric and not categorical datatype"""
    try:
        if isinstance(value, bool):
            return False
        float(value)  # Try to convert the value to float
        return True
    except (TypeError, ValueError):
        return False


def _is_numeric_as_str(column, value):
    """Check if a column contains numerical data stored as `str`"""
    try:
        if isinstance(value, str) and _is_numeric(value):
            return True
        return False
    except ValueError:
        pass


def _generate_column_styles(
    column_indices, unique_id, background_color="#FFFFCC", text_color="black"
):
    """
    Generate CSS styles to change the background-color of all columns
    with data-type mismatch.

    Parameters
    ----------
        column_indices (list): List of column indices with data-type mismatch.
        unique_id (str): Unique ID for the current table.
        background_color (str, optional): Background color for the mismatched columns.
        text_color (str, optional): Text color for the mismatched columns.

    Returns:
        str: HTML style tags containing the CSS styles for the mismatched columns.
    """

    styles = ""
    for index in column_indices:
        styles = f"""{styles}
        #profile-table-{unique_id} td:nth-child({index + 1}) {{
            background-color: {background_color};
            color: {text_color};
        }}
        """
    return f"<style>{styles}</style>"


def _generate_message(column_indices, columns):
    """Generate a message indicating all columns with a datatype mismatch"""
    message = "Columns "
    for c in column_indices:
        col = columns[c - 1]
        message = f"{message}<code>{col}</code>"
    message = (
        f"{message} have a datatype mismatch -> numeric values stored as a string."
    )
    message = f"{message} <br> Cannot calculate mean/min/max/std/percentiles"
    return message


def _assign_column_specific_stats(col_stats, is_numeric):
    """
    Assign NaN values to categorical/numerical specific statistic.

    Parameters
    ----------
        col_stats (dict): Dictionary containing column statistics.
        is_numeric (bool): Flag indicating whether the column is numeric or not.

    Returns:
        dict: Updated col_stats dictionary.
    """
    categorical_stats = ["top", "freq"]
    numerical_stats = ["mean", "min", "max", "std", "25%", "50%", "75%"]

    if is_numeric:
        for stat in categorical_stats:
            col_stats[stat] = math.nan
    else:
        for stat in numerical_stats:
            col_stats[stat] = math.nan

    return col_stats


@modify_exceptions
class Columns(DatabaseInspection):
    """
    Represents the columns in a database table
    """

    def __init__(self, name, schema, conn=None) -> None:
        is_table_exists(name, schema)

        inspector = _get_inspector(conn)

        # this returns a list of dictionaries. e.g.,
        # [{"name": "column_a", "type": "INT"}
        #  {"name": "column_b", "type": "FLOAT"}]
        if not schema and "." in name:
            schema, name = name.split(".")
        columns = inspector.get_columns(name, schema) or []

        self._table = PrettyTable()
        self._table.field_names = _get_row_with_most_keys(columns)

        for row in columns:
            self._table.add_row(
                list(_add_missing_keys(self._table.field_names, row).values())
            )

        self._table_html = self._table.get_html_string()
        self._table_txt = self._table.get_string()


@modify_exceptions
class TableDescription(DatabaseInspection):
    """
     Generates descriptive statistics.

     --------------------------------------
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

    ------------------------------------------
    Following statistics will be calculated for :-

    Categorical columns - [Count, Unique, Top, Freq]

    Numerical columns - [Count, Unique, Mean, Max, Min,
                         STD, 25h, 50h and 75h percentiles]

    """

    def __init__(self, table_name, schema=None) -> None:
        is_table_exists(table_name, schema)

        if schema:
            table_name = f"{schema}.{table_name}"

        conn = ConnectionManager.current

        columns_query_result = conn.raw_execute(f"SELECT * FROM {table_name} WHERE 1=0")
        if ConnectionManager.current.is_dbapi_connection:
            columns = [i[0] for i in columns_query_result.description]
        else:
            columns = columns_query_result.keys()

        table_stats = dict({})
        columns_to_include_in_report = set()
        columns_with_styles = []
        message_check = False

        for i, column in enumerate(columns):
            table_stats[column] = dict()

            # check the datatype of a column
            try:
                result = ConnectionManager.current.raw_execute(
                    f"""SELECT {column} FROM {table_name} LIMIT 1"""
                ).fetchone()

                value = result[0]
                is_numeric = isinstance(value, (int, float)) or (
                    isinstance(value, str) and _is_numeric(value)
                )
            except ValueError:
                is_numeric = True

            if _is_numeric_as_str(column, value):
                columns_with_styles.append(i + 1)
                message_check = True
            # Note: index is reserved word in sqlite
            try:
                result_col_freq_values = ConnectionManager.current.raw_execute(
                    f"""SELECT DISTINCT {column} as top,
                    COUNT({column}) as frequency FROM {table_name}
                    GROUP BY top ORDER BY frequency Desc""",
                ).fetchall()

                table_stats[column]["freq"] = result_col_freq_values[0][1]
                table_stats[column]["top"] = result_col_freq_values[0][0]

                columns_to_include_in_report.update(["freq", "top"])

            except Exception:
                pass

            try:
                # get all non None values, min, max and avg.
                result_value_values = ConnectionManager.current.raw_execute(
                    f"""
                    SELECT MIN({column}) AS min,
                    MAX({column}) AS max,
                    COUNT({column}) AS count
                    FROM {table_name}
                    WHERE {column} IS NOT NULL
                    """,
                ).fetchall()

                columns_to_include_in_report.update(["count", "min", "max"])
                table_stats[column]["count"] = result_value_values[0][2]

                table_stats[column]["min"] = round(result_value_values[0][0], 4)
                table_stats[column]["max"] = round(result_value_values[0][1], 4)

                columns_to_include_in_report.update(["count", "min", "max"])

            except Exception:
                pass

            try:
                # get unique values
                result_value_values = ConnectionManager.current.raw_execute(
                    f"""
                    SELECT
                    COUNT(DISTINCT {column}) AS unique_count
                    FROM {table_name}
                    WHERE {column} IS NOT NULL
                    """,
                ).fetchall()
                table_stats[column]["unique"] = result_value_values[0][0]
                columns_to_include_in_report.update(["unique"])
            except Exception:
                pass

            try:
                results_avg = ConnectionManager.current.raw_execute(
                    f"""
                                SELECT AVG({column}) AS avg
                                FROM {table_name}
                                WHERE {column} IS NOT NULL
                                """,
                ).fetchall()

                columns_to_include_in_report.update(["mean"])
                table_stats[column]["mean"] = format(float(results_avg[0][0]), ".4f")

            except Exception:
                table_stats[column]["mean"] = math.nan

            # These keys are numeric and work only on duckdb
            special_numeric_keys = ["std", "25%", "50%", "75%"]

            try:
                # Note: stddev_pop and PERCENTILE_DISC will work only on DuckDB
                result = ConnectionManager.current.raw_execute(
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

                columns_to_include_in_report.update(special_numeric_keys)
                for i, key in enumerate(special_numeric_keys):
                    # r_key = f'key_{key.replace("%", "")}'
                    table_stats[column][key] = format(float(result[0][i]), ".4f")

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

            table_stats[column] = _assign_column_specific_stats(
                table_stats[column], is_numeric
            )

        self._table = PrettyTable()
        self._table.field_names = [" "] + list(table_stats.keys())

        custom_order = [
            "count",
            "unique",
            "top",
            "freq",
            "mean",
            "std",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
        ]

        for row in custom_order:
            if row.lower() in [r.lower() for r in columns_to_include_in_report]:
                values = [row]
                for column in table_stats:
                    if row in table_stats[column]:
                        value = table_stats[column][row]
                    else:
                        value = ""
                    # value = util.convert_to_scientific(value)
                    values.append(value)

                self._table.add_row(values)

        unique_id = str(uuid.uuid4()).replace("-", "")
        column_styles = _generate_column_styles(columns_with_styles, unique_id)

        if message_check:
            message_content = _generate_message(columns_with_styles, list(columns))
            warning_background = "#FFFFCC"
            warning_title = "Warning: "
        else:
            message_content = ""
            warning_background = "white"
            warning_title = ""

        current = ConnectionManager.current
        database = current.dialect
        db_driver = current._get_database_information()["driver"]

        if database and "duckdb" in database:
            db_message = ""
        else:
            db_message = f"""Following statistics are not available in
            {db_driver}: STD, 25%, 50%, 75%"""

        db_html = (
            f"<div style='position: sticky; left: 0; padding: 10px; "
            f"font-size: 12px; color: #FFA500'>"
            f"<strong></strong> {db_message}"
            "</div>"
        )

        message_html = (
            f"<div style='position: sticky; left: 0; padding: 10px; "
            f"font-size: 12px; color: black; background-color: {warning_background};'>"
            f"<strong>{warning_title}</strong> {message_content}"
            "</div>"
        )

        # Inject css to html to make first column sticky
        sticky_column_css = """<style>
 #profile-table td:first-child {
  position: sticky;
  left: 0;
  background-color: var(--jp-cell-editor-background);
  font-weight: bold;
}
 #profile-table thead tr th:first-child {
  position: sticky;
  left: 0;
  background-color: var(--jp-cell-editor-background);
  font-weight: bold; /* Adding bold text */
}
            </style>"""
        self._table_html = HTML(
            db_html
            + sticky_column_css
            + column_styles
            + self._table.get_html_string(
                attributes={"id": f"profile-table-{unique_id}"}
            )
            + message_html
        ).__html__()

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


def support_only_sql_alchemy_connection(command):
    """
    Throws a sql.exceptions.RuntimeError if connection is not SQLAlchemy
    """
    if ConnectionManager.current.is_dbapi_connection:
        raise exceptions.RuntimeError(
            f"{command} is only supported with SQLAlchemy "
            "connections, not with DBAPI connections"
        )


def _is_table_exists(table: str, conn) -> bool:
    """
    Runs a SQL query to check if table exists
    """
    if not conn:
        conn = ConnectionManager.current

    identifiers = conn.get_curr_identifiers()

    for iden in identifiers:
        if isinstance(iden, tuple):
            query = "SELECT * FROM {0}{1}{2} WHERE 1=0".format(iden[0], table, iden[1])
        else:
            query = "SELECT * FROM {0}{1}{0} WHERE 1=0".format(iden, table)
        try:
            conn.execute(query)
            return True
        except Exception:
            pass

    return False


def _get_list_of_existing_tables() -> list:
    """
    Returns a list of table names for a given connection
    """
    tables = []
    tables_rows = get_table_names()._table
    for row in tables_rows:
        table_name = row.get_string(fields=["Name"], border=False, header=False).strip()

        tables.append(table_name)
    return tables


def is_table_exists(
    table: str,
    schema: str = None,
    ignore_error: bool = False,
    conn=None,
) -> bool:
    """
    Checks if a given table exists for a given connection

    Parameters
    ----------
    table: str
        Table name

    schema: str, default None
        Schema name

    ignore_error: bool, default False
        Avoid raising a ValueError
    """
    if table is None:
        if ignore_error:
            return False
        else:
            raise exceptions.UsageError("Table cannot be None")
    if not ConnectionManager.current:
        raise exceptions.RuntimeError("No active connection")
    if not conn:
        conn = ConnectionManager.current

    table = util.strip_multiple_chars(table, "\"'")

    if schema:
        table_ = f"{schema}.{table}"
    else:
        table_ = table

    _is_exist = _is_table_exists(table_, conn)

    if not _is_exist:
        if not ignore_error:
            try_find_suggestions = not conn.is_dbapi_connection
            expected = []
            existing_schemas = []
            existing_tables = []

            if try_find_suggestions:
                existing_schemas = get_schema_names()

            if schema and schema not in existing_schemas:
                expected = existing_schemas
                invalid_input = schema
            else:
                if try_find_suggestions:
                    existing_tables = _get_list_of_existing_tables()

                expected = existing_tables
                invalid_input = table

            if schema:
                err_message = (
                    f"There is no table with name {table!r} in schema {schema!r}"
                )
            else:
                err_message = (
                    f"There is no table with name {table!r} in the default schema"
                )

            if table not in get_all_keys():
                suggestions = util.find_close_match(invalid_input, expected)
                suggestions_store = util.find_close_match(invalid_input, get_all_keys())
                suggestions.extend(suggestions_store)
                suggestions_message = util.get_suggestions_message(suggestions)
                if suggestions_message:
                    err_message = f"{err_message}{suggestions_message}"
            raise exceptions.TableNotFoundError(err_message)

    return _is_exist


def fetch_sql_with_pagination(
    table, offset, n_rows, sort_column=None, sort_order=None
) -> tuple:
    """
    Returns next n_rows and columns from table starting at the offset

    Parameters
    ----------
    table : str
        Table name

    offset : int
        Specifies the number of rows to skip before
        it starts to return rows from the query expression.

    n_rows : int
        Number of rows to return.

    sort_column : str, default None
        Sort by column

    sort_order : 'DESC' or 'ASC', default None
        Order list
    """
    is_table_exists(table)

    order_by = "" if not sort_column else f"ORDER BY {sort_column} {sort_order}"

    query = f"""
    SELECT * FROM {table} {order_by}
    OFFSET {offset} ROWS FETCH NEXT {n_rows} ROWS ONLY"""

    rows = ConnectionManager.current.execute(query).fetchall()

    columns = ConnectionManager.current.raw_execute(
        f"SELECT * FROM {table} WHERE 1=0"
    ).keys()

    return rows, columns
