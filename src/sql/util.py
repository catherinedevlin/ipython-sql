from sql import inspect
import difflib
import sql.run
from sql.connection import Connection
from sql.store import store


def convert_to_scientific(value):
    """
    Converts value to scientific notation if necessary

    Parameters
    ----------
    value : any
        Value to format.
    """
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and _is_long_number(value)
    ):
        new_value = "{:,.3e}".format(value)

    else:
        new_value = value

    return new_value


def _is_long_number(num) -> bool:
    """
    Checks if num's digits > 10
    """
    if "." in str(num):
        split_by_decimal = str(num).split(".")
        if len(split_by_decimal[0]) > 10 or len(split_by_decimal[1]) > 10:
            return True
    return False


def is_table_exists(
    table: str,
    schema: str = None,
    ignore_error: bool = False,
    with_: str = None,
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

    with_: list, default None
        Temporary table

    ignore_error: bool, default False
        Avoid raising a ValueError
    """
    if table is None:
        if ignore_error:
            return False
        else:
            raise ValueError("Table cannot be None")
    if not Connection.current:
        raise RuntimeError("No active connection")
    if not conn:
        conn = Connection.current

    table = strip_multiple_chars(table, "\"'")

    if schema:
        table_ = f"{schema}.{table}"
    else:
        table_ = table

    _is_exist = _is_table_exists(table_, with_, conn)

    if not _is_exist:
        if not ignore_error:
            expected = []
            existing_schemas = inspect.get_schema_names()
            if schema and schema not in existing_schemas:
                expected = existing_schemas
                invalid_input = schema
            else:
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

            if table in list(store):
                # Suggest user use --with when given table
                # is in the store
                suggestion_message = (
                    ", but there is a stored query."
                    f"\nDid you miss passing --with {table}?"
                )
                err_message = f"{err_message}{suggestion_message}"
            else:
                suggestions = difflib.get_close_matches(invalid_input, expected)

                if len(suggestions) > 0:
                    _suggestions_string = pretty_print(suggestions, last_delimiter="or")
                    suggestions_message = f"\nDid you mean : {_suggestions_string}"
                    err_message = f"{err_message}{suggestions_message}"

            raise ValueError(err_message)

    return _is_exist


def _get_list_of_existing_tables() -> list:
    """
    Returns a list of table names for a given connection
    """
    tables = []
    tables_rows = inspect.get_table_names()._table
    for row in tables_rows:
        table_name = row.get_string(fields=["Name"], border=False, header=False).strip()

        tables.append(table_name)
    return tables


def pretty_print(
    obj: list, delimiter: str = ",", last_delimiter: str = "and", repr_: bool = False
) -> str:
    """
    Returns a formatted string representation of an array
    """
    if repr_:
        sorted_ = sorted(repr(element) for element in obj)
    else:
        sorted_ = sorted(f"'{element}'" for element in obj)

    if len(sorted_) > 1:
        sorted_[-1] = f"{last_delimiter} {sorted_[-1]}"

    return f"{delimiter} ".join(sorted_)


def strip_multiple_chars(string: str, chars: str) -> str:
    """
    Trims characters from the start and end of the string
    """
    return string.translate(str.maketrans("", "", chars))


def _is_table_exists(table: str, with_: str, conn) -> bool:
    """
    Runs a SQL query to check if table exists
    """
    if not conn:
        conn = Connection.current
    identifiers = conn.get_curr_identifiers()
    if with_:
        return table in list(store)
    else:
        for iden in identifiers:
            if isinstance(iden, tuple):
                query = "SELECT * FROM {0}{1}{2} WHERE 1=0".format(
                    iden[0], table, iden[1]
                )
            else:
                query = "SELECT * FROM {0}{1}{0} WHERE 1=0".format(iden, table)
            try:
                query = conn._transpile_query(query)
                sql.run.raw_run(conn, query)
                return True
            except Exception:
                pass

    return False
