from sqlalchemy import inspect
from prettytable import PrettyTable
from ploomber_core.exceptions import modify_exceptions

from sql.connection import Connection
from sql.telemetry import telemetry


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
        inspector = _get_inspector(conn)

        columns = inspector.get_columns(name, schema)

        if not columns:
            if schema:
                raise ValueError(
                    f"There is no table with name {name!r} in schema {schema!r}"
                )
            else:
                raise ValueError(
                    f"There is no table with name {name!r} in the default schema"
                )

        self._table = PrettyTable()
        self._table.field_names = list(columns[0].keys())

        for row in columns:
            self._table.add_row(list(row.values()))

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
