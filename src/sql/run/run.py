import sqlparse

from sql import exceptions, display
from sql.run.resultset import ResultSet
from sql.run.pgspecial import handle_postgres_special


# TODO: conn also has access to config, we should clean this up to provide a clean
# way to access the config
def run_statements(conn, sql, config, parameters=None):
    """
    Run a SQL query (supports running multiple SQL statements) with the given
    connection. This is the function that's called when executing SQL magic.

    Parameters
    ----------
    conn : sql.connection.AbstractConnection
        The connection to use

    sql : str
        SQL query to execution

    config
        Configuration object

    Examples
    --------

    .. literalinclude:: ../../examples/run_statements.py

    """
    if not sql.strip():
        return "Connected: %s" % conn.name

    for statement in sqlparse.split(sql):
        # strip all comments from sql
        statement = sqlparse.format(statement, strip_comments=True)
        # trailing comment after semicolon can be confused as its own statement,
        # so we ignore it here.
        if not statement:
            continue

        first_word = sql.strip().split()[0].lower()

        if first_word == "begin":
            raise exceptions.RuntimeError("JupySQL does not support transactions")

        # postgres metacommand
        if first_word.startswith("\\") and is_postgres_or_redshift(conn.dialect):
            result = handle_postgres_special(conn, statement)

        # regular query
        else:
            result = conn.raw_execute(statement, parameters=parameters)

            if (
                config.feedback >= 1
                and hasattr(result, "rowcount")
                and result.rowcount > 0
            ):
                display.message_success(f"{result.rowcount} rows affected.")

    result_set = ResultSet(result, config, statement, conn)
    return select_df_type(result_set, config)


def is_postgres_or_redshift(dialect):
    """Checks if dialect is postgres or redshift"""
    return "postgres" in str(dialect) or "redshift" in str(dialect)


def select_df_type(resultset, config):
    """
    Converts the input resultset to either a Pandas DataFrame
    or Polars DataFrame based on the config settings.
    """
    if config.autopandas:
        return resultset.DataFrame()
    elif config.autopolars:
        return resultset.PolarsDataFrame(**config.polars_dataframe_kwargs)
    else:
        return resultset
