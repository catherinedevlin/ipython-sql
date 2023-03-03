import json
import re
from ploomber_core.exceptions import modify_exceptions
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_magic,
    magics_class,
    needs_local_scope,
    no_var_expand,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from sqlalchemy.exc import OperationalError, ProgrammingError, DatabaseError

import sql.connection
import sql.parse
import sql.run
from sql.store import store
from sql.command import SQLCommand
from sql.magic_plot import SqlPlotMagic
from sql.magic_cmd import SqlCmdMagic


from traitlets.config.configurable import Configurable
from traitlets import Bool, Int, Unicode, observe

try:
    from pandas.core.frame import DataFrame, Series
except ImportError:
    DataFrame = None
    Series = None

from sql.telemetry import telemetry


@magics_class
class RenderMagic(Magics):
    """
    %sqlrender magic which prints composed queries
    """

    @line_magic
    @magic_arguments()
    # TODO: only accept one arg
    @argument("line", default="", nargs="*", type=str)
    @argument(
        "-w",
        "--with",
        type=str,
        help="Use a saved query",
        action="append",
        dest="with_",
    )
    @telemetry.log_call("sqlrender")
    def sqlrender(self, line):
        args = parse_argstring(self.sqlrender, line)
        return str(store[args.line[0]])


@magics_class
class SqlMagic(Magics, Configurable):
    """Runs SQL statement on a database, specified by SQLAlchemy connect string.

    Provides the %%sql magic."""

    displaycon = Bool(True, config=True, help="Show connection string after execution")
    autolimit = Int(
        0,
        config=True,
        allow_none=True,
        help="Automatically limit the size of the returned result sets",
    )
    style = Unicode(
        "DEFAULT",
        config=True,
        help=(
            "Set the table printing style to any of prettytable's "
            "defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)"
        ),
    )
    short_errors = Bool(
        True,
        config=True,
        help="Don't display the full traceback on SQL Programming Error",
    )
    displaylimit = Int(
        None,
        config=True,
        allow_none=True,
        help=(
            "Automatically limit the number of rows "
            "displayed (full result set is still stored)"
        ),
    )
    autopandas = Bool(
        False,
        config=True,
        help="Return Pandas DataFrames instead of regular result sets",
    )
    autopolars = Bool(
        False,
        config=True,
        help="Return Polars DataFrames instead of regular result sets",
    )
    column_local_vars = Bool(
        False, config=True, help="Return data into local variables from column names"
    )
    feedback = Bool(True, config=True, help="Print number of rows affected by DML")
    dsn_filename = Unicode(
        "odbc.ini",
        config=True,
        help="Path to DSN file. "
        "When the first argument is of the form [section], "
        "a sqlalchemy connection string is formed from the "
        "matching section in the DSN file.",
    )
    autocommit = Bool(True, config=True, help="Set autocommit mode")

    @telemetry.log_call("init")
    def __init__(self, shell):
        self._store = store

        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    @observe("autopandas", "autopolars")
    def _mutex_autopandas_autopolars(self, change):
        # When enabling autopandas or autopolars, automatically disable the
        # other one in case it was enabled and print a warning
        if change["new"]:
            other = "autopolars" if change["name"] == "autopandas" else "autopandas"
            if getattr(self, other):
                setattr(self, other, False)
                print(f"Disabled '{other}' since '{change['name']}' was enabled.")

    @no_var_expand
    @needs_local_scope
    @line_magic("sql")
    @cell_magic("sql")
    @line_magic("jupysql")
    @cell_magic("jupysql")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="sql")
    @argument(
        "-l", "--connections", action="store_true", help="list active connections"
    )
    @argument("-x", "--close", type=str, help="close a session by name")
    @argument(
        "-c", "--creator", type=str, help="specify creator function for new connection"
    )
    @argument(
        "-s",
        "--section",
        type=str,
        help="section of dsn_file to be used for generating a connection string",
    )
    @argument(
        "-p",
        "--persist",
        action="store_true",
        help="create a table name in the database from the named DataFrame",
    )
    @argument(
        "-n",
        "--no-index",
        action="store_true",
        help="Do not store Data Frame index when persisting",
    )
    @argument(
        "--append",
        action="store_true",
        help=(
            "create, or append to, a table name in the database from the "
            "named DataFrame"
        ),
    )
    @argument(
        "-a",
        "--connection_arguments",
        type=str,
        help="specify dictionary of connection arguments to pass to SQL driver",
    )
    @argument("-f", "--file", type=str, help="Run SQL from file at this path")
    @argument("-S", "--save", type=str, help="Save this query for later use")
    @argument(
        "-w",
        "--with",
        type=str,
        help="Use a saved query",
        action="append",
        dest="with_",
    )
    @argument(
        "-N",
        "--no-execute",
        action="store_true",
        help="Do not execute query (use it with --save)",
    )
    @argument(
        "-A",
        "--alias",
        type=str,
        help="Assign an alias to the connection",
    )
    def execute(self, line="", cell="", local_ns=None):
        """
        Runs SQL statement against a database, specified by
        SQLAlchemy connect string.

        If no database connection has been established, first word
        should be a SQLAlchemy connection string, or the user@db name
        of an established connection.

        Examples::

          %%sql postgresql://me:mypw@localhost/mydb
          SELECT * FROM mytable

          %%sql me@mydb
          DELETE FROM mytable

          %%sql
          DROP TABLE mytable

        SQLAlchemy connect string syntax examples:

          postgresql://me:mypw@localhost/mydb
          sqlite://
          mysql+pymysql://me:mypw@localhost/mydb

        """
        return self._execute(line=line, cell=cell, local_ns=local_ns)

    @telemetry.log_call("execute", payload=True)
    def _execute(self, payload, line, cell, local_ns):
        """
        This function implements the cell logic; we create this private
        method so we can control how the function is called. Otherwise,
        decorating ``SqlMagic.execute`` will break when adding the ``@log_call``
        decorator with ``payload=True``
        """
        # line is the text after the magic, cell is the cell's body

        # Examples

        # %sql {line}
        # note that line magic has no body

        # %%sql {line}
        # {cell}

        if local_ns is None:
            local_ns = {}

        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        command = SQLCommand(self, user_ns, line, cell)
        # args.line: contains the line after the magic with all options removed
        args = command.args

        if args.connections:
            return sql.connection.Connection.connections
        elif args.close:
            return sql.connection.Connection._close(args.close)

        connect_arg = command.connection

        if args.section:
            connect_arg = sql.parse.connection_from_dsn_section(args.section, self)

        if args.connection_arguments:
            try:
                # check for string deliniators, we need to strip them for json parse
                raw_args = args.connection_arguments
                if len(raw_args) > 1:
                    targets = ['"', "'"]
                    head = raw_args[0]
                    tail = raw_args[-1]
                    if head in targets and head == tail:
                        raw_args = raw_args[1:-1]
                args.connection_arguments = json.loads(raw_args)
            except Exception as e:
                print(e)
                raise e
        else:
            args.connection_arguments = {}
        if args.creator:
            args.creator = user_ns[args.creator]

        # this creates a new connection or use an existing one
        # depending on the connect_arg value
        conn = sql.connection.Connection.set(
            connect_arg,
            displaycon=self.displaycon,
            connect_args=args.connection_arguments,
            creator=args.creator,
            alias=args.alias,
        )
        payload[
            "connection_info"
        ] = sql.connection.Connection._get_curr_connection_info()
        if args.persist:
            return self._persist_dataframe(
                command.sql, conn, user_ns, append=False, index=not args.no_index
            )

        if args.append:
            return self._persist_dataframe(
                command.sql, conn, user_ns, append=True, index=not args.no_index
            )

        if not command.sql:
            return

        # store the query if needed
        if args.save:
            self._store.store(args.save, command.sql_original, with_=args.with_)

        if args.no_execute:
            print("Skipping execution...")
            return

        try:
            result = sql.run.run(conn, command.sql, self, user_ns)

            if (
                result is not None
                and not isinstance(result, str)
                and self.column_local_vars
            ):
                # Instead of returning values, set variables directly in the
                # users namespace. Variable names given by column names

                if self.autopandas or self.autopolars:
                    keys = result.keys()
                else:
                    keys = result.keys
                    result = result.dict()

                if self.feedback:
                    print(
                        "Returning data to local variables [{}]".format(", ".join(keys))
                    )

                self.shell.user_ns.update(result)

                return None
            else:
                if command.result_var:
                    self.shell.user_ns.update({command.result_var: result})
                    return None

                # Return results into the default ipython _ variable
                return result

        # JA: added DatabaseError for MySQL
        except (ProgrammingError, OperationalError, DatabaseError) as e:
            # Sqlite apparently return all errors as OperationalError :/

            if self.short_errors:
                print(e)
            else:
                raise

    legal_sql_identifier = re.compile(r"^[A-Za-z0-9#_$]+")

    @modify_exceptions
    def _persist_dataframe(self, raw, conn, user_ns, append=False, index=True):
        """Implements PERSIST, which writes a DataFrame to the RDBMS"""
        if not DataFrame:
            raise ImportError("Must `pip install pandas` to use DataFrames")

        frame_name = raw.strip(";")

        # Get the DataFrame from the user namespace
        if not frame_name:
            raise SyntaxError("Syntax: %sql --persist <name_of_data_frame>")
        try:
            frame = eval(frame_name, user_ns)
        except SyntaxError:
            raise SyntaxError("Syntax: %sql --persist <name_of_data_frame>")
        if not isinstance(frame, DataFrame) and not isinstance(frame, Series):
            raise TypeError("%s is not a Pandas DataFrame or Series" % frame_name)

        # Make a suitable name for the resulting database table
        table_name = frame_name.lower()
        table_name = self.legal_sql_identifier.search(table_name).group(0)

        if_exists = "append" if append else "fail"

        frame.to_sql(table_name, conn.session.engine, if_exists=if_exists, index=index)
        return "Persisted %s" % table_name


def load_ipython_extension(ip):
    """Load the extension in IPython."""

    # this fails in both Firefox and Chrome for OS X.
    # I get the error: TypeError: IPython.CodeCell.config_defaults is undefined

    # js = "IPython.CodeCell.config_defaults.highlight_modes['magic_sql'] = {'reg':[/^%%sql/]};" # noqa
    # display_javascript(js, raw=True)
    ip.register_magics(SqlMagic)
    ip.register_magics(RenderMagic)
    ip.register_magics(SqlPlotMagic)
    ip.register_magics(SqlCmdMagic)
