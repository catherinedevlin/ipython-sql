import json
import re

try:
    from ipywidgets import interact
except ModuleNotFoundError:
    interact = None
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

import warnings
import shlex
from difflib import get_close_matches
import sql.connection
import sql.parse
import sql.run
from sql.parse import _option_strings_from_parser
from sql import display, exceptions
from sql.store import store
from sql.command import SQLCommand
from sql.magic_plot import SqlPlotMagic
from sql.magic_cmd import SqlCmdMagic
from sql._patch import patch_ipython_usage_error
from sql import query_util
from sql.util import get_suggestions_message, show_deprecation_warning
from ploomber_core.dependencies import check_installed

from sql.error_message import detail
from traitlets.config.configurable import Configurable
from traitlets import Bool, Int, TraitError, Unicode, Dict, observe, validate


try:
    from pandas.core.frame import DataFrame, Series
except ModuleNotFoundError:
    DataFrame = None
    Series = None

from sql.telemetry import telemetry


SUPPORT_INTERACTIVE_WIDGETS = ["Checkbox", "Text", "IntSlider", ""]


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
            "defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, "
            "RANDOM, SINGLE_BORDER, DOUBLE_BORDER, MARKDOWN )"
        ),
    )
    short_errors = Bool(
        True,
        config=True,
        help="Don't display the full traceback on SQL Programming Error",
    )
    displaylimit = Int(
        sql.run.DEFAULT_DISPLAYLIMIT_VALUE,
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
    polars_dataframe_kwargs = Dict(
        {},
        config=True,
        help=(
            "Polars DataFrame constructor keyword arguments"
            "(e.g. infer_schema_length, nan_to_null, schema_overrides, etc)"
        ),
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

    # To verify displaylimit is valid positive integer
    # If:
    #   None -> We treat it as 0 (no limit)
    #   Positive Integer -> Pass
    #   Negative Integer -> raise Error
    @validate("displaylimit")
    def _valid_displaylimit(self, proposal):
        if proposal["value"] is None:
            print("displaylimit: Value None will be treated as 0 (no limit)")
            return 0
        try:
            value = int(proposal["value"])
            if value < 0:
                raise TraitError(
                    "{}: displaylimit cannot be a negative integer".format(value)
                )
            return value
        except ValueError:
            raise TraitError("{}: displaylimit is not an integer".format(value))

    @observe("autopandas", "autopolars")
    def _mutex_autopandas_autopolars(self, change):
        # When enabling autopandas or autopolars, automatically disable the
        # other one in case it was enabled and print a warning
        if change["new"]:
            other = "autopolars" if change["name"] == "autopandas" else "autopandas"
            if getattr(self, other):
                setattr(self, other, False)
                print(f"Disabled '{other}' since '{change['name']}' was enabled.")

    def check_random_arguments(self, line="", cell=""):
        # check only for cell magic
        if cell != "":
            tokens = shlex.split(line, posix=False)
            arguments = []

            # Iterate through the tokens to separate arguments and SQL code
            # If the token starts with "--", it is an argument
            breakLoop = False
            for token in tokens:
                if token.startswith("--") or token.startswith("-"):
                    arguments.append(token)
                    breakLoop = True
                else:
                    if breakLoop:
                        break

            declared_argument = _option_strings_from_parser(SqlMagic.execute.parser)
            for check_argument in arguments:
                if check_argument not in declared_argument:
                    raise exceptions.UsageError(
                        "Unrecognized argument(s): {}".format(check_argument)
                    )

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
        "-P",
        "--persist-replace",
        action="store_true",
        help="replace the DataFrame if it exists, otherwise perform --persist",
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
    @argument(
        "--interact",
        type=str,
        action="append",
        help="Interactive mode",
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
        return self._execute(
            line=line, cell=cell, local_ns=local_ns, is_interactive_mode=False
        )

    @telemetry.log_call("execute", payload=True)
    @modify_exceptions
    def _execute(self, payload, line, cell, local_ns, is_interactive_mode=False):
        def interactive_execute_wrapper(**kwargs):
            for key, value in kwargs.items():
                local_ns[key] = value
            return self._execute(line, cell, local_ns, is_interactive_mode=True)

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

        self.check_random_arguments(line, cell)

        if local_ns is None:
            local_ns = {}

        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        command = SQLCommand(self, user_ns, line, cell)
        # args.line: contains the line after the magic with all options removed

        args = command.args

        with_ = self._store.infer_dependencies(command.sql_original, args.save)
        if with_:
            command.set_sql_with(with_)
            print(f"Generating CTE with stored snippets : {', '.join(with_)}")
        else:
            with_ = None

        # Create the interactive slider
        if args.interact and not is_interactive_mode:
            check_installed(["ipywidgets"], "--interactive argument")
            interactive_dict = {}
            for key in args.interact:
                interactive_dict[key] = local_ns[key]
            print(
                "Interactive mode, please interact with below "
                "widget(s) to control the variable"
            )
            interact(interactive_execute_wrapper, **interactive_dict)
            return
        if args.connections:
            return sql.connection.Connection.connections_table()
        elif args.close:
            return sql.connection.Connection.close(args.close)

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
        payload["connection_info"] = conn._get_curr_sqlalchemy_connection_info()

        if args.persist_replace and args.append:
            raise exceptions.UsageError(
                """You cannot simultaneously persist and append data to a dataframe;
                  please choose to utilize either one or the other."""
            )
        if args.persist and args.persist_replace:
            warnings.warn("Please use either --persist or --persist-replace")
            return self._persist_dataframe(
                command.sql,
                conn,
                user_ns,
                append=False,
                index=not args.no_index,
                replace=True,
            )
        elif args.persist:
            return self._persist_dataframe(
                command.sql, conn, user_ns, append=False, index=not args.no_index
            )
        elif args.persist_replace:
            return self._persist_dataframe(
                command.sql,
                conn,
                user_ns,
                append=False,
                index=not args.no_index,
                replace=True,
            )
        if args.append:
            return self._persist_dataframe(
                command.sql, conn, user_ns, append=True, index=not args.no_index
            )

        if not command.sql:
            return
        if args.with_:
            show_deprecation_warning()
        # store the query if needed
        if args.save:
            if "-" in args.save:
                warnings.warn(
                    "Using hyphens will be deprecated soon, "
                    "please use "
                    + str(args.save.replace("-", "_"))
                    + " instead for the save argument.",
                    FutureWarning,
                )
            self._store.store(args.save, command.sql_original, with_=with_)

        if args.no_execute:
            display.message("Skipping execution...")
            return

        try:
            result = sql.run.run(conn, command.sql, self)

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
                    if command.return_result_var:
                        return result
                    return None

                # Return results into the default ipython _ variable
                return result

        # JA: added DatabaseError for MySQL
        except (ProgrammingError, OperationalError, DatabaseError) as e:
            # Sqlite apparently return all errors as OperationalError :/
            detailed_msg = detail(e, command.sql)
            if self.short_errors:
                if detailed_msg is not None:
                    err = exceptions.UsageError(detailed_msg)
                    raise err
                    # TODO: move to error_messages.py
                    # Added here due to circular dependency issue (#545)
                elif "no such table" in str(e):
                    tables = query_util.extract_tables_from_query(command.sql)
                    for table in tables:
                        suggestions = get_close_matches(table, list(self._store))
                        if len(suggestions) > 0:
                            err_message = f"There is no table with name {table!r}."
                            suggestions_message = get_suggestions_message(suggestions)
                            raise exceptions.TableNotFoundError(
                                f"{err_message}{suggestions_message}"
                            )
                    print(e)
                else:
                    print(e)
            else:
                if detailed_msg is not None:
                    print(detailed_msg)
                e.modify_exception = True
                raise e

    legal_sql_identifier = re.compile(r"^[A-Za-z0-9#_$]+")

    @modify_exceptions
    def _persist_dataframe(
        self, raw, conn, user_ns, append=False, index=True, replace=False
    ):
        """Implements PERSIST, which writes a DataFrame to the RDBMS"""
        if not DataFrame:
            raise exceptions.MissingPackageError(
                "You must install pandas to persist results: pip install pandas"
            )

        frame_name = raw.strip(";")

        # invalid identifier
        if not frame_name.isidentifier():
            raise exceptions.UsageError(
                f"Expected {frame_name!r} to be a pd.DataFrame but it's"
                " not a valid identifier"
            )

        # missing argument
        if not frame_name:
            raise exceptions.UsageError(
                "Missing argument: %sql --persist <name_of_data_frame>"
            )

        # undefined variable
        if frame_name not in user_ns:
            raise exceptions.UsageError(
                f"Expected {frame_name!r} to be a pd.DataFrame but it's undefined"
            )

        frame = user_ns[frame_name]

        if not isinstance(frame, DataFrame) and not isinstance(frame, Series):
            raise exceptions.TypeError(
                f"{frame_name!r} is not a Pandas DataFrame or Series"
            )

        # Make a suitable name for the resulting database table
        table_name = frame_name.lower()
        table_name = self.legal_sql_identifier.search(table_name).group(0)

        if replace:
            if_exists = "replace"
        elif append:
            if_exists = "append"
        else:
            if_exists = "fail"

        try:
            frame.to_sql(
                table_name, conn.session.engine, if_exists=if_exists, index=index
            )
        except ValueError:
            raise exceptions.ValueError(
                f"""Table {table_name!r} already exists. Consider using \
--persist-replace to drop the table before persisting the data frame"""
            )

        display.message_success(f"Success! Persisted {table_name} to the database.")


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

    patch_ipython_usage_error(ip)
