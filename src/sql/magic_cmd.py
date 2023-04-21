import sys
import argparse

from IPython.utils.process import arg_split
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.core.error import UsageError
from sqlglot import select, condition
from sqlalchemy import text
from sql import util

from prettytable import PrettyTable

try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable

import sql.connection
from sql import inspect
import sql.run


class CmdParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        if message:
            self._print_message(message, sys.stderr)

    def error(self, message):
        raise UsageError(message)


@magics_class
class SqlCmdMagic(Magics, Configurable):
    """%sqlcmd magic"""

    @line_magic("sqlcmd")
    @magic_arguments()
    @argument("line", type=str, help="Command name")
    def _validate_execute_inputs(self, line):
        """
        Function to validate %sqlcmd inputs.
        Raises UsageError in case of an invalid input, executes command otherwise.
        """

        # We relly on SQLAlchemy when inspecting tables
        util.support_only_sql_alchemy_connection("%sqlcmd")

        AVAILABLE_SQLCMD_COMMANDS = ["tables", "columns", "test", "profile"]

        if line == "":
            raise UsageError(
                "Missing argument for %sqlcmd. "
                "Valid commands are: {}".format(", ".join(AVAILABLE_SQLCMD_COMMANDS))
            )
        else:
            split = arg_split(line)
            command, others = split[0].strip(), split[1:]

            if command in AVAILABLE_SQLCMD_COMMANDS:
                return self.execute(command, others)
            else:
                raise UsageError(
                    f"%sqlcmd has no command: {command!r}. "
                    "Valid commands are: {}".format(
                        ", ".join(AVAILABLE_SQLCMD_COMMANDS)
                    )
                )

    @argument("cmd_name", default="", type=str, help="Command name")
    @argument("others", default="", type=str, help="Other tags")
    def execute(self, cmd_name="", others="", cell="", local_ns=None):
        """
        Command
        """
        if cmd_name == "tables":
            parser = CmdParser()

            parser.add_argument(
                "-s", "--schema", type=str, help="Schema name", required=False
            )

            args = parser.parse_args(others)

            return inspect.get_table_names(schema=args.schema)
        elif cmd_name == "columns":
            parser = CmdParser()

            parser.add_argument(
                "-t", "--table", type=str, help="Table name", required=True
            )
            parser.add_argument(
                "-s", "--schema", type=str, help="Schema name", required=False
            )

            args = parser.parse_args(others)
            return inspect.get_columns(name=args.table, schema=args.schema)
        elif cmd_name == "test":
            parser = CmdParser()

            parser.add_argument(
                "-t", "--table", type=str, help="Table name", required=True
            )
            parser.add_argument(
                "-c", "--column", type=str, help="Column name", required=False
            )
            parser.add_argument(
                "-g",
                "--greater",
                type=str,
                help="Greater than a certain number.",
                required=False,
            )
            parser.add_argument(
                "-goe",
                "--greater-or-equal",
                type=str,
                help="Greater or equal than a certain number.",
                required=False,
            )
            parser.add_argument(
                "-l",
                "--less-than",
                type=str,
                help="Less than a certain number.",
                required=False,
            )
            parser.add_argument(
                "-loe",
                "--less-than-or-equal",
                type=str,
                help="Less than or equal to a certain number.",
                required=False,
            )
            parser.add_argument(
                "-nn",
                "--no-nulls",
                help="Returns rows in specified column that are not null.",
                action="store_true",
            )

            args = parser.parse_args(others)

            COMPARATOR_ARGS = [
                args.greater,
                args.greater_or_equal,
                args.less_than,
                args.less_than_or_equal,
            ]

            if args.table and not any(COMPARATOR_ARGS):
                raise UsageError("Please use a valid comparator.")

            if args.table and any(COMPARATOR_ARGS) and not args.column:
                raise UsageError("Please pass a column to test.")

            if args.greater and args.greater_or_equal:
                return ValueError(
                    "You cannot use both greater and greater "
                    "than or equal to arguments at the same time."
                )
            elif args.less_than and args.less_than_or_equal:
                return ValueError(
                    "You cannot use both less and less than "
                    "or equal to arguments at the same time."
                )

            conn = sql.connection.Connection.current.session
            result_dict = run_each_individually(args, conn)

            if any(len(rows) > 1 for rows in list(result_dict.values())):
                for comparator, rows in result_dict.items():
                    if len(rows) > 1:
                        print(f"\n{comparator}:\n")
                        _pretty = PrettyTable()
                        _pretty.field_names = rows[0]
                        for row in rows[1:]:
                            _pretty.add_row(row)
                        print(_pretty)
                raise UsageError(
                    "The above values do not not match your test requirements."
                )
            else:
                return True

        elif cmd_name == "profile":
            parser = CmdParser()
            parser.add_argument(
                "-t", "--table", type=str, help="Table name", required=True
            )

            parser.add_argument(
                "-s", "--schema", type=str, help="Schema name", required=False
            )

            parser.add_argument(
                "-o", "--output", type=str, help="Store report location", required=False
            )

            args = parser.parse_args(others)

            report = inspect.get_table_statistics(schema=args.schema, name=args.table)

            if args.output:
                with open(args.output, "w") as f:
                    f.write(report._repr_html_())

            return report


def return_test_results(args, conn, query):
    try:
        columns = []
        column_data = conn.execute(text(query)).cursor.description
        res = conn.execute(text(query)).fetchall()
        for column in column_data:
            columns.append(column[0])
        res = [columns, *res]
        return res
    except Exception as e:
        if "column" in str(e):
            raise UsageError(f"Referenced column '{args.column}' not found!")


def run_each_individually(args, conn):
    base_query = select("*").from_(args.table)

    storage = {}

    if args.greater:
        where = condition(args.column + "<=" + args.greater)
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["greater"] = res
    if args.greater_or_equal:
        where = condition(args.column + "<" + args.greater_or_equal)

        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["greater_or_equal"] = res

    if args.less_than_or_equal:
        where = condition(args.column + ">" + args.less_than_or_equal)
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["less_than_or_equal"] = res
    if args.less_than:
        where = condition(args.column + ">=" + args.less_than)
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["less_than"] = res
    if args.no_nulls:
        where = condition("{} is NULL".format(args.column))
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["null"] = res

    return storage
