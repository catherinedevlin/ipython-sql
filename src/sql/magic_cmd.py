import sys
import argparse

from IPython.utils.process import arg_split
from IPython.core.magic import (
    Magics,
    line_magic,
    magics_class
)
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.core.error import UsageError
from sqlglot import select, condition
from sqlalchemy import text

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
    @argument("line", default="", type=str, help="Command name")
    def execute(self, line="", cell=""):
        """
        Command
        """
        split = arg_split(line)
        cmd_name, others = split[0].strip(), split[1:]

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

            if len(result_dict.keys()):
                print(
                    "Test failed. Returned are samples of the failures from your data:"
                )
                return result_dict
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

            report = inspect.get_table_statistics(
                schema=args.schema, name=args.table
            )

            if args.output:
                with open(args.output, "w") as f:
                    f.write(report._repr_html_())

            return report

        raise UsageError(
            f"%sqlcmd has no command: {cmd_name!r}. "
            "Valid commands are: 'tables', 'columns', 'profile'"
        )


def run_each_individually(args, conn):
    base_query = select("*").from_(args.table)
    storage = {}

    if args.greater:
        where = condition(args.column + ">" + args.greater)
        current_query = base_query.where(where).sql()

        res = conn.execute(text(current_query)).fetchone()

        if res is not None:
            storage["greater"] = res
    if args.greater_or_equal:
        where = condition(args.column + ">=" + args.greater_or_equal)

        current_query = base_query.where(where).sql()

        res = conn.execute(text(current_query)).fetchone()
        if res is not None:
            storage["greater_or_equal"] = res
    if args.less_than_or_equal:
        where = condition(args.column + "<=" + args.less_than_or_equal)
        current_query = base_query.where(where).sql()

        res = conn.execute(text(current_query)).fetchone()
        if res is not None:
            storage["less_than_or_equal"] = res
    if args.less_than:
        where = condition(args.column + "<" + args.less_than)
        current_query = base_query.where(where).sql()

        res = conn.execute(text(current_query)).fetchone()
        if res is not None:
            storage["less_than"] = res
    if args.no_nulls:
        where = condition("{} is NULL".format(args.column))
        current_query = base_query.where(where).sql()

        res = conn.execute(text(current_query)).fetchone()
        if res is not None:
            storage["null"] = res

    return storage
