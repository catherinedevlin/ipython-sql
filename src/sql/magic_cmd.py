import sys
import argparse
import shlex

from IPython.core.magic import Magics, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments
from sql.inspect import support_only_sql_alchemy_connection
from sql.cmd.tables import tables
from sql.cmd.columns import columns
from sql.cmd.test import test
from sql.cmd.profile import profile
from sql.cmd.explore import explore
from sql.cmd.snippets import snippets
from sql.cmd.connect import connect
from sql.connection import ConnectionManager
from sql.util import check_duplicate_arguments

try:
    from traitlets.config.configurable import Configurable
except ModuleNotFoundError:
    from IPython.config.configurable import Configurable
from sql import exceptions


class CmdParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        if message:
            self._print_message(message, sys.stderr)

    def error(self, message):
        raise exceptions.UsageError(message)


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

        # We rely on SQLAlchemy when inspecting tables

        AVAILABLE_SQLCMD_COMMANDS = [
            "tables",
            "columns",
            "test",
            "profile",
            "explore",
            "snippets",
            "connect",
        ]
        COMMANDS_CONNECTION_REQUIRED = [
            "tables",
            "columns",
            "test",
            "profile",
            "explore",
        ]
        COMMANDS_SQLALCHEMY_ONLY = ["tables", "columns", "test", "explore"]

        VALID_COMMANDS_MSG = (
            f"Missing argument for %sqlcmd. "
            f"Valid commands are: {', '.join(AVAILABLE_SQLCMD_COMMANDS)}"
        )

        if line == "":
            raise exceptions.UsageError(VALID_COMMANDS_MSG)
        else:
            # directly use shlex since SqlCmdMagic does not use magic_args from parse.py
            split = shlex.split(line, posix=False)
            command, others = split[0].strip(), split[1:]
            if others:
                check_duplicate_arguments(
                    self.execute,
                    "sqlcmd",
                    split,
                    disallowed_aliases={
                        "-t": "--table",
                        "-s": "--schema",
                        "-o": "--output",
                    },
                )

            if command in AVAILABLE_SQLCMD_COMMANDS:
                if (
                    command in COMMANDS_CONNECTION_REQUIRED
                    and not ConnectionManager.current
                ):
                    raise exceptions.RuntimeError(
                        f"Cannot execute %sqlcmd {command} because there "
                        "is no active connection. Connect to a database "
                        "and try again."
                    )

                if command in COMMANDS_SQLALCHEMY_ONLY:
                    support_only_sql_alchemy_connection(f"%sqlcmd {command}")

                return self.execute(command, others)
            else:
                raise exceptions.UsageError(
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

        router = {
            "tables": tables,
            "columns": columns,
            "test": test,
            "profile": profile,
            "explore": explore,
            "snippets": snippets,
            "connect": connect,
        }

        cmd = router.get(cmd_name)
        if cmd:
            return cmd(others)
