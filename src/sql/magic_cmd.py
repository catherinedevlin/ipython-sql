import sys
import argparse

from IPython.utils.process import arg_split
from IPython.core.magic import (
    Magics,
    line_magic,
    magics_class,
)
from IPython.core.magic_arguments import argument, magic_arguments
from IPython.core.error import UsageError


try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable


from sql import inspect
import sql.run
from sql.command import SQLCommand

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
    @argument("within", default="", type=str, help="Command name")
    @argument("table", default="", type=str, help="Command name")
    @argument("column", default="", type=str, help="Command name")
    def execute(self, line="", cell="", local_ns={}):
        """
        Command
        """
        split = arg_split(line)
        cmd_name, others = split[0].strip(), split[1:]

        if cmd_name == "test":
            if local_ns is None:
                local_ns = {}

            user_ns = self.shell.user_ns.copy()
            user_ns.update(local_ns)
            #command = SQLCommand(self, user_ns, line, cell)

        else:
            raise UsageError(
                f"%sqlcmd has no command: {cmd_name!r}. "
                "Valid commands are: 'tables', 'columns'"
            )
        #
