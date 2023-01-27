from IPython.core.magic import (
    Magics,
    line_magic,
    magics_class,
)
from IPython.core.magic_arguments import argument, magic_arguments


try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable


from sql.command import SQLCmdCommand
from sql import inspect


@magics_class
class SqlCmdMagic(Magics, Configurable):
    """%sqlcmd magic"""

    @line_magic("sqlcmd")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Command name")
    @argument("-t", "--table", type=str, help="Table name", required=False)
    @argument("-s", "--schema", type=str, help="Schema name", required=False)
    def execute(self, line="", cell="", local_ns=None):
        """
        Command
        """
        cmd = SQLCmdCommand(self, line)

        if cmd.args.line[0].strip() == "tables":
            return inspect.get_table_names()
        elif cmd.args.line[0].strip() == "columns":
            return inspect.get_columns(name=cmd.args.table, schema=cmd.args.schema)
        else:
            raise ValueError(f"Unknown command: {cmd.args.line[0].strip()}")
