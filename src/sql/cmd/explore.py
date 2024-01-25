from sql.widgets import TableWidget
from sql.cmd.cmd_utils import CmdParser
from sql.util import expand_args, is_rendering_required


def explore(others, user_ns):
    """
    Implementation of `%sqlcmd explore`
    This function takes in a string containing command line arguments,
    parses them to extract the name of the table, and displays an interactive
    widget for exploring the contents of the specified table. It also uses the
    kernel namespace for expanding arguments declared as variables.

    Parameters
    ----------
    others : str,
        A string containing the command line arguments.

    user_ns : dict,
        User namespace of IPython kernel

    """
    parser = CmdParser()
    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)
    args = parser.parse_args(others)
    if is_rendering_required(" ".join(others)):
        expand_args(args, user_ns)

    table_widget = TableWidget(args.table, args.schema)
    return table_widget
