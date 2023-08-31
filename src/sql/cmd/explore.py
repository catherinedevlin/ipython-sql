from sql.widgets import TableWidget
from sql.cmd.cmd_utils import CmdParser


def explore(others):
    """
    Implementation of `%sqlcmd explore`
    This function takes in a string containing command line arguments,
    parses them to extract the name of the table, and displays an interactive
    widget for exploring the contents of the specified table.

    Parameters
    ----------
    others : str,
        A string containing the command line arguments.

    """
    parser = CmdParser()
    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)
    args = parser.parse_args(others)
    table_widget = TableWidget(args.table, args.schema)
    return table_widget
