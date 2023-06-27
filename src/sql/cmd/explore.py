from sql.widgets import TableWidget
from IPython.display import display
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
    args = parser.parse_args(others)

    table_widget = TableWidget(args.table)
    display(table_widget)
