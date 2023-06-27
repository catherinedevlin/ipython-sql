from sql import inspect
from sql.util import sanitize_identifier
from sql.cmd.cmd_utils import CmdParser


def columns(others):
    """
    Implementation of `%sqlcmd columns`
    This function takes in a string containing command line arguments,
    parses them to extract the name of the table and the schema, and returns
    a list of columns for the specified table.

    Parameters
    ----------
    others : str,
        A string containing the command line arguments.

    Returns
    -------
    columns: list
        information of the columns in the specified table
    """
    parser = CmdParser()

    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)

    args = parser.parse_args(others)
    return inspect.get_columns(name=sanitize_identifier(args.table), schema=args.schema)
