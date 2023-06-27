from sql import inspect
from sql.cmd.cmd_utils import CmdParser


def tables(others):
    """
    Implementation of `%sqlcmd tables`

    This function takes in a string containing command line arguments,
    parses them to extract the schema name, and returns a list of table names
    present in the specified schema or in the default schema if none is specified.

    Parameters
    ----------
    others : str,
            A string containing the command line arguments.

    Returns
    -------
    table_names: list
        list of tables in the schema

    """
    parser = CmdParser()

    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)

    args = parser.parse_args(others)

    return inspect.get_table_names(schema=args.schema)
