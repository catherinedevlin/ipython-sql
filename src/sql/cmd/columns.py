from sql import inspect
from sql.util import sanitize_identifier
from sql.cmd.cmd_utils import CmdParser
from sql.util import expand_args, is_rendering_required


def columns(others, user_ns):
    """
    Implementation of `%sqlcmd columns`
    This function takes in a string containing command line arguments,
    parses them to extract the name of the table and the schema, and returns
    a list of columns for the specified table. It also uses the kernel
    namespace for expanding arguments declared as variables.

    Parameters
    ----------
    others : str,
        A string containing the command line arguments.

    user_ns : dict,
        User namespace of IPython kernel

    Returns
    -------
    columns: list
        information of the columns in the specified table
    """
    parser = CmdParser()

    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)

    args = parser.parse_args(others)

    if is_rendering_required(" ".join(others)):
        expand_args(args, user_ns)

    return inspect.get_columns(name=sanitize_identifier(args.table), schema=args.schema)
