from sql import inspect
from sql.cmd.cmd_utils import CmdParser
from sql.util import expand_args, is_rendering_required


def tables(others, user_ns):
    """
    Implementation of `%sqlcmd tables`

    This function takes in a string containing command line arguments,
    parses them to extract the schema name, and returns a list of table names
    present in the specified schema or in the default schema if none is specified.
    It also uses the kernel namespace for expanding arguments declared as variables.

    Parameters
    ----------
    others : str,
            A string containing the command line arguments.

    user_ns : dict,
        User namespace of IPython kernel

    Returns
    -------
    table_names: list
        list of tables in the schema

    """
    parser = CmdParser()

    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)

    args = parser.parse_args(others)
    if is_rendering_required(" ".join(others)):
        expand_args(args, user_ns)

    return inspect.get_table_names(schema=args.schema)
