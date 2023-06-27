from sql import inspect
from sql.cmd.cmd_utils import CmdParser


def profile(others):
    """
    Implementation of `%sqlcmd profile`
    This function takes in a string containing command line arguments,
    parses them to extract the name of the table, the schema, and the output location.
    It then retrieves statistical information about the specified table and either
    returns the report or writes it to the specified location.


    Parameters
    ----------
    others : str,
        A string containing the command line arguments.

    Returns
    -------
    report: PrettyTable
        statistics of the table
    """
    parser = CmdParser()
    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)

    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)

    parser.add_argument(
        "-o", "--output", type=str, help="Store report location", required=False
    )

    args = parser.parse_args(others)

    report = inspect.get_table_statistics(schema=args.schema, name=args.table)

    if args.output:
        with open(args.output, "w") as f:
            f.write(report._repr_html_())

    return report
