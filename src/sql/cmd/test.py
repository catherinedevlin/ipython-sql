from sql import exceptions
import sql.connection
from sqlglot import select, condition
from prettytable import PrettyTable
from sql.cmd.cmd_utils import CmdParser


def return_test_results(args, conn, query):
    columns = []

    try:
        column_data = conn.execute(query).cursor.description
        res = conn.execute(query).fetchall()
        for column in column_data:
            columns.append(column[0])
        res = [columns, *res]
        return res
    except Exception as e:
        if "column" in str(e):
            raise exceptions.UsageError(
                f"Referenced column '{args.column}' not found!"
            ) from e


def run_each_individually(args, conn):
    if args.schema:
        table_ = f"{args.schema}.{args.table}"
    else:
        table_ = args.table
    base_query = select("*").from_(table_)

    storage = {}

    if args.greater:
        where = condition(args.column + "<=" + args.greater)
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["greater"] = res
    if args.greater_or_equal:
        where = condition(args.column + "<" + args.greater_or_equal)

        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["greater_or_equal"] = res

    if args.less_than_or_equal:
        where = condition(args.column + ">" + args.less_than_or_equal)
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["less_than_or_equal"] = res
    if args.less_than:
        where = condition(args.column + ">=" + args.less_than)
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["less_than"] = res
    if args.no_nulls:
        where = condition("{} is NULL".format(args.column))
        current_query = base_query.where(where).sql()

        res = return_test_results(args, conn, query=current_query)

        if res is not None:
            storage["null"] = res

    return storage


def test(others):
    """
    Implementation of `%sqlcmd test`

    This function takes in a string containing command line arguments,
    parses them to extract the table name, column name, and conditions
    to return if those conditions are satisfied in that table

    Parameters
    ----------
    others : str,
            A string containing the command line arguments.

    Returns
    -------
    result: bool
        Result of the test

    table: PrettyTable
        table with rows because of which the test fails


    """
    parser = CmdParser()

    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="Schema name", required=False)
    parser.add_argument("-c", "--column", type=str, help="Column name", required=False)
    parser.add_argument(
        "-g",
        "--greater",
        type=str,
        help="Greater than a certain number.",
        required=False,
    )
    parser.add_argument(
        "-goe",
        "--greater-or-equal",
        type=str,
        help="Greater or equal than a certain number.",
        required=False,
    )
    parser.add_argument(
        "-l",
        "--less-than",
        type=str,
        help="Less than a certain number.",
        required=False,
    )
    parser.add_argument(
        "-loe",
        "--less-than-or-equal",
        type=str,
        help="Less than or equal to a certain number.",
        required=False,
    )
    parser.add_argument(
        "-nn",
        "--no-nulls",
        help="Returns rows in specified column that are not null.",
        action="store_true",
    )

    args = parser.parse_args(others)

    COMPARATOR_ARGS = [
        args.greater,
        args.greater_or_equal,
        args.less_than,
        args.less_than_or_equal,
    ]

    if args.table and not any(COMPARATOR_ARGS):
        raise exceptions.UsageError("Please use a valid comparator.")

    if args.table and any(COMPARATOR_ARGS) and not args.column:
        raise exceptions.UsageError("Please pass a column to test.")

    if args.greater and args.greater_or_equal:
        return exceptions.UsageError(
            "You cannot use both greater and greater "
            "than or equal to arguments at the same time."
        )
    elif args.less_than and args.less_than_or_equal:
        return exceptions.UsageError(
            "You cannot use both less and less than "
            "or equal to arguments at the same time."
        )

    conn = sql.connection.ConnectionManager.current
    result_dict = run_each_individually(args, conn)

    if any(len(rows) > 1 for rows in list(result_dict.values())):
        for comparator, rows in result_dict.items():
            if len(rows) > 1:
                print(f"\n{comparator}:\n")
                _pretty = PrettyTable()
                _pretty.field_names = rows[0]
                for row in rows[1:]:
                    _pretty.add_row(row)
                print(_pretty)
        raise exceptions.UsageError(
            "The above values do not match your test requirements."
        )
    else:
        return True
