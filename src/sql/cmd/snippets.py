from sql import util
from sql.exceptions import UsageError
from sql.cmd.cmd_utils import CmdParser


def _modify_display_msg(key, remaining_keys, dependent_keys=None):
    """

    Parameters
    ----------
    key : str,
            deleted stored snippet
    remaining_keys: list
            snippets remaining after key is deleted
    dependent_keys: list
            snippets dependent on key

    Returns
    -------
    msg: str
        Formatted message
    """
    msg = f"{key} has been deleted.\n"
    if dependent_keys:
        msg = f"{msg}{', '.join(dependent_keys)} depend on {key}\n"
    if remaining_keys:
        msg = f"{msg}Stored snippets : {', '.join(remaining_keys)}"
    else:
        msg = f"{msg}There are no stored snippets"
    return msg


def snippets(others):
    """
    Implementation of `%sqlcmd snippets`
    This function handles all the arguments related to %sqlcmd snippets, namely
    listing stored snippets, and delete/ force delete/ force delete a snippet and
    all its dependent snippets.


    Parameters
    ----------
    others : str,
        A string containing the command line arguments.

    """
    parser = CmdParser()
    parser.add_argument(
        "-d", "--delete", type=str, help="Delete stored snippet", required=False
    )
    parser.add_argument(
        "-D",
        "--delete-force",
        type=str,
        help="Force delete stored snippet",
        required=False,
    )
    parser.add_argument(
        "-A",
        "--delete-force-all",
        type=str,
        help="Force delete all stored snippets",
        required=False,
    )
    args = parser.parse_args(others)
    SNIPPET_ARGS = [args.delete, args.delete_force, args.delete_force_all]
    if SNIPPET_ARGS.count(None) == len(SNIPPET_ARGS):
        return ", ".join(util.get_all_keys())
    if args.delete:
        deps = util.get_key_dependents(args.delete)
        if deps:
            deps = ", ".join(deps)
            raise UsageError(
                f"The following tables are dependent on {args.delete}: {deps}.\n"
                f"Pass --delete-force to only delete {args.delete}.\n"
                f"Pass --delete-force-all to delete {deps} and {args.delete}"
            )
        else:
            key = args.delete
            remaining_keys = util.del_saved_key(key)
            return _modify_display_msg(key, remaining_keys)

    elif args.delete_force:
        key = args.delete_force
        deps = util.get_key_dependents(key)
        remaining_keys = util.del_saved_key(key)
        return _modify_display_msg(key, remaining_keys, deps)

    elif args.delete_force_all:
        deps = util.get_key_dependents(args.delete_force_all)
        deps.append(args.delete_force_all)
        for key in deps:
            remaining_keys = util.del_saved_key(key)
        return _modify_display_msg(", ".join(deps), remaining_keys)
