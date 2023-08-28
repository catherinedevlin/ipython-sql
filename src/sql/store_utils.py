from sql.store import store, get_dependents_for_key
from sql import exceptions


def get_all_keys():
    """
    Function to get list of all stored snippets in the current session
    """
    return list(store)


def get_key_dependents(key: str) -> list:
    """
    Function to find the stored snippets dependent on key
    Parameters
    ----------
    key : str, name of the table

    Returns
    -------
    list
        List of snippets dependent on key

    """
    deps = get_dependents_for_key(store, key)
    return deps


def del_saved_key(key: str) -> str:
    """
    Deletes a stored snippet
    Parameters
    ----------
    key : str, name of the snippet to be deleted

    Returns
    -------
    list
        Remaining stored snippets
    """
    all_keys = get_all_keys()
    if key not in all_keys:
        raise exceptions.UsageError(f"No such saved snippet found : {key}")
    del store[key]
    return get_all_keys()


def is_saved_snippet(table: str) -> bool:
    return table in get_all_keys()
