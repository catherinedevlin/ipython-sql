from sqlglot import parse_one, exp
from sqlglot.errors import ParseError


def extract_tables_from_query(query):
    """
    Function to extract names of tables from
    a syntactically correct query

    Parameters
    ----------
    query : str, user query

    Returns
    -------
    list
        List of tables in the query
        [] if error in parsing the query
    """
    try:
        tables = [table.name for table in parse_one(query).find_all(exp.Table)]
        return tables
    except ParseError:
        # TODO : Instead of returning [] replace with call to
        # error_messages.py::parse_sqlglot_error. Currently this
        # is not possible because of an exception raised in test
        # fixtures. (#546). This function can also be moved to util.py
        # after #545 is resolved.
        return []
