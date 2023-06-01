import sqlglot
import sqlparse

SYNTAX_ERROR = "\nLooks like there is a syntax error in your query."
ORIGINAL_ERROR = "\nOriginal error message from DB driver:\n"


def parse_sqlglot_error(e, q):
    """
    Function to parse the error message from sqlglot

    Parameters
    ----------
    e: sqlglot.errors.ParseError, exception
            while parsing through sqlglot
    q : str, user query

    Returns
    -------
    str
        Formatted error message containing description
        and positions
    """
    err = e.errors
    position = ""
    for item in err:
        position += (
            f"Syntax Error in {q}: {item['description']} at "
            f"Line {item['line']}, Column {item['col']}\n"
        )
    msg = "Possible reason: \n" + position if position else ""
    return msg


def detail(original_error, query=None):
    original_error = str(original_error)
    return_msg = SYNTAX_ERROR
    if "syntax error" in original_error:
        query_list = sqlparse.split(query)
        for q in query_list:
            try:
                q = q.strip()
                q = q[:-1] if q.endswith(";") else q
                parse = sqlglot.transpile(q)
                suggestions = ""
                if q.upper() not in [suggestion.upper() for suggestion in parse]:
                    suggestions += f"Did you mean : {parse}\n"
                return_msg = (
                    return_msg + "Possible reason: \n" + suggestions
                    if suggestions
                    else return_msg
                )

            except sqlglot.errors.ParseError as e:
                parse_msg = parse_sqlglot_error(e, q)
                return_msg = return_msg + parse_msg if parse_msg else return_msg

        return return_msg + "\n" + ORIGINAL_ERROR + original_error + "\n"

    if "fe_sendauth: no password supplied" in original_error:
        return (
            "\nLooks like you have run into some issues. "
            "Review our DB connection via URL strings guide: "
            "https://jupysql.ploomber.io/en/latest/connecting.html ."
            " Using Ubuntu? Check out this guide: "
            "https://help.ubuntu.com/community/PostgreSQL#fe_sendauth:_"
            "no_password_supplied\n" + ORIGINAL_ERROR + original_error + "\n"
        )

    return None
