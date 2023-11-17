from sql import display
from sql import util
from sql.store import get_all_keys
from sql.exceptions import RuntimeError, TableNotFoundError


ORIGINAL_ERROR = "\nOriginal error message from DB driver:\n"
CTE_MSG = (
    "If using snippets, you may pass the --with argument explicitly.\n"
    "For more details please refer: "
    "https://jupysql.ploomber.io/en/latest/compose.html#with-argument"
)
POSTGRES_MSG = """\nLooks like you have run into some issues.
                Review our DB connection via URL strings guide:
                https://jupysql.ploomber.io/en/latest/connecting.html .
                 Using Ubuntu? Check out this guide: "
                https://help.ubuntu.com/community/PostgreSQL#fe_sendauth:_
                no_password_supplied\n"""


def _snippet_typo_error_message(query):
    """Function to generate message for possible
    snippets if snippet name in user query is a
    typo
    """
    if query:
        tables = util.extract_tables_from_query(query)
        for table in tables:
            suggestions = util.find_close_match(table, get_all_keys())
            err_message = f"There is no table with name {table!r}."
            if len(suggestions) > 0:
                # If snippet is found in suggestions, this snippet
                # must not be misspelled (a different table name is)
                # so we don't show this message.
                if table in suggestions:
                    continue
                suggestions_message = util.get_suggestions_message(suggestions)
                return f"{err_message}{suggestions_message}"
    return ""


def _detailed_message_with_error_type(error, query):
    """Function to generate descriptive error message.
    Currently it handles syntax error messages, table not found messages
    and password issue when connecting to postgres
    """
    original_error = str(error)
    syntax_error_substrings = [
        "syntax error",
        "error in your sql syntax",
        "incorrect syntax",
        "invalid sql",
    ]
    not_found_substrings = [
        r"(\btable with name\b).+(\bdoes not exist\b)",
        r"(\btable\b).+(\bdoes not exist\b)",
        r"(\bobject\b).+(\bdoes not exist\b)",
        r"(\brelation\b).+(\bdoes not exist\b)",
        r"(\btable\b).+(\bdoesn't exist\b)",
        "not found",
        "could not find",
        "no such table",
    ]
    if util.if_substring_exists(original_error.lower(), syntax_error_substrings):
        return f"{CTE_MSG}\n\n{ORIGINAL_ERROR}{original_error}\n", RuntimeError
    elif util.if_substring_exists(original_error.lower(), not_found_substrings):
        typo_err_msg = _snippet_typo_error_message(query)
        if typo_err_msg:
            return (
                f"{CTE_MSG}\n\n{typo_err_msg}\n\n"
                f"{ORIGINAL_ERROR}{original_error}\n",
                TableNotFoundError,
            )
        else:
            return (
                f"{CTE_MSG}\n\n{ORIGINAL_ERROR}{original_error}\n",
                RuntimeError,
            )
    elif "fe_sendauth: no password supplied" in original_error:
        return f"{POSTGRES_MSG}\n{ORIGINAL_ERROR}{original_error}\n", RuntimeError
    return None, None


def _display_error_msg_with_trace(error, message):
    """Displays the detailed error message and prints
    original stack trace as well."""
    if message is not None:
        display.message(message)
    error.modify_exception = True
    raise error


def _raise_error(error, message, error_type):
    """Raise specific error from the detailed message. If detailed
    message is None reraise original error"""
    if message is not None:
        raise error_type(message) from error
    else:
        raise RuntimeError(str(error)) from error


def handle_exception(error, query=None, short_error=True):
    """
    This function is the entry point for detecting error type
    and handling it accordingly.
    """
    if util.is_sqlalchemy_error(error) or util.is_non_sqlalchemy_error(error):
        detailed_message, error_type = _detailed_message_with_error_type(error, query)
        if short_error:
            _raise_error(error, detailed_message, error_type)
        else:
            _display_error_msg_with_trace(error, detailed_message)
    else:
        raise error
