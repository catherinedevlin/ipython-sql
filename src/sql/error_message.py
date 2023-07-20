ORIGINAL_ERROR = "\nOriginal error message from DB driver:\n"
CTE_MSG = (
    "If using snippets, you may pass the --with argument explicitly.\n"
    "For more details please refer: "
    "https://jupysql.ploomber.io/en/latest/compose.html#with-argument"
)


def _is_syntax_error(error):
    """
    Function to detect whether error message from DB driver
    is related to syntax error in user query.
    """
    error_lower = error.lower()
    return (
        "syntax error" in error_lower
        or ("catalog error" in error_lower and "does not exist" in error_lower)
        or "error in your sql syntax" in error_lower
        or "incorrect syntax" in error_lower
        or "not found" in error_lower
    )


def detail(original_error):
    original_error = str(original_error)
    if _is_syntax_error(original_error):
        return f"{CTE_MSG}\n\n{ORIGINAL_ERROR}{original_error}\n"

    if "fe_sendauth: no password supplied" in original_error:
        specific_error = """\nLooks like you have run into some issues.
            Review our DB connection via URL strings guide:
            https://jupysql.ploomber.io/en/latest/connecting.html .
             Using Ubuntu? Check out this guide: "
            https://help.ubuntu.com/community/PostgreSQL#fe_sendauth:_
            no_password_supplied\n"""

        return f"{specific_error}\n{ORIGINAL_ERROR}{original_error}\n"

    return None
