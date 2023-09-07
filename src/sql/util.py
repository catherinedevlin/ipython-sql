import warnings
import difflib
from sql import exceptions, display
import json
from pathlib import Path
from sqlglot import parse_one, exp
from sqlglot.errors import ParseError
from sqlalchemy.exc import SQLAlchemyError
from ploomber_core.dependencies import requires
import ast
from os.path import isfile


try:
    import toml
except ModuleNotFoundError:
    toml = None

SINGLE_QUOTE = "'"
DOUBLE_QUOTE = '"'

CONFIGURATION_DOCS_STR = "https://jupysql.ploomber.io/en/latest/api/configuration.html#loading-from-pyproject-toml"  # noqa


def sanitize_identifier(identifier):
    if (identifier[0] == SINGLE_QUOTE and identifier[-1] == SINGLE_QUOTE) or (
        identifier[0] == DOUBLE_QUOTE and identifier[-1] == DOUBLE_QUOTE
    ):
        return identifier[1:-1]
    else:
        return identifier


def convert_to_scientific(value):
    """
    Converts value to scientific notation if necessary

    Parameters
    ----------
    value : any
        Value to format.
    """
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and _is_long_number(value)
    ):
        new_value = "{:,.3e}".format(value)

    else:
        new_value = value

    return new_value


def _is_long_number(num) -> bool:
    """
    Checks if num's digits > 10
    """
    if "." in str(num):
        split_by_decimal = str(num).split(".")
        if len(split_by_decimal[0]) > 10 or len(split_by_decimal[1]) > 10:
            return True
    return False


def get_suggestions_message(suggestions):
    suggestions_message = ""
    if len(suggestions) > 0:
        _suggestions_string = pretty_print(suggestions, last_delimiter="or")
        suggestions_message = f"\nDid you mean: {_suggestions_string}"
    return suggestions_message


def pretty_print(
    obj: list, delimiter: str = ",", last_delimiter: str = "and", repr_: bool = False
) -> str:
    """
    Returns a formatted string representation of an array
    """
    if repr_:
        sorted_ = sorted(repr(element) for element in obj)
    else:
        sorted_ = sorted(f"'{element}'" for element in obj)

    if len(sorted_) > 1:
        sorted_[-1] = f"{last_delimiter} {sorted_[-1]}"

    return f"{delimiter} ".join(sorted_)


def strip_multiple_chars(string: str, chars: str) -> str:
    """
    Trims characters from the start and end of the string
    """
    return string.translate(str.maketrans("", "", chars))


def flatten(src, ltypes=(list, tuple)):
    """The flatten function creates a new tuple / list
    with all sub-tuple / sub-list elements concatenated into it recursively

    Parameters
    ----------
    src : tuple / list
        Source tuple / list with all sub-tuple / sub-list elements
    ltypes : tuple, optional
        sub element's data type, by default (list, tuple)

    Returns
    -------
    tuple / list
        Flatten tuple / list
    """
    ltype = type(src)
    # Create a process list to handle flatten elements
    process_list = list(src)
    i = 0
    while i < len(process_list):
        while isinstance(process_list[i], ltypes):
            if not process_list[i]:
                process_list.pop(i)
                i -= 1
                break
            else:
                process_list[i : i + 1] = process_list[i]
        i += 1

    # If input src data type is tuple, return tuple
    if not isinstance(process_list, ltype):
        return tuple(process_list)
    return process_list


def parse_sql_results_to_json(rows, columns) -> str:
    """
    Serializes sql rows to a JSON formatted ``str``
    """
    dicts = [dict(zip(list(columns), row)) for row in rows]
    rows_json = json.dumps(dicts, indent=4, sort_keys=True, default=str).replace(
        "null", '"None"'
    )

    return rows_json


def show_deprecation_warning():
    """
    Raises CTE deprecation warning
    """
    warnings.warn(
        "CTE dependencies are now automatically inferred, "
        "you can omit the --with arguments. Using --with will "
        "raise an exception in the next major release so please remove it.",
        FutureWarning,
    )


def find_path_from_root(file_name):
    """
    Recursively finds an absolute path to file_name starting
    from current to root directory
    """
    current = Path().resolve()
    while not (current / file_name).exists():
        if current == current.parent:
            return None

        current = current.parent

    return str(Path(current, file_name))


def find_close_match(word, possibilities):
    """Find closest match between invalid input and possible options"""
    return difflib.get_close_matches(word, possibilities)


def find_close_match_config(word, possibilities, n=3):
    """Finds closest matching configurations and displays message"""
    closest_matches = difflib.get_close_matches(word, possibilities, n=n)
    if not closest_matches:
        display.message_html(
            f"'{word}' is an invalid configuration. Please review our "
            "<a href='https://jupysql.ploomber.io/en/latest/api/configuration.html#options'>"  # noqa
            "configuration guideline</a>."
        )
    else:
        display.message(
            f"'{word}' is an invalid configuration. Did you mean "
            f"{pretty_print(closest_matches, last_delimiter='or')}?"
        )


def get_line_content_from_toml(file_path, line_number):
    """
    Locates a line that error occurs when loading a toml file
    and returns the line, key, and value
    """
    with open(file_path, "r") as file:
        lines = file.readlines()
        eline = lines[line_number - 1].strip()
        ekey, evalue = None, None
        if "=" in eline:
            ekey, evalue = map(str.strip, eline.split("="))
        return eline, ekey, evalue


def to_upper_if_snowflake_conn(conn, upper):
    return (
        upper.upper()
        if callable(conn._get_sqlglot_dialect)
        and conn._get_sqlglot_dialect() == "snowflake"
        else upper
    )


@requires(["toml"])
def load_toml(file_path):
    """
    Returns toml file content in a dictionary format
    and raises error if it fails to load the toml file
    """
    try:
        with open(file_path, "r") as file:
            content = file.read()
            return toml.loads(content)
    except toml.TomlDecodeError as e:
        raise parse_toml_error(e, file_path)


def parse_toml_error(e, file_path):
    eline, ekey, evalue = get_line_content_from_toml(file_path, e.lineno)
    if "Duplicate keys!" in str(e):
        return exceptions.ConfigurationError(
            f"Duplicate key found: '{ekey}' in {file_path}"
        )
    elif "Only all lowercase booleans" in str(e):
        return exceptions.ConfigurationError(
            f"Invalid value '{evalue}' in '{eline}' in {file_path}. "
            "Valid boolean values: true, false"
        )
    elif "invalid literal for int()" in str(e):
        return exceptions.ConfigurationError(
            f"Invalid value '{evalue}' in '{eline}' in {file_path}. "
            "To use str value, enclose it with ' or \"."
        )
    else:
        return e


def get_user_configs(file_path):
    """
    Returns saved configuration settings in a toml file from given file_path

    Parameters
    ----------
    file_path : str
        file path to a toml file

    Returns
    -------
    dict
        saved configuration settings
    """
    data = load_toml(file_path)
    section_names = ["tool", "jupysql", "SqlMagic"]
    while section_names:
        section_to_find, sections_from_user = section_names.pop(0), data.keys()
        if section_to_find not in sections_from_user:
            close_match = difflib.get_close_matches(section_to_find, sections_from_user)
            if not close_match:
                MESSAGE_PREFIX = (
                    f"Tip: You may define configurations in "
                    f"{file_path}. Please review our "
                )
                display.message_html(
                    f"{MESSAGE_PREFIX}<a href='{CONFIGURATION_DOCS_STR}'>"
                    "configuration guideline</a>."
                )
                return {}
            else:
                raise exceptions.ConfigurationError(
                    f"{pretty_print(close_match)} is an invalid section "
                    f"name in {file_path}. "
                    f"Did you mean '{section_to_find}'?"
                )
        data = data[section_to_find]
    if not data:
        if section_to_find == "SqlMagic":
            MESSAGE_PREFIX = (
                f"[tool.jupysql.SqlMagic] present in {file_path} but empty. "
                f"Please review our "
            )
            display.message_html(
                f"{MESSAGE_PREFIX}<a href='{CONFIGURATION_DOCS_STR}'>"
                "configuration guideline</a>."
            )
    else:
        display.message(f"Loading configurations from {file_path}")
    return data


def get_default_configs(sql):
    """
    Returns a dictionary of SqlMagic configuration settings users can set
    with their default values.
    """
    default_configs = sql.trait_defaults()
    del default_configs["parent"]
    del default_configs["config"]
    return default_configs


def _are_numeric_values(*values):
    return all([isinstance(value, (int, float)) for value in values])


def validate_mutually_exclusive_args(arg_names, args):
    """
    Raises ValueError if a list of values from arg_names filtered by
    args' boolean representations is longer than one.

    Parameters
    ----------
    arg_names : list
        args' names in string
    args : list
        args values
    """
    specified_args = [arg_name for arg_name, arg in zip(arg_names, args) if arg]
    if len(specified_args) > 1:
        raise exceptions.ValueError(
            f"{pretty_print(specified_args)} are specified. "
            "You can only specify one of them."
        )


def validate_nonidentifier_connection(arg):
    """
    Raises UsageError if a connection is passed to `%sql/%%sql` through
    object property, list, or dictionary.

    Parameters
    ----------
    arg : str
        argument to check whether it is a valid connection or not
    """
    if not arg.isidentifier() and is_valid_python_code(arg) and not arg.endswith(";"):
        raise exceptions.UsageError(
            f"'{arg}' is not a valid connection identifier. "
            "Please pass the variable's name directly, as passing "
            "object attributes, dictionaries or lists won't work."
        )


def is_valid_python_code(code):
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


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
        # TODO : Instead of returning [] return the
        # exact parse error
        return []


def is_sqlalchemy_error(error):
    """Function to check if error is SQLAlchemy error"""
    return isinstance(error, SQLAlchemyError)


def is_non_sqlalchemy_error(error):
    """Function to check if error is a specific non-SQLAlchemy error"""
    specific_db_errors = [
        "duckdb.CatalogException",
        "Parser Error",
        "pyodbc.ProgrammingError",
    ]
    return any(msg in str(error) for msg in specific_db_errors)


def if_substring_exists(string, substrings):
    """Function to check if any of substring in
    substrings exist in string"""
    return any(msg in string for msg in substrings)


def enclose_table_with_double_quotations(table, conn):
    """
    Function to enclose a file path, schema name,
    or table name with double quotations
    """
    if isfile(table):
        _table = f'"{table}"'
    elif "." in table and not table.startswith('"'):
        parts = table.split(".")
        _table = f'"{parts[0]}"."{parts[1]}"'
    else:
        _table = table

    use_backticks = conn.is_use_backtick_template()
    if use_backticks:
        _table = _table.replace('"', "`")

    return _table
