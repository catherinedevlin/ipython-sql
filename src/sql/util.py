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
import re


try:
    import toml
except ModuleNotFoundError:
    toml = None

SINGLE_QUOTE = "'"
DOUBLE_QUOTE = '"'

CONFIGURATION_DOCS_STR = "https://jupysql.ploomber.io/en/latest/api/configuration.html#loading-from-a-file"  # noqa


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


def check_duplicate_arguments(
    magic_execute, cmd_from, args, allowed_duplicates=None, disallowed_aliases=None
) -> bool:
    """
    Raises UsageError when duplicate arguments are passed to magics.
    Returns true if no duplicates in arguments or aliases.

    Parameters
    ----------
    magic_execute
        The execute method of the magic class.
    cmd_from
        Which magic class invoked this function. One of 'sql', 'sqlplot' or 'sqlcmd'.
    args
        The arguments passed to the magic command.
    allowed_duplicates
        The duplicate arguments that are allowed for the class which invoked this
        function. Defaults to None.
    disallowed_aliases
        The aliases for the arguments that are not allowed to be used together
        for the class that invokes this function. Defaults to None.

    Returns
    -------
    boolean
        When there are no duplicates, a True bool is returned.
    """
    allowed_duplicates = allowed_duplicates or []
    disallowed_aliases = disallowed_aliases or {}

    aliased_arguments = {}
    unaliased_arguments = []

    # Separates the aliased_arguments and unaliased_arguments.
    # Aliased arguments example: '-w' and '--with'
    if cmd_from != "sqlcmd":
        for decorator in magic_execute.decorators:
            decorator_args = decorator.args
            if len(decorator_args) > 1:
                aliased_arguments[decorator_args[0]] = decorator_args[1]
            else:
                if decorator_args[0].startswith("--") or decorator_args[0].startswith(
                    "-"
                ):
                    unaliased_arguments.append(decorator_args[0])

    if aliased_arguments == {}:
        aliased_arguments = disallowed_aliases

    # Separate arguments from passed options
    args = [arg for arg in args if arg.startswith("--") or arg.startswith("-")]

    # Separate single and double hyphen arguments
    # Using sets here for better performance of looking up hash tables
    single_hyphen_opts = set()
    double_hyphen_opts = set()

    for arg in args:
        if arg.startswith("--"):
            double_hyphen_opts.add(arg)
        elif arg.startswith("-"):
            single_hyphen_opts.add(arg)

    # Get duplicate arguments
    duplicate_args = []
    visited_args = set()
    for arg in args:
        if arg not in allowed_duplicates:
            if arg not in visited_args:
                visited_args.add(arg)
            else:
                duplicate_args.append(arg)

    # Check if alias pairs are present and track the pair for the error message
    # Example: would filter out `-w` and `--with` if both are present
    alias_pairs_present = [
        (opt, aliased_arguments[opt])
        for opt in single_hyphen_opts
        if opt in aliased_arguments
        if aliased_arguments[opt] in double_hyphen_opts
    ]

    # Generate error message based on presence of duplicates and
    # aliased arguments
    error_message = ""
    if duplicate_args:
        duplicates_error = (
            f"Duplicate arguments in %{cmd_from}. "
            "Please use only one of each of the following: "
            f"{', '.join(sorted(duplicate_args))}. "
        )
    else:
        duplicates_error = ""

    if alias_pairs_present:
        arg_list = sorted([" or ".join(pair) for pair in alias_pairs_present])
        alias_error = (
            f"Duplicate aliases for arguments in %{cmd_from}. "
            "Please use either one of "
            f"{', '.join(arg_list)}."
        )
    else:
        alias_error = ""

    error_message = f"{duplicates_error}{alias_error}"

    # If there is an error message to be raised, raise it
    if error_message:
        raise exceptions.UsageError(error_message)

    return True


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

    return Path(current, file_name)


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


def get_user_configs(primary_path, alternate_path):
    """
    Returns saved configuration settings in a toml file from given file_path

    Parameters
    ----------
    primary_path : Path
        file path to toml in project directory
    alternate_path : Path
        file path to ~/.jupysql/config

    Returns
    -------
    dict
        saved configuration settings
    Path
        the path of the file used to get user configurations
    """
    data = None
    display_tip = True  # Set to true if tip is to be displayed
    configuration_docs_displayed = False  # To disable showing guidelines once shown

    # Look for user configurations in pyproject.toml and ~/.jupysql/config
    # in that particular order
    path_list = [primary_path, alternate_path]
    for file_path in path_list:
        section_to_find = None
        section_found = False
        if file_path and file_path.exists():
            data = load_toml(file_path)
            section_names = ["tool", "jupysql", "SqlMagic"]

            # Look for SqlMagic section in toml file
            while section_names:
                section_found = False
                section_to_find, sections_from_user = section_names.pop(0), data.keys()

                if section_to_find not in sections_from_user:
                    close_match = difflib.get_close_matches(
                        section_to_find, sections_from_user
                    )

                    if not close_match:
                        if display_tip:
                            display.message(
                                f"Tip: You may define configurations in {primary_path}"
                                f" or {alternate_path}. "
                            )
                            display_tip = False
                        break
                    else:
                        raise exceptions.ConfigurationError(
                            f"{pretty_print(close_match)} is an invalid section "
                            f"name in {file_path}. "
                            f"Did you mean '{section_to_find}'?"
                        )

                section_found = True
                data = data[section_to_find]

        if section_to_find == "SqlMagic" and section_found and not data:
            display.message(
                f"[tool.jupysql.SqlMagic] present in {file_path} but empty. "
            )
            display_tip = False

        if not display_tip and not configuration_docs_displayed:
            display.message_html(
                f"Please review our <a href='{CONFIGURATION_DOCS_STR}'>"
                "configuration guideline</a>."
            )
            configuration_docs_displayed = True

        if not data and not section_found and file_path and file_path.exists():
            display.message(f"Did not find user configurations in {file_path}.")
        elif section_found and data:
            return data, file_path

    return data, None


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
        "Catalog Error",
        "Parser Error",
        "pyodbc.ProgrammingError",
        # Clickhouse errors
        "DB::Exception:",
    ]
    return any(msg in str(error) for msg in specific_db_errors)


def if_substring_exists(string, substrings):
    """Function to check if any of substring in
    substrings exist in string"""
    return any((msg in string) or (re.search(msg, string)) for msg in substrings)


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
