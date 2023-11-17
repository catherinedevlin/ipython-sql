import re
import itertools
import shlex
from os.path import expandvars
from pathlib import Path
import configparser
import warnings

from sqlalchemy.engine.url import URL

from sql import exceptions
from sql.util import check_duplicate_arguments

# Keywords used to identify the beginning of SQL queries
# in split_args_and_sql(). Should cover all cases but can
# be edited to include special keywords.
SQL_COMMANDS = [
    "select",
    "from",
    "with",
    "pivot",
    "create",
    "update",
    "delete",
    "insert",
    "alter",
    "drop",
    "describe",
]


class ConnectionsFile:
    def __init__(self, path_to_file) -> None:
        self.parser = configparser.ConfigParser()
        dsn_file = Path(path_to_file)

        cfg_content = dsn_file.read_text()
        self.parser.read_string(cfg_content)

    def get_default_connection_url(self):
        try:
            section = self.parser.items("default")
        except configparser.NoSectionError:
            return None

        url = URL.create(**dict(section))
        return str(url.render_as_string(hide_password=False))


def connection_str_from_dsn_section(section, config):
    """Return a SQLAlchemy connection string from a section in a DSN file

    Parameters
    ----------
    section : str
        The section name in the DSN file

    config : Config
        The config object, must have a dsn_filename attribute
    """
    parser = configparser.ConfigParser()
    dsn_file = Path(config.dsn_filename)

    try:
        cfg_content = dsn_file.read_text()
    except FileNotFoundError as e:
        raise exceptions.FileNotFoundError(
            f"%config SqlMagic.dsn_filename ({str(config.dsn_filename)!r}) not found."
            " Ensure the file exists or change the configuration: "
            "%config SqlMagic.dsn_filename = 'path/to/file.ini'"
        ) from e

    try:
        parser.read_string(cfg_content)
    except configparser.Error as e:
        raise exceptions.RuntimeError(
            "An error happened when loading "
            "your %config SqlMagic.dsn_filename "
            f"({config.dsn_filename!r})\n{type(e).__name__}: {e}"
        ) from e

    try:
        cfg = parser.items(section)
    except configparser.NoSectionError as e:
        raise exceptions.KeyError(
            f"The section {section!r} does not exist in the "
            f"connections file {config.dsn_filename!r}"
        ) from e

    cfg_dict = dict(cfg)

    try:
        url = URL.create(**cfg_dict)
    except TypeError as e:
        if "unexpected keyword argument" in str(e):
            raise exceptions.TypeError(
                f"%config SqlMagic.dsn_filename ({config.dsn_filename!r}) is invalid. "
                "It must only contain the following keys: drivername, username, "
                "password, host, port, database, query"
            ) from e
        else:
            raise

    return str(url.render_as_string(hide_password=False))


def _connection_string(arg, path_to_file):
    """
    Given a string, return a SQLAlchemy connection string if possible.

    Scenarios:

    - If the string is a valid URL, return it
    - If the string is a valid section in the DSN file return the connection string
    - Otherwise return an empty string

    Parameters
    ----------
    arg : str
        The string to parse

    path_to_file : str
        The path to the DSN file
    """
    # for environment variables
    arg = expandvars(arg)

    # if it's a URL, return it
    if "@" in arg or "://" in arg:
        return arg

    # if it's a section in the DSN file, return the connection string
    if arg.startswith("[") and arg.endswith("]"):
        section = arg.lstrip("[").rstrip("]")
        parser = configparser.ConfigParser()
        parser.read(path_to_file)
        cfg_dict = dict(parser.items(section))
        url = URL.create(**cfg_dict)
        url_ = str(url.render_as_string(hide_password=False))

        warnings.warn(
            "Starting connections with: %sql [section_name] is deprecated "
            "and will be removed in a future release. "
            "Please use: %sql --section section_name instead.",
            category=FutureWarning,
        )

        return url_

    return ""


def parse(arg, path_to_file):
    """Extract connection info and result variable from SQL

    Please don't add any more syntax requiring
    special parsing.
    Instead, add @arguments to SqlMagic.execute.

    We're grandfathering the
    connection string and `<<` operator in.

    Parameters
    ----------
    arg : str
        The string to parse

    path_to_file : str
        The path to the DSN file
    """
    result = {
        "connection": "",
        "sql": "",
        "result_var": None,
        "return_result_var": False,
    }

    pieces = arg.split(None, 1)
    if not pieces:
        return result

    result["connection"] = _connection_string(pieces[0], path_to_file)

    if result["connection"]:
        if len(pieces) == 1:
            return result
        arg = pieces[1]

    pointer = arg.find("<<")
    if pointer != -1:
        left = arg[:pointer].replace(" ", "").replace("\n", "")
        right = arg[pointer + 2 :].strip(" ")

        if "=" in left:
            result["result_var"] = left[:-1]
            result["return_result_var"] = True
        else:
            result["result_var"] = left

        result["sql"] = right
    else:
        result["sql"] = arg
    return result


def _option_strings_from_parser(parser):
    """Extracts the expected option strings (-a, --append, etc) from argparse parser

    Thanks Martijn Pieters
    https://stackoverflow.com/questions/28881456/how-can-i-list-all-registered-arguments-from-an-argumentparser-instance

    :param parser: [description]
    :type parser: IPython.core.magic_arguments.MagicArgumentParser
    """
    opts = [a.option_strings for a in parser._actions]
    return list(itertools.chain.from_iterable(opts))


def without_sql_comment(parser, line):
    """Strips -- comment from a line

    The argparser unfortunately expects -- to precede an option,
    but in SQL that delineates a comment.  So this removes comments
    so a line can safely be fed to the argparser.

    :param line: A line of SQL, possibly mixed with option strings
    :type line: str
    """

    args = _option_strings_from_parser(parser)
    result = itertools.takewhile(
        lambda word: (not word.startswith("--")) or (word in args),
        shlex.split(line, posix=False),
    )
    return " ".join(result)


def split_args_and_sql(line):
    """Separates line into args and sql query

    The argparser expects - to precede an argument, but postgreSQL
    and duckDB allow for -> and ->> to be used as JSON operators.
    This function splits the line into two - args and sql.
    This way we can only pass the args into the argparser, and
    add in the sql later.

    Parameters
    ----------
    line: str
        A line of SQL, preceded by option/argument strings

    Returns
    -------
    arg_line: str
        Portion of input line that contains only arguments

    sql_line: str
        Portion of input line that contains only SQL query/statements
    """
    arg_line, sql_line = line, ""

    # Only separate when json arrow operators used
    # Otherwise, behavior is unchanged
    if not any(op in line for op in ["->", "-->"]):
        return arg_line, sql_line

    # Identify beginning of sql query using keywords
    split_idx = -1
    for token in line.split():
        if token.lower() in SQL_COMMANDS:
            # Found index at which to split line
            split_idx = line.find(token)
            break

    # Split line into args and sql, beginning at sql keyword
    if split_idx != -1:
        arg_line, sql_line = line[:split_idx], line[split_idx:]

    return arg_line, sql_line


def magic_args(magic_execute, line, cmd_from, allowed_duplicates=None):
    """
    Returns the parsed arguments from the line as parsed by magic_execute
    """
    allowed_duplicates = allowed_duplicates or []
    line = without_sql_comment(parser=magic_execute.parser, line=line)
    arg_line, sql_line = split_args_and_sql(line)

    args = shlex.split(arg_line, posix=False)
    if len(args) > 1:
        check_duplicate_arguments(magic_execute, cmd_from, args, allowed_duplicates)
    parsed = magic_execute.parser.parse_args(args)

    if sql_line:
        parsed.line = shlex.split(sql_line, posix=False)
    return parsed


def escape_string_literals_with_colon_prefix(query):
    """
    Given a query, replaces all occurrences of ':variable' with '\:variable' and
    ":variable" with "\:variable" so that the query can be passed to sqlalchemy.text
    without the literals being interpreted as bind parameters. Also calls
    escape_string_slicing_with_colon_prefix(). It doesn't replace
    the occurrences of :variable (without quotes)
    """  # noqa

    # Define the regular expression pattern for valid Python identifiers
    identifier_pattern = r"\b[a-zA-Z_][a-zA-Z0-9_]*\b"

    double_quoted_variable_pattern = r'(?<!\\)":(' + identifier_pattern + r')(?<!\\)"'

    # Define the regular expression pattern for matching ':variable' format
    single_quoted_variable_pattern = r"(?<!\\)':(" + identifier_pattern + r")(?<!\\)\'"

    # Replace ":variable" and ':variable' with "\:variable"
    query_quoted = re.sub(double_quoted_variable_pattern, r'"\\:\1"', query)
    query_quoted = re.sub(single_quoted_variable_pattern, r"'\\:\1'", query_quoted)

    double_found = re.findall(double_quoted_variable_pattern, query)
    single_found = re.findall(single_quoted_variable_pattern, query)

    # Escape occurrences of : for string slicing
    query_quoted, _ = escape_string_slicing_notation(query_quoted)

    return query_quoted, double_found + single_found


def escape_string_slicing_notation(query):
    """
    Given a query, replaces all occurrences of 'example'[x:y] with 'example'[x\:y].
    Escaping the colon using \ ensures correct string slicing behavior rather
    than being interpreted as a bind parameter.

    Parameters
    ----------
    query: str
        query to be parsed and cleaned
    """  # noqa
    identifier_pattern = r"\b[0-9_]*\b"

    # Define the regular expression pattern for matching [x:y]
    string_slicing_pattern = r"(?<!\\):(" + identifier_pattern + r")(?<!\\)\]"

    # Replace [x:y] with [x\:y]
    query_escaped = re.sub(string_slicing_pattern, r"\\:\1]", query)

    occurences_found = re.findall(string_slicing_pattern, query)

    return query_escaped, occurences_found


def find_named_parameters(input_string):
    # Define the regular expression pattern for valid Python identifiers
    identifier_pattern = r"\b[a-zA-Z_][a-zA-Z0-9_]*\b"

    # Define the regular expression pattern for matching :variable format
    variable_pattern = r'(?<!["\'])\:(' + identifier_pattern + ")"

    # Use findall to extract all matches of :variable from the input string
    matches = re.findall(variable_pattern, input_string)

    return matches
