import re
import itertools
import shlex
from os.path import expandvars

from six.moves import configparser as CP
from sqlalchemy.engine.url import URL
from IPython.core.magic_arguments import parse_argstring


def connection_from_dsn_section(section, config):
    parser = CP.ConfigParser()
    parser.read(config.dsn_filename)
    cfg_dict = dict(parser.items(section))
    return str(URL.create(**cfg_dict).render_as_string(hide_password=False))


def _connection_string(s, config):
    s = expandvars(s)  # for environment variables
    if "@" in s or "://" in s:
        return s
    if s.startswith("[") and s.endswith("]"):
        section = s.lstrip("[").rstrip("]")
        parser = CP.ConfigParser()
        parser.read(config.dsn_filename)
        cfg_dict = dict(parser.items(section))
        return str(URL.create(**cfg_dict))
    return ""


def parse(cell, config):
    """Extract connection info and result variable from SQL

    Please don't add any more syntax requiring
    special parsing.
    Instead, add @arguments to SqlMagic.execute.

    We're grandfathering the
    connection string and `<<` operator in.
    """
    result = {
        "connection": "",
        "sql": "",
        "result_var": None,
        "return_result_var": False,
    }

    pieces = cell.split(None, 1)
    if not pieces:
        return result
    result["connection"] = _connection_string(pieces[0], config)
    if result["connection"]:
        if len(pieces) == 1:
            return result
        cell = pieces[1]

    pointer = cell.find("<<")
    if pointer != -1:
        left = cell[:pointer].replace(" ", "").replace("\n", "")
        right = cell[pointer + 2 :].strip(" ")

        if "=" in left:
            result["result_var"] = left[:-1]
            result["return_result_var"] = True
        else:
            result["result_var"] = left

        result["sql"] = right
    else:
        result["sql"] = cell
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


def magic_args(magic_execute, line):
    line = without_sql_comment(parser=magic_execute.parser, line=line)
    return parse_argstring(magic_execute, line)


def escape_string_literals_with_colon_prefix(query):
    """
    Given a query, replaces all occurrences of ':variable' with '\:variable' and
    ":variable" with "\:variable" so that the query can be passed to sqlalchemy.text
    without the literals being interpreted as bind parameters. It doesn't replace
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

    return query_quoted, double_found + single_found


def find_named_parameters(input_string):
    # Define the regular expression pattern for valid Python identifiers
    identifier_pattern = r"\b[a-zA-Z_][a-zA-Z0-9_]*\b"

    # Define the regular expression pattern for matching :variable format
    variable_pattern = r'(?<!["\'])\:(' + identifier_pattern + ")"

    # Use findall to extract all matches of :variable from the input string
    matches = re.findall(variable_pattern, input_string)

    return matches
