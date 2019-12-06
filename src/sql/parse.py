from os.path import expandvars
import six
from six.moves import configparser as CP
from sqlalchemy.engine.url import URL
import json


def parse(cell, config, ns={}):
    """Separate input into (connection info, SQL statement)"""

    parts = [part.strip() for part in cell.split(None, 1)]
    if not parts:
        return {'connection': '', 'connect_args': {}, 'sql': '', 'flags': {}}
    parts[0] = expandvars(parts[0])  # for environment variables
    cargs = {}
    if parts[0].startswith('[') and parts[0].endswith(']'):
        section = parts[0].lstrip('[').rstrip(']')
        parser = CP.ConfigParser()
        parser.read(config.dsn_filename)
        cfg_dict = dict(parser.items(section))

        connection = str(URL(**cfg_dict))
        sql = parts[1] if len(parts) > 1 else ''
    elif '@' in parts[0] or '://' in parts[0]:
        sql = ''
        connection = parts[0]
        if len(parts) > 1:
            raw_cargs = parts[1]
            if raw_cargs.startswith(":"):
                name_end = raw_cargs.find(' ')
                ns_name = raw_cargs[1:] if name_end < 0 else raw_cargs[1:name_end]
                cargs = ns[ns_name]
                sql = '' if name_end < 0 else raw_cargs[name_end + 1:]
            elif raw_cargs.startswith('{'):
                try:
                    obj_end = match_bracket(raw_cargs, '{')
                    cargs = json.loads(raw_cargs[:obj_end])
                    sql = raw_cargs[obj_end:].strip()
                except ValueError as e:
                    print("Invalid connect_args: provide a variable refernce with :var or a valid json object")
                    print("WARNING: ignoring connect_args")
                    sql = raw_cargs
            else:
                sql = raw_cargs
    else:
        connection = ''
        sql = cell
    flags, sql = parse_sql_flags(sql.strip())
    return {'connection': connection.strip(),
            'connect_args': cargs,
            'sql': sql,
            'flags': flags}


def parse_sql_flags(sql):
    words = sql.split()
    flags = {
        'persist': False,
        'result_var': None
    }
    if not words:
        return (flags, "")
    num_words = len(words)
    trimmed_sql = sql
    if words[0].lower() == 'persist':
        flags['persist'] = True
        trimmed_sql = " ".join(words[1:])
    elif num_words >= 2 and words[1] == '<<':
        flags['result_var'] = words[0]
        trimmed_sql = " ".join(words[2:])
    return (flags, trimmed_sql.strip())

def match_bracket(s, bracket_type):
    """Gives position of matching bracket + 1 (for convenience) or ValueError if brackets are unbalanced"""
    closing_bracket = '}' if bracket_type == '{' else ']' if bracket_type == "[" else ')'
    stack = []
    for i, c in enumerate(s):
        if c == bracket_type:
            stack.append(i)
        elif c == closing_bracket:
            stack.pop()
            if len(stack) == 0:
                return i + 1
    if len(stack) > 0:
        raise IndexError("Unbalanced brackets")
