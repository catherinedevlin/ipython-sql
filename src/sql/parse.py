import json
import re
from os.path import expandvars

import six
from six.moves import configparser as CP
from sqlalchemy.engine.url import URL


def connection_from_dsn_section(section, config):
    parser = CP.ConfigParser()
    parser.read(config.dsn_filename)
    cfg_dict = dict(parser.items(section))
    return str(URL(**cfg_dict))


def _connection_string(s):

    s = expandvars(s)  # for environment variables
    if "@" in s or "://" in s:
        return s
    if s.startswith("[") and s.endswith("]"):
        section = s.lstrip("[").rstrip("]")
        parser = CP.ConfigParser()
        parser.read(config.dsn_filename)
        cfg_dict = dict(parser.items(section))
        return str(URL(**cfg_dict))
    return ""


def parse(cell, config):
    """Extract connection info and result variable from SQL
    
    Please don't add any more syntax requiring 
    special parsing.  
    Instead, add @arguments to SqlMagic.execute.
    
    We're grandfathering the 
    connection string and `<<` operator in.
    """

    result = {"connection": "", "sql": "", "result_var": None}

    pieces = cell.split(None, 3)
    if not pieces:
        return result
    result["connection"] = _connection_string(pieces[0])
    if result["connection"]:
        pieces.pop(0)
    if len(pieces) > 1 and pieces[1] == "<<":
        result["result_var"] = pieces.pop(0)
        pieces.pop(0)  # discard << operator
    result["sql"] = (" ".join(pieces)).strip()
    return result


# def parse_sql_flags(sql):
#     words = sql.split()
#     flags = {
#         'persist': False,
#         'result_var': None
#     }
#     if not words:
#         return (flags, "")
#     num_words = len(words)
#     trimmed_sql = sql
#     if words[0].lower() == 'persist':
#         flags['persist'] = True
#         trimmed_sql =  " ".join(words[1:])
#     elif num_words >= 2 and words[1] == '<<':
#         flags['result_var'] = words[0]
#         trimmed_sql = " ".join(words[2:])
#     return (flags, trimmed_sql.strip())
