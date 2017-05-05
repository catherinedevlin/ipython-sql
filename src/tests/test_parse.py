import os
from sql.parse import parse
from six.moves import configparser
try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable

empty_config = Configurable()
default_flags = {'persist': False, 'result_var': None}
def test_parse_no_sql():
    assert parse("will:longliveliz@localhost/shakes", empty_config) == \
           {'connection': "will:longliveliz@localhost/shakes",
            'sql': '',
            'flags': default_flags}

def test_parse_with_sql():
    assert parse("postgresql://will:longliveliz@localhost/shakes SELECT * FROM work",
                 empty_config) == \
           {'connection': "postgresql://will:longliveliz@localhost/shakes",
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_parse_sql_only():
    assert parse("SELECT * FROM work", empty_config) == \
           {'connection': "",
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_parse_postgresql_socket_connection():
    assert parse("postgresql:///shakes SELECT * FROM work", empty_config) == \
           {'connection': "postgresql:///shakes",
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_expand_environment_variables_in_connection():
    os.environ['DATABASE_URL'] = 'postgresql:///shakes'
    assert parse("$DATABASE_URL SELECT * FROM work", empty_config) == \
            {'connection': "postgresql:///shakes",
            'sql': 'SELECT * FROM work',
            'flags': default_flags}
