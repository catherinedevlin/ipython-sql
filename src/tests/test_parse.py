from pathlib import Path 
import os
from sql.parse import parse, connection_from_dsn_section
from six.moves import configparser
try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable
import json

empty_config = Configurable()
default_connect_args = {'options': '-csearch_path=test'}
def test_parse_no_sql():
    assert parse("will:longliveliz@localhost/shakes", empty_config) == \
           {'connection': "will:longliveliz@localhost/shakes",
            'sql': '',
            'result_var': None}

def test_parse_with_sql():
    assert parse("postgresql://will:longliveliz@localhost/shakes SELECT * FROM work",
                 empty_config) == \
           {'connection': "postgresql://will:longliveliz@localhost/shakes",
            'sql': 'SELECT * FROM work',
            'result_var': None}

def test_parse_sql_only():
    assert parse("SELECT * FROM work", empty_config) == \
           {'connection': "",
            'sql': 'SELECT * FROM work',
            'result_var': None}

def test_parse_postgresql_socket_connection():
    assert parse("postgresql:///shakes SELECT * FROM work", empty_config) == \
           {'connection': "postgresql:///shakes",
            'sql': 'SELECT * FROM work',
            'result_var': None}

def test_expand_environment_variables_in_connection():
    os.environ['DATABASE_URL'] = 'postgresql:///shakes'
    assert parse("$DATABASE_URL SELECT * FROM work", empty_config) == \
           {'connection': "postgresql:///shakes",
            'sql': 'SELECT * FROM work',
            'result_var': None}

def test_parse_shovel_operator():
    assert parse("dest << SELECT * FROM work", empty_config) == \
           {'connection': "",
            'sql': 'SELECT * FROM work',
            'result_var': "dest"}

def test_parse_connect_plus_shovel():
    assert parse("sqlite:// dest << SELECT * FROM work", empty_config) == \
           {'connection': "sqlite://",
            'sql': 'SELECT * FROM work',
            'result_var': None}

def test_parse_shovel_operator():
    assert parse("dest << SELECT * FROM work", empty_config) == \
           {'connection': "",
            'sql': 'SELECT * FROM work',
            'result_var': "dest"}

def test_parse_connect_plus_shovel():
    assert parse("sqlite:// dest << SELECT * FROM work", empty_config) == \
           {'connection': "sqlite://",
            'sql': 'SELECT * FROM work',
            'result_var': "dest"}

class DummyConfig:
    dsn_filename = Path('src/tests/test_dsn_config.ini')

def test_connection_from_dsn_section():

    result = connection_from_dsn_section(section='DB_CONFIG_1',
        config = DummyConfig())
    assert result == 'postgres://goesto11:seentheelephant@my.remote.host:5432/pgmain'
    result = connection_from_dsn_section(section='DB_CONFIG_2',
        config = DummyConfig())
    assert result == 'mysql://thefin:fishputsfishonthetable@127.0.0.1/dolfin'
