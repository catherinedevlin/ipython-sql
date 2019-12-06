import os
from sql.parse import parse, match_bracket
from six.moves import configparser
try:
    from traitlets.config.configurable import Configurable
except ImportError:
    from IPython.config.configurable import Configurable
import json

empty_config = Configurable()
default_flags = {'persist': False, 'result_var': None}
default_connect_args = {'options': '-csearch_path=test'}
def test_parse_no_sql():
    assert parse("will:longliveliz@localhost/shakes", empty_config) == \
           {'connection': "will:longliveliz@localhost/shakes",
		   	'connect_args': {},
            'sql': '',
            'flags': default_flags}

def test_parse_with_sql():
    assert parse("postgresql://will:longliveliz@localhost/shakes SELECT * FROM work",
                 empty_config) == \
           {'connection': "postgresql://will:longliveliz@localhost/shakes",
		    'connect_args': {},
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_parse_sql_only():
    assert parse("SELECT * FROM work", empty_config) == \
           {'connection': "",
		    'connect_args': {},
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_parse_postgresql_socket_connection():
    assert parse("postgresql:///shakes SELECT * FROM work", empty_config) == \
           {'connection': "postgresql:///shakes",
		    'connect_args': {},
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_expand_environment_variables_in_connection():
    os.environ['DATABASE_URL'] = 'postgresql:///shakes'
    assert parse("$DATABASE_URL SELECT * FROM work", empty_config) == \
            {'connection': "postgresql:///shakes",
			'connect_args': {},
            'sql': 'SELECT * FROM work',
            'flags': default_flags}

def test_bracket_matching_obj():
	assert match_bracket("{}", "{") == 2
	assert match_bracket("{\"test\": \"key\"}", "{") == 15
	assert match_bracket("{         }", "{") == 11
	assert match_bracket("{{}}", "{") == 4

def test_parse_with_ns_connect_args():
	assert parse("postgresql://will:longliveliz@localhost/shakes :args", empty_config, { "args": default_connect_args}) ==\
		{'connection': "postgresql://will:longliveliz@localhost/shakes",
		 'connect_args': default_connect_args,
		 'sql': '',
		 'flags': default_flags
		}

def test_parse_with_json_connect_args():
	jsonArgs = json.dumps(default_connect_args)
	assert parse("postgresql://will:longliveliz@localhost/shakes {}".format(jsonArgs), empty_config) ==\
		{'connection': "postgresql://will:longliveliz@localhost/shakes",
		 'connect_args': default_connect_args,
		 'sql': '',
		 'flags': default_flags
		}

def test_parse_with_ns_connect_args_and_sql():
	assert parse("postgresql://will:longliveliz@localhost/shakes :args SELECT * FROM work", empty_config, { "args": default_connect_args}) ==\
		{'connection': "postgresql://will:longliveliz@localhost/shakes",
		 'connect_args': default_connect_args,
		 'sql': 'SELECT * FROM work',
		 'flags': default_flags
		}

def test_parse_with_json_connect_args_and_sql():
	jsonArgs = json.dumps(default_connect_args)
	assert parse("postgresql://will:longliveliz@localhost/shakes {} SELECT * FROM work".format(jsonArgs), empty_config) ==\
		{'connection': "postgresql://will:longliveliz@localhost/shakes",
		 'connect_args': default_connect_args,
		 'sql': 'SELECT * FROM work',
		 'flags': default_flags
		}