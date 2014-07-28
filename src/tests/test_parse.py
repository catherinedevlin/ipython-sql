from sql.parse import parse
from six.moves import configparser
from IPython.config.configurable import Configurable

empty_config = Configurable()

def test_parse_no_sql():
    assert parse("will:longliveliz@localhost/shakes", empty_config) == \
           {'connection': "will:longliveliz@localhost/shakes",
            'sql': ''}
    
def test_parse_with_sql():
    assert parse("postgresql://will:longliveliz@localhost/shakes SELECT * FROM work", 
                 empty_config) == \
           {'connection': "postgresql://will:longliveliz@localhost/shakes",
            'sql': 'SELECT * FROM work'}    
    
def test_parse_sql_only():
    assert parse("SELECT * FROM work", empty_config) == \
           {'connection': "",
            'sql': 'SELECT * FROM work'} 
    
def test_parse_postgresql_socket_connection():
    assert parse("postgresql:///shakes SELECT * FROM work", empty_config) == \
           {'connection': "postgresql:///shakes",
            'sql': 'SELECT * FROM work'}            