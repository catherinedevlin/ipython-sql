from sql.magic import execute, SqlMagic
import sys
import nose
import re

ip = get_ipython()

def test_memory_db():
    config = SqlMagic(shell = ip)
    execute('', 'sqlite:// CREATE TABLE test (n INT, name TEXT)', config)
    execute('', "sqlite:// INSERT INTO test VALUES (1, 'foo');", config)
    execute('', "sqlite:// INSERT INTO test VALUES (2, 'bar');", config)
    assert execute('', "sqlite:// SELECT * FROM test;", config)[0][0] == 1
    assert execute('', "sqlite:// SELECT * FROM test;", config)[1]['name'] == 'bar'

def test_html():
    config = SqlMagic(shell = ip)
    result = execute('', "sqlite:// SELECT * FROM test;", config)
    assert '<td>foo</td>' in result._repr_html_().lower()

def test_print():
    config = SqlMagic(shell = ip)
    result = execute('', "sqlite:// SELECT * FROM test;", config)
    assert re.search(r'1\s+\|\s+foo', str(result))

def test_plain_style():
    config = SqlMagic(shell = ip)
    config.style = 'PLAIN_COLUMNS'
    result = execute('', "sqlite:// SELECT * FROM test;", config)
    assert re.search(r'1\s+foo', str(result))

def test_multi_sql():
    config = SqlMagic(shell = ip)
    result = execute('', """
        sqlite://
        CREATE TABLE writer (first_name, last_name, year_of_death);
        INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
        INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
        SELECT last_name FROM writer;
        """, config )
    assert 'Shakespeare' in str(result) and 'Brecht' in str(result)
    
def test_duplicate_column_names_accepted():
    config = SqlMagic(shell = ip)
    result = execute('', """
        sqlite://
        SELECT last_name, last_name FROM writer;
        """, config)
    assert (u'Brecht', u'Brecht') in result
