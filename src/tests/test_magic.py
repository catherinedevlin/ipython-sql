from sql.magic import execute
import sys
import nose
import re

def test_memory_db():
    execute('', 'sqlite:// CREATE TABLE test (n INT, name TEXT)')
    execute('', "sqlite:// INSERT INTO test VALUES (1, 'foo');")
    execute('', "sqlite:// INSERT INTO test VALUES (2, 'bar');")
    assert execute('', "sqlite:// SELECT * FROM test;")[0][0] == 1
    assert execute('', "sqlite:// SELECT * FROM test;")[1]['name'] == 'bar'

def test_html():
    result = execute('', "sqlite:// SELECT * FROM test;")
    assert '<td>foo</td>' in result._repr_html_().lower()

def test_print():
    result = execute('', "sqlite:// SELECT * FROM test;")
    assert re.search(r'1\s+\|\s+foo', str(result))

def test_plain_style():
    config = SqlMagic.style = 'PLAIN_COLUMNS'
    result = execute('', "sqlite:// SELECT * FROM test;", config)
    assert re.search(r'1\s+foo', str(result))

def test_multi_sql():
    result = execute('', """
        sqlite://
        CREATE TABLE writer (first_name, last_name, year_of_death);
        INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
        INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
        SELECT last_name FROM writer;
        """, )
    assert 'Shakespeare' in str(result) and 'Brecht' in str(result)
    
def test_duplicate_column_names_accepted():
    result = execute('', """
        sqlite://
        SELECT last_name, last_name FROM writer;
        """, )
    assert (u'Brecht', u'Brecht') in result
