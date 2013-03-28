from sql.magic import execute
import sys
import nose
import re

def test_memory_db():
    execute('', 'sqlite:// CREATE TABLE test (n INT, name TEXT)')
    execute('', "sqlite:// INSERT INTO test VALUES (1, 'foo');")
    execute('', "sqlite:// INSERT INTO test VALUES (2, 'bar');")
    assert execute('', "sqlite:// SELECT * FROM test;")[0][0] == 1
    assert execute('', "sqlite:// SELECT * FROM test;")[1]['name'] == u'bar'

def test_html():
    result = execute('', "sqlite:// SELECT * FROM test;")
    assert '<td>foo</td>' in result._repr_html_().lower()

def test_print():
    result = execute('', "sqlite:// SELECT * FROM test;")
    assert re.search(r'1\s+foo', str(result))
