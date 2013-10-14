from sql.magic import SqlMagic
import re

ip = get_ipython()

def setup():
    sqlmagic = SqlMagic(shell=ip)
    ip.register_magics(sqlmagic)

def test_memory_db():
    ip.run_line_magic('sql', 'sqlite:// CREATE TABLE test (n INT, name TEXT)')
    ip.run_line_magic('sql', "sqlite:// INSERT INTO test VALUES (1, 'foo');")
    ip.run_line_magic('sql', "sqlite:// INSERT INTO test VALUES (2, 'bar');")
    assert ip.run_line_magic('sql', "sqlite:// SELECT * FROM test;")[0][0] == 1
    assert ip.run_line_magic('sql', "sqlite:// SELECT * FROM test;")[1]['name'] == 'bar'

def test_html():
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert '<td>foo</td>' in result._repr_html_().lower()

def test_print():
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert re.search(r'1\s+\|\s+foo', str(result))

def test_plain_style():
    ip.run_line_magic('config',  "SqlMagic.style = 'PLAIN_COLUMNS'")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert re.search(r'1\s+foo', str(result))

def test_multi_sql():
    result = ip.run_cell_magic('sql', '', """
        sqlite://
        CREATE TABLE writer (first_name, last_name, year_of_death);
        INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
        INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
        SELECT last_name FROM writer;
        """)
    assert 'Shakespeare' in str(result) and 'Brecht' in str(result)
    
def test_access_results_by_keys():
    assert ip.run_line_magic('sql', "sqlite:// SELECT * FROM writer;")['William'] == (u'William', u'Shakespeare', 1616)
    
def test_duplicate_column_names_accepted():
    result = ip.run_cell_magic('sql', '', """
        sqlite://
        SELECT last_name, last_name FROM writer;
        """)
    assert (u'Brecht', u'Brecht') in result

def test_autolimit():
    ip.run_line_magic('config',  "SqlMagic.autolimit = 0")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert len(result) == 2
    ip.run_line_magic('config',  "SqlMagic.autolimit = 1")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert len(result) == 1


def test_displaylimit():
    ip.run_line_magic('config',  "SqlMagic.displaylimit = 0")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    assert result._repr_html_().count("<tr>") == 3
    ip.run_line_magic('config',  "SqlMagic.displaylimit = 1")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    assert result._repr_html_().count("<tr>") == 2
