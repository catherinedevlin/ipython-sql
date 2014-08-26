from nose import with_setup
from sql.magic import SqlMagic
import re

ip = get_ipython()

def setup():
    sqlmagic = SqlMagic(shell=ip)
    ip.register_magics(sqlmagic)

def _setup():
    ip.run_line_magic('sql', 'sqlite:// CREATE TABLE test (n INT, name TEXT)')
    ip.run_line_magic('sql', "sqlite:// INSERT INTO test VALUES (1, 'foo');")
    ip.run_line_magic('sql', "sqlite:// INSERT INTO test VALUES (2, 'bar');")

def _teardown():
    ip.run_line_magic('sql', 'sqlite:// DROP TABLE test')

@with_setup(_setup, _teardown)
def test_memory_db():
    assert ip.run_line_magic('sql', "sqlite:// SELECT * FROM test;")[0][0] == 1
    assert ip.run_line_magic('sql', "sqlite:// SELECT * FROM test;")[1]['name'] == 'bar'

@with_setup(_setup, _teardown)
def test_html():
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert '<td>foo</td>' in result._repr_html_().lower()

@with_setup(_setup, _teardown)
def test_print():
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert re.search(r'1\s+\|\s+foo', str(result))

@with_setup(_setup, _teardown)
def test_plain_style():
    ip.run_line_magic('config',  "SqlMagic.style = 'PLAIN_COLUMNS'")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert re.search(r'1\s+foo', str(result))


def _setup_writer():
    ip.run_line_magic('sql', 'sqlite:// CREATE TABLE writer (first_name, last_name, year_of_death)')
    ip.run_line_magic('sql', "sqlite:// INSERT INTO writer VALUES ('William', 'Shakespeare', 1616)")
    ip.run_line_magic('sql', "sqlite:// INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956)")

def _teardown_writer():
    ip.run_line_magic('sql', "sqlite:// DROP TABLE writer")

@with_setup(_setup_writer, _teardown_writer)
def test_multi_sql():
    result = ip.run_cell_magic('sql', '', """
        sqlite://
        SELECT last_name FROM writer;
        """)
    assert 'Shakespeare' in str(result) and 'Brecht' in str(result)


@with_setup(_setup_writer, _teardown_writer)
def test_access_results_by_keys():
    assert ip.run_line_magic('sql', "sqlite:// SELECT * FROM writer;")['William'] == (u'William', u'Shakespeare', 1616)

@with_setup(_setup_writer, _teardown_writer)
def test_duplicate_column_names_accepted():
    result = ip.run_cell_magic('sql', '', """
        sqlite://
        SELECT last_name, last_name FROM writer;
        """)
    assert (u'Brecht', u'Brecht') in result

@with_setup(_setup, _teardown)
def test_autolimit():
    ip.run_line_magic('config',  "SqlMagic.autolimit = 0")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert len(result) == 2
    ip.run_line_magic('config',  "SqlMagic.autolimit = 1")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    assert len(result) == 1


@with_setup(_setup, _teardown)
def test_persist():
    ip.run_cell("results = %sql SELECT * FROM test;")
    ip.runcode("results_dframe = results.DataFrame()")
    ip.run_line_magic('sql', 'PERSIST results_dframe')
    persisted = ip.run_line_magic('sql', 'SELECT * FROM results_dframe')
    assert 'foo' in str(persisted)

@with_setup(_setup_writer, _teardown_writer)
def test_unnamed_persist():
    ip.run_cell("results = %sql SELECT * FROM writer;")
    ip.run_line_magic('sql', 'PERSIST results.DataFrame()')
    persisted = ip.run_line_magic('sql', 'SELECT * FROM results')
    assert 'Shakespeare' in str(persisted)

@with_setup(_setup_writer, _teardown_writer)
def test_displaylimit():
    ip.run_line_magic('config',  "SqlMagic.autolimit = 0")
    ip.run_line_magic('config',  "SqlMagic.displaylimit = 0")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    assert result._repr_html_().count("<tr>") == 3
    ip.run_line_magic('config',  "SqlMagic.displaylimit = 1")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    assert result._repr_html_().count("<tr>") == 2

"""
def test_control_feedback():
    ip.run_line_magic('config',  "SqlMagic.feedback = False")

def test_local_over_global():
    ip.run_line_magic('', "x = 22")
    result = ip.run_line_magic('sql', "sqlite:// SELECT :x")
    assert result[0][0] == 22
"""
