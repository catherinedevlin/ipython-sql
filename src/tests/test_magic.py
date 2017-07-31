from nose import with_setup
from nose.tools import raises
from sql.magic import SqlMagic
from textwrap import dedent
import os.path
import re
import tempfile

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
    assert re.search(r'1\s+\|\s+foo', str(result))


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
def test_result_var():
    ip.run_cell_magic('sql', '', """
        sqlite://
        x <<
        SELECT last_name FROM writer;
        """)
    result = ip.user_global_ns['x']
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

@raises(NameError)
def test_persist_nonexistent_raises():
    ip.run_line_magic('sql',  "sqlite://")
    ip.run_line_magic('sql', 'PERSIST no_such_dataframe')

@raises(TypeError)
def test_persist_non_frame_raises():
    ip.run_cell("not_a_dataframe = 22")
    ip.run_line_magic('sql', "sqlite://")
    ip.run_line_magic('sql', 'PERSIST not_a_dataframe')

@raises(SyntaxError)
def test_persist_bare():
    ip.run_line_magic('sql', "sqlite://")
    ip.run_line_magic('sql', 'PERSIST')

@with_setup(_setup_writer, _teardown_writer)
def test_persist_frame_at_its_creation():
    ip.run_cell("results = %sql SELECT * FROM writer;")
    ip.run_line_magic('sql', 'PERSIST results.DataFrame()')
    persisted = ip.run_line_magic('sql', 'SELECT * FROM results')
    assert 'Shakespeare' in str(persisted)

# TODO: support
# @with_setup(_setup_writer, _teardown_writer)
# def test_persist_with_connection_info():
#     ip.run_cell("results = %sql SELECT * FROM writer;")
#     ip.run_line_magic('sql', 'sqlite:// PERSIST results.DataFrame()')
#     persisted = ip.run_line_magic('sql', 'SELECT * FROM results')
#     assert 'Shakespeare' in str(persisted)

def test_displaylimit():
    ip.run_line_magic('config',  "SqlMagic.autolimit = None")
    ip.run_line_magic('config',  "SqlMagic.displaylimit = None")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM (VALUES ('apple'), ('banana'), ('cherry')) AS Result ORDER BY 1;")
    assert 'apple' in result._repr_html_()
    assert 'banana' in result._repr_html_()
    assert 'cherry' in result._repr_html_()
    ip.run_line_magic('config',  "SqlMagic.displaylimit = 1")
    assert 'apple' in result._repr_html_()
    assert 'cherry' not in result._repr_html_()

@with_setup(_setup_writer, _teardown_writer)
def test_column_local_vars():
    ip.run_line_magic('config',  "SqlMagic.column_local_vars = True")
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    assert result is None
    assert 'William' in ip.user_global_ns['first_name']
    assert 'Shakespeare' in ip.user_global_ns['last_name']
    assert len(ip.user_global_ns['first_name']) == 2
    ip.run_line_magic('config',  "SqlMagic.column_local_vars = False")

@with_setup(_setup, _teardown)
def test_userns_not_changed():
    ip.run_cell(dedent("""
    def function():
        local_var = 'local_val'
        %sql sqlite:// INSERT INTO test VALUES (2, 'bar');
    function()"""))
    assert 'local_var' not in ip.user_ns

def test_bind_vars():
    ip.user_global_ns['x'] = 22
    result = ip.run_line_magic('sql', "sqlite:// SELECT :x")
    assert result[0][0] == 22

@with_setup(_setup, _teardown)
def test_autopandas():
    ip.run_line_magic('config',  "SqlMagic.autopandas = True")
    dframe = ip.run_cell("%sql SELECT * FROM test;")
    assert dframe.success
    assert dframe.result.name[0] == 'foo'

@with_setup(_setup, _teardown)
def test_csv():
    ip.run_line_magic('config',  "SqlMagic.autopandas = False")  # uh-oh
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    result = result.csv()
    for row in result.splitlines():
        assert row.count(',') == 1
    assert len(result.splitlines()) == 3

@with_setup(_setup, _teardown)
def test_csv_to_file():
    ip.run_line_magic('config',  "SqlMagic.autopandas = False")  # uh-oh
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM test;")
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, 'test.csv')
        output = result.csv(fname)
        assert os.path.exists(output.file_path)
        with open(output.file_path) as csvfile:
            content = csvfile.read()
            for row in content.splitlines():
                assert row.count(',') == 1
            assert len(content.splitlines()) == 3

@with_setup(_setup_writer, _teardown_writer)
def test_dict():
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    result = result.dict()
    assert isinstance(result, dict)
    assert 'first_name' in result
    assert 'last_name' in result
    assert 'year_of_death' in result
    assert len(result['last_name']) == 2

@with_setup(_setup_writer, _teardown_writer)
def test_dicts():
    result = ip.run_line_magic('sql',  "sqlite:// SELECT * FROM writer;")
    for row in result.dicts():
        assert isinstance(row, dict)
        assert 'first_name' in row
        assert 'last_name' in row
        assert 'year_of_death' in row
