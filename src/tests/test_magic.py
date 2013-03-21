from sql.magic import execute
import sys
import nose

def get_stdout():
    if not hasattr(sys.stdout, "getvalue"):
        nose.failure("need to run in buffered mode")
    return sys.stdout.getvalue().strip() 


def test_memory_db():
    execute('', 'sqlite:// CREATE TABLE test (n INT, name TEXT)')
    execute('', "sqlite:// INSERT INTO test VALUES (1, 'foo');")
    execute('', "sqlite:// INSERT INTO test VALUES (2, 'bar');")
    execute('', "sqlite:// SELECT * FROM test;")
    output = get_stdout()
    # assert 'bar' in output
    
   