import pytest
from IPython.core.interactiveshell import InteractiveShell

from sql.magic import SqlMagic, RenderMagic


def runsql(ip_session, statements):
    if isinstance(statements, str):
        statements = [statements]
    for statement in statements:
        result = ip_session.run_line_magic("sql", "sqlite:// %s" % statement)
    return result  # returns only last result


@pytest.fixture
def ip():
    """Provides an IPython session in which tables have been created"""
    ip_session = InteractiveShell()
    ip_session.register_magics(SqlMagic)
    ip_session.register_magics(RenderMagic)

    runsql(
        ip_session,
        [
            "CREATE TABLE test (n INT, name TEXT)",
            "INSERT INTO test VALUES (1, 'foo')",
            "INSERT INTO test VALUES (2, 'bar')",
            "CREATE TABLE author (first_name, last_name, year_of_death)",
            "INSERT INTO author VALUES ('William', 'Shakespeare', 1616)",
            "INSERT INTO author VALUES ('Bertold', 'Brecht', 1956)",
        ],
    )
    yield ip_session
    runsql(ip_session, "DROP TABLE test")
    runsql(ip_session, "DROP TABLE author")
