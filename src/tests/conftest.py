import os
import urllib.request
from pathlib import Path

import pytest
from IPython.core.interactiveshell import InteractiveShell

from sql.magic import SqlMagic, RenderMagic
from sql.magic_plot import SqlPlotMagic
from sql.magic_cmd import SqlCmdMagic
from sql.connection import Connection

PATH_TO_TESTS = Path(__file__).absolute().parent
PATH_TO_TMP_ASSETS = PATH_TO_TESTS / "tmp"
PATH_TO_TMP_ASSETS.mkdir(exist_ok=True)


def path_to_tests():
    return PATH_TO_TESTS


@pytest.fixture
def chinook_db():
    path = PATH_TO_TMP_ASSETS / "my.db"
    if not path.is_file():
        url = (
            "https://raw.githubusercontent.com"
            "/lerocha/chinook-database/master/"
            "ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
        )
        urllib.request.urlretrieve(url, path)

    return str(path)


def runsql(ip_session, statements):
    if isinstance(statements, str):
        statements = [statements]
    for statement in statements:
        result = ip_session.run_line_magic("sql", "sqlite:// %s" % statement)
    return result  # returns only last result


@pytest.fixture
def clean_conns():
    Connection.current = None
    Connection.connections = dict()
    yield


@pytest.fixture
def ip_empty():
    ip_session = InteractiveShell()
    ip_session.register_magics(SqlMagic)
    ip_session.register_magics(RenderMagic)
    ip_session.register_magics(SqlPlotMagic)
    ip_session.register_magics(SqlCmdMagic)
    yield ip_session


@pytest.fixture
def ip(ip_empty):
    """Provides an IPython session in which tables have been created"""

    # runsql creates an inmemory sqlitedatabase
    runsql(
        ip_empty,
        [
            "CREATE TABLE test (n INT, name TEXT)",
            "INSERT INTO test VALUES (1, 'foo')",
            "INSERT INTO test VALUES (2, 'bar')",
            "CREATE TABLE author (first_name, last_name, year_of_death)",
            "INSERT INTO author VALUES ('William', 'Shakespeare', 1616)",
            "INSERT INTO author VALUES ('Bertold', 'Brecht', 1956)",
            "CREATE TABLE empty_table (column INT, another INT)",
        ],
    )
    yield ip_empty
    runsql(ip_empty, "DROP TABLE test")
    runsql(ip_empty, "DROP TABLE author")


@pytest.fixture
def tmp_empty(tmp_path):
    """
    Create temporary path using pytest native fixture,
    them move it, yield, and restore the original path
    """
    old = os.getcwd()
    os.chdir(str(tmp_path))
    yield str(Path(tmp_path).resolve())
    os.chdir(old)
