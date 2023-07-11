import pytest
import sqlalchemy.exc

from sqlalchemy.exc import OperationalError
from IPython.core.error import UsageError

from sql.error_message import ORIGINAL_ERROR, CTE_MSG
from ploomber_core.exceptions import COMMUNITY


COMMUNITY = COMMUNITY.strip()


@pytest.fixture
def query_incorrect_column_name():
    return (
        "sql",
        "",
        """
        sqlite://
        SELECT first_(name FROM author;
        """,
    )


@pytest.fixture
def query_multiple():
    return (
        "sql",
        "",
        """
        sqlite://
        INSERT INTO author VALUES ('Charles', 'Dickens', 1812);
        ALTER TABLE author RENAME another_name;
        """,
    )


def _common_strings_check(err):
    assert ORIGINAL_ERROR.strip() in str(err.value)
    assert CTE_MSG.strip() in str(err.value)
    assert COMMUNITY in str(err.value)
    assert isinstance(err.value, UsageError)


def test_syntax_error_incorrect_column_name(ip, query_incorrect_column_name):
    with pytest.raises(UsageError) as err:
        ip.run_cell_magic(*query_incorrect_column_name)
    _common_strings_check(err)


message_incorrect_column_name_long = f"""\
(sqlite3.OperationalError) near "FROM": syntax error
{COMMUNITY}
[SQL: SELECT first_(name FROM author;]
"""  # noqa


def test_syntax_error_incorrect_column_name_long(
    ip, capsys, query_incorrect_column_name
):
    ip.run_line_magic("config", "SqlMagic.short_errors = False")
    with pytest.raises(OperationalError) as err:
        ip.run_cell_magic(*query_incorrect_column_name)
    out, _ = capsys.readouterr()
    assert message_incorrect_column_name_long.strip() in str(err.value).strip()
    assert isinstance(err.value, sqlalchemy.exc.OperationalError)


def test_syntax_error_multiple_statements(ip, query_multiple):
    with pytest.raises(UsageError) as err:
        ip.run_cell_magic(*query_multiple)
    _common_strings_check(err)


message_multiple_statements_long = f"""\
(sqlite3.OperationalError) near ";": syntax error
{COMMUNITY}
[SQL: ALTER TABLE author RENAME another_name;]
"""  # noqa


def test_syntax_error_multiple_statements_long(ip, capsys, query_multiple):
    ip.run_line_magic("config", "SqlMagic.short_errors = False")
    with pytest.raises(OperationalError) as err:
        ip.run_cell_magic(*query_multiple)
    out, _ = capsys.readouterr()
    assert message_multiple_statements_long.strip() in str(err.value).strip()
    assert isinstance(err.value, sqlalchemy.exc.OperationalError)
