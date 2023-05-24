import pytest
import sqlalchemy.exc

from sqlalchemy.exc import OperationalError
from IPython.core.error import UsageError

from sql.error_message import SYNTAX_ERROR, ORIGINAL_ERROR
from ploomber_core.exceptions import COMMUNITY


COMMUNITY = COMMUNITY.strip()


@pytest.fixture
def query_no_suggestion():
    return (
        "sql",
        "",
        """
        sqlite://
        SELECT FROM author;
        """,
    )


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
def query_suggestion():
    return (
        "sql",
        "",
        """
        sqlite://
        ALTER TABLE author RENAME new_author;
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
    assert SYNTAX_ERROR.strip() in str(err.value)
    assert ORIGINAL_ERROR.strip() in str(err.value)
    assert COMMUNITY in str(err.value)
    assert isinstance(err.value, UsageError)


def test_syntax_error_no_suggestion(ip, query_no_suggestion):
    with pytest.raises(UsageError) as err:
        ip.run_cell_magic(*query_no_suggestion)
    _common_strings_check(err)


message_no_suggestion_long = f"""\
(sqlite3.OperationalError) near "FROM": syntax error
{COMMUNITY}
[SQL: SELECT FROM author;]
"""  # noqa


def test_syntax_error_no_suggestion_long(ip, capsys, query_no_suggestion):
    ip.run_line_magic("config", "SqlMagic.short_errors = False")
    with pytest.raises(OperationalError) as err:
        ip.run_cell_magic(*query_no_suggestion)
    out, _ = capsys.readouterr()
    assert message_no_suggestion_long.strip() in str(err.value).strip()
    assert SYNTAX_ERROR.strip() in out
    assert isinstance(err.value, sqlalchemy.exc.OperationalError)


def test_syntax_error_incorrect_column_name(ip, query_incorrect_column_name):
    with pytest.raises(UsageError) as err:
        ip.run_cell_magic(*query_incorrect_column_name)
    assert "Syntax Error in SELECT first_(name FROM author:" in str(err.value)
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
    assert SYNTAX_ERROR.strip() in out
    assert isinstance(err.value, sqlalchemy.exc.OperationalError)


def test_syntax_error_suggestion(ip, query_suggestion):
    with pytest.raises(UsageError) as err:
        ip.run_cell_magic(*query_suggestion)
    assert "Did you mean : ['ALTER TABLE author RENAME TO new_author']" in str(
        err.value
    )
    _common_strings_check(err)


message_error_suggestion_long = f"""\
(sqlite3.OperationalError) near ";": syntax error
{COMMUNITY}
[SQL: ALTER TABLE author RENAME new_author;]
"""  # noqa


def test_syntax_error_suggestion_long(ip, capsys, query_suggestion):
    ip.run_line_magic("config", "SqlMagic.short_errors = False")
    with pytest.raises(OperationalError) as err:
        ip.run_cell_magic(*query_suggestion)
    out, _ = capsys.readouterr()
    assert message_error_suggestion_long.strip() in str(err.value).strip()
    assert SYNTAX_ERROR.strip() in out
    assert isinstance(err.value, sqlalchemy.exc.OperationalError)


def test_syntax_error_multiple_statements(ip, query_multiple):
    with pytest.raises(UsageError) as err:
        ip.run_cell_magic(*query_multiple)
    assert (
        "Did you mean : ['ALTER TABLE author RENAME TO another_name']"
        in str(err.value).strip()
    )
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
    assert SYNTAX_ERROR.strip() in out
    assert isinstance(err.value, sqlalchemy.exc.OperationalError)
