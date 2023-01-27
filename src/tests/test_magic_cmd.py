import pytest
from IPython.core.error import UsageError


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlcmd stuff",
            UsageError,
            "Unknown command: stuff",
        ],
        [
            "%sqlcmd columns",
            UsageError,
            "the following arguments are required: -t/--table",
        ],
    ],
)
def test_error(tmp_empty, ip, cell, error_type, error_message):
    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(out.error_in_exec) == error_message


def test_tables(ip):
    out = ip.run_cell("%sqlcmd tables").result._repr_html_()
    assert "author" in out
    assert "empty_table" in out
    assert "test" in out


# try with schema
def test_columns(ip):
    out = ip.run_cell("%sqlcmd columns -t author").result._repr_html_()
    assert "first_name" in out
    assert "last_name" in out
    assert "year_of_death" in out
