from sql.command import SQLCommand

import pytest


@pytest.mark.parametrize(
    "line, cell, parsed_sql, parsed_connection",
    [
        ("something --no-execute", "", "something\n", ""),
        ("sqlite://", "", "", "sqlite://"),
        ("SELECT * FROM TABLE", "", "SELECT * FROM TABLE\n", ""),
        ("SELECT * FROM", "TABLE", "SELECT * FROM\nTABLE", ""),
    ],
    ids=[
        "arg-with-option",
        "connection-string",
        "sql-query",
        "sql-query-in-line-and-cell",  # NOTE: I'm unsure under which circumstances this happens # noqa
    ],
)
def test_parsed(ip, line, cell, parsed_sql, parsed_connection):
    sql_line = ip.magics_manager.lsmagic()["line"]["sql"].__self__

    cmd = SQLCommand(sql_line, line, cell)

    assert cmd.parsed == {
        "connection": parsed_connection,
        "result_var": None,
        "sql": parsed_sql,
    }


def test_args():
    pass
    # assert cmd.args.__dict__ == {
    #     "line": ["something"],
    #     "connections": False,
    #     "close": None,
    #     "creator": None,
    #     "section": None,
    #     "persist": False,
    #     "no_index": False,
    #     "append": False,
    #     "connection_arguments": None,
    #     "file": None,
    #     "save": None,
    #     "with_": None,
    #     "no_execute": True,
    # }

    # assert cmd.command_text == "something\n"
