import os
import re
from pathlib import Path

import pytest
import sys

from sql.magic import load_ipython_extension
from sql.connection import ConnectionManager
from sql.util import get_default_configs
from IPython.core.error import UsageError


def get_current_configs(magic):
    cfg = magic.trait_values()
    del cfg["parent"]
    del cfg["config"]
    return cfg


def get_default_testing_configs(sql):
    """
    Returns a dictionary of SqlMagic configuration settings users can set
    with their default values.
    """
    cfg = get_default_configs(sql)
    # we're overriding this in conftest.py
    cfg["dsn_filename"] = "default.ini"
    return cfg


def test_dsn_filename_default_value(sql_magic):
    assert sql_magic.dsn_filename == str(
        Path("~/.jupysql/connections.ini").expanduser()
    )


def test_dsn_filename_resolves_user_directory(sql_magic):
    sql_magic.dsn_filename = "~/connections.ini"

    path = Path("~/connections.ini").expanduser()
    expected = str(path)

    # setting the value should not create the file
    assert not path.exists()

    # but it should resolve the path
    assert sql_magic.dsn_filename == expected


def test_no_error_if_connection_file_doesnt_exist(tmp_empty, ip_no_magics):
    ip_no_magics.run_cell("%config SqlMagic.dsn_filename = 'connections.ini'")

    load_ipython_extension(ip_no_magics)

    assert not Path("connections.ini").exists()


def test_no_error_if_connection_file_doesnt_have_default_section(
    tmp_empty, ip_no_magics
):
    Path("connections.ini").write_text(
        """
[duck]
drivername = sqlite
"""
    )

    ip_no_magics.run_cell("%config SqlMagic.dsn_filename = 'connections.ini'")

    load_ipython_extension(ip_no_magics)

    assert not ConnectionManager.connections


def test_start_ini_default_connection_if_any(tmp_empty, ip_no_magics):
    Path("connections.ini").write_text(
        """
[default]
drivername = sqlite
"""
    )

    ip_no_magics.run_cell("%config SqlMagic.dsn_filename = 'connections.ini'")

    load_ipython_extension(ip_no_magics)

    assert set(ConnectionManager.connections) == {"default"}
    assert ConnectionManager.current.dialect == "sqlite"


def test_start_ini_default_connection_using_pyproject_if_any(tmp_empty, ip_no_magics):
    Path("pyproject.toml").write_text(
        """
[tool.jupysql.SqlMagic]
dsn_filename = 'myconnections.ini'
"""
    )

    Path("myconnections.ini").write_text(
        """
[default]
drivername = duckdb
"""
    )

    load_ipython_extension(ip_no_magics)

    assert set(ConnectionManager.connections) == {"default"}
    assert ConnectionManager.current.dialect == "duckdb"


def test_magic_initialization_when_default_connection_fails(
    tmp_empty, ip_no_magics, capsys
):
    ip_no_magics.run_cell("%config SqlMagic.dsn_filename = 'connections.ini'")

    Path("connections.ini").write_text(
        """
[default]
drivername = someunknowndriver
"""
    )

    load_ipython_extension(ip_no_magics)

    captured = capsys.readouterr()
    assert "Cannot start default connection" in captured.out


def test_magic_initialization_with_no_pyproject(tmp_empty, ip_no_magics):
    load_ipython_extension(ip_no_magics)


def test_magic_initialization_with_corrupted_pyproject(tmp_empty, ip_no_magics, capsys):
    Path("pyproject.toml").write_text(
        """
[tool.jupysql.SqlMagic]
dsn_filename = myconnections.ini
"""
    )

    load_ipython_extension(ip_no_magics)

    captured = capsys.readouterr()
    assert "Could not load configuration file" in captured.out


@pytest.mark.parametrize(
    "file_content, expect, config_expected",
    [
        (
            """
[tool.jupysql.SqlMagic]
autocommit = false
autolimit = 1
style = "RANDOM"
""",
            [
                "Found pyproject.toml from '%s'",
                "Settings changed:",
                r"autocommit\s*\|\s*False",
                r"autolimit\s*\|\s*1",
                r"style\s*\|\s*RANDOM",
            ],
            {"autocommit": False, "autolimit": 1, "style": "RANDOM"},
        ),
        (
            """
[tool.jupysql.SqlMagic]
""",
            ["Found pyproject.toml from '%s'"],
            {},
        ),
        (
            """
[test]
github = "ploomber/jupysql"
""",
            ["Found pyproject.toml from '%s'"],
            {},
        ),
        (
            """
[tool.pkgmt]
github = "ploomber/jupysql"
""",
            ["Found pyproject.toml from '%s'"],
            {},
        ),
        (
            """
[tool.jupysql.test]
github = "ploomber/jupysql"
""",
            ["Found pyproject.toml from '%s'"],
            {},
        ),
        (
            "",
            ["Found pyproject.toml from '%s'"],
            {},
        ),
    ],
)
def test_loading_valid_pyproject_toml_shows_feedback_and_modifies_config(
    tmp_empty,
    ip_no_magics,
    capsys,
    file_content,
    expect,
    config_expected,
):
    Path("pyproject.toml").write_text(file_content)
    toml_dir = os.getcwd()

    os.mkdir("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)

    magic = ip_no_magics.find_magic("sql").__self__

    combined = {**get_default_testing_configs(magic), **config_expected}
    out, _ = capsys.readouterr()

    expect[0] = expect[0] % (re.escape(toml_dir))
    assert all(re.search(substring, out) for substring in expect)
    assert get_current_configs(magic) == combined


@pytest.mark.parametrize(
    "file_content, error_msg",
    [
        (
            """
[tool.jupysql.SqlMagic]
autocommit = true
autocommit = true
""",
            "Duplicate key found : 'autocommit'",
        ),
        (
            """
[tool.jupySql.SqlMagic]
autocommit = true
""",
            "'jupySql' is an invalid section name. Did you mean 'jupysql'?",
        ),
        (
            """
[tool.jupysql.SqlMagic]
autocommit = True
""",
            (
                "Invalid value 'True' in 'autocommit = True'. "
                "Valid boolean values: true, false"
            ),
        ),
        (
            """
[tool.jupysql.SqlMagic]
autocommit = invalid
""",
            (
                "Invalid value 'invalid' in 'autocommit = invalid'. "
                "To use str value, enclose it with ' or \"."
            ),
        ),
    ],
)
def test_error_on_toml_parsing(
    tmp_empty, ip_no_magics, capsys, file_content, error_msg
):
    Path("pyproject.toml").write_text(file_content)
    toml_dir = os.getcwd()
    found_statement = "Found pyproject.toml from '%s'" % (toml_dir)
    os.makedirs("sub")
    os.chdir("sub")

    with pytest.raises(UsageError) as excinfo:
        load_ipython_extension(ip_no_magics)

    out, _ = capsys.readouterr()

    assert out.strip() == found_statement
    assert excinfo.value.error_type == "ConfigurationError"
    assert str(excinfo.value) == error_msg


def test_valid_and_invalid_configs(tmp_empty, ip_no_magics, capsys):
    Path("pyproject.toml").write_text(
        """
[tool.jupysql.SqlMagic]
autocomm = true
autop = false
autolimit = "text"
invalid = false
displaycon = false
"""
    )
    toml_dir = os.getcwd()
    os.makedirs("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)
    out, _ = capsys.readouterr()
    expect = [
        "Found pyproject.toml from '%s'" % (re.escape(toml_dir)),
        "'autocomm' is an invalid configuration. Did you mean 'autocommit'?",
        (
            "'autop' is an invalid configuration. "
            "Did you mean 'autopandas', or 'autopolars'?"
        ),
        (
            "'text' is an invalid value for 'autolimit'. "
            "Please use int value instead."
        ),
        r"displaycon\s*\|\s*False",
    ]
    assert all(re.search(substring, out) for substring in expect)

    # confirm the correct changes are applied
    confirm = {"displaycon": False, "autolimit": 0}
    sql = ip_no_magics.find_cell_magic("sql").__self__
    assert all([getattr(sql, config) == value for config, value in confirm.items()])


def test_toml_optional_message(tmp_empty, monkeypatch, ip, capsys):
    monkeypatch.setitem(sys.modules, "toml", None)
    Path("pyproject.toml").write_text(
        """
[tool.jupysql.SqlMagic]
autocommit = true
"""
    )

    ip.run_cell("%load_ext sql")
    out, _ = capsys.readouterr()
    assert (
        "The 'toml' package isn't installed. "
        "To load settings from the pyproject.toml file, "
        "install with: pip install toml"
    ) in out
