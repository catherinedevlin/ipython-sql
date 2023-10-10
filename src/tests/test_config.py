import os
import re
from pathlib import Path

import pytest
import sys
from unittest.mock import Mock

from sql.magic import load_ipython_extension
from sql.connection import ConnectionManager
from sql.util import get_default_configs, CONFIGURATION_DOCS_STR
from sql import display
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


def test_load_home_toml_if_no_pyproject_toml(
    tmp_empty, ip_no_magics, capsys, monkeypatch
):
    monkeypatch.setattr(
        Path, "expanduser", lambda path: Path(str(path).replace("~", tmp_empty))
    )
    home_toml = Path("~/.jupysql/config").expanduser()
    home_toml.parent.mkdir(exist_ok=True)
    home_toml.write_text(
        """
[tool.jupysql.SqlMagic]
autocommit = false
autolimit = 1
style = "RANDOM"
"""
    )

    expect = [
        "Settings changed:",
        r"autocommit\s*\|\s*False",
        r"autolimit\s*\|\s*1",
        r"style\s*\|\s*RANDOM",
    ]

    config_expected = {"autocommit": False, "autolimit": 1, "style": "RANDOM"}

    os.mkdir("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)
    magic = ip_no_magics.find_magic("sql").__self__
    combined = {**get_default_testing_configs(magic), **config_expected}
    out, _ = capsys.readouterr()
    assert all(re.search(substring, out) for substring in expect)
    assert get_current_configs(magic) == combined


def test_start_ini_default_connection_using_toml_if_any(tmp_empty, ip_no_magics):
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


def test_magic_initialization_with_no_toml(tmp_empty, ip_no_magics):
    load_ipython_extension(ip_no_magics)


def test_magic_initialization_with_corrupted_pyproject_toml(
    tmp_empty, ip_no_magics, capsys
):
    Path("pyproject.toml").write_text(
        """
[tool.jupysql.SqlMagic]
dsn_filename = myconnections.ini
"""
    )

    load_ipython_extension(ip_no_magics)

    captured = capsys.readouterr()
    assert "Could not load configuration file" in captured.out


def test_magic_initialization_with_corrupted_home_toml(
    tmp_empty, ip_no_magics, capsys, monkeypatch
):
    monkeypatch.setattr(
        Path, "expanduser", lambda path: Path(str(path).replace("~", tmp_empty))
    )
    home_toml = Path("~/.jupysql/config").expanduser()
    home_toml.parent.mkdir(exist_ok=True)
    home_toml.write_text(
        """
[tool.jupysql.SqlMagic]
dsn_filename = myconnections.ini
"""
    )

    load_ipython_extension(ip_no_magics)

    captured = capsys.readouterr()
    assert "Could not load configuration file" in captured.out


def test_loading_valid_pyproject_toml_shows_feedback_and_modifies_config(
    tmp_empty, ip_no_magics, capsys
):
    Path("pyproject.toml").write_text(
        """
[tool.jupysql.SqlMagic]
autocommit = false
autolimit = 1
style = "RANDOM"
"""
    )

    expect = [
        "Loading configurations from {path}",
        "Settings changed:",
        r"autocommit\s*\|\s*False",
        r"autolimit\s*\|\s*1",
        r"style\s*\|\s*RANDOM",
    ]

    config_expected = {"autocommit": False, "autolimit": 1, "style": "RANDOM"}

    toml_path = str(Path(os.getcwd()).joinpath("pyproject.toml"))

    os.mkdir("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)
    magic = ip_no_magics.find_magic("sql").__self__
    combined = {**get_default_testing_configs(magic), **config_expected}
    out, _ = capsys.readouterr()
    expect[0] = expect[0].format(path=re.escape(toml_path))
    assert all(re.search(substring, out) for substring in expect)
    assert get_current_configs(magic) == combined


def test_loading_valid_home_toml_shows_feedback_and_modifies_config(
    tmp_empty, ip_no_magics, capsys, monkeypatch
):
    monkeypatch.setattr(
        Path, "expanduser", lambda path: Path(str(path).replace("~", tmp_empty))
    )
    home_toml = Path("~/.jupysql/config").expanduser()
    home_toml.parent.mkdir(exist_ok=True)
    home_toml.write_text(
        """
[tool.jupysql.SqlMagic]
autocommit = false
autolimit = 1
style = "RANDOM"
"""
    )

    expect = [
        "Loading configurations from {path}",
        "Settings changed:",
        r"autocommit\s*\|\s*False",
        r"autolimit\s*\|\s*1",
        r"style\s*\|\s*RANDOM",
    ]

    config_expected = {"autocommit": False, "autolimit": 1, "style": "RANDOM"}

    os.mkdir("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)
    magic = ip_no_magics.find_magic("sql").__self__
    combined = {**get_default_testing_configs(magic), **config_expected}
    out, _ = capsys.readouterr()
    expect[0] = expect[0].format(path=re.escape(str(home_toml)))
    assert all(re.search(substring, out) for substring in expect)
    assert get_current_configs(magic) == combined


@pytest.mark.parametrize(
    "file_content, param",
    [
        (
            """
[tool.jupysql.SqlMagic]
""",
            "[tool.jupysql.SqlMagic] present in {path} but empty.",
        ),
        ("", "Tip: You may define configurations in {path}."),
    ],
    ids=["empty_sqlmagic_key", "missing_sqlmagic_key"],
)
def test_loading_toml_display_configuration_docs_link(
    tmp_empty, ip_no_magics, file_content, param, monkeypatch
):
    Path("pyproject.toml").write_text(file_content)
    toml_path = str(Path(os.getcwd()).joinpath("pyproject.toml"))

    os.mkdir("sub")
    os.chdir("sub")
    mock = Mock()
    monkeypatch.setattr(display, "message_html", mock)

    load_ipython_extension(ip_no_magics)
    param = (
        f"{param.format(path=toml_path)} Please review our "
        f"<a href='{CONFIGURATION_DOCS_STR}'>configuration guideline</a>."
    )
    mock.assert_called_once_with(param)


@pytest.mark.parametrize(
    "file_content",
    [
        (
            """
[test]
github = "ploomber/jupysql"
"""
        ),
        (
            """
[tool.pkgmt]
github = "ploomber/jupysql"
"""
        ),
        (
            """
[tool.jupysql.test]
github = "ploomber/jupysql"
"""
        ),
    ],
)
def test_load_toml_user_configurations_not_specified(
    tmp_empty, ip_no_magics, capsys, file_content
):
    Path("pyproject.toml").write_text(file_content)
    os.mkdir("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)
    out, _ = capsys.readouterr()
    assert "Loading configurations from" not in out


@pytest.mark.parametrize(
    "file_content, error_msg",
    [
        (
            """
[tool.jupysql.SqlMagic]
autocommit = true
autocommit = true
""",
            "Duplicate key found: 'autocommit' in {path}",
        ),
        (
            """
[tool.jupySql.SqlMagic]
autocommit = true
""",
            "'jupySql' is an invalid section name in {path}. Did you mean 'jupysql'?",
        ),
        (
            """
[tool.jupysql.SqlMagic]
autocommit = True
""",
            (
                "Invalid value 'True' in 'autocommit = True' in {path}. "
                "Valid boolean values: true, false"
            ),
        ),
        (
            """
[tool.jupysql.SqlMagic]
autocommit = invalid
""",
            (
                "Invalid value 'invalid' in 'autocommit = invalid' in {path}. "
                "To use str value, enclose it with ' or \"."
            ),
        ),
    ],
)
def test_error_on_toml_parsing(
    tmp_empty, ip_no_magics, capsys, file_content, error_msg
):
    Path("pyproject.toml").write_text(file_content)
    toml_path = str(Path(os.getcwd()).joinpath("pyproject.toml"))
    os.makedirs("sub")
    os.chdir("sub")

    with pytest.raises(UsageError) as excinfo:
        load_ipython_extension(ip_no_magics)

    out, _ = capsys.readouterr()

    assert excinfo.value.error_type == "ConfigurationError"
    assert str(excinfo.value) == error_msg.format(path=toml_path)


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
    toml_path = str(Path(os.getcwd()).joinpath("pyproject.toml"))
    os.makedirs("sub")
    os.chdir("sub")

    load_ipython_extension(ip_no_magics)
    out, _ = capsys.readouterr()
    expect = [
        f"Loading configurations from {re.escape(toml_path)}",
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
        "To load settings from pyproject.toml or ~/.jupysql/config, "
        "install with: pip install toml"
    ) in out
