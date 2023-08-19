# we have a lot of config tests in test_magic.py, we should move them here
from pathlib import Path


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
