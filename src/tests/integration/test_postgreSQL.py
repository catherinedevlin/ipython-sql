import pytest


def test_meta_cmd_display(ip_with_postgreSQL):
    out = ip_with_postgreSQL.run_cell("%sql \d")  # noqa: W605
    assert len(out.result) > 0
    assert ("public", "taxi", "table", "ploomber_app") in out.result


# Known issue, addressing in https://github.com/ploomber/jupysql/issues/90
@pytest.mark.xfail(reason="known autocommit mode issue")
def test_auto_commit_mode_on(ip_with_postgreSQL, capsys):
    ip_with_postgreSQL.run_cell("%config SqlMagic.autocommit=True")
    ip_with_postgreSQL.run_cell("%sql CREATE DATABASE new_db")
    out, _ = capsys.readouterr()
    assert "CREATE DATABASE cannot run inside a transaction block" not in out
