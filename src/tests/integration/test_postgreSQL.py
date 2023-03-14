def test_meta_cmd_display(ip_with_postgreSQL):
    out = ip_with_postgreSQL.run_cell("%sql \d")  # noqa: W605
    assert len(out.result) > 0
    assert ("public", "taxi", "table", "ploomber_app") in out.result


def test_auto_commit_mode_on(ip_with_postgreSQL, capsys):
    ip_with_postgreSQL.run_cell("%config SqlMagic.autocommit=True")
    out_after_creating = ip_with_postgreSQL.run_cell("%sql CREATE DATABASE new_db")
    out_all_dbs = ip_with_postgreSQL.run_cell("%sql \l").result  # noqa: W605
    out, _ = capsys.readouterr()
    assert out_after_creating.error_in_exec is None
    assert any(row[0] == "new_db" for row in out_all_dbs)
    assert "CREATE DATABASE cannot run inside a transaction block" not in out
