from unittest.mock import Mock

from pytest import raises

from sql.run import _commit, add_commit_blacklist_dialect


class TestAddCommitBlacklistDialect:
    def test_after_adding_dialect_commit_is_not_issued(self):
        mock_connection = Mock()
        mock_connection.dialect = "bigquery"
        mock_config = Mock()
        mock_config.autocommit = True

        _commit(mock_connection, mock_config)
        mock_connection.session.execute.assert_called_once_with("commit")
        mock_connection.reset_mock()

        add_commit_blacklist_dialect("bigquery")
        _commit(mock_connection, mock_config)
        mock_connection.session.execute.assert_not_called()

    def test_hatred_towards_drivernames(self):
        with raises(ValueError, match=r"Dialects do not have '\+' inside"):
            add_commit_blacklist_dialect("databricks+connector")
