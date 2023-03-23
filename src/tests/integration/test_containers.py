import os
import pytest
from sql import _testing

is_on_github = False
if "GITHUB_ACTIONS" in os.environ:
    is_on_github = True


@pytest.mark.parametrize(
    "container_context, excepted_database_ready_string, configKey",
    [
        (
            _testing.postgres,
            "database system is ready to accept connections",
            "postgreSQL",
        ),
        (_testing.mysql, "mysqld: ready for connections", "mySQL"),
        (_testing.mariadb, "mysqld: ready for connections", "mariaDB"),
    ],
)
def test_invidual_container(
    container_context, excepted_database_ready_string, configKey
):
    if is_on_github:
        return
    with container_context() as container:
        assert any(
            excepted_database_ready_string in str(line, "utf-8")
            for line in container.logs(stream=True)
        )
        assert _testing.database_ready(database=configKey)


def test_database_config_helper(monkeypatch):
    mock_tmp_dir = "some_folder"
    mock_config_key = "someDatabaseKey"
    mock_config_dict = {
        "drivername": "some_driver_name",
        "username": "some_username",
        "password": "some_password",
        "database": "some_db",
        "host": "some_host",
        "port": "1234",
        "alias": "some_alias",
        "docker_ct": {
            "name": "some_name",
            "image": "some_image",
            "ports": {1234: 5678},
        },
    }
    monkeypatch.setattr(
        _testing,
        "databaseConfig",
        {
            mock_config_key: mock_config_dict,
        },
    )

    monkeypatch.setattr(_testing, "TMP_DIR", "some_folder")

    assert (
        _testing.DatabaseConfigHelper.get_database_config(mock_config_key)
        == mock_config_dict
    )
    assert (
        _testing.DatabaseConfigHelper.get_database_url(mock_config_key)
        == "some_driver_name://some_username:some_password@some_host:1234/some_db"
    )
    assert _testing.DatabaseConfigHelper.get_tmp_dir() == mock_tmp_dir
