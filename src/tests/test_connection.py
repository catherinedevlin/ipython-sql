import sys
from unittest.mock import Mock

from sqlalchemy.engine import Engine

from sql.connection import Connection


def test_password_isnt_displayed(monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg2", Mock())
    monkeypatch.setattr(Engine, "connect", Mock())

    Connection.from_connect_str("postgresql://user:topsecret@somedomain.com/db")

    assert "topsecret" not in Connection.connection_list()
