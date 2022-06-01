import os
import re

import sqlalchemy


class ConnectionError(Exception):
    pass


def rough_dict_get(dct, sought, default=None):
    """
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    """

    sought = sought.split("@")
    for (key, val) in dct.items():
        if not any(s.lower() not in key.lower() for s in sought):
            return val
    return default


class Connection(object):
    current = None
    connections = {}

    @classmethod
    def tell_format(cls):
        return """Connection info needed in SQLAlchemy format, example:
               postgresql://username:password@hostname/dbname
               or an existing connection: %s""" % str(
            cls.connections.keys()
        )

    def __init__(
        self,
        connect_str=None,
        connect_args={},
        creator=None,
        name=None,
        connect_args_are_create_engine_kwargs=False,
    ):
        if name and not name.startswith("@"):
            raise ValueError("name must start with @")

        try:
            if creator:
                if connect_args_are_create_engine_kwargs:
                    self._engine = sqlalchemy.create_engine(
                        connect_str, creator=creator, **connect_args
                    )
                else:
                    self._engine = sqlalchemy.create_engine(
                        connect_str, connect_args=connect_args, creator=creator
                    )
            else:
                if connect_args_are_create_engine_kwargs:
                    self._engine = sqlalchemy.create_engine(connect_str, **connect_args)
                else:
                    self._engine = sqlalchemy.create_engine(
                        connect_str, connect_args=connect_args
                    )
        except:  # TODO: bare except; but what's an ArgumentError?
            print(self.tell_format())
            raise
        self.dialect = self._engine.url.get_dialect()
        self.metadata = sqlalchemy.MetaData(bind=self._engine)
        self.name = name or self.assign_name(self._engine)
        self._session = None
        self.connections[name or repr(self.metadata.bind.url)] = self
        self.connect_args = connect_args
        Connection.current = self

    @property
    def session(self):
        """Lazily connect to the database."""

        if not self._session:
            self._session = self._engine.connect()
        return self._session

    @classmethod
    def set(
        cls,
        descriptor,
        displaycon,
        connect_args={},
        creator=None,
        name=None,
        connect_args_are_create_engine_kwargs=False,
    ):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = rough_dict_get(cls.connections, descriptor)
                # http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#custom-dbapi-connect-arguments
                cls.current = existing or Connection(
                    descriptor,
                    connect_args,
                    creator,
                    name,
                    connect_args_are_create_engine_kwargs,
                )
        else:
            if cls.connections:
                if displaycon:
                    print(cls.connection_list())
            else:
                if os.getenv("DATABASE_URL"):
                    cls.current = Connection(
                        os.getenv("DATABASE_URL"), connect_args, creator
                    )
                else:
                    raise ConnectionError(
                        "Environment variable $DATABASE_URL not set, and no connect string given."
                    )
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        name = "%s@%s" % (engine.url.username or "", engine.url.database)
        return name

    @classmethod
    def connection_list(cls):
        result = []
        for key in sorted(cls.connections):
            engine_url = cls.connections[
                key
            ].metadata.bind.url  # type: sqlalchemy.engine.url.URL
            if cls.connections[key] == cls.current:
                template = " * {}"
            else:
                template = "   {}"
            result.append(template.format(engine_url.__repr__()))
        return "\n".join(result)

    def _close(cls, descriptor):
        if isinstance(descriptor, Connection):
            conn = descriptor
        else:
            conn = cls.connections.get(descriptor) or cls.connections.get(
                descriptor.lower()
            )
        if not conn:
            raise Exception(
                "Could not close connection because it was not found amongst these: %s"
                % str(cls.connections.keys())
            )
        cls.connections.pop(conn.name, None)
        cls.connections.pop(str(conn.metadata.bind.url), None)
        conn.session.close()

    def close(self):
        self.__class__._close(self)
