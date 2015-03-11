import sqlalchemy


class Connection(object):
    @classmethod
    def from_engine(cls, engine):
        # Call the constructor
        return cls(engine)

    @classmethod
    def from_str(cls, connect_str):
        # Create an engine, and pass it off
        engine = sqlalchemy.create_engine(connect_str)
        return cls.from_engine(engine)

    def __init__(self, engine, name=None):
        self.metadata = sqlalchemy.MetaData(bind=engine)
        self.session = engine.connect()
        self.name = '%s@%s' % (engine.url.username, engine.url.database)
        self.engine = engine


class ConnectionManager(object):
    def __init__(self):
        self._connections = {}
        self._current = None

    def get(self, handle):
        if not handle:
            assert self._current is not None
            return self._current

        try:
            return self._connections[handle]
        except KeyError:
            # Connection doesn't exist yet
            pass

        self.register(handle)
        self._current = self._connections[handle]
        return self._current

    def register(self, descriptor, name=None):
        conn = None
        cxn_string = None  # raw connection string, may contain password

        if isinstance(descriptor, sqlalchemy.engine.base.Engine):
            conn = Connection.from_engine(descriptor)

        elif isinstance(descriptor, basestring):
            cxn_string = descriptor
            conn = Connection.from_str(descriptor)

        elif isinstance(descriptor, Connection):
            pass

        assert isinstance(conn, Connection)

        # there are potentially 3 names for a giveen connection
        # 1) the short name '{user}@{database}'  (always exists)
        # 2) the raw connection string (if provided)
        # 3) a manually specified name (if provided)

        names = [conn.name]  # short name (1)
        if cxn_string:  # connection string (2)
            names.append(cxn_string)
        if name:  # manually specified name (3)
            names.append(name)

        for name in names:
            self._connections[name] = conn

    def unregister(self, descriptor):
        if isinstance(descriptor, sqlalchemy.engine.base.Engine):
            return self.unregister_engine(self, descriptor)
        elif isinstance(descriptor, Connection):
            return self.unregister_connection(descriptor)
        return self.unregister_name(self, descriptor)

    def unregister_engine(self, engine):
        names = [name for name, conn in self._connections.items()
                 if conn.engine == engine]
        for n in names:
            self._connections.pop(n)

    def unregister_connection(self, conn):
        names = [name for name, c in self._connections.items()
                 if c == conn]
        for n in names:
            self._connections.pop(n)

    def unregister_name(self, name):
        conn = self._connections[name]
        return self.unregister_connection(conn)


MANAGER = ConnectionManager()


def get_connection(descriptor):
    return MANAGER.get(descriptor)


def register(connection, name=None):
    return MANAGER.register(connection, name)


def unregister(connection):
    return MANAGER.unregister(connection)
