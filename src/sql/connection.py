import sqlalchemy

class Connection(object):
    current = None
    connections = {}
    @classmethod
    def tell_format(cls):
        return "Format: (postgresql|mysql)://username:password@hostname/dbname, or one of %s" \
               % str(cls.connections.keys())
    def __init__(self, connect_str=None, creator=None):
        try:
            if creator:
                engine = sqlalchemy.create_engine(connect_str, creator=creator)
            else:
                engine = sqlalchemy.create_engine(connect_str)
        except: # TODO: bare except; but what's an ArgumentError?
            print(self.tell_format())
            raise
        self.dialect = engine.url.get_dialect()
        self.metadata = sqlalchemy.MetaData(bind=engine)
        self.name = self.assign_name(engine)
        self.session = engine.connect()
        self.connections[self.name] = self
        self.connections[str(self.metadata.bind.url)] = self
        Connection.current = self

    @classmethod
    def get(cls, descriptor, creator=None):
        if isinstance(descriptor, Connection):
            cls.current = descriptor
        elif descriptor:
            conn = cls.connections.get(descriptor) or \
                   cls.connections.get(descriptor.lower())
            if conn:
                cls.current = conn
            else:
                # http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#custom-dbapi-connect-arguments
                cls.current = Connection(descriptor, creator)
        if cls.current:
            return cls.current
        else:
            raise Exception(cls.tell_format())
    @classmethod
    def assign_name(cls, engine):
        core_name = '%s@%s' % (engine.url.username, engine.url.database)
        incrementer = 1
        name = core_name
        while name in cls.connections:
            name = '%s_%d' % (core_name, incrementer)
            incrementer += 1
        return name

    @classmethod
    def _close(cls, descriptor):
        if isinstance(descriptor, Connection):
            conn = descriptor
        else:
            conn = cls.connections.get(descriptor) or \
                   cls.connections.get(descriptor.lower())
        if not conn:
            raise Exception("Could not close connection because it was not found amongst these: %s" \
                %str(cls.connections.keys()))
        cls.connections.pop(conn.name)
        cls.connections.pop(str(conn.metadata.bind.url))
        conn.session.close()

    def close(self):
        self.__class__._close(self)


