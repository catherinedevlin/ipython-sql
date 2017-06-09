import sqlalchemy
import os

class ConnectionError(Exception):
    pass


class Connection(object):
    current = None
    connections = {}

    @classmethod
    def tell_format(cls):
        return """Connection info needed in SQLAlchemy format, example:
               postgresql://username:password@hostname/dbname
               or an existing connection: %s""" % str(cls.connections.keys())

    def __init__(self, connect_str=None):
        try:
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
    def set(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = cls.connections.get(descriptor) or \
                           cls.connections.get(descriptor.lower())
            cls.current = existing or Connection(descriptor)
        else:
            if cls.connections:
                print(cls.connection_list())
            else:
                if os.getenv('DATABASE_URL'):
                    cls.current = Connection(os.getenv('DATABASE_URL'))
                else:
                    raise ConnectionError('Environment variable $DATABASE_URL not set, and no connect string given.')
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        core_name = '%s@%s' % (engine.url.username or '', engine.url.database)
        incrementer = 1
        name = core_name
        while name in cls.connections:
            name = '%s_%d' % (core_name, incrementer)
            incrementer += 1
        return name

    @classmethod
    def connection_list(cls):
        result = []
        for key in sorted(cls.connections):
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(key))
        return '\n'.join(result)
