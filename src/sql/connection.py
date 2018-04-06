import sqlalchemy
import os
import re

class ConnectionError(Exception):
    pass


def rough_dict_get(dct, sought, default=None):
    '''
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    '''
  
    sought = sought.split('@') 
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
        self.connections[repr(self.metadata.bind.url)] = self
        Connection.current = self

    @classmethod
    def set(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = rough_dict_get(cls.connections, descriptor)
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
        name = '%s@%s' % (engine.url.username or '', engine.url.database)
        return name

    @classmethod
    def connection_list(cls):
        result = []
        for key in sorted(cls.connections):
            engine_url = cls.connections[key].metadata.bind.url # type: sqlalchemy.engine.url.URL
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(engine_url.__repr__()))
        return '\n'.join(result)
