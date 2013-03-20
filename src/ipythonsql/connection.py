import sqlalchemy

connections = {} 
_connection = None

class Connection(object):
    def __init__(self, connect_str=None):
        try:
            engine = sqlalchemy.create_engine(connect_str)
        except: # TODO: bare except; but what's an ArgumentError?
            print("Format: (postgresql|mysql)://username:password@hostname/dbname, or one of %s" %
                   str(connections.keys()))       
            raise
        self.metadata = sqlalchemy.MetaData(bind=engine)
        self.name = assign_name(engine)
        self.session = engine.connect() 
        connections[self.name] = self
        _connection = self
    
def connection(descriptor):
    result = connections.get(descriptor.lower())
    if result:
        return result
    elif _connection:
        return _connection
    else:
        return Connection(descriptor)

def assign_name(engine):
    core_name = '%s@%s' % (engine.url.username, engine.url.database)
    incrementer = 1
    name = core_name
    while name in connections:
        name = '%s_%d' % (core_name, incrementer)
        incrementer += 1
    return name
