from ConfigParser import ConfigParser
from sqlalchemy.engine.url import URL


def parse(cell, config):
    parts = [part.strip() for part in cell.split(None, 1)]
    if not parts:
        return {'connection': '', 'sql': ''}
    if parts[0].startswith('[') and parts[0].endswith(']'):
        parser = ConfigParser()
        parser.read(config.dsn_filename)
        section = parts[0].lstrip('[').rstrip(']')
        connection = str(URL(drivername=parser.get(section, 'drivername'),
                             username=parser.get(section, 'username'),
                             password=parser.get(section, 'password'),
                             host=parser.get(section, 'host'),
                             database=parser.get(section, 'database')))
        sql = parts[1] if len(parts) > 1 else ''
    elif '@' in parts[0] or '://' in parts[0]:
        connection = parts[0]
        if len(parts) > 1:
            sql = parts[1]
        else:
            sql = ''
    else:
        connection = ''
        sql = cell
    return {'connection': connection.strip(),
            'sql': sql.strip()
            }
   