from six import moves
from sqlalchemy.engine.url import URL


def connection_from_dsn_section(section, config):
    parser = moves.ConfigParser()
    parser.read(config.dsn_filename)
    cfg_dict = dict(parser.items(section))
    return str(URL(**cfg_dict))

