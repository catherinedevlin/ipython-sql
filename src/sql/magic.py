import sys

from IPython.core.magic import Magics, magics_class, cell_magic 
from IPython.core.plugin import Plugin
from IPython.utils.traitlets import Bool, Instance
from IPython.zmq import displayhook
from IPython.core import displaypub
import ipy_table
import texttable

import connection
import parse
import run

@magics_class
class SQLMagics(Magics):

    @cell_magic('sql')
    def execute(self, line, cell):
        """
        Run SQL
        If no database connection has been established, first word
        should be a SQLAlchemy connection string, or the user@db name
        of an established connection.

        Examples::

          %%sql postgresql://me:mypw@localhost/mydb
          SELECT * FROM mytable

          %%sql me@mydb
          DELETE FROM mytable
          
          %%sql
          DROP TABLE mytable
          
        """
        parsed = parse.parse('%s\n%s' % (line, cell))
        conn = connection.Connection.get(parsed['connection'])
        result = run.run(conn, parsed['sql'])
        return result
    
class SQLMagic(Plugin):
    shell = Instance('IPython.core.interactiveshell.InteractiveShellABC')
    
    def __init__(self, shell, config):
        super(SQLMagic, self).__init__(shell=shell, config=config)
        shell.register_magics(SQLMagics)
        
_loaded = False

def load_ipython_extension(ip):
    """Load the extension in IPython."""
    global _loaded
    if not _loaded:
        plugin = SQLMagic(shell=ip, config=ip.config)
        ip.plugin_manager.register_plugin('sqlmagic', plugin)
        _loaded = True    
