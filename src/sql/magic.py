import sys

from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
from IPython.utils.traitlets import Instance, Bool, Int

import sql.connection
import sql.parse
import sql.run

def execute(line, cell='', config={}, magics=None):
    # save locals so they can be referenced in bind vars
    if magics:
        user_ns = magics.shell.user_ns
    else:
        user_ns = {}

    parsed = sql.parse.parse('%s\n%s' % (line, cell))
    conn = sql.connection.Connection.get(parsed['connection'])
    result = sql.run.run(conn, parsed['sql'], config, user_ns)
    return result    

@magics_class
class SQLMagics(Magics):
    """Runs SQL statement on a database, specified by SQLAlchemy connect string.
    
    Provides the %%sql magic."""
    
    @line_magic('sql')
    @cell_magic('sql')
    def execute(self, line, cell=''):
        """Runs SQL statement against a database, specified by SQLAlchemy connect string.
    
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
          
        SQLAlchemy connect string syntax examples:
            
          postgresql://me:mypw@localhost/mydb
          sqlite://
          mysql+pymysql://me:mypw@localhost/mydb
          
        """
        return execute(line, cell, self.shell.config.get('SqlMagic') or {}, self)

       
def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(SQLMagics)
