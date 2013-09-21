from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
from IPython.config.configurable import Configurable
from IPython.utils.traitlets import Int, Unicode

import sql.connection
import sql.parse
import sql.run



@magics_class
class SqlMagic(Magics, Configurable):
    """Runs SQL statement on a database, specified by SQLAlchemy connect string.
    
    Provides the %%sql magic."""

    autolimit = Int(0, config=True, help="Automatically limit the size of the returned result sets")
    style = Unicode('DEFAULT', config=True, help="Set the table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)")

    def __init__(self, shell):
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    
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
        # save locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns

        parsed = sql.parse.parse('%s\n%s' % (line, cell))
        conn = sql.connection.Connection.get(parsed['connection'])
        result = sql.run.run(conn, parsed['sql'], self, user_ns)
        return result

       
def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(SqlMagic)
