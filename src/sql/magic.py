from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.config.configurable import Configurable
from IPython.utils.traitlets import Bool, Int, Unicode

from sqlalchemy.exc import ProgrammingError, OperationalError

import sql.connection
import sql.parse
import sql.run


@magics_class
class SqlMagic(Magics, Configurable):
    """Runs SQL statement on a database, specified by SQLAlchemy connect string.

    Provides the %%sql magic."""

    autolimit = Int(0, config=True, help="Automatically limit the size of the returned result sets")
    style = Unicode('DEFAULT', config=True, help="Set the table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)")
    short_errors = Bool(True, config=True, help="Don't display the full traceback on SQL Programming Error")
    displaylimit = Int(0, config=True, help="Automatic,ally limit the number of rows displayed (full result set is still stored)")
    autopandas = Bool(False, config=True, help="Return Pandas DataFrames instead of regular result sets")
    feedback = Bool(True, config=True, help="Print number of rows affected by DML")

    def __init__(self, shell):
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    @needs_local_scope
    @line_magic('sql')
    @cell_magic('sql')
    def execute(self, line, cell='', local_ns={}):
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
        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns
        user_ns.update(local_ns)

        parsed = sql.parse.parse('%s\n%s' % (line, cell))
        conn = sql.connection.Connection.get(parsed['connection'])
        try:
            result = sql.run.run(conn, parsed['sql'], self, user_ns)
            return result
        except (ProgrammingError, OperationalError) as e:
            # Sqlite apparently return all errors as OperationalError :/
            if self.short_errors:
                print(e)
            else:
                raise


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(SqlMagic)
