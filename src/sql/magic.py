import re
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.config.configurable import Configurable
from IPython.utils.traitlets import Bool, Int, Unicode
try:
    from pandas.core.frame import DataFrame, Series
except ImportError:
    DataFrame = None
    Series = None

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
    dsn_filename = Unicode('odbc.ini', config=True, help="Path to DSN file. \
                           When the first argument is of the form [section], \
                           a sqlalchemy connection string is formed from the \
                           matching section in the DSN file.")

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

        parsed = sql.parse.parse('%s\n%s' % (line, cell), self)
        conn = sql.connection.Connection.get(parsed['connection'])
        first_word = parsed['sql'].split(None, 1)[:1]
        if first_word and first_word[0].lower() == 'persist':
            return self._persist_dataframe(parsed['sql'], conn, user_ns)
        try:
            result = sql.run.run(conn, parsed['sql'], self, user_ns)
            return result
        except (ProgrammingError, OperationalError) as e:
            # Sqlite apparently return all errors as OperationalError :/
            if self.short_errors:
                print(e)
            else:
                raise

    legal_sql_identifier = re.compile(r'^[A-Za-z0-9#_$]+')
    def _persist_dataframe(self, raw, conn, user_ns):
        if not DataFrame:
            raise ImportError("Must `pip install pandas` to use DataFrames")
        pieces = raw.split()
        if len(pieces) != 2:
            raise SyntaxError("Format: %sql [connection] persist <DataFrameName>")
        frame_name = pieces[1].strip(';')
        frame = eval(frame_name, user_ns)
        if not isinstance(frame, DataFrame) and not isinstance(frame, Series):
            raise TypeError('%s is not a Pandas DataFrame or Series' % frame_name)
        table_name = frame_name.lower()
        table_name = self.legal_sql_identifier.search(table_name).group(0)
        frame.to_sql(table_name, conn.session.engine)
        return 'Persisted %s' % table_name


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(SqlMagic)
