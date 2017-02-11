import re

from IPython.core.magic import Magics, magics_class, line_cell_magic, needs_local_scope
try:
    from traitlets.config.configurable import Configurable
    from traitlets import Bool, Int, Unicode
except ImportError:
    from IPython.config.configurable import Configurable
    from IPython.utils.traitlets import Bool, Int, Unicode
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

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
    displaylimit = Int(0, config=True, help="Automatically limit the number of rows displayed (full result set is still stored)")
    autopandas = Bool(False, config=True, help="Return Pandas DataFrames instead of regular result sets")
    column_local_vars = Bool(False, config=True, help="Return data into local variables from column names")
    feedback = Bool(True, config=True, help="Print number of rows affected by DML")
    dsn_filename = Unicode('odbc.ini', config=True, help="Path to DSN file. "
                           "When the first argument is of the form [section], "
                           "a sqlalchemy connection string is formed from the "
                           "matching section in the DSN file.")


    def __init__(self, shell):
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        # Add ourself to the list of module configurable via %config
        self.shell.configurables.append(self)

    @line_cell_magic('sql')
    @magic_arguments()
    @argument('line', default='', nargs='*', type=str, help='sql')
    @argument('-connections', action='store_true', help="list active connections")
    @argument('-close', type=str, help="close a session by name")
    @argument('-creator', type=str, help="specify creator function for new connection")
    @argument('-section', type=str, help="section of dsn_file to be used for generating a connection string")
    @argument('-persist', action='store_true', help="create a table name in the database from the named DataFrame")
    @argument('-s', '--store', type=str, help="store result in given variable without printing")
    @argument('-sp', type=str, help="store result in given variable and print")
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
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        args = parse_argstring(self.execute, line)
        line = ' '.join(args.line)
        print("Line: {}".format(line))
        print("Cell: {}".format(cell))
        print("Args: {}".format(args))
        if '@' in line or '://' in line:
            connection = line
            query = cell
        else:
            connection = ''
            query = '\n'.join((line, cell))
        print("Query: {}".format(query))
        if args.connections:
            return sql.connection.Connection.connections
        elif args.close:
            return sql.connection.Connection._close(args.close)

        if args.section:
            connection = connection_from_dsn_section(args.section, self)
        elif args.creator:
            args.creator = user_ns[args.creator]

        conn = sql.connection.Connection.get(connection, creator=args.creator)
        if args.persist:
            return self._persist_dataframe(query, conn, user_ns)
        if query:
            return self._execute(conn, query, user_ns, args)

    def _execute(self, conn, query, user_ns, args):
        try:
            result = sql.run.run(conn, query, self, user_ns)

            if result is not None and not isinstance(result, str) and self.column_local_vars:
                #Instead of returning values, set variables directly in the
                #users namespace. Variable names given by column names

                if self.autopandas:
                    keys = result.keys()
                else:
                    keys = result.keys
                    result = result.dict()

                if self.feedback:
                    print('Returning data to local variables [{}]'.format(
                        ', '.join(keys)))

                self.shell.user_ns.update(result)

                return None
            else:

                if args.store:
                    result_var = args.store
                    print("Returning data to local variable {}".format(result_var))
                    self.shell.user_ns.update({result_var: result})
                    return None

                #Return results into the default ipython _ variable
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
        if not raw:
            raise SyntaxError("Format: %sql [connection] persist <DataFrameName>")
        frame_name = raw.strip(';')
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
