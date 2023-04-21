import os
from difflib import get_close_matches

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchModuleError
from IPython.core.error import UsageError
import difflib
import sqlglot
from sql.store import store
from sql.telemetry import telemetry

PLOOMBER_SUPPORT_LINK_STR = (
    "For technical support: https://ploomber.io/community"
    "\nDocumentation: https://jupysql.ploomber.io/en/latest/connecting.html"
)
IS_SQLALCHEMY_ONE = int(sqlalchemy.__version__.split(".")[0]) == 1

# Check Full List: https://docs.sqlalchemy.org/en/20/dialects
MISSING_PACKAGE_LIST_EXCEPT_MATCHERS = {
    # SQLite
    "sqlite": "sqlite",
    "pysqlcipher3": "pysqlcipher3",
    # DuckDB
    "duckdb": "duckdb-engine",
    # MySQL + MariaDB
    "pymysql": "pymysql",
    "mysqldb": "mysqlclient",
    "mariadb": "mariadb",
    "mysql": "mysql-connector-python",
    "asyncmy": "asyncmy",
    "aiomysql": "aiomysql",
    "cymysql": "cymysql",
    "pyodbc": "pyodbc",
    # PostgreSQL
    "psycopg2": "psycopg2",
    "psycopg": "psycopg",
    "pg8000": "pg8000",
    "asyncpg": "asyncpg",
    "psycopg2cffi": "psycopg2cffi",
    # Oracle
    "cx_oracle": "cx_oracle",
    "oracledb": "oracledb",
    # MSSQL
    "pyodbc": "pyodbc",
    "pymssql": "pymssql",
}

DIALECT_NAME_SQLALCHEMY_TO_SQLGLOT_MAPPING = {"postgresql": "postgres", "mssql": "tsql"}


def extract_module_name_from_ModuleNotFoundError(e):
    return e.name


def extract_module_name_from_NoSuchModuleError(e):
    return str(e).split(":")[-1].split(".")[-1]


"""
When there is ModuleNotFoundError or NoSuchModuleError case
Three types of suggestions will be shown when the missing module name is:
1. Excepted in the pre-defined map, suggest the user to install the driver pkg
2. Closely matched to the pre-defined map, suggest the user to type correct driver name
3. Not found in the pre-defined map, suggest user to use valid driver pkg
"""


def get_missing_package_suggestion_str(e):
    suggestion_prefix = "To fix it, "
    module_name = None
    if isinstance(e, ModuleNotFoundError):
        module_name = extract_module_name_from_ModuleNotFoundError(e)
    elif isinstance(e, NoSuchModuleError):
        module_name = extract_module_name_from_NoSuchModuleError(e)

    module_name = module_name.lower()
    # Excepted
    for matcher, suggested_package in MISSING_PACKAGE_LIST_EXCEPT_MATCHERS.items():
        if matcher == module_name:
            return suggestion_prefix + "try to install package: " + suggested_package
    # Closely matched
    close_matches = difflib.get_close_matches(
        module_name, MISSING_PACKAGE_LIST_EXCEPT_MATCHERS.keys()
    )
    if close_matches:
        return f'Perhaps you meant to use driver the dialect: "{close_matches[0]}"'
    # Not found
    return (
        suggestion_prefix + "make sure you are using correct driver name:\n"
        "Ref: https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls"
    )


def rough_dict_get(dct, sought, default=None):
    """
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    """

    sought = sought.split("@")
    for key, val in dct.items():
        if not any(s.lower() not in key.lower() for s in sought):
            return val
    return default


class Connection:
    """Manages connections to databases

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        The SQLAlchemy engine to use
    """

    # the active connection
    current = None

    # all connections
    connections = {}

    @classmethod
    def _suggest_fix_no_module_found(module_name):
        DEFAULT_PREFIX = "\n\n"

        prefix = DEFAULT_PREFIX
        suffix = "To fix it:"
        suggest_str = "Install X package and try again"
        options = [f"{prefix}{suffix}", suggest_str]
        return "\n\n".join(options)

    @classmethod
    def _suggest_fix(cls, env_var, connect_str=None):
        """
        Returns an error message that we can display to the user
        to tell them how to pass the connection string
        """
        DEFAULT_PREFIX = "\n\n"

        if connect_str:
            matches = get_close_matches(connect_str, list(cls.connections), n=1)

            if matches:
                prefix = (
                    "\n\nPerhaps you meant to use the existing "
                    f"connection: %sql {matches[0]!r}?\n\n"
                )

            else:
                prefix = DEFAULT_PREFIX
        else:
            matches = None
            prefix = DEFAULT_PREFIX

        connection_string = (
            "Pass a valid connection string:\n    "
            "Example: %sql postgresql://username:password@hostname/dbname"
        )

        suffix = "To fix it:" if not matches else "Otherwise, try the following:"
        options = [f"{prefix}{suffix}", connection_string]

        keys = list(cls.connections.keys())

        if keys:
            keys_ = ",".join(repr(k) for k in keys)
            options.append(
                f"Pass a connection key (one of: {keys_})"
                f"\n    Example: %sql {keys[0]!r}"
            )

        if env_var:
            options.append("Set the environment variable $DATABASE_URL")

        if len(options) >= 3:
            options.insert(-1, "OR")

        options.append(PLOOMBER_SUPPORT_LINK_STR)

        return "\n\n".join(options)

    @classmethod
    def _error_no_connection(cls):
        """Error when there isn't any connection"""
        return UsageError("No active connection." + cls._suggest_fix(env_var=True))

    @classmethod
    def _error_invalid_connection_info(cls, e, connect_str):
        return UsageError(
            "An error happened while creating the connection: "
            f"{e}.{cls._suggest_fix(env_var=False, connect_str=connect_str)}"
        )

    @classmethod
    def _error_module_not_found(cls, e):
        return ModuleNotFoundError("test")

    def __init__(self, engine, alias=None):
        self.url = engine.url
        self.name = self.assign_name(engine)
        self.dialect = self.url.get_dialect()
        self.session = engine.connect()

        if IS_SQLALCHEMY_ONE:
            self.metadata = sqlalchemy.MetaData(bind=engine)

        self.connections[
            alias
            or (
                repr(sqlalchemy.MetaData(bind=engine).bind.url)
                if IS_SQLALCHEMY_ONE
                else repr(engine.url)
            )
        ] = self

        self.connect_args = None
        self.alias = alias
        Connection.current = self

    @classmethod
    def from_connect_str(
        cls, connect_str=None, connect_args=None, creator=None, alias=None
    ):
        """Creates a new connection from a connection string"""
        connect_args = connect_args or {}

        try:
            if creator:
                engine = sqlalchemy.create_engine(
                    connect_str,
                    connect_args=connect_args,
                    creator=creator,
                )
            else:
                engine = sqlalchemy.create_engine(
                    connect_str,
                    connect_args=connect_args,
                )
        except (ModuleNotFoundError, NoSuchModuleError) as e:
            suggestion_str = get_missing_package_suggestion_str(e)
            raise UsageError(
                "\n\n".join(
                    [
                        str(e),
                        suggestion_str,
                        PLOOMBER_SUPPORT_LINK_STR,
                    ]
                )
            ) from e
        except Exception as e:
            raise cls._error_invalid_connection_info(e, connect_str) from e

        connection = cls(engine, alias=alias)
        connection.connect_args = connect_args

        return connection

    @classmethod
    def set(cls, descriptor, displaycon, connect_args=None, creator=None, alias=None):
        """
        Set the current database connection. This method is called from the magic to
        determine which connection to use (either use an existing one or open a new one)
        """
        connect_args = connect_args or {}

        if descriptor:
            is_custom_connection_ = Connection.is_custom_connection(descriptor)

            if isinstance(descriptor, Connection):
                cls.current = descriptor
            elif isinstance(descriptor, Engine):
                cls.current = Connection(descriptor)
            elif is_custom_connection_:
                cls.current = CustomConnection(descriptor, alias=alias)
            else:
                existing = rough_dict_get(cls.connections, descriptor)

                # NOTE: I added one indentation level, otherwise
                # the "existing" variable would not exist if
                # passing an engine object as descriptor.
                # Since I never saw this breaking, my guess
                # is that we're missing some unit tests
                # when descriptor is a connection object
                # http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#custom-dbapi-connect-arguments # noqa
                cls.current = existing or Connection.from_connect_str(
                    connect_str=descriptor,
                    connect_args=connect_args,
                    creator=creator,
                    alias=alias,
                )

        else:
            if cls.connections:
                if displaycon:
                    # display list of connections
                    print(cls.connection_list())
            elif os.getenv("DATABASE_URL"):
                cls.current = Connection.from_connect_str(
                    connect_str=os.getenv("DATABASE_URL"),
                    connect_args=connect_args,
                    creator=creator,
                    alias=alias,
                )
            else:
                raise cls._error_no_connection()

        return cls.current

    @classmethod
    def assign_name(cls, engine):
        name = "%s@%s" % (engine.url.username or "", engine.url.database)
        return name

    @classmethod
    def connection_list(cls):
        """Returns the list of connections, appending '*' to the current one"""
        result = []
        for key in sorted(cls.connections):
            conn = cls.connections[key]

            if cls.is_custom_connection(conn):
                engine_url = conn.url
            else:
                engine_url = conn.metadata.bind.url if IS_SQLALCHEMY_ONE else conn.url

            prefix = "* " if conn == cls.current else "  "

            if conn.alias:
                repr_ = f"{prefix} ({conn.alias}) {engine_url!r}"
            else:
                repr_ = f"{prefix} {engine_url!r}"

            result.append(repr_)

        return "\n".join(result)

    @classmethod
    def close(cls, descriptor):
        if isinstance(descriptor, Connection):
            conn = descriptor
        else:
            conn = cls.connections.get(descriptor) or cls.connections.get(
                descriptor.lower()
            )
        if not conn:
            raise Exception(
                "Could not close connection because it was not found amongst these: %s"
                % str(cls.connections.keys())
            )

        if descriptor in cls.connections:
            cls.connections.pop(descriptor)
        else:
            cls.connections.pop(
                str(conn.metadata.bind.url) if IS_SQLALCHEMY_ONE else str(conn.url)
            )
            conn.session.close()

    def is_custom_connection(conn=None) -> bool:
        """
        Checks if given connection is custom
        """
        is_custom_connection_ = False

        if conn is None:
            if not Connection.current:
                raise RuntimeError("No active connection")
            else:
                conn = Connection.current.session

        if isinstance(conn, (CustomConnection, CustomSession)):
            is_custom_connection_ = True
        else:
            # TODO: Better check when user passes a custom
            # connection
            if (
                isinstance(
                    conn, (sqlalchemy.engine.base.Connection, Connection, str, bool)
                )
                or conn.__class__.__name__ == "DataFrame"
            ):
                is_custom_connection_ = False
            else:
                is_custom_connection_ = True

        return is_custom_connection_

    def _get_curr_sqlalchemy_connection_info(self):
        """Get the dialect, driver, and database server version info of current
        connected dialect

        Returns
        -------
        dict
            The dictionary which contains the SQLAlchemy-based dialect
            information, or None if there is no current connection.
        """

        if not self.session:
            return None

        try:
            engine = self.metadata.bind if IS_SQLALCHEMY_ONE else self.session
        except Exception:
            engine = self.session

        return {
            "dialect": getattr(engine.dialect, "name", None),
            "driver": getattr(engine.dialect, "driver", None),
            "server_version_info": getattr(engine.dialect, "server_version_info", None),
        }

    def _get_curr_sqlglot_dialect(self):
        """Get the dialect name in sqlglot package scope

        Returns
        -------
        str
            Available dialect in sqlglot package, see more:
            https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py
        """
        connection_info = self._get_curr_sqlalchemy_connection_info()
        if not connection_info:
            return None

        return DIALECT_NAME_SQLALCHEMY_TO_SQLGLOT_MAPPING.get(
            connection_info["dialect"], connection_info["dialect"]
        )

    def is_use_backtick_template(self):
        """Get if the dialect support backtick (`) syntax as identifier

        Returns
        -------
        bool
            Indicate if the dialect can use backtick identifier in the SQL clause
        """
        cur_dialect = self._get_curr_sqlglot_dialect()
        if not cur_dialect:
            return False
        try:
            return (
                "`" in sqlglot.Dialect.get_or_raise(cur_dialect).Tokenizer.IDENTIFIERS
            )
        except (ValueError, AttributeError, TypeError):
            return False

    def get_curr_identifiers(self) -> list:
        """
        Returns list of identifiers for current connection

        Default identifiers are : ["", '"']
        """
        identifiers = ["", '"']
        try:
            connection_info = self._get_curr_sqlalchemy_connection_info()
            if connection_info:
                cur_dialect = connection_info["dialect"]
                identifiers_ = sqlglot.Dialect.get_or_raise(
                    cur_dialect
                ).Tokenizer.IDENTIFIERS

                identifiers = [*set(identifiers + identifiers_)]
        except ValueError:
            pass
        except AttributeError:
            # this might be a custom connection..
            pass

        return identifiers

    def _transpile_query(self, query):
        """Translate the given SQL clause that's compatible to current connected
        dialect by sqlglot

        Parameters
        ----------
        query : str
            Original SQL clause

        Returns
        -------
        str
            SQL clause that's compatible to current connected dialect
        """
        write_dialect = self._get_curr_sqlglot_dialect()
        try:
            query = sqlglot.parse_one(query).sql(dialect=write_dialect)
        finally:
            return query

    def _prepare_query(self, query, with_=None) -> str:
        """
        Returns a textual representation of a query based
        on the current connection

        Parameters
        ----------
        query : str
            SQL query

        with_ : string, default None
            The key to use in with sql clause
        """
        if with_:
            query = str(store.render(query, with_=with_))

        query = self._transpile_query(query)

        if self.is_custom_connection():
            query = str(query)
        else:
            query = sqlalchemy.sql.text(query)

        return query

    def execute(self, query, with_=None):
        """
        Executes SQL query on a given connection
        """
        query = self._prepare_query(query, with_)
        return self.session.execute(query)


class CustomSession(sqlalchemy.engine.base.Connection):
    """
    Custom sql alchemy session
    """

    def __init__(self, connection, engine):
        self.engine = engine
        self.dialect = dict(
            {
                "name": connection.dialect,
                "driver": connection.dialect,
                "server_version_info": connection.dialect,
            }
        )

    def execute(self, query):
        cur = self.engine.cursor()
        cur.execute(query)
        return cur


class CustomConnection(Connection):
    """
    Custom connection for unsupported drivers in sqlalchemy
    """

    @telemetry.log_call("CustomConnection", payload=True)
    def __init__(self, payload, engine=None, alias=None):
        try:
            payload["engine"] = type(engine)
        except Exception as e:
            payload["engine_parsing_error"] = str(e)

        if engine is None:
            raise ValueError("Engine cannot be None")

        connection_name_ = "custom_driver"
        self.url = str(engine)
        self.name = connection_name_
        self.dialect = connection_name_
        self.session = CustomSession(self, engine)

        self.connections[alias or connection_name_] = self

        self.connect_args = None
        self.alias = alias
        Connection.current = self
