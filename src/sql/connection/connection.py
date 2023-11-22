import warnings
import difflib
import abc
import os
from difflib import get_close_matches
import atexit
from functools import partial

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.exc import (
    NoSuchModuleError,
    OperationalError,
    StatementError,
    PendingRollbackError,
    InternalError,
    ProgrammingError,
)
from IPython.core.error import UsageError
import sqlglot
import sqlparse
from ploomber_core.exceptions import modify_exceptions


from sql.store import store
from sql.telemetry import telemetry
from sql import exceptions, display
from sql.error_handler import handle_exception
from sql.parse import (
    escape_string_literals_with_colon_prefix,
    find_named_parameters,
    ConnectionsFile,
)
from sql.warnings import JupySQLQuotedNamedParametersWarning, JupySQLRollbackPerformed
from sql import _current
from sql.connection import error_handling

BASE_DOC_URL = "https://jupysql.ploomber.io/en/latest"


PLOOMBER_DOCS_LINK_STR = f"{BASE_DOC_URL}/connecting.html"

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
    # snowflake
    "snowflake": "snowflake-sqlalchemy",
}

BASE_DRIVERS_URL = f"{BASE_DOC_URL}/howto/db-drivers.html"

DBNAME_2_DOC_LINK = {
    "psycopg2": f"{BASE_DRIVERS_URL}#postgresql",
    "duckdb": f"{BASE_DRIVERS_URL}#duckdb",
}

DIALECT_NAME_SQLALCHEMY_TO_SQLGLOT_MAPPING = {"postgresql": "postgres", "mssql": "tsql"}

# All the DBs and their respective documentation links
DB_DOCS_LINKS = {
    "duckdb": f"{BASE_DOC_URL}/integrations/duckdb.html",
    "mysql": f"{BASE_DOC_URL}/integrations/mysql.html",
    "mssql": f"{BASE_DOC_URL}/integrations/mssql.html",
    "mariadb": f"{BASE_DOC_URL}/integrations/mariadb.html",
    "clickhouse": f"{BASE_DOC_URL}/integrations/clickhouse.html",
    "postgresql": f"{BASE_DOC_URL}/integrations/postgres-connect.html",
    "questdb": f"{BASE_DOC_URL}/integrations/questdb.html",
}


def extract_module_name_from_ModuleNotFoundError(e):
    return e.name


def extract_module_name_from_NoSuchModuleError(e):
    return str(e).split(":")[-1].split(".")[-1]


class ResultSetCollection:
    def __init__(self) -> None:
        self._result_sets = []

    def append(self, result):
        if result in self._result_sets:
            self._result_sets.remove(result)

        self._result_sets.append(result)

    def is_last(self, result):
        # if there are no results, return True to prevent triggering
        # a query in the database
        if not len(self._result_sets):
            return True

        return self._result_sets[-1] is result

    def close_all(self):
        for r in self._result_sets:
            r.close()

        self._result_sets = []

    def __iter__(self):
        return iter(self._result_sets)

    def __len__(self):
        return len(self._result_sets)


def get_missing_package_suggestion_str(e):
    """Provide a better error when a user tries to connect to a database but they're
    missing the database driver
    """
    suggestion_prefix = "To fix it, "

    module_name = None

    if isinstance(e, ModuleNotFoundError):
        module_name = extract_module_name_from_ModuleNotFoundError(e)
    elif isinstance(e, NoSuchModuleError):
        module_name = extract_module_name_from_NoSuchModuleError(e)

    module_name = module_name.lower()

    error_message = (
        suggestion_prefix + "make sure you are using correct driver name:\n"
        "Ref: https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls"
    )

    # Exact match
    suggested_package = MISSING_PACKAGE_LIST_EXCEPT_MATCHERS.get(module_name)

    if suggested_package:
        error_message = (
            suggestion_prefix
            + "run this in your notebook: "
            + error_handling.install_command(suggested_package)
        )
    else:
        # Closely matched
        close_matches = difflib.get_close_matches(
            module_name, MISSING_PACKAGE_LIST_EXCEPT_MATCHERS.keys()
        )

        if close_matches:
            error_message = (
                f'Perhaps you meant to use driver the dialect: "{close_matches[0]}"'
            )

    error_suffix = (
        DBNAME_2_DOC_LINK.get(module_name)
        if DBNAME_2_DOC_LINK.get(module_name)
        else PLOOMBER_DOCS_LINK_STR
    )

    return error_message + "\n\nFor more details, see: " + error_suffix


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


def _error_invalid_connection_info(e, connect_str):
    err = UsageError(
        "An error happened while creating the connection: "
        f"{e}.{_suggest_fix(env_var=False, connect_str=connect_str)}"
    )
    err.modify_exception = True
    return err


class ConnectionManager:
    """A class to manage and create database connections"""

    # all connections
    connections = {}

    # the active connection
    current = None

    @classmethod
    def set(
        cls,
        descriptor,
        displaycon,
        connect_args=None,
        creator=None,
        alias=None,
        config=None,
    ):
        """
        Set the current database connection. This method is called from the magic to
        determine which connection to use (either use an existing one or open a new one)

        Parameters
        ----------
        descriptor : str or sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
            A connection string or an existing connection. It opens a new connection
            if needed, otherwise it just assigns the connection as the current
            connection.

        alias : str, optional
            A name to identify the connection

        config : object, optional
            An object with configuration options. Options must be accessible via
            attributes. As of 0.9.0, only the autocommit option is needed.
        """
        connect_args = connect_args or {}

        if descriptor:
            if isinstance(descriptor, SQLAlchemyConnection):
                cls.current = descriptor
            elif isinstance(descriptor, Engine):
                cls.current = SQLAlchemyConnection(
                    descriptor, config=config, alias=alias
                )
            elif is_pep249_compliant(descriptor):
                cls.current = DBAPIConnection(descriptor, config=config, alias=alias)
            else:
                existing = rough_dict_get(cls.connections, descriptor)
                if existing and existing.alias == alias:
                    cls.current = existing
                elif existing and alias is None:
                    if (
                        _current._config_feedback_normal_or_more()
                        and cls.current != existing
                    ):
                        display.message(f"Switching to connection {descriptor!r}")
                    cls.current = existing

                # passing the same URL but different alias: create a new connection
                elif existing is None or existing.alias != alias:
                    is_connect_and_switch, is_connect = False, False
                    if cls.current and cls.current.alias != alias:
                        is_connect_and_switch = True
                    else:
                        is_connect = True

                    cls.current = cls.from_connect_str(
                        connect_str=descriptor,
                        connect_args=connect_args,
                        creator=creator,
                        alias=alias,
                        config=config,
                    )
                    if _current._config_feedback_normal_or_more():
                        identifier = alias or cls.current.url
                        if is_connect_and_switch:
                            display.message(
                                f"Connecting and switching to connection {identifier!r}"
                            )
                        if is_connect:
                            display.message(f"Connecting to {identifier!r}")

        else:
            if cls.connections:
                if displaycon and _current._config_feedback_normal_or_more():
                    cls.display_current_connection()
            elif os.getenv("DATABASE_URL"):
                cls.current = cls.from_connect_str(
                    connect_str=os.getenv("DATABASE_URL"),
                    connect_args=connect_args,
                    creator=creator,
                    alias=alias,
                    config=config,
                )
            else:
                raise cls._error_no_connection()

        return cls.current

    @classmethod
    def close_all(cls, verbose=False):
        """Close all connections"""
        connections = ConnectionManager.connections.copy()
        for name, conn in connections.items():
            conn.close()

            if verbose:
                display.message(f"Closing {name}")

        cls.connections = {}

    @classmethod
    def _error_no_connection(cls):
        """Error when there isn't any connection"""
        err = UsageError("No active connection." + _suggest_fix(env_var=True))
        err.modify_exception = True
        return err

    @classmethod
    def display_current_connection(cls):
        for conn in cls._get_connections():
            if conn["current"]:
                alias = conn.get("alias")
                if alias:
                    display.message(f"Running query in {alias!r}")
                else:
                    display.message(f"Running query in {conn['url']!r}")

    @classmethod
    def _get_connections(cls):
        """
        Return a list of dictionaries
        """
        connections = []

        for key in sorted(cls.connections):
            conn = cls.connections[key]

            is_current = conn == cls.current

            connections.append(
                {
                    "current": is_current,
                    "key": key,
                    "url": conn.url,
                    "alias": conn.alias,
                    "connection": conn,
                }
            )

        return connections

    @classmethod
    def close_connection_with_descriptor(cls, descriptor):
        """Close a connection with the given descriptor"""
        if isinstance(descriptor, SQLAlchemyConnection):
            conn = descriptor
        else:
            conn = cls.connections.get(descriptor) or cls.connections.get(
                descriptor.lower()
            )

        if not conn:
            raise exceptions.RuntimeError(
                "Could not close connection because it was not found amongst these: %s"
                % str(list(cls.connections.keys()))
            )

        if descriptor in cls.connections:
            cls.connections.pop(descriptor)
        else:
            cls.connections.pop(
                str(conn.metadata.bind.url) if IS_SQLALCHEMY_ONE else str(conn.url)
            )

        conn.close()

    @classmethod
    def connections_table(cls):
        """Returns the current connections as a table"""
        connections = cls._get_connections()

        def map_values(d):
            d["current"] = "*" if d["current"] else ""
            d["alias"] = d["alias"] if d["alias"] else ""
            return d

        return display.ConnectionsTable(
            headers=["current", "url", "alias"],
            rows_maps=[map_values(c) for c in connections],
        )

    @classmethod
    def from_connect_str(
        cls, connect_str=None, connect_args=None, creator=None, alias=None, config=None
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
            raise exceptions.MissingPackageError(
                "\n\n".join([str(e), suggestion_str])
            ) from e
        except Exception as e:
            raise _error_invalid_connection_info(e, connect_str) from e

        connection = SQLAlchemyConnection(engine, alias=alias, config=config)
        connection.connect_args = connect_args

        return connection

    @classmethod
    def load_default_connection_from_file_if_any(cls, config):
        try:
            connections_file = ConnectionsFile(path_to_file=config.dsn_filename)
        except FileNotFoundError:
            return

        default_url = connections_file.get_default_connection_url()

        if default_url is not None:
            try:
                cls.set(
                    default_url,
                    displaycon=False,
                    alias="default",
                    config=config,
                )
            except Exception as e:
                # this is executed during the magic initialization, we don't want
                # to raise an exception here because it would prevent the magic
                # from being used
                display.message_warning(
                    "WARNING: Cannot start default connection from .ini file:"
                    f"\n\n{str(e)}"
                )


class AbstractConnection(abc.ABC):
    """The abstract base class for all connections"""

    def __init__(self, alias):
        self.alias = alias

        ConnectionManager.current = self
        ConnectionManager.connections[alias] = self

        self._result_sets = ResultSetCollection()

    @abc.abstractproperty
    def dialect(self):
        """Returns a string with the SQL dialect name"""
        pass

    @abc.abstractmethod
    def raw_execute(self, query, parameters=None):
        """Run the query without any pre-processing"""
        pass

    @abc.abstractmethod
    def _get_database_information(self):
        """
        Get the dialect, driver, and database server version info of current
        connection
        """
        pass

    @abc.abstractmethod
    def to_table(self, table_name, data_frame, if_exists, index, schema=None):
        """Create a table from a pandas DataFrame"""
        pass

    def close(self):
        """Close the connection"""
        for rs in self._result_sets:
            # this might be None if it didn't run any query
            if rs._sqlaproxy is not None:
                rs._sqlaproxy.close()

        self._connection.close()

    def _get_sqlglot_dialect(self):
        """
        Get the sqlglot dialect, this is similar to the dialect property except it
        maps some dialects to their sqlglot equivalent. This method should only be
        used for the transpilation process, for any other purposes, use the dialect
        property.

        Returns
        -------
        str
            Available dialect in sqlglot package, see more:
            https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py
        """
        connection_info = self._get_database_information()
        return DIALECT_NAME_SQLALCHEMY_TO_SQLGLOT_MAPPING.get(
            connection_info["dialect"], connection_info["dialect"]
        )

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
        write_dialect = self._get_sqlglot_dialect()

        # we write queries to be duckdb-compatible so we don't need to transpile
        # them. Furthermore, sqlglot does not guarantee roundtrip conversion
        # so calling transpile might break queries
        if write_dialect == "duckdb":
            return query

        try:
            return ";\n".join(
                [p.sql(dialect=write_dialect) for p in sqlglot.parse(query)]
            )
        except Exception:
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
            query = self._resolve_cte(query, with_)

        query = self._transpile_query(query)

        return query

    def _resolve_cte(self, query, with_):
        return str(store.render(query, with_=with_))

    def execute(self, query, with_=None):
        """
        Executes SQL query on a given connection
        """
        query_prepared = self._prepare_query(query, with_)
        return self.raw_execute(query_prepared)

    def is_use_backtick_template(self):
        """Get if the dialect support backtick (`) syntax as identifier

        Returns
        -------
        bool
            Indicate if the dialect can use backtick identifier in the SQL clause
        """
        cur_dialect = self._get_sqlglot_dialect()
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
            connection_info = self._get_database_information()
            if connection_info:
                cur_dialect = connection_info["dialect"]
                identifiers_ = sqlglot.Dialect.get_or_raise(
                    cur_dialect
                ).Tokenizer.IDENTIFIERS

                identifiers = [*set(identifiers + identifiers_)]
        except ValueError:
            pass
        except AttributeError:
            # this might be a DBAPI connection
            pass

        return identifiers


# some dialects break when commit is used
_COMMIT_BLACKLIST_DIALECTS = (
    "athena",
    "bigquery",
    "clickhouse",
    "ingres",
    "mssql",
    "teradata",
    "vertica",
)


# TODO: the autocommit is read only during initialization, if the user changes it
# it won't have any effect
class SQLAlchemyConnection(AbstractConnection):
    """Manages connections to databases

    Parameters
    ----------
    engine: sqlalchemy.engine.Engine
        The SQLAlchemy engine to use
    """

    is_dbapi_connection = False

    def __init__(self, engine, alias=None, config=None):
        if IS_SQLALCHEMY_ONE:
            self._metadata = sqlalchemy.MetaData(bind=engine)
        else:
            self._metadata = None

        # this returns a url with the password replaced by ***
        self._url = (
            repr(sqlalchemy.MetaData(bind=engine).bind.url)
            if IS_SQLALCHEMY_ONE
            else repr(engine.url)
        )

        self._connection_sqlalchemy = self._start_sqlalchemy_connection(
            engine, self._url
        )

        db_info = self._get_database_information()
        self._dialect = db_info["dialect"]
        self._driver = db_info["driver"]

        autocommit = True if config is None else config.autocommit

        if autocommit:
            success = set_sqlalchemy_isolation_level(self._connection_sqlalchemy)
            self._requires_manual_commit = not success

            # TODO: I noticed we don't have any unit tests for this
            # even if autocommit is true, we should not use it for some dialects
            self._requires_manual_commit = (
                all(
                    blacklisted_dialect not in str(self._dialect)
                    for blacklisted_dialect in _COMMIT_BLACKLIST_DIALECTS
                )
                and self._requires_manual_commit
            )
        else:
            self._requires_manual_commit = False

        # TODO: we're no longer using this. I believe this is only used via the
        # config.feedback option
        self.name = default_alias_for_engine(engine)

        # calling init from AbstractConnection must be the last thing we do as it
        # register the connection
        super().__init__(alias=alias or self._url)

    @property
    def dialect(self):
        return self._dialect

    @property
    def driver(self):
        return self._driver

    def _connection_execute(self, query, parameters=None):
        """Call the connection execute method

        Parameters
        ----------
        query : str
            SQL query

        parameters : dict, default None
            Parameters to use in the query (:variable format)
        """
        parameters = parameters or {}
        # we do not support multiple statements
        if len(sqlparse.split(query)) > 1:
            raise NotImplementedError("Only one statement is supported.")

        operation = partial(self._execute_with_parameters, query, parameters)
        out = self._execute_with_error_handling(operation)

        if self._requires_manual_commit:
            # Calling connection.commit() when using duckdb-engine will yield
            # empty results if we commit after a SELECT or SUMMARIZE statement,
            # see: https://github.com/Mause/duckdb_engine/issues/734.
            if self.dialect == "duckdb":
                no_commit = detect_duckdb_summarize_or_select(query)
                if no_commit:
                    return out

            # in sqlalchemy 1.x, connection has no commit attribute
            if IS_SQLALCHEMY_ONE:
                # TODO: I moved this from run.py where we were catching all exceptions
                # because some drivers do not support commits. However, I noticed
                # that if I remove the try catch we get this error in SQLite:
                #  "cannot commit - no transaction is active", we need to investigate
                # further, catching generic exceptions is not a good idea
                try:
                    self._connection.execute("commit")
                except Exception:
                    pass
            else:
                self._connection.commit()

        return out

    def _execute_with_parameters(self, query, parameters):
        """Execute the query with the given parameters"""
        if IS_SQLALCHEMY_ONE:
            out = self._connection.execute(sqlalchemy.text(query), **parameters)
        else:
            out = self._connection.execute(
                sqlalchemy.text(query), parameters=parameters
            )

        return out

    def raw_execute(self, query, parameters=None, with_=None):
        """Run the query without any preprocessing

        Parameters
        ----------
        query : str
            SQL query

        parameters : dict, default None
            Parameters to use in the query. They should appear in the query with the
            :name format (no quotes around them)

        with_ : list, default None
            List of CTEs to use in the query
        """
        # mssql with pyodbc does not support multiple open result sets, so we need
        # to close them all before issuing a new query
        if self.dialect == "mssql" and self.driver == "pyodbc":
            self._result_sets.close_all()

        if with_:
            query = self._resolve_cte(query, with_)

        query, quoted_named_parameters = escape_string_literals_with_colon_prefix(query)

        if quoted_named_parameters and parameters:
            intersection = set(quoted_named_parameters) & set(parameters)

            if intersection:
                intersection_ = ", ".join(sorted(intersection))
                warnings.warn(
                    f"The following variables are defined: {intersection_}. However "
                    "the parameters are quoted in the query, if you want to use "
                    "them as named parameters, remove the quotes.",
                    category=JupySQLQuotedNamedParametersWarning,
                )

        if parameters:
            required_parameters = set(sqlalchemy.text(query).compile().params)
            available_parameters = set(parameters)
            missing_parameters = required_parameters - available_parameters

            if missing_parameters:
                raise exceptions.InvalidQueryParameters(
                    "Cannot execute query because the following "
                    "variables are undefined: {}".format(", ".join(missing_parameters))
                )

            return self._connection_execute(query, parameters)
        else:
            try:
                return self._connection_execute(query)
            except StatementError as e:
                # add a more helpful message if the users passes :variable but
                # the feature isn't enabled
                if parameters is None:
                    named_params = find_named_parameters(query)

                    if named_params:
                        named_params_ = ", ".join(named_params)
                        e.add_detail(
                            f"Your query contains named parameters ({named_params_}) "
                            "but the named parameters feature is disabled. Enable it "
                            "with: %config SqlMagic.named_parameters=True"
                        )
                raise

    def _execute_with_error_handling(self, operation):
        """Execute a database operation and handle errors

        Parameters
        ----------
        operation : callable
            A callable that takes no parameters to execute a database operation
        """
        rollback_needed = False

        try:
            out = operation()

        # this is a generic error but we've seen it in postgres. it helps recover
        # from a idle session timeout (happens in psycopg 2 and psycopg 3)
        except PendingRollbackError:
            warnings.warn(
                "Found invalid transaction. JupySQL executed a ROLLBACK operation.",
                category=JupySQLRollbackPerformed,
            )
            rollback_needed = True

        # postgres error
        except InternalError as e:
            # message from psycopg 2 and psycopg 3
            message = (
                "current transaction is aborted, "
                "commands ignored until end of transaction block"
            )
            if type(e.orig).__name__ == "InFailedSqlTransaction" and message in str(
                e.orig
            ):
                warnings.warn(
                    (
                        "Current transaction is aborted. "
                        "JupySQL executed a ROLLBACK operation."
                    ),
                    category=JupySQLRollbackPerformed,
                )
                rollback_needed = True
            else:
                raise

        # postgres error
        except OperationalError as e:
            # message from psycopg 2 and psycopg 3
            message = "server closed the connection unexpectedly"

            if type(e.orig).__name__ == "OperationalError" and message in str(e.orig):
                warnings.warn(
                    "Server closed connection. JupySQL executed a ROLLBACK operation.",
                    category=JupySQLRollbackPerformed,
                )
                rollback_needed = True
            else:
                raise

        except ProgrammingError as e:
            # error when accessing previously non-existing file with duckdb using
            # sqlalchemy 2.x
            if "duckdb.InvalidInputException" in str(e) and "please ROLLBACK" in str(e):
                rollback_needed = True
            else:
                raise

        if rollback_needed:
            self._connection.rollback()
            out = operation()

        return out

    def _get_database_information(self):
        dialect = self._connection_sqlalchemy.dialect

        return {
            "dialect": getattr(dialect, "name", None),
            "driver": getattr(dialect, "driver", None),
            # NOTE: this becomes available after calling engine.connect()
            "server_version_info": getattr(dialect, "server_version_info", None),
        }

    @property
    def url(self):
        """Returns an obfuscated connection string (password hidden)"""
        return self._url

    @property
    def connection_sqlalchemy(self):
        """Returns the SQLAlchemy connection object"""
        return self._connection_sqlalchemy

    @property
    def _connection(self):
        """Returns the SQLAlchemy connection object"""
        return self._connection_sqlalchemy

    def close(self):
        super().close()

        # NOTE: in SQLAlchemy 2.x, we need to call engine.dispose() to completely
        # close the connection, calling connection.close() is not enough
        self._connection.engine.dispose()

    @classmethod
    @modify_exceptions
    def _start_sqlalchemy_connection(cls, engine, connect_str):
        try:
            connection = engine.connect()
            return connection
        except OperationalError as e:
            handle_exception(e)
        except Exception as e:
            raise _error_invalid_connection_info(e, connect_str) from e

    def to_table(self, table_name, data_frame, if_exists, index, schema=None):
        """Create a table from a pandas DataFrame"""
        operation = partial(
            data_frame.to_sql,
            table_name,
            self.connection_sqlalchemy,
            if_exists=if_exists,
            index=index,
            schema=schema,
        )

        try:
            self._execute_with_error_handling(operation)
        except ValueError:
            raise exceptions.ValueError(
                f"Table {table_name!r} already exists. Consider using "
                "--persist-replace to drop the table before "
                "persisting the data frame"
            )

        display.message_success(f"Success! Persisted {table_name} to the database.")


class DBAPIConnection(AbstractConnection):
    """A connection object for generic DBAPI connections"""

    is_dbapi_connection = True

    @telemetry.log_call("DBAPIConnection", payload=True)
    def __init__(self, payload, connection, alias=None, config=None):
        try:
            payload["engine"] = type(connection)
        except Exception as e:
            payload["engine_parsing_error"] = str(e)

        # detect if the engine is a native duckdb connection
        _is_duckdb_native = _check_if_duckdb_dbapi_connection(connection)

        self._dialect = "duckdb" if _is_duckdb_native else None
        self._driver = None

        # TODO: implement the dialect blacklist and add unit tests
        self._requires_manual_commit = True if config is None else config.autocommit

        self._connection = connection
        self._connection_class_name = type(connection).__name__

        # calling init from AbstractConnection must be the last thing we do as it
        # register the connection
        super().__init__(alias=alias or self._connection_class_name)

        # TODO: delete this
        self.name = self._connection_class_name

    @property
    def dialect(self):
        return self._dialect

    @property
    def driver(self):
        return self._driver

    def raw_execute(self, query, parameters=None, with_=None):
        """Run the query without any preprocessing

        Parameters
        ----------
        query : str
            SQL query

        parameters : dict, default None
            This parameter is added for consistency with SQLAlchemy connections but
            it is not used
        """
        # we do not support multiple statements (this might actually work in some
        # drivers but we need to add this for consistency with SQLAlchemyConnection)
        if len(sqlparse.split(query)) > 1:
            raise NotImplementedError("Only one statement is supported.")

        if with_:
            query = self._resolve_cte(query, with_)

        cur = self._connection.cursor()
        cur.execute(query)

        if self._requires_manual_commit:
            self._connection.commit()

        return cur

    def _get_database_information(self):
        return {
            "dialect": self.dialect,
            "driver": self._connection_class_name,
            "server_version_info": None,
        }

    @property
    def url(self):
        """Returns None since DBAPI connections don't have a url"""
        return None

    @property
    def connection_sqlalchemy(self):
        """
        Raises NotImplementedError since DBAPI connections don't have a SQLAlchemy
        connection object
        """
        raise NotImplementedError(
            "This feature is only available for SQLAlchemy connections"
        )

    def to_table(self, table_name, data_frame, if_exists, index, schema=None):
        raise exceptions.NotImplementedError(
            "--persist/--persist-replace is not available for DBAPI connections"
            " (only available for SQLAlchemy connections)"
        )


def _check_if_duckdb_dbapi_connection(conn):
    """Check if the connection is a native duckdb connection"""
    # NOTE: duckdb defines df and pl to efficiently convert results to
    # pandas.DataFrame and polars.DataFrame respectively
    return hasattr(conn, "df") and hasattr(conn, "pl")


def _suggest_fix(env_var, connect_str=None):
    """
    Returns an error message that we can display to the user
    to tell them how to pass the connection string
    """
    DEFAULT_PREFIX = "\n\n"
    prefix = ""

    if connect_str:
        matches = get_close_matches(
            connect_str, list(ConnectionManager.connections), n=1
        )
        matches_db = get_close_matches(
            connect_str.lower(), list(DB_DOCS_LINKS.keys()), cutoff=0.3, n=1
        )

        if matches:
            prefix = prefix + (
                "\n\nPerhaps you meant to use the existing "
                f"connection: %sql {matches[0]!r}?"
            )

        if matches_db:
            prefix = prefix + (
                f"\n\nPerhaps you meant to use the {matches_db[0]!r} db \n"
                f"To find more information regarding connection: "
                f"{DB_DOCS_LINKS[matches_db[0]]}\n\n"
            )

        if not matches and not matches_db:
            prefix = DEFAULT_PREFIX
    else:
        matches = None
        matches_db = None
        prefix = DEFAULT_PREFIX

    connection_string = (
        "Pass a valid connection string:\n    "
        "Example: %sql postgresql://username:password@hostname/dbname"
    )

    suffix = "To fix it:" if not matches else "Otherwise, try the following:"
    options = [f"{prefix}{suffix}", connection_string]

    keys = list(ConnectionManager.connections.keys())

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

    options.append(f"For more details, see: {PLOOMBER_DOCS_LINK_STR}")

    return "\n\n".join(options)


def is_pep249_compliant(conn):
    """
    Checks if given connection object complies with PEP 249
    """
    pep249_methods = [
        "close",
        "commit",
        # "rollback",
        # "cursor",
        # PEP 249 doesn't require the connection object to have
        # a cursor method strictly
        # ref: https://peps.python.org/pep-0249/#id52
    ]

    for method_name in pep249_methods:
        # Checking whether the connection object has the method
        # and if it is callable
        if not hasattr(conn, method_name) or not callable(getattr(conn, method_name)):
            return False

    return True


def default_alias_for_engine(engine):
    if not engine.url.username:
        # keeping this for compatibility
        return str(engine.url)

    return f"{engine.url.username}@{engine.url.database}"


def set_sqlalchemy_isolation_level(conn):
    """
    Sets the autocommit setting for a database connection using SQLAlchemy.
    This better handles some edge cases than calling .commit() on the connection but
    not all drivers support it.
    """
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        return True
    except Exception:
        return False


def detect_duckdb_summarize_or_select(query):
    """
    Checks if the SQL query is a DuckDB SELECT or SUMMARIZE statement.

    Note:
    Assumes there is only one SQL statement in the query.
    """
    statements = sqlparse.parse(query)
    if statements:
        if len(statements) > 1:
            raise NotImplementedError("Multiple statements are not supported")
        stype = statements[0].get_type()
        if stype == "SELECT":
            return True
        elif stype == "UNKNOWN":
            # Further analysis is required
            sql_stripped = sqlparse.format(query, strip_comments=True)
            words = sql_stripped.split()
            return len(words) > 0 and (
                words[0].lower() == "from" or words[0].lower() == "summarize"
            )
    return False


atexit.register(ConnectionManager.close_all, verbose=True)
