import codecs
import csv
import operator
import os.path
import re
from functools import reduce
from io import StringIO
import html

import prettytable
import sqlalchemy
import sqlparse
from sql.connection import Connection
from sqlalchemy.exc import ResourceClosedError
from sql import exceptions, display
from .column_guesser import ColumnGuesserMixin
from sql.warnings import JupySQLDataFramePerformanceWarning

try:
    from pgspecial.main import PGSpecial
except ModuleNotFoundError:
    PGSpecial = None
from sqlalchemy.orm import Session

from sql.telemetry import telemetry
import logging
import warnings
from collections.abc import Iterable


def unduplicate_field_names(field_names):
    """Append a number to duplicate field names to make them unique."""
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + "_" + str(i) in res:
                i += 1
            k += "_" + str(i)
        res.append(k)
    return res


class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class CsvResultDescriptor(object):
    """
    Provides IPython Notebook-friendly output for the
    feedback after a ``.csv`` called.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def __repr__(self):
        return "CSV results at %s" % os.path.join(os.path.abspath("."), self.file_path)

    def _repr_html_(self):
        return '<a href="%s">CSV results</a>' % os.path.join(
            ".", "files", self.file_path
        )


def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking spaces.
    """
    spaces = "&nbsp;" * len(match_obj.group(2))
    return "%s%s" % (match_obj.group(1), spaces)


_cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")


class ResultSet(ColumnGuesserMixin):
    """
    Results of a SQL query. Fetches rows lazily (only the necessary rows to show the
    preview based on the current configuration)
    """

    # user to overcome a duckdb-engine limitation, see @sqlaproxy for details
    LAST_BY_CONNECTION = {}

    def __init__(self, sqlaproxy, config, statement=None, conn=None):
        ResultSet.LAST_BY_CONNECTION[conn] = self

        self.config = config
        self.truncated = False
        self.statement = statement

        self._sqlaproxy = sqlaproxy
        self._conn = conn
        self._dialect = conn._get_curr_sqlglot_dialect()
        self._keys = None
        self._field_names = None
        self._results = []

        # https://peps.python.org/pep-0249/#description
        self.is_dbapi_results = hasattr(sqlaproxy, "description")

        # note that calling this will fetch the keys
        self.pretty_table = self._init_table()

        self._mark_fetching_as_done = False

        if self.config.autolimit == 1:
            # if autolimit is 1, we only want to fetch one row
            self.fetchmany(size=1)
            self._done_fetching()
        else:
            # in all other cases, 2 allows us to know if there are more rows
            # for example when creating a table, the results contains one row, in
            # such case, fetching 2 rows will tell us that there are no more rows
            # and can set the _mark_fetching_as_done flag to True
            self.fetchmany(size=2)

        self._finished_init = True

    @property
    def sqlaproxy(self):
        # there is a problem when using duckdb + sqlalchemy: duckdb-engine doesn't
        # create separate cursors, so whenever we have >1 ResultSet, the old ones
        # become outdated and fetching their results will return the results from
        # the last ResultSet. To fix this, we have to re-issue the query
        is_last_result = ResultSet.LAST_BY_CONNECTION.get(self._conn) is self
        is_duckdb_sqlalchemy = (
            self._dialect == "duckdb" and not self._conn.is_dbapi_connection()
        )

        if (
            # skip this if we're initializing the object (we're running __init__)
            hasattr(self, "_finished_init")
            # this only applies to duckdb + sqlalchemy with outdated results
            and is_duckdb_sqlalchemy
            and not is_last_result
        ):
            self._sqlaproxy = self._conn.session.execute(self.statement)
            self._sqlaproxy.fetchmany(size=len(self._results))

            ResultSet.LAST_BY_CONNECTION[self._conn] = self

        return self._sqlaproxy

    def _extend_results(self, elements):
        """Store the DB fetched results into the internal list of results"""
        to_add = self.config.displaylimit - len(self._results)
        self._results.extend(elements)
        self.pretty_table.add_rows(elements[:to_add])

    def mark_fetching_as_done(self):
        self._mark_fetching_as_done = True
        # NOTE: don't close the connection here (self.sqlaproxy.close()),
        # because we need to keep it open for the next query

    def _done_fetching(self):
        return self._mark_fetching_as_done

    @property
    def field_names(self):
        if self._field_names is None:
            self._field_names = unduplicate_field_names(self.keys)

        return self._field_names

    @property
    def keys(self):
        """
        Return the keys of the results (the column names)
        """
        if self._keys is not None:
            return self._keys

        if not self.is_dbapi_results:
            try:
                self._keys = self.sqlaproxy.keys()
            # sqlite raises this error when running a script that doesn't return rows
            # e.g, 'CREATE TABLE' but others don't (e.g., duckdb)
            except ResourceClosedError:
                self._keys = []
                return self._keys

        elif isinstance(self.sqlaproxy.description, Iterable):
            self._keys = [i[0] for i in self.sqlaproxy.description]
        else:
            self._keys = []

        return self._keys

    def _repr_html_(self):
        self.fetch_for_repr_if_needed()

        _cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")

        result = self.pretty_table.get_html_string()

        HTML = (
            "%s\n<span style='font-style:italic;font-size:11px'>"
            "<code>ResultSet</code> : to convert to pandas, call <a href="
            "'https://jupysql.ploomber.io/en/latest/integrations/pandas.html'>"
            "<code>.DataFrame()</code></a> or to polars, call <a href="
            "'https://jupysql.ploomber.io/en/latest/integrations/polars.html'>"
            "<code>.PolarsDataFrame()</code></a></span><br>"
        )
        result = HTML % (result)

        # to create clickable links
        result = html.unescape(result)
        result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)

        if self.config.displaylimit != 0 and not self._done_fetching():
            HTML = (
                '%s\n<span style="font-style:italic;text-align:center;">'
                "Truncated to displaylimit of %d</span>"
                "<br>"
                '<span style="font-style:italic;text-align:center;">'
                "If you want to see more, please visit "
                '<a href="https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit">displaylimit</a>'  # noqa: E501
                " configuration</span>"
            )
            result = HTML % (result, self.config.displaylimit)
        return result

    def __len__(self):
        self.fetchall()

        return len(self._results)

    def __iter__(self):
        self.fetchall()

        for result in self._results:
            yield result

    def __str__(self):
        self.fetch_for_repr_if_needed()
        return str(self.pretty_table)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, another: object) -> bool:
        return self._results == another

    def __getitem__(self, key):
        """
        Access by integer (row position within result set)
        or by string (value of leftmost column)
        """
        try:
            return self._results[key]
        except TypeError:
            result = [row for row in self if row[0] == key]
            if not result:
                raise KeyError(key)
            if len(result) > 1:
                raise KeyError('%d results for "%s"' % (len(result), key))
            return result[0]

    def __getattr__(self, attr):
        err_msg = (
            f"'{attr}' is not a valid operation, you can convert this "
            "into a pandas data frame by calling '.DataFrame()' or a "
            "polars data frame by calling '.PolarsDataFrame()'"
        )
        raise AttributeError(err_msg)

    def dict(self):
        """Returns a single dict built from the result set

        Keys are column names; values are a tuple"""
        return dict(zip(self.keys, zip(*self)))

    def dicts(self):
        "Iterator yielding a dict for each row"
        for row in self:
            yield dict(zip(self.keys, row))

    @telemetry.log_call("data-frame", payload=True)
    def DataFrame(self, payload):
        """Returns a Pandas DataFrame instance built from the result set."""
        payload["connection_info"] = self._conn._get_curr_sqlalchemy_connection_info()
        import pandas as pd

        kwargs = {"columns": (self and self.keys) or []}
        return _convert_to_data_frame(self, "df", pd.DataFrame, kwargs)

    @telemetry.log_call("polars-data-frame")
    def PolarsDataFrame(self, **polars_dataframe_kwargs):
        """Returns a Polars DataFrame instance built from the result set."""
        import polars as pl

        polars_dataframe_kwargs["schema"] = self.keys
        return _convert_to_data_frame(self, "pl", pl.DataFrame, polars_dataframe_kwargs)

    @telemetry.log_call("pie")
    def pie(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Values (pie slice sizes) are taken from the
        rightmost column (numerical values required).
        All other columns are used to label the pie slices.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passed
        through to ``matplotlib.pylab.pie``.
        """
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        import matplotlib.pylab as plt

        ax = plt.gca()

        ax.pie(self.ys[0], labels=self.xlabels, **kwargs)
        ax.set_title(title or self.ys[0].name)
        return ax

    @telemetry.log_call("plot")
    def plot(self, title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The first and last columns are taken as the X and Y
        values.  Any columns between are ignored.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passed
        through to ``matplotlib.pylab.plot``.
        """
        import matplotlib.pylab as plt

        self.guess_plot_columns()
        self.x = self.x or range(len(self.ys[0]))

        ax = plt.gca()

        coords = reduce(operator.add, [(self.x, y) for y in self.ys])
        ax.plot(*coords, **kwargs)

        if hasattr(self.x, "name"):
            ax.set_xlabel(self.x.name)

        ylabel = ", ".join(y.name for y in self.ys)

        ax.set_title(title or ylabel)
        ax.set_ylabel(ylabel)

        return ax

    @telemetry.log_call("bar")
    def bar(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab bar plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The last quantitative column is taken as the Y values;
        all other columns are combined to label the X axis.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns
        key_word_sep: string used to separate column values
                      from each other in labels

        Any additional keyword arguments will be passed
        through to ``matplotlib.pylab.bar``.
        """
        import matplotlib.pylab as plt

        ax = plt.gca()

        self.guess_pie_columns(xlabel_sep=key_word_sep)
        ax.bar(range(len(self.ys[0])), self.ys[0], **kwargs)

        if self.xlabels:
            ax.set_xticks(range(len(self.xlabels)), self.xlabels, rotation=45)

        ax.set_xlabel(self.xlabel)
        ax.set_ylabel(self.ys[0].name)
        return ax

    @telemetry.log_call("generate-csv")
    def csv(self, filename=None, **format_params):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
        Any other parameters will be passed on to csv.writer."""
        if filename:
            encoding = format_params.get("encoding", "utf-8")
            outfile = open(filename, "w", newline="", encoding=encoding)
        else:
            outfile = StringIO()

        writer = UnicodeWriter(outfile, **format_params)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            return CsvResultDescriptor(filename)
        else:
            return outfile.getvalue()

    def fetchmany(self, size):
        """Fetch n results and add it to the results"""
        if not self._done_fetching():
            try:
                returned = self.sqlaproxy.fetchmany(size=size)
            # sqlite raises this error when running a script that doesn't return rows
            # e.g, 'CREATE TABLE' but others don't (e.g., duckdb)
            except ResourceClosedError:
                self.mark_fetching_as_done()
                return

            self._extend_results(returned)

            if len(returned) < size:
                self.mark_fetching_as_done()

            if (
                self.config.autolimit is not None
                and self.config.autolimit != 0
                and len(self._results) >= self.config.autolimit
            ):
                self.mark_fetching_as_done()

    def fetch_for_repr_if_needed(self):
        if self.config.displaylimit == 0:
            self.fetchall()

        missing = self.config.displaylimit - len(self._results)

        if missing > 0:
            self.fetchmany(missing)

    def fetchall(self):
        if not self._done_fetching():
            self._extend_results(self.sqlaproxy.fetchall())
            self.mark_fetching_as_done()

    def _init_table(self):
        pretty = CustomPrettyTable(self.field_names)

        if isinstance(self.config.style, str):
            _style = prettytable.__dict__[self.config.style.upper()]
            pretty.set_style(_style)

        return pretty


def display_affected_rowcount(rowcount):
    if rowcount > 0:
        display.message_success(f"{rowcount} rows affected.")


class FakeResultProxy(object):
    """A fake class that pretends to behave like the ResultProxy from
    SqlAlchemy.
    """

    def __init__(self, cursor, headers):
        if cursor is None:
            cursor = []
            headers = []
        if isinstance(cursor, list):
            self.from_list(source_list=cursor)
        else:
            self.fetchall = cursor.fetchall
            self.fetchmany = cursor.fetchmany
            self.rowcount = cursor.rowcount
        self.keys = lambda: headers
        self.returns_rows = True

    def from_list(self, source_list):
        "Simulates SQLA ResultProxy from a list."

        self.fetchall = lambda: source_list
        self.rowcount = len(source_list)

        def fetchmany(size):
            pos = 0
            while pos < len(source_list):
                yield source_list[pos : pos + size]
                pos += size

        self.fetchmany = fetchmany


# some dialects have autocommit
# specific dialects break when commit is used:

_COMMIT_BLACKLIST_DIALECTS = (
    "athena",
    "bigquery",
    "clickhouse",
    "ingres",
    "mssql",
    "teradata",
    "vertica",
)


def _commit(conn, config, manual_commit):
    """Issues a commit, if appropriate for current config and dialect"""

    _should_commit = (
        config.autocommit
        and all(
            dialect not in str(conn.dialect) for dialect in _COMMIT_BLACKLIST_DIALECTS
        )
        and manual_commit
    )

    if _should_commit:
        try:
            with Session(conn.session) as session:
                session.commit()
        except sqlalchemy.exc.OperationalError:
            display.message("The database does not support the COMMIT command")


def is_postgres_or_redshift(dialect):
    """Checks if dialect is postgres or redshift"""
    return "postgres" in str(dialect) or "redshift" in str(dialect)


def is_pytds(dialect):
    """Checks if driver is pytds"""
    return "pytds" in str(dialect)


def handle_postgres_special(conn, statement):
    """Execute a PostgreSQL special statement using PGSpecial module."""
    if not PGSpecial:
        raise exceptions.MissingPackageError("pgspecial not installed")

    pgspecial = PGSpecial()
    _, cur, headers, _ = pgspecial.execute(conn.session.connection.cursor(), statement)[
        0
    ]
    return FakeResultProxy(cur, headers)


def set_autocommit(conn, config):
    """Sets the autocommit setting for a database connection."""
    if is_pytds(conn.dialect):
        warnings.warn(
            "Autocommit is not supported for pytds, thus is automatically disabled"
        )
        return False
    if config.autocommit:
        try:
            conn.session.execution_options(isolation_level="AUTOCOMMIT")
        except Exception as e:
            logging.debug(
                f"The database driver doesn't support such "
                f"AUTOCOMMIT execution option"
                f"\nPerhaps you can try running a manual COMMIT command"
                f"\nMessage from the database driver\n\t"
                f"Exception:  {e}\n",  # noqa: F841
            )
            return True
    return False


def select_df_type(resultset, config):
    """
    Converts the input resultset to either a Pandas DataFrame
    or Polars DataFrame based on the config settings.
    """
    if config.autopandas:
        return resultset.DataFrame()
    elif config.autopolars:
        return resultset.PolarsDataFrame(**config.polars_dataframe_kwargs)
    else:
        return resultset
    # returning only last result, intentionally


def run(conn, sql, config):
    """Run a SQL query with the given connection

    Parameters
    ----------
    conn : sql.connection.Connection
        The connection to use

    sql : str
        SQL query to execution

    config
        Configuration object
    """
    if not sql.strip():
        # returning only when sql is empty string
        return "Connected: %s" % conn.name

    for statement in sqlparse.split(sql):
        first_word = sql.strip().split()[0].lower()
        manual_commit = False

        # attempting to run a transaction
        if first_word == "begin":
            raise exceptions.RuntimeError("JupySQL does not support transactions")

        # postgres metacommand
        if first_word.startswith("\\") and is_postgres_or_redshift(conn.dialect):
            result = handle_postgres_special(conn, statement)

        # regular query
        else:
            manual_commit = set_autocommit(conn, config)
            is_dbapi_connection = Connection.is_dbapi_connection(conn)

            # if regular sqlalchemy, pass a text object
            if not is_dbapi_connection:
                statement = sqlalchemy.sql.text(statement)

            result = conn.session.execute(statement)
            _commit(conn=conn, config=config, manual_commit=manual_commit)

            if result and config.feedback:
                if hasattr(result, "rowcount"):
                    display_affected_rowcount(result.rowcount)

    resultset = ResultSet(result, config, statement, conn)
    return select_df_type(resultset, config)


def raw_run(conn, sql):
    return conn.session.execute(sqlalchemy.sql.text(sql))


class CustomPrettyTable(prettytable.PrettyTable):
    def add_rows(self, data):
        for row in data:
            formatted_row = []
            for cell in row:
                if isinstance(cell, str) and cell.startswith("http"):
                    formatted_row.append("<a href={}>{}</a>".format(cell, cell))
                else:
                    formatted_row.append(cell)
            self.add_row(formatted_row)


def _statement_is_select(statement):
    statement_ = statement.lower().strip()
    # duckdb also allows FROM without SELECT
    return statement_.startswith("select") or statement_.startswith("from")


def _convert_to_data_frame(
    result_set, converter_name, constructor, constructor_kwargs=None
):
    constructor_kwargs = constructor_kwargs or {}
    has_converter_method = hasattr(result_set.sqlaproxy, converter_name)

    # native duckdb connection
    if hasattr(result_set.sqlaproxy, converter_name):
        # we need to re-execute the statement because if we fetched some rows
        # already, .df() will return None. But only if it's a select statement
        # otherwise we might end up re-execute INSERT INTO or CREATE TABLE
        # statements
        is_select = _statement_is_select(result_set.statement)

        if is_select:
            result_set.sqlaproxy.execute(result_set.statement)

        return getattr(result_set.sqlaproxy, converter_name)()
    else:
        frame = constructor(
            (tuple(row) for row in result_set),
            **constructor_kwargs,
        )

        # NOTE: in JupySQL 0.7.9, we were opening a raw new connection so people
        # using SQLALchemy still had the native performance to convert to data frames
        # but this led to other problems because the native connection didn't
        # have the same state as the SQLAlchemy connection, yielding confusing
        # errors. So we decided to remove this and just warn the user that
        # performance might be slow and they could use a native connection
        if (
            result_set._dialect == "duckdb"
            and not has_converter_method
            and len(frame) >= 1_000
        ):
            DOCS = "https://jupysql.ploomber.io/en/latest/integrations/duckdb.html"
            WARNINGS = "https://jupysql.ploomber.io/en/latest/tutorials/duckdb-native-sqlalchemy.html#supress-warnings"  # noqa: E501

            warnings.warn(
                "It looks like you're using DuckDB with SQLAlchemy. "
                "For faster conversions, use "
                f" a DuckDB native connection. Docs: {DOCS}."
                f" to suppress this warning, see: {WARNINGS}",
                category=JupySQLDataFramePerformanceWarning,
            )

        return frame
