import re
import operator
from functools import reduce
from io import StringIO
from html import unescape
from collections.abc import Iterable

import prettytable
import warnings

from sql.column_guesser import ColumnGuesserMixin
from sql.run.csv import CSVWriter, CSVResultDescriptor
from sql.telemetry import telemetry
from sql.run.table import CustomPrettyTable
from sql._current import _config_feedback_all

from sql.exceptions import RuntimeError


class ResultSet(ColumnGuesserMixin):
    """
    Results of a SQL query. Fetches rows lazily (only the necessary rows to show the
    preview based on the current configuration)
    """

    def __init__(self, sqlaproxy, config, statement=None, conn=None):
        self._closed = False
        self._config = config
        self._statement = statement
        self._sqlaproxy = sqlaproxy
        self._conn = conn
        self._dialect = conn._get_sqlglot_dialect()
        self._keys = None
        self._field_names = None
        self._results = []
        # https://peps.python.org/pep-0249/#description
        self._is_dbapi_results = hasattr(sqlaproxy, "description")

        # note that calling this will fetch the keys
        self._pretty_table = self._init_table()

        self._mark_fetching_as_done = False

        if self._config.autolimit == 1:
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

        if conn:
            conn._result_sets.append(self)

    @property
    def sqlaproxy(self):
        conn = self._conn

        # mssql with pyodbc does not support multiple open result sets, so we need
        # to close them all. when running this, we might've already closed the results
        # so we need to check for that and re-open the results if needed
        if conn.dialect == "mssql" and conn.driver == "pyodbc" and self._closed:
            self._conn._result_sets.close_all()
            self._sqlaproxy = self._conn.raw_execute(self._statement)
            self._sqlaproxy.fetchmany(size=len(self._results))
            self._conn._result_sets.append(self)

        # there is a problem when using duckdb + sqlalchemy: duckdb-engine doesn't
        # create separate cursors, so whenever we have >1 ResultSet, the old ones
        # become outdated and fetching their results will return the results from
        # the last ResultSet. To fix this, we have to re-issue the query
        is_last_result = self._conn._result_sets.is_last(self)

        is_duckdb_sqlalchemy = (
            self._dialect == "duckdb" and not self._conn.is_dbapi_connection
        )

        if (
            # skip this if we're initializing the object (we're running __init__)
            hasattr(self, "_finished_init")
            # this only applies to duckdb + sqlalchemy with outdated results
            and is_duckdb_sqlalchemy
            and not is_last_result
        ):
            self._sqlaproxy = self._conn.raw_execute(self._statement)
            self._sqlaproxy.fetchmany(size=len(self._results))

            # ensure we make his result set the last one
            self._conn._result_sets.append(self)

        return self._sqlaproxy

    def _extend_results(self, elements):
        """Store the DB fetched results into the internal list of results"""
        to_add = self._config.displaylimit - len(self._results)
        self._results.extend(elements)
        self._pretty_table.add_rows(
            elements if self._config.displaylimit == 0 else elements[:to_add]
        )

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

        if not self._is_dbapi_results:
            try:
                self._keys = self.sqlaproxy.keys()
            # sqlite with sqlalchemy raises sqlalchemy.exc.ResourceClosedError,
            # psycopg2 raises psycopg2.ProgrammingError error when running a script
            # that doesn't return rows e.g, 'CREATE TABLE' but others don't
            # (e.g., duckdb), so here we catch all
            except Exception:
                self._keys = []
                return self._keys

        elif isinstance(self.sqlaproxy.description, Iterable):
            self._keys = [i[0] for i in self.sqlaproxy.description]
        else:
            self._keys = []

        return self._keys

    def _repr_html_(self):
        self.fetch_for_repr_if_needed()
        result = self._pretty_table.get_html_string()
        return self._add_footer(result, html=True)

    def _add_footer(self, result, *, html):
        if _config_feedback_all():
            data_frame_footer = (
                (
                    "\n<span style='font-style:italic;font-size:11px'>"
                    "<code>ResultSet</code>: to convert to pandas, call <a href="
                    "'https://jupysql.ploomber.io/en/latest/integrations/pandas.html'>"
                    "<code>.DataFrame()</code></a> or to polars, call <a href="
                    "'https://jupysql.ploomber.io/en/latest/integrations/polars.html'>"
                    "<code>.PolarsDataFrame()</code></a></span><br>"
                )
                if html
                else (
                    "\nResultSet: to convert to pandas, call .DataFrame() "
                    "or to polars, call .PolarsDataFrame()"
                )
            )

            result = f"{result}{data_frame_footer}"

        # to create clickable links
        result = unescape(result)
        _cell_with_spaces_pattern = re.compile(r"(<td>)( {2,})")
        result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)

        if self._config.displaylimit != 0 and not self._done_fetching():
            displaylimit_footer = (
                (
                    '\n<span style="font-style:italic;text-align:center;">'
                    'Truncated to <a href="https://jupysql.ploomber.io/en/'
                    'latest/api/configuration.html#displaylimit">'
                    f"displaylimit</a> of {self._config.displaylimit}.</span>"
                )
                if html
                else f"\nTruncated to displaylimit of {self._config.displaylimit}."
            )

            result = f"{result}{displaylimit_footer}"

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
        result = str(self._pretty_table)
        return self._add_footer(result, html=False)

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
        payload["connection_info"] = self._conn._get_database_information()
        import pandas as pd

        return _convert_to_data_frame(self, "df", pd.DataFrame)

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
        warnings.warn(
            (
                ".pie() is deprecated and will be removed in a future version. "
                "Use %sqlplot pie instead. "
                "For more help, find us at https://ploomber.io/community "
            ),
            UserWarning,
        )

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
        warnings.warn(
            (
                ".plot() is deprecated and will be removed in a future version. "
                "For more help, find us at https://ploomber.io/community "
            ),
            UserWarning,
        )

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
        warnings.warn(
            (
                ".bar() is deprecated and will be removed in a future version. "
                "Use %sqlplot bar instead. "
                "For more help, find us at https://ploomber.io/community "
            ),
            UserWarning,
        )

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

        writer = CSVWriter(outfile, **format_params)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            return CSVResultDescriptor(filename)
        else:
            return outfile.getvalue()

    def fetchmany(self, size):
        """Fetch n results and add it to the results"""
        if not self._done_fetching():
            try:
                returned = self.sqlaproxy.fetchmany(size=size)
            # sqlite with sqlalchemy raises sqlalchemy.exc.ResourceClosedError,
            # psycopg2 raises psycopg2.ProgrammingError error when running a script
            # that doesn't return rows e.g, 'CREATE TABLE' but others don't
            # (e.g., duckdb), so here we catch all
            except Exception as e:
                if not any(
                    substring in str(e)
                    for substring in [
                        "This result object does not return rows",
                        "no results to fetch",
                    ]
                ):
                    # raise specific DB driver errors
                    raise RuntimeError(f"Error running the query: {str(e)}") from e
                self.mark_fetching_as_done()
                return

            self._extend_results(returned)

            if len(returned) < size:
                self.mark_fetching_as_done()

            if (
                self._config.autolimit is not None
                and self._config.autolimit != 0
                and len(self._results) >= self._config.autolimit
            ):
                self.mark_fetching_as_done()

    def fetch_for_repr_if_needed(self):
        if self._config.displaylimit == 0:
            self.fetchall()

        missing = self._config.displaylimit - len(self._results)

        if missing > 0:
            self.fetchmany(missing)

    def fetchall(self):
        if not self._done_fetching():
            self._extend_results(self.sqlaproxy.fetchall())
            self.mark_fetching_as_done()

    def _init_table(self):
        pretty = CustomPrettyTable(self.field_names)

        if isinstance(self._config.style, str):
            _style = prettytable.__dict__[self._config.style.upper()]
            pretty.set_style(_style)

        return pretty

    def close(self):
        self._sqlaproxy.close()
        self._closed = True


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


def _convert_to_data_frame(
    result_set, converter_name, constructor, constructor_kwargs=None
):
    """
    Convert the result set to a pandas DataFrame, using native DuckDB methods if
    possible
    """
    constructor_kwargs = constructor_kwargs or {}

    # maybe create accessors in the connection objects?
    if result_set._conn.is_dbapi_connection:
        native_connection = result_set.sqlaproxy
    else:
        native_connection = result_set._conn._connection.connection

    has_converter_method = hasattr(native_connection, converter_name)

    # native duckdb connection
    if has_converter_method:
        # we need to re-execute the statement because if we fetched some rows
        # already, .df() will return None. But only if it's a select statement
        # otherwise we might end up re-execute INSERT INTO or CREATE TABLE
        # statements.
        is_select = _statement_is_select(result_set._statement)

        if is_select:
            # If command includes PIVOT, current transaction must be closed.
            # Otherwise, re-executing the statement will return
            # TransactionContext Error: cannot start a transaction within a transaction
            if "pivot" in result_set._statement.lower():
                # fetchall retrieves the previous results and completes the transaction
                # nothing is done with the results from fetchall()
                native_connection.fetchall()

            native_connection.execute(result_set._statement)

        return getattr(native_connection, converter_name)()
    else:
        if converter_name == "df":
            constructor_kwargs["columns"] = result_set.keys

        frame = constructor(
            (tuple(row) for row in result_set),
            **constructor_kwargs,
        )

        return frame


def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking spaces.
    """
    spaces = "&nbsp;" * len(match_obj.group(2))
    return "%s%s" % (match_obj.group(1), spaces)


def _statement_is_select(statement):
    statement_ = statement.lower().strip()
    # duckdb also allows FROM without SELECT
    return (
        statement_.startswith("select")
        or statement_.startswith("from")
        or statement_.startswith("with")
        or statement_.startswith("pivot")
    )
