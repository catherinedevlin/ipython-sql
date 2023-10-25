"""
Plot using the SQL backend
"""
from ploomber_core.dependencies import requires
from ploomber_core.exceptions import modify_exceptions
from jinja2 import Template

from sql import exceptions, display
from sql.stats import _summary_stats
from sql.util import (
    _are_numeric_values,
    validate_mutually_exclusive_args,
    to_upper_if_snowflake_conn,
    enclose_table_with_double_quotations,
)
from sql.display import message

try:
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
except ModuleNotFoundError:
    plt = None
    Normalize = None

try:
    import numpy as np
except ModuleNotFoundError:
    np = None

import sql.connection
from sql.telemetry import telemetry
import warnings


def _whishi(conn, table, column, hival, with_=None):
    if not conn:
        conn = sql.connection.ConnectionManager.current
    template = Template(
        """
SELECT COUNT(*), MAX("{{column}}")
FROM (
    SELECT "{{column}}"
    FROM {{table}}
    WHERE "{{column}}" <= {{hival}}
) AS _whishi
"""
    )

    query = template.render(table=table, column=column, hival=hival)

    values = conn.execute(query, with_).fetchone()
    keys = ["N", "wiskhi_max"]
    return {k: float(v) for k, v in zip(keys, values)}


def _whislo(conn, table, column, loval, with_=None):
    if not conn:
        conn = sql.connection.ConnectionManager.current
    template = Template(
        """
SELECT COUNT(*), MIN("{{column}}")
FROM (
    SELECT "{{column}}"
    FROM {{table}}
    WHERE "{{column}}" >= {{loval}}
) AS _whislo
"""
    )

    query = template.render(table=table, column=column, loval=loval)

    values = conn.execute(query, with_).fetchone()
    keys = ["N", "wisklo_min"]
    return {k: float(v) for k, v in zip(keys, values)}


def _percentile(conn, table, column, pct, with_=None):
    if not conn:
        conn = sql.connection.ConnectionManager.current.connection
    template = Template(
        """
SELECT
percentile_disc({{pct}}) WITHIN GROUP (ORDER BY "{{column}}") AS pct,
FROM {{table}}
"""
    )
    query = template.render(table=table, column=column, pct=pct)

    values = conn.execute(query, with_).fetchone()[0]
    return values


def _between(conn, table, column, whislo, whishi, with_=None):
    template = Template(
        """
SELECT "{{column}}"
FROM {{table}}
WHERE "{{column}}" < {{whislo}}
OR  "{{column}}" > {{whishi}}
"""
    )
    query = template.render(table=table, column=column, whislo=whislo, whishi=whishi)

    results = [float(n[0]) for n in conn.execute(query, with_).fetchall()]
    return results


# https://github.com/matplotlib/matplotlib/blob/b5ac96a8980fdb9e59c9fb649e0714d776e26701/lib/matplotlib/cbook/__init__.py
@modify_exceptions
def _boxplot_stats(conn, table, column, whis=1.5, autorange=False, with_=None):
    """Compute statistics required to create a boxplot"""
    if not conn:
        conn = sql.connection.ConnectionManager.current

    def _compute_conf_interval(N, med, iqr):
        notch_min = med - 1.57 * iqr / np.sqrt(N)
        notch_max = med + 1.57 * iqr / np.sqrt(N)

        return notch_min, notch_max

    stats = dict()

    # arithmetic mean
    s_stats = _summary_stats(conn, table, column, with_=with_)

    stats["mean"] = s_stats["mean"]
    q1, med, q3 = s_stats["q1"], s_stats["med"], s_stats["q3"]
    N = s_stats["N"]

    # interquartile range
    stats["iqr"] = q3 - q1

    if stats["iqr"] == 0 and autorange:
        whis = (0, 100)

    # conf. interval around median
    stats["cilo"], stats["cihi"] = _compute_conf_interval(N, med, stats["iqr"])

    # lowest/highest non-outliers
    if np.iterable(whis) and not isinstance(whis, str):
        loval, hival = _percentile(conn, table, column, whis, with_=with_)

    elif np.isreal(whis):
        loval = q1 - whis * stats["iqr"]
        hival = q3 + whis * stats["iqr"]
    else:
        raise ValueError("whis must be a float or list of percentiles")

    # get high extreme
    wiskhi_d = _whishi(conn, table, column, hival, with_=with_)

    if wiskhi_d["N"] == 0 or wiskhi_d["wiskhi_max"] < q3:
        stats["whishi"] = q3
    else:
        stats["whishi"] = wiskhi_d["wiskhi_max"]

    # get low extreme
    wisklo_d = _whislo(conn, table, column, loval, with_=with_)

    if wisklo_d["N"] == 0 or wisklo_d["wisklo_min"] > q1:
        stats["whislo"] = q1
    else:
        stats["whislo"] = wisklo_d["wisklo_min"]

    # compute a single array of outliers
    stats["fliers"] = np.array(
        _between(conn, table, column, stats["whislo"], stats["whishi"], with_=with_)
    )

    # add in the remaining stats
    stats["q1"], stats["med"], stats["q3"] = q1, med, q3

    bxpstats = {k: v for k, v in stats.items()}

    return bxpstats


# https://github.com/matplotlib/matplotlib/blob/ddc260ce5a53958839c244c0ef0565160aeec174/lib/matplotlib/axes/_axes.py#L3915
@requires(["matplotlib"])
@telemetry.log_call("boxplot", payload=True)
def boxplot(
    payload, table, column, *, orient="v", with_=None, conn=None, ax=None, schema=None
):
    """Plot boxplot

    Parameters
    ----------
    table : str
        Table name where the data is located

    column : str, list
        Column(s) to plot

    orient : str {"h", "v"}, default="v"
        Boxplot orientation (vertical/horizontal)

    conn : connection, default=None
        Database connection. If None, it uses the current connection

    Notes
    -----
    .. versionchanged:: 0.5.2
        Added ``with_``, and ``orient`` arguments. Added plot title and axis labels.
        Allowing to pass lists in ``column``. Function returns a ``matplotlib.Axes``
        object.

    .. versionadded:: 0.4.4

    Returns
    -------
    ax : matplotlib.Axes
        Generated plot

    Examples
    --------
    .. plot:: ../examples/plot_boxplot.py

    **Customize plot:**

    .. plot:: ../examples/plot_boxplot_custom.py

    **Horizontal boxplot:**

    .. plot:: ../examples/plot_boxplot_horizontal.py

    **Plot multiple columns from the same table:**

    .. plot:: ../examples/plot_boxplot_many.py
    """
    if not conn:
        conn = sql.connection.ConnectionManager.current

    payload["connection_info"] = conn._get_database_information()

    _table = enclose_table_with_double_quotations(table, conn)
    if schema:
        _table = f'"{schema}"."{_table}"'

    ax = ax or plt.gca()
    vert = orient == "v"

    set_ticklabels = ax.set_xticklabels if vert else ax.set_yticklabels
    set_label = ax.set_ylabel if vert else ax.set_xlabel

    if isinstance(column, str):
        stats = [_boxplot_stats(conn, _table, column, with_=with_)]
        ax.bxp(stats, vert=vert)
        ax.set_title(f"{column!r} from {table!r}")
        set_label(column)
        set_ticklabels([column])
    else:
        stats = [_boxplot_stats(conn, _table, col, with_=with_) for col in column]
        ax.bxp(stats, vert=vert)
        ax.set_title(f"Boxplot from {table!r}")
        set_ticklabels(column)

    return ax


def _min_max(conn, table, column, with_=None, use_backticks=False):
    if not conn:
        conn = sql.connection.ConnectionManager.current
    template_ = """
SELECT
    MIN("{{column}}"),
    MAX("{{column}}")
FROM {{table}}
"""
    if use_backticks:
        template_ = template_.replace('"', "`")

    template = Template(template_)
    query = template.render(table=table, column=column)

    min_, max_ = conn.execute(query, with_).fetchone()
    return min_, max_


def _get_bar_width(ax, bins, bin_size, binwidth):
    """
    Return a single bar width based on number of bins
    or a list of bar widths if `breaks` is given.
    If bins values are str, calculate value based on figure size.

    Parameters
    ----------
    ax : matplotlib.Axes
        Generated plot

    bins : tuple
        Contains bins' midpoints as float

    bin_size : int or list or None
        Calculated bin_size from the _histogram function
        or from consecutive differences in `breaks`

    binwidth : int or float or None
        Specified binwidth from a user

    Returns
    -------
    width : float
        A single bar width
    """
    if _are_numeric_values(bin_size) or isinstance(bin_size, list):
        width = bin_size
    elif _are_numeric_values(binwidth):
        width = binwidth
    else:
        fig = plt.gcf()
        bbox = ax.get_window_extent()
        width_inch = bbox.width / fig.dpi
        width = width_inch / len(bins)

    return width


@requires(["matplotlib"])
@telemetry.log_call("histogram", payload=True)
def histogram(
    payload,
    table,
    column,
    bins,
    with_=None,
    conn=None,
    category=None,
    cmap=None,
    color=None,
    edgecolor=None,
    ax=None,
    facet=None,
    breaks=None,
    binwidth=None,
    schema=None,
):
    """Plot histogram

    Parameters
    ----------
    table : str
        Table name where the data is located

    column : str, list
        Column(s) to plot

    bins : int
        Number of bins

    conn : connection, default=None
        Database connection. If None, it uses the current connection

    Notes
    -----
    .. versionchanged:: 0.5.2
        Added plot title and axis labels. Allowing to pass lists in ``column``.
        Function returns a ``matplotlib.Axes`` object.

    .. versionchanged:: 0.7.9
        Added support for NULL values, additional filter query with new logic.
        Skips the rows with NULL in the column, does not raise ValueError

    Returns
    -------
    ax : matplotlib.Axes
        Generated plot

    Examples
    --------
    .. plot:: ../examples/plot_histogram.py

    **Plot multiple columns from the same table**:

    .. plot:: ../examples/plot_histogram_many.py
    """
    if not conn:
        conn = sql.connection.ConnectionManager.current
    if isinstance(breaks, list):
        if len(breaks) < 2:
            raise exceptions.ValueError(
                f"Breaks given : {breaks}. When using breaks, please ensure "
                "to specify at least two points."
            )
        if not all([b2 > b1 for b1, b2 in zip(breaks[:-1], breaks[1:])]):
            raise exceptions.ValueError(
                f"Breaks given : {breaks}. When using breaks, please ensure that "
                "breaks are strictly increasing."
            )

    if _are_numeric_values(binwidth):
        if binwidth <= 0:
            raise exceptions.ValueError(
                f"Binwidth given : {binwidth}. When using binwidth, please ensure to "
                "pass a positive value."
            )
        binwidth = float(binwidth)
    elif binwidth is not None:
        raise exceptions.ValueError(
            f"Binwidth given : {binwidth}. When using binwidth, please ensure to "
            "pass a numeric value."
        )

    validate_mutually_exclusive_args(
        ["bins", "breaks", "binwidth"], [bins, breaks, binwidth]
    )

    _table = enclose_table_with_double_quotations(table, conn)
    if schema:
        _table = f'"{schema}"."{_table}"'

    ax = ax or plt.gca()
    payload["connection_info"] = conn._get_database_information()
    if category:
        if isinstance(column, list):
            if len(column) > 1:
                raise ValueError(
                    f"""Columns given : {column}.
                    When using a stacked histogram,
                    please ensure that you specify only one column."""
                )
            else:
                column = " ".join(column)

        if column is None or len(column) == 0:
            raise ValueError("Column name has not been specified")

        bin_, height, bin_size = _histogram(
            _table,
            column,
            bins,
            with_=with_,
            conn=conn,
            breaks=breaks,
            binwidth=binwidth,
        )
        width = _get_bar_width(ax, bin_, bin_size, binwidth)
        data = _histogram_stacked(
            _table,
            column,
            category,
            bin_,
            bin_size,
            with_=with_,
            conn=conn,
            facet=facet,
            breaks=breaks,
            binwidth=binwidth,
        )
        cmap = plt.get_cmap(cmap or "viridis")
        norm = Normalize(vmin=0, vmax=len(data))

        bottom = np.zeros(len(bin_))
        for i, values in enumerate(data):
            values_ = values[1:]

            if isinstance(color, list):
                color_ = color[0]
                if len(color) > 1:
                    warnings.warn(
                        "If you want to colorize each bar with multiple "
                        "colors please use cmap attribute instead "
                        "of 'fill'",
                        UserWarning,
                    )
            else:
                color_ = color or cmap(norm(i + 1))

            if isinstance(edgecolor, list):
                edgecolor_ = edgecolor[0]
            else:
                edgecolor_ = edgecolor or "None"

            ax.bar(
                bin_,
                values_,
                align="center",
                label=values[0],
                width=width,
                bottom=bottom,
                edgecolor=edgecolor_,
                color=color_,
            )
            bottom += values_

        ax.set_title(f"Histogram from {table!r}")
        # reverses legend order so alphabetically first goes on top
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[::-1], labels[::-1])
    elif isinstance(column, str):
        bin_, height, bin_size = _histogram(
            _table,
            column,
            bins,
            with_=with_,
            conn=conn,
            facet=facet,
            breaks=breaks,
            binwidth=binwidth,
        )
        width = _get_bar_width(ax, bin_, bin_size, binwidth)

        ax.bar(
            bin_,
            height,
            align="center",
            width=width,
            color=color,
            edgecolor=edgecolor or "None",
            label=column,
        )
        ax.set_title(f"{column!r} from {table!r}")
        ax.set_xlabel(column)

    else:
        if breaks and len(column) > 1:
            raise exceptions.UsageError(
                "Multiple columns don't support breaks. Please use bins instead."
            )
        for i, col in enumerate(column):
            bin_, height, bin_size = _histogram(
                _table,
                col,
                bins,
                with_=with_,
                conn=conn,
                facet=facet,
                breaks=breaks,
                binwidth=binwidth,
            )
            width = _get_bar_width(ax, bin_, bin_size, binwidth)

            if isinstance(color, list):
                color_ = color[i]
            else:
                color_ = color

            if isinstance(edgecolor, list):
                edgecolor_ = edgecolor[i]
            else:
                edgecolor_ = edgecolor or "None"

            ax.bar(
                bin_,
                height,
                align="center",
                width=width,
                alpha=0.5,
                label=col,
                color=color_,
                edgecolor=edgecolor_,
            )
            ax.set_title(f"Histogram from {table!r}")
            ax.legend()

    ax.set_ylabel("Count")

    return ax


@modify_exceptions
def _histogram(
    table, column, bins, with_=None, conn=None, facet=None, breaks=None, binwidth=None
):
    """Compute bins and heights"""
    if not conn:
        conn = sql.connection.ConnectionManager.current
    use_backticks = conn.is_use_backtick_template()

    # Snowflake will use UPPERCASE in the table and column name
    column = to_upper_if_snowflake_conn(conn, column)
    table = to_upper_if_snowflake_conn(conn, table)
    # FIXME: we're computing all the with elements twice
    min_, max_ = _min_max(conn, table, column, with_=with_, use_backticks=use_backticks)

    # Define all relevant filters here
    filter_query_1 = f'"{column}" IS NOT NULL'

    filter_query_2 = f"{facet['key']} == '{facet['value']}'" if facet else None

    filter_query = _filter_aggregate(filter_query_1, filter_query_2)

    bin_size = None

    if _are_numeric_values(min_, max_):
        if breaks:
            if min_ > breaks[-1]:
                raise exceptions.UsageError(
                    f"All break points are lower than the min data point of {min_}."
                )
            elif max_ < breaks[0]:
                raise exceptions.UsageError(
                    f"All break points are higher than the max data point of {max_}."
                )

            cases, bin_size = [], []
            for b_start, b_end in zip(breaks[:-1], breaks[1:]):
                case = f"WHEN {{{{column}}}} > {b_start} AND {{{{column}}}} <= {b_end} \
                        THEN {(b_start+b_end)/2}"
                cases.append(case)
                bin_size.append(b_end - b_start)
            cases[0] = cases[0].replace(">", ">=", 1)
            bin_midpoints = [
                (b_start + b_end) / 2 for b_start, b_end in zip(breaks[:-1], breaks[1:])
            ]
            all_bins = " union ".join([f"select {mid} as bin" for mid in bin_midpoints])

            # Group data based on the intervals in breaks
            # Left join is used to ensure count=0
            template_ = (
                "select all_bins.bin, coalesce(count_table.count, 0) as count "
                f"from ({all_bins}) as all_bins "
                "left join ("
                f"select case {' '.join(cases)} end as bin, "
                "count(*) as count "
                "from {{table}} "
                "{{filter_query}} "
                "group by bin) "
                "as count_table on all_bins.bin = count_table.bin "
                "order by all_bins.bin;"
            )

            breaks_filter_query = (
                f'"{column}" >= {breaks[0]} and "{column}" <= {breaks[-1]}'
            )
            filter_query = _filter_aggregate(
                filter_query_1, filter_query_2, breaks_filter_query
            )

            if use_backticks:
                template_ = template_.replace('"', "`")

            template = Template(template_)

            query = template.render(
                table=table, column=column, filter_query=filter_query
            )
        elif not binwidth and not isinstance(bins, int):
            raise ValueError(
                f"bins are '{bins}'. Please specify a valid number of bins."
            )
        else:
            # Use bins - 1 instead of bins and round half down instead of floor
            # to mimic right-closed histogram intervals in R ggplot
            range_ = max_ - min_
            if binwidth:
                bin_size = binwidth
                if binwidth > range_:
                    message(
                        f"Specified binwidth {binwidth} is larger than "
                        f"the range {range_}. Please choose a smaller binwidth."
                    )
            else:
                bin_size = range_ / (bins - 1)
            template_ = """
                select
                ceiling("{{column}}"/{{bin_size}} - 0.5)*{{bin_size}} as bin,
                count(*) as count
                from {{table}}
                {{filter_query}}
                group by bin
                order by bin;
                """

            if use_backticks:
                template_ = template_.replace('"', "`")

            template = Template(template_)

            query = template.render(
                table=table, column=column, bin_size=bin_size, filter_query=filter_query
            )
    else:
        template_ = """
        select
            "{{column}}" as col, count ("{{column}}")
        from {{table}}
        {{filter_query}}
        group by col
        order by col;
        """

        if use_backticks:
            template_ = template_.replace('"', "`")

        template = Template(template_)

        query = template.render(table=table, column=column, filter_query=filter_query)

    data = conn.execute(query, with_).fetchall()

    bin_, height = zip(*data)

    return bin_, height, bin_size


@modify_exceptions
def _histogram_stacked(
    table,
    column,
    category,
    bins,
    bin_size,
    with_=None,
    conn=None,
    facet=None,
    breaks=None,
    binwidth=None,
):
    """Compute the corresponding heights of each bin based on the category"""
    if not conn:
        conn = sql.connection.ConnectionManager.current

    cases = []
    if breaks:
        breaks_filter_query = (
            f'"{column}" >= {breaks[0]} and "{column}" <= {breaks[-1]}'
        )
        for b_start, b_end in zip(breaks[:-1], breaks[1:]):
            case = f'SUM(CASE WHEN {column} > {b_start} AND {column} <= {b_end} \
                    THEN 1 ELSE 0 END) AS "{(b_start+b_end)/2}",'
            cases.append(case)
        cases[0] = cases[0].replace(">", ">=", 1)
    else:
        if binwidth:
            bin_size = binwidth
        tolerance = bin_size / 1000  # Use to avoid floating point error
        for bin in bins:
            # Use round half down instead of floor to mimic
            # right-closed histogram intervals in R ggplot
            case = (
                f"SUM(CASE WHEN ABS(CEILING({column}/{bin_size} - 0.5)*{bin_size} "
                f"- {bin}) <= {tolerance} THEN 1 ELSE 0 END) AS '{bin}',"
            )
            cases.append(case)

    cases = " ".join(cases)

    filter_query_1 = f'"{column}" IS NOT NULL'

    filter_query_2 = f"{facet['key']} == '{facet['value']}'" if facet else None

    if breaks:
        filter_query = _filter_aggregate(
            filter_query_1, filter_query_2, breaks_filter_query
        )
    else:
        filter_query = _filter_aggregate(filter_query_1, filter_query_2)

    template = Template(
        """
        SELECT {{category}},
        {{cases}}
        FROM {{table}}
        {{filter_query}}
        GROUP BY {{category}}
        ORDER BY {{category}} DESC;
        """
    )
    query = template.render(
        table=table,
        column=column,
        bin_size=bin_size,
        category=category,
        filter_query=filter_query,
        cases=cases,
    )

    data = conn.execute(query, with_).fetchall()

    return data


@modify_exceptions
def _filter_aggregate(*filter_queries):
    """Return a single filter query based on multiple queries.

    Parameters:
    ----------
    *filter_queries (str):
    Variable length argument list of filter queries.
    Filter query is  string with a filtering condition in SQL
    (e.g., "age > 25").
    (e.g., "column is NULL").

    Notes
    -----
    .. versionadded:: 0.7.9

    Returns:
    -----
    final_filter (str):
    A string that represents a SQL WHERE clause

    """
    final_filter = ""
    for idx, query in enumerate(filter_queries):
        if query is not None:
            if idx == 0:
                final_filter = f"{final_filter}WHERE {query}"
                continue
            final_filter = f"{final_filter} AND {query}"
    return final_filter


@modify_exceptions
def _bar(table, column, with_=None, conn=None):
    """get x and height for bar plot"""
    if not conn:
        conn = sql.connection.ConnectionManager.current
    use_backticks = conn.is_use_backtick_template()

    if isinstance(column, list):
        if len(column) > 2:
            raise exceptions.UsageError(
                f"Passed columns: {column}\n"
                "Bar chart currently supports, either a single column"
                " on which group by and count is applied or"
                " two columns: labels and size"
            )

        x_ = column[0]
        height_ = column[1]

        display.message(f"Removing NULLs, if there exists any from {x_} and {height_}")
        template_ = """
            select "{{x_}}" as x,
            "{{height_}}" as height
            from {{table}}
            where "{{x_}}" is not null
            and "{{height_}}" is not null;
            """

        xlabel = x_
        ylabel = height_

        if use_backticks:
            template_ = template_.replace('"', "`")

        template = Template(template_)
        query = template.render(table=table, x_=x_, height_=height_)

    else:
        display.message(f"Removing NULLs, if there exists any from {column}")
        template_ = """
                select "{{column}}" as x,
                count("{{column}}") as height
                from {{table}}
                where "{{column}}" is not null
                group by "{{column}}";
                """

        xlabel = column
        ylabel = "Count"

        if use_backticks:
            template_ = template_.replace('"', "`")

        template = Template(template_)
        query = template.render(table=table, column=column)

    data = conn.execute(query, with_).fetchall()

    x, height = zip(*data)

    if x[0] is None:
        raise ValueError("Data contains NULLs")

    return x, height, xlabel, ylabel


@requires(["matplotlib"])
@telemetry.log_call("bar", payload=True)
def bar(
    payload,
    table,
    column,
    show_num=False,
    orient="v",
    with_=None,
    conn=None,
    cmap=None,
    color=None,
    edgecolor=None,
    ax=None,
    schema=None,
):
    """Plot Bar Chart

    Parameters
    ----------
    table : str
        Table name where the data is located

    column : str
        Column(s) to plot

    show_num: bool
        Show numbers on top of plot

    orient : str, default='v'
        Orientation of the plot. 'v' for vertical and 'h' for horizontal

    conn : connection, default=None
        Database connection. If None, it uses the current connection

    Notes
    -----

    .. versionadded:: 0.7.6

    Returns
    -------
    ax : matplotlib.Axes
        Generated plot

    """

    if not conn:
        conn = sql.connection.ConnectionManager.current

    _table = enclose_table_with_double_quotations(table, conn)
    if schema:
        _table = f'"{schema}"."{_table}"'

    ax = ax or plt.gca()
    payload["connection_info"] = conn._get_database_information()

    if column is None:
        raise exceptions.UsageError("Column name has not been specified")

    x, height_, xlabel, ylabel = _bar(_table, column, with_=with_, conn=conn)

    if color and cmap:
        # raise a userwarning
        warnings.warn(
            "Both color and cmap are given. cmap will be ignored", UserWarning
        )

    if (not color) and cmap:
        cmap = plt.get_cmap(cmap)
        norm = Normalize(vmin=0, vmax=len(x))
        color = [cmap(norm(i)) for i in range(len(x))]

    if orient == "h":
        ax.barh(
            x,
            height_,
            align="center",
            edgecolor=edgecolor,
            color=color,
        )
        ax.set_xlabel(ylabel)
        ax.set_ylabel(xlabel)
    else:
        ax.bar(
            x,
            height_,
            align="center",
            edgecolor=edgecolor,
            color=color,
        )
        ax.set_ylabel(ylabel)
        ax.set_xlabel(xlabel)

    if show_num:
        if orient == "v":
            for i, v in enumerate(height_):
                ax.text(
                    i,
                    v,
                    str(v),
                    color="black",
                    fontweight="bold",
                    ha="center",
                    va="bottom",
                )
        else:
            for i, v in enumerate(height_):
                ax.text(
                    v,
                    i,
                    str(v),
                    color="black",
                    fontweight="bold",
                    ha="left",
                    va="center",
                )

    ax.set_title(table)

    return ax


@modify_exceptions
def _pie(table, column, with_=None, conn=None):
    """get x and height for pie chart"""
    if not conn:
        conn = sql.connection.ConnectionManager.current
    use_backticks = conn.is_use_backtick_template()

    if isinstance(column, list):
        if len(column) > 2:
            raise exceptions.UsageError(
                f"Passed columns: {column}\n"
                "Pie chart currently supports, either a single column"
                " on which group by and count is applied or"
                " two columns: labels and size"
            )

        labels_ = column[0]
        size_ = column[1]

        display.message(
            f"Removing NULLs, if there exists any from {labels_} and {size_}"
        )
        template_ = """
                select "{{labels_}}" as labels,
                "{{size_}}" as size
                from {{table}}
                where "{{labels_}}" is not null
                and "{{size_}}" is not null;
                """
        if use_backticks:
            template_ = template_.replace('"', "`")

        template = Template(template_)
        query = template.render(table=table, labels_=labels_, size_=size_)

    else:
        display.message(f"Removing NULLs, if there exists any from {column}")
        template_ = """
                select "{{column}}" as x,
                count("{{column}}") as height
                from {{table}}
                where "{{column}}" is not null
                group by "{{column}}";
                """
        if use_backticks:
            template_ = template_.replace('"', "`")

        template = Template(template_)
        query = template.render(table=table, column=column)

    data = conn.execute(query, with_).fetchall()

    labels, size = zip(*data)

    if labels[0] is None:
        raise ValueError("Data contains NULLs")

    return labels, size


@requires(["matplotlib"])
@telemetry.log_call("bar", payload=True)
def pie(
    payload,
    table,
    column,
    show_num=False,
    with_=None,
    conn=None,
    cmap=None,
    color=None,
    ax=None,
    schema=None,
):
    """Plot Pie Chart

    Parameters
    ----------
    table : str
        Table name where the data is located

    column : str
        Column(s) to plot

    show_num: bool
        Show numbers on top of plot

    conn : connection, default=None
        Database connection. If None, it uses the current connection

    Notes
    -----

    .. versionadded:: 0.7.6

    Returns
    -------
    ax : matplotlib.Axes
        Generated plot
    """

    if not conn:
        conn = sql.connection.ConnectionManager.current

    _table = enclose_table_with_double_quotations(table, conn)
    if schema:
        _table = f'"{schema}"."{_table}"'

    ax = ax or plt.gca()
    payload["connection_info"] = conn._get_database_information()

    if column is None:
        raise exceptions.UsageError("Column name has not been specified")

    labels, size_ = _pie(_table, column, with_=with_, conn=conn)

    if color and cmap:
        # raise a userwarning
        warnings.warn(
            "Both color and cmap are given. cmap will be ignored", UserWarning
        )

    if (not color) and cmap:
        cmap = plt.get_cmap(cmap)
        norm = Normalize(vmin=0, vmax=len(labels))
        color = [cmap(norm(i)) for i in range(len(labels))]

    if show_num:
        ax.pie(
            size_,
            labels=labels,
            colors=color,
            autopct="%1.2f%%",
        )
    else:
        ax.pie(
            size_,
            labels=labels,
            colors=color,
        )

    ax.set_title(table)

    return ax
