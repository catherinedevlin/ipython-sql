"""
Plot using the SQL backend
"""
from ploomber_core.dependencies import requires
from ploomber_core.exceptions import modify_exceptions
from jinja2 import Template


from sql import exceptions, display
from sql.stats import _summary_stats

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
    FROM "{{table}}"
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
    FROM "{{table}}"
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
FROM "{{table}}"
"""
    )
    query = template.render(table=table, column=column, pct=pct)

    values = conn.execute(query, with_).fetchone()[0]
    return values


def _between(conn, table, column, whislo, whishi, with_=None):
    template = Template(
        """
SELECT "{{column}}"
FROM "{{table}}"
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
def boxplot(payload, table, column, *, orient="v", with_=None, conn=None, ax=None):
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

    ax = plt.gca()
    vert = orient == "v"

    set_ticklabels = ax.set_xticklabels if vert else ax.set_yticklabels
    set_label = ax.set_ylabel if vert else ax.set_xlabel

    if isinstance(column, str):
        stats = [_boxplot_stats(conn, table, column, with_=with_)]
        ax.bxp(stats, vert=vert)
        ax.set_title(f"{column!r} from {table!r}")
        set_label(column)
        set_ticklabels([column])
    else:
        stats = [_boxplot_stats(conn, table, col, with_=with_) for col in column]
        ax.bxp(stats, vert=vert)
        ax.set_title(f"Boxplot from {table!r}")
        set_ticklabels(column)

    return ax


def _min_max(con, table, column, with_=None, use_backticks=False):
    if not con:
        con = sql.connection.ConnectionManager.current
    template_ = """
SELECT
    MIN("{{column}}"),
    MAX("{{column}}")
FROM "{{table}}"
"""
    if use_backticks:
        template_ = template_.replace('"', "`")

    template = Template(template_)
    query = template.render(table=table, column=column)

    min_, max_ = con.execute(query, with_).fetchone()
    return min_, max_


def _are_numeric_values(*values):
    return all([isinstance(value, (int, float)) for value in values])


def _get_bar_width(ax, bins, bin_size):
    """
    Return a single bar width based on number of bins
    If bins values are str, calculate value based on figure size.

    Parameters
    ----------
    ax : matplotlib.Axes
        Generated plot

    bins : tuple
        Contains bins' midpoints as float

    bin_size : int or None
        Calculated bin_size from the _histogram function

    Returns
    -------
    width : float
        A single bar width
    """

    if _are_numeric_values(bin_size):
        width = bin_size
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

        bin_, height, bin_size = _histogram(table, column, bins, with_=with_, conn=conn)
        width = _get_bar_width(ax, bin_, bin_size)
        data = _histogram_stacked(
            table, column, category, bin_, bin_size, with_=with_, conn=conn, facet=facet
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
            table, column, bins, with_=with_, conn=conn, facet=facet
        )
        width = _get_bar_width(ax, bin_, bin_size)

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
        for i, col in enumerate(column):
            bin_, height, bin_size = _histogram(
                table, col, bins, with_=with_, conn=conn, facet=facet
            )
            width = _get_bar_width(ax, bin_, bin_size)

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
def _histogram(table, column, bins, with_=None, conn=None, facet=None):
    """Compute bins and heights"""
    if not conn:
        conn = sql.connection.ConnectionManager.current
    use_backticks = conn.is_use_backtick_template()

    # FIXME: we're computing all the with elements twice
    min_, max_ = _min_max(conn, table, column, with_=with_, use_backticks=use_backticks)

    # Define all relevant filters here
    filter_query_1 = f'"{column}" IS NOT NULL'

    filter_query_2 = f"{facet['key']} == '{facet['value']}'" if facet else None

    filter_query = _filter_aggregate(filter_query_1, filter_query_2)

    bin_size = None

    if _are_numeric_values(min_, max_):
        if not isinstance(bins, int):
            raise ValueError(
                f"bins are '{bins}'. Please specify a valid number of bins."
            )

        # Use bins - 1 instead of bins and round half down instead of floor
        # to mimic right-closed histogram intervals in R ggplot
        range_ = max_ - min_
        bin_size = range_ / (bins - 1)
        template_ = """
            select
            ceiling("{{column}}"/{{bin_size}} - 0.5)*{{bin_size}} as bin,
            count(*) as count
            from "{{table}}"
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
            "{{column}}" as col, count ({{column}})
        from "{{table}}"
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
):
    """Compute the corresponding heights of each bin based on the category"""
    if not conn:
        conn = sql.connection.ConnectionManager.current

    cases = []
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

    filter_query = _filter_aggregate(filter_query_1, filter_query_2)

    template = Template(
        """
        SELECT {{category}},
        {{cases}}
        FROM "{{table}}"
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
            from "{{table}}"
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
                from "{{table}}"
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

    ax = ax or plt.gca()
    payload["connection_info"] = conn._get_database_information()

    if column is None:
        raise exceptions.UsageError("Column name has not been specified")

    x, height_, xlabel, ylabel = _bar(table, column, with_=with_, conn=conn)

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
                from "{{table}}"
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
                from "{{table}}"
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

    ax = ax or plt.gca()
    payload["connection_info"] = conn._get_database_information()

    if column is None:
        raise exceptions.UsageError("Column name has not been specified")

    labels, size_ = _pie(table, column, with_=with_, conn=conn)

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
