"""
Plot using the SQL backend
"""
from ploomber_core.dependencies import requires
from ploomber_core.exceptions import modify_exceptions
from jinja2 import Template

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError:
    plt = None

try:
    import numpy as np
except ModuleNotFoundError:
    np = None


from sql.store import store
import sql.connection
from sql.telemetry import telemetry


def _summary_stats(con, table, column, with_=None):
    """Compute percentiles and mean for boxplot"""
    template = Template(
        """
SELECT
percentile_disc(0.25) WITHIN GROUP (ORDER BY "{{column}}") AS q1,
percentile_disc(0.50) WITHIN GROUP (ORDER BY "{{column}}") AS med,
percentile_disc(0.75) WITHIN GROUP (ORDER BY "{{column}}") AS q3,
AVG("{{column}}") AS mean,
COUNT(*) AS N
FROM "{{table}}"
"""
    )
    query = template.render(table=table, column=column)

    if with_:
        query = str(store.render(query, with_=with_))

    values = con.execute(query).fetchone()
    keys = ["q1", "med", "q3", "mean", "N"]
    return {k: float(v) for k, v in zip(keys, values)}


def _whishi(con, table, column, hival, with_=None):
    template = Template(
        """
SELECT COUNT(*), MAX("{{column}}")
FROM (
    SELECT "{{column}}"
    FROM "{{table}}"
    WHERE "{{column}}" <= {{hival}}
)
"""
    )

    query = template.render(table=table, column=column, hival=hival)

    if with_:
        query = str(store.render(query, with_=with_))

    values = con.execute(query).fetchone()
    keys = ["N", "wiskhi_max"]
    return {k: float(v) for k, v in zip(keys, values)}


def _whislo(con, table, column, loval, with_=None):
    template = Template(
        """
SELECT COUNT(*), MIN("{{column}}")
FROM (
    SELECT "{{column}}"
    FROM "{{table}}"
    WHERE "{{column}}" >= {{loval}}
)
"""
    )

    query = template.render(table=table, column=column, loval=loval)

    if with_:
        query = str(store.render(query, with_=with_))

    values = con.execute(query).fetchone()
    keys = ["N", "wisklo_min"]
    return {k: float(v) for k, v in zip(keys, values)}


def _percentile(con, table, column, pct, with_=None):
    template = Template(
        """
SELECT
percentile_disc({{pct}}) WITHIN GROUP (ORDER BY "{{column}}") AS pct,
FROM "{{table}}"
"""
    )
    query = template.render(table=table, column=column, pct=pct)

    if with_:
        query = str(store.render(query, with_=with_))

    values = con.execute(query).fetchone()[0]
    return values


def _between(con, table, column, whislo, whishi, with_=None):
    template = Template(
        """
SELECT "{{column}}"
FROM "{{table}}"
WHERE "{{column}}" < {{whislo}}
OR  "{{column}}" > {{whishi}}
"""
    )
    query = template.render(table=table, column=column, whislo=whislo, whishi=whishi)

    if with_:
        query = str(store.render(query, with_=with_))

    results = [float(n[0]) for n in con.execute(query).fetchall()]
    return results


# https://github.com/matplotlib/matplotlib/blob/b5ac96a8980fdb9e59c9fb649e0714d776e26701/lib/matplotlib/cbook/__init__.py
@modify_exceptions
def _boxplot_stats(con, table, column, whis=1.5, autorange=False, with_=None):
    """Compute statistics required to create a boxplot"""

    def _compute_conf_interval(N, med, iqr):
        notch_min = med - 1.57 * iqr / np.sqrt(N)
        notch_max = med + 1.57 * iqr / np.sqrt(N)

        return notch_min, notch_max

    stats = dict()

    # arithmetic mean
    s_stats = _summary_stats(con, table, column, with_=with_)

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
        loval, hival = _percentile(con, table, column, whis, with_=with_)

    elif np.isreal(whis):
        loval = q1 - whis * stats["iqr"]
        hival = q3 + whis * stats["iqr"]
    else:
        raise ValueError("whis must be a float or list of percentiles")

    # get high extreme
    wiskhi_d = _whishi(con, table, column, hival, with_=with_)

    if wiskhi_d["N"] == 0 or wiskhi_d["wiskhi_max"] < q3:
        stats["whishi"] = q3
    else:
        stats["whishi"] = wiskhi_d["wiskhi_max"]

    # get low extreme
    wisklo_d = _whislo(con, table, column, loval, with_=with_)

    if wisklo_d["N"] == 0 or wisklo_d["wisklo_min"] > q1:
        stats["whislo"] = q1
    else:
        stats["whislo"] = wisklo_d["wisklo_min"]

    # compute a single array of outliers
    stats["fliers"] = np.array(
        _between(con, table, column, stats["whislo"], stats["whishi"], with_=with_)
    )

    # add in the remaining stats
    stats["q1"], stats["med"], stats["q3"] = q1, med, q3

    bxpstats = {k: v for k, v in stats.items()}

    return bxpstats


# https://github.com/matplotlib/matplotlib/blob/ddc260ce5a53958839c244c0ef0565160aeec174/lib/matplotlib/axes/_axes.py#L3915
@requires(["matplotlib"])
@telemetry.log_call("boxplot", payload=True)
def boxplot(payload, table, column, *, orient="v", with_=None, conn=None):
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
        conn = sql.connection.Connection.current.session

    payload["connection_info"] = sql.connection.Connection._get_curr_connection_info()

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


def _min_max(con, table, column, with_=None):
    template = Template(
        """
SELECT
    MIN("{{column}}"),
    MAX("{{column}}")
FROM "{{table}}"
"""
    )
    query = template.render(table=table, column=column)

    if with_:
        query = str(store.render(query, with_=with_))

    min_, max_ = con.execute(query).fetchone()
    return min_, max_


@requires(["matplotlib"])
@telemetry.log_call("histogram", payload=True)
def histogram(payload, table, column, bins, with_=None, conn=None):
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

    .. versionadded:: 0.4.4

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
    ax = plt.gca()
    payload["connection_info"] = sql.connection.Connection._get_curr_connection_info()
    if isinstance(column, str):
        bin_, height = _histogram(table, column, bins, with_=with_, conn=conn)
        ax.bar(bin_, height, align="center", width=bin_[-1] - bin_[-2])
        ax.set_title(f"{column!r} from {table!r}")
        ax.set_xlabel(column)
    else:
        for col in column:
            bin_, height = _histogram(table, col, bins, with_=with_, conn=conn)
            ax.bar(
                bin_,
                height,
                align="center",
                width=bin_[-1] - bin_[-2],
                alpha=0.5,
                label=col,
            )
            ax.set_title(f"Histogram from {table!r}")
            ax.legend()

    ax.set_ylabel("Count")

    return ax


@modify_exceptions
def _histogram(table, column, bins, with_=None, conn=None):
    """Compute bins and heights"""
    if not conn:
        conn = sql.connection.Connection.current.session

    # FIXME: we're computing all the with elements twice
    min_, max_ = _min_max(conn, table, column, with_=with_)
    range_ = max_ - min_
    bin_size = range_ / bins

    template = Template(
        """
select
  floor("{{column}}"/{{bin_size}})*{{bin_size}},
  count(*) as count
from "{{table}}"
group by 1
order by 1;
"""
    )
    query = template.render(table=table, column=column, bin_size=bin_size)

    if with_:
        query = str(store.render(query, with_=with_))

    data = conn.execute(query).fetchall()
    bin_, height = zip(*data)

    if bin_[0] is None:
        raise ValueError("Data contains NULLs")

    return bin_, height
