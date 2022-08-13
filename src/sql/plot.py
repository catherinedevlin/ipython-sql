"""
Plot using the SQL backend
"""
import functools

import matplotlib.pyplot as plt
import numpy as np
from jinja2 import Template

from sql.store import store
import sql.connection

# TODO: support for a select statement to define table and column

# %%time
# %%memit


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # print(f'start: {func.__name__}')
        result = func(*args, **kwargs)
        # print(f'end: {func.__name__}')
        return result

    return wrapper


# TODO: test with NAs
@log
def _summary_stats(con, table, column):
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
    values = con.execute(query).fetchone()
    keys = ["q1", "med", "q3", "mean", "N"]
    return {k: float(v) for k, v in zip(keys, values)}


@log
def _whishi(con, table, column, hival):
    template = Template(
        """
WITH subset AS (
    SELECT "{{column}}"
    FROM "{{table}}"
    WHERE "{{column}}" <= {{hival}}
)
SELECT COUNT(*), MAX("{{column}}")
FROM subset
"""
    )

    query = template.render(table=table, column=column, hival=hival)
    values = con.execute(query).fetchone()
    keys = ["N", "wiskhi_max"]
    return {k: float(v) for k, v in zip(keys, values)}


@log
def _whislo(con, table, column, loval):
    template = Template(
        """
WITH subset AS (
    SELECT "{{column}}"
    FROM "{{table}}"
    WHERE "{{column}}" >= {{loval}}
)
SELECT COUNT(*), MIN("{{column}}")
FROM subset
"""
    )

    query = template.render(table=table, column=column, loval=loval)
    values = con.execute(query).fetchone()
    keys = ["N", "wisklo_min"]
    return {k: float(v) for k, v in zip(keys, values)}


@log
def _percentile(con, table, column, pct):
    template = Template(
        """
SELECT
percentile_disc({{pct}}) WITHIN GROUP (ORDER BY "{{column}}") AS pct,
FROM "{{table}}"
"""
    )
    query = template.render(table=table, column=column, pct=pct)
    values = con.execute(query).fetchone()[0]
    return values


@log
def _between(con, table, column, whislo, whishi):
    template = Template(
        """
SELECT "{{column}}"
FROM "{{table}}"
WHERE "{{column}}" < {{whislo}}
OR  "{{column}}" > {{whishi}}
"""
    )
    query = template.render(table=table, column=column, whislo=whislo, whishi=whishi)
    results = [float(n[0]) for n in con.execute(query).fetchall()]
    return results


# https://github.com/matplotlib/matplotlib/blob/b5ac96a8980fdb9e59c9fb649e0714d776e26701/lib/matplotlib/cbook/__init__.py
def boxplot_stats(con, table, column, whis=1.5, autorange=False):
    def _compute_conf_interval(N, med, iqr):
        notch_min = med - 1.57 * iqr / np.sqrt(N)
        notch_max = med + 1.57 * iqr / np.sqrt(N)

        return notch_min, notch_max

    stats = dict()

    # arithmetic mean
    s_stats = _summary_stats(con, table, column)

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
        loval, hival = _percentile(con, table, column, whis)

    elif np.isreal(whis):
        loval = q1 - whis * stats["iqr"]
        hival = q3 + whis * stats["iqr"]
    else:
        raise ValueError("whis must be a float or list of percentiles")

    # get high extreme
    wiskhi_d = _whishi(con, table, column, hival)

    if wiskhi_d["N"] == 0 or wiskhi_d["wiskhi_max"] < q3:
        stats["whishi"] = q3
    else:
        stats["whishi"] = wiskhi_d["wiskhi_max"]

    # get low extreme
    wisklo_d = _whislo(con, table, column, loval)

    if wisklo_d["N"] == 0 or wisklo_d["wisklo_min"] > q1:
        stats["whislo"] = q1
    else:
        stats["whislo"] = wisklo_d["wisklo_min"]

    # compute a single array of outliers
    stats["fliers"] = np.array(
        _between(con, table, column, stats["whislo"], stats["whishi"])
    )

    # add in the remaining stats
    stats["q1"], stats["med"], stats["q3"] = q1, med, q3

    # output is a list of dicts
    bxpstats = [{k: v for k, v in stats.items()}]

    return bxpstats


# https://github.com/matplotlib/matplotlib/blob/ddc260ce5a53958839c244c0ef0565160aeec174/lib/matplotlib/axes/_axes.py#L3915
def boxplot(table, column, conn=None):
    if not conn:
        conn = sql.connection.Connection.current.session

    stats = boxplot_stats(conn, table, column)
    ax = plt.gca()
    ax.bxp(stats)


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


# TODO: add unit tests
def histogram(table, column, bins, with_=None, conn=None):
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

    ax = plt.gca()
    ax.bar(bin_, height, align="center", width=bin_[-1] - bin_[-2])
