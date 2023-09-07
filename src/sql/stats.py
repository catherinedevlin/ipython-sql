from jinja2 import Template
from sqlalchemy.exc import ProgrammingError

import sql.connection
from sql.util import flatten
from sql import exceptions


def _summary_stats(conn, table, column, with_=None):
    if conn.dialect in {"duckdb", "postgresql"}:
        return _summary_stats_parallel(conn, table, column, with_=with_)
    elif conn.dialect in {"redshift"}:
        return _summary_stats_redshift(conn, table, column, with_=with_)
    else:
        return _summary_stats_one_by_one(conn, table, column, with_=with_)


def _summary_stats_one_by_one(conn, table, column, with_=None):
    if not conn:
        conn = sql.connection.ConnectionManager.current.connection

    template_percentile = Template(
        """
SELECT
percentile_disc(0.25) WITHIN GROUP (ORDER BY "{{column}}") OVER (),
percentile_disc(0.50) WITHIN GROUP (ORDER BY "{{column}}") OVER (),
percentile_disc(0.75) WITHIN GROUP (ORDER BY "{{column}}") OVER ()
FROM {{table}}
"""
    )
    query = template_percentile.render(table=table, column=column)

    percentiles = list(conn.execute(query, with_).fetchone())

    template = Template(
        """
SELECT
AVG("{{column}}") AS mean,
COUNT(*) AS N
FROM {{table}}
"""
    )
    query = template.render(table=table, column=column)

    other = list(conn.execute(query, with_).fetchone())

    keys = ["q1", "med", "q3", "mean", "N"]

    return {k: float(v) for k, v in zip(keys, percentiles + other)}


def _summary_stats_redshift(conn, table, column, with_=None):
    if not conn:
        conn = sql.connection.ConnectionManager.current.connection

    template_percentile = Template(
        """
SELECT
approximate percentile_disc(0.25) WITHIN GROUP (ORDER BY "{{column}}"),
approximate percentile_disc(0.50) WITHIN GROUP (ORDER BY "{{column}}"),
approximate percentile_disc(0.75) WITHIN GROUP (ORDER BY "{{column}}")
FROM {{table}}
"""
    )
    query = template_percentile.render(table=table, column=column)

    percentiles = list(conn.execute(query, with_).fetchone())

    template = Template(
        """
SELECT
AVG("{{column}}") AS mean,
COUNT(*) AS N
FROM {{table}}
"""
    )
    query = template.render(table=table, column=column)

    other = list(conn.execute(query, with_).fetchone())

    keys = ["q1", "med", "q3", "mean", "N"]

    return {k: float(v) for k, v in zip(keys, percentiles + other)}


def _summary_stats_parallel(conn, table, column, with_=None):
    """Compute percentiles and mean for boxplot"""

    if not conn:
        conn = sql.connection.ConnectionManager.current

    driver = conn._get_database_information()["driver"]

    template = Template(
        """
    SELECT
    percentile_disc([0.25, 0.50, 0.75]) WITHIN GROUP \
    (ORDER BY "{{column}}") AS percentiles,
    AVG("{{column}}") AS mean,
    COUNT(*) AS N
    FROM {{table}}
"""
    )

    query = template.render(table=table, column=column)

    try:
        values = conn.execute(query, with_).fetchone()
    except ProgrammingError as e:
        print(e)
        raise exceptions.RuntimeError(
            f"\nEnsure that percentile_disc function is available on {driver}."
        )
    except Exception as e:
        raise e

    keys = ["q1", "med", "q3", "mean", "N"]
    return {k: float(v) for k, v in zip(keys, flatten(values))}
