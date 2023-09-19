from jinja2 import Template
import math
import sql.connection
from sql.telemetry import telemetry
from sql.util import enclose_table_with_double_quotations


class facet:
    def __init__():
        pass

    def get_facet_values(self, table, column, with_):
        conn = sql.connection.ConnectionManager.current
        table = enclose_table_with_double_quotations(table, conn)
        template = Template(
            """
            SELECT
            distinct ({{column}})
            FROM {{table}}
            ORDER BY {{column}}
            """
        )
        query = template.render(table=table, column=column)

        values = conn.execute(query, with_).fetchall()
        # Added to make histogram more inclusive to NULLs
        # Filter out NULL values
        # If value[0] is NULL we skip it

        values = [value for value in values if value[0] is not None]
        n_plots = len(values)
        n_cols = len(values) if len(values) < 3 else 3
        n_rows = math.ceil(n_plots / n_cols)
        return values, n_rows, n_cols


class facet_wrap(facet):
    """
    Splits a plot into a matrix of panels

    Parameters
    ----------
    facet : str
        Column to groupby and plot on different panels.
    """

    @telemetry.log_call("facet-wrap-init")
    def __init__(self, facet: str, legend=True):
        self.facet = facet
        self.legend = legend

    def __add__(self, other):
        return other.__add__(other)
