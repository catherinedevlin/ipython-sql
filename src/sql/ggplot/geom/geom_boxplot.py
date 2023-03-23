from sql import plot
from sql.ggplot.geom.geom import geom


class geom_boxplot(geom):
    """
    Boxplot
    """

    def __init__(self):
        pass

    def draw(self, gg, ax=None):
        plot.boxplot(
            table=gg.table,
            column=gg.mapping.x,
            conn=gg.conn,
            with_=gg.with_,
            ax=ax or gg.axs[0]
        )

        return gg
