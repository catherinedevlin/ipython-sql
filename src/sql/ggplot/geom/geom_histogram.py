from sql import plot
from sql.ggplot.geom.geom import geom
from sql.telemetry import telemetry


class geom_histogram(geom):
    """
    Histogram plot

    Parameters
    ----------
    bins: int
        Number of bins

    fill : str
        Create a stacked graph which is a combination of
        'x' and 'fill'

    cmap : str, default 'viridis
        Apply a color map to the stacked graph

    breaks : list
        Divide bins with custom intervals
    """

    def __init__(self, bins=None, fill=None, cmap=None, breaks=None, **kwargs):
        self.bins = bins
        self.fill = fill
        self.cmap = cmap
        self.breaks = breaks
        super().__init__(**kwargs)

    @telemetry.log_call("ggplot-histogram")
    def draw(self, gg, ax=None, facet=None):
        plot.histogram(
            table=gg.table,
            column=gg.mapping.x,
            cmap=self.cmap,
            bins=self.bins,
            conn=gg.conn,
            with_=gg.with_,
            category=self.fill,
            color=gg.mapping.fill,
            edgecolor=gg.mapping.color,
            facet=facet,
            ax=ax or gg.axs[0],
            breaks=self.breaks,
        )
        return gg
