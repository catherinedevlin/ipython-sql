from sql.ggplot.aes import aes
from sql.ggplot.geom.geom import geom
from sql.ggplot.facet_wrap import facet_wrap
import matplotlib as mpl
import matplotlib.pyplot as plt
from ploomber_core.dependencies import requires
from sql.telemetry import telemetry


def _expand_to_multipanel_ax(figure, ax_to_clear=None):
    figure.subplots_adjust(hspace=0.7, wspace=0.5)
    if ax_to_clear:
        ax_to_clear.remove()


def _create_single_panel_ax():
    figure, ax = plt.subplots()
    axs = [ax]
    return figure, axs


@requires(["matplotlib"])
class ggplot:
    """
    Create a new ggplot
    """

    figure: mpl.figure.Figure
    axs: list

    @telemetry.log_call("ggplot-init")
    def __init__(self, table, mapping: aes = None, conn=None, with_=None) -> None:
        self.table = table
        self.with_ = [with_] if with_ else None
        self.mapping = mapping if mapping is not None else aes()
        self.conn = conn

        figure, axs = _create_single_panel_ax()

        self.axs = axs
        self.figure = figure

    def __add__(self, other) -> "ggplot":
        """
        Add to ggplot
        """
        self._draw(other)

        return self

    def __iadd__(self, other):
        return other.__add__(self)

    def _draw(self, other) -> mpl.figure.Figure:
        """
        Draws plot
        """
        if isinstance(other, geom):
            self.geom = other
            other.draw(self)

        if isinstance(other, facet_wrap):
            _expand_to_multipanel_ax(self.figure, ax_to_clear=self.axs[0])

            values, n_rows, n_cols = other.get_facet_values(
                self.table, other.facet, with_=self.with_
            )

            for i, value in enumerate(values):
                ax_ = self.figure.add_subplot(n_rows, n_cols, i + 1)
                facet_key_val = {"key": other.facet, "value": value[0]}
                self.geom.draw(self, ax_, facet_key_val)
                handles, labels = ax_.get_legend_handles_labels()
                ax_.set_title(value[0])
                ax_.tick_params(axis="both", labelsize=7)
                # reverses legend order so alphabetically first goes on top
                ax_.legend(handles[::-1], labels[::-1], prop={"size": 10})
                if other.legend is False:
                    plt.legend("", frameon=False)
                self.axs.append(ax_)

        return self.figure

    def get_base(self, object) -> str:
        """
        Returns the base class of an object
        """
        for base in object.__class__.__bases__:
            return base.__name__
