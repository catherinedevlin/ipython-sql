from IPython.core.magic import (
    Magics,
    line_magic,
    magics_class,
)
from IPython.core.magic_arguments import argument, magic_arguments
from ploomber_core.exceptions import modify_exceptions

try:
    from traitlets.config.configurable import Configurable
except ModuleNotFoundError:
    from IPython.config.configurable import Configurable


from sql import plot
from sql.command import SQLPlotCommand
from sql import exceptions
from sql import util

SUPPORTED_PLOTS = ["histogram", "boxplot", "bar", "pie"]


@magics_class
class SqlPlotMagic(Magics, Configurable):
    """%sqlplot magic"""

    @line_magic("sqlplot")
    @magic_arguments()
    @argument("line", default="", nargs="*", type=str, help="Plot name")
    @argument("-t", "--table", type=str, help="Table to use", required=True)
    @argument(
        "-c", "--column", type=str, nargs="+", help="Column(s) to use", required=True
    )
    @argument(
        "-b",
        "--bins",
        type=int,
        default=50,
        help="Histogram bins",
    )
    @argument(
        "-o",
        "--orient",
        type=str,
        default="v",
        help="Boxplot orientation (v/h)",
    )
    @argument(
        "-w",
        "--with",
        type=str,
        help="Use a saved query",
        action="append",
        dest="with_",
    )
    @argument(
        "-S",
        "--show-numbers",
        action="store_true",
        help="Show number of observations",
    )
    @modify_exceptions
    def execute(self, line="", cell="", local_ns=None):
        """
        Plot magic
        """

        cmd = SQLPlotCommand(self, line)

        if len(cmd.args.column) == 1:
            column = cmd.args.column[0]
        else:
            column = cmd.args.column

        if not cmd.args.line:
            plot_str = util.pretty_print(SUPPORTED_PLOTS, last_delimiter="or")
            raise exceptions.UsageError(
                "Missing the first argument, must be any of: "
                f"{plot_str}\n"
                "Example: %sqlplot histogram"
            )

        column = util.sanitize_identifier(column)
        table = util.sanitize_identifier(cmd.args.table)

        if cmd.args.line[0] in {"box", "boxplot"}:
            util.is_table_exists(table, with_=cmd.args.with_)
            return plot.boxplot(
                table=table,
                column=column,
                with_=cmd.args.with_,
                orient=cmd.args.orient,
                conn=None,
            )
        elif cmd.args.line[0] in {"hist", "histogram"}:
            util.is_table_exists(table, with_=cmd.args.with_)

            return plot.histogram(
                table=table,
                column=column,
                bins=cmd.args.bins,
                with_=cmd.args.with_,
                conn=None,
            )
        elif cmd.args.line[0] in {"bar"}:
            util.is_table_exists(table, with_=cmd.args.with_)

            return plot.bar(
                table=table,
                column=column,
                with_=cmd.args.with_,
                orient=cmd.args.orient,
                show_num=cmd.args.show_numbers,
                conn=None,
            )
        elif cmd.args.line[0] in {"pie"}:
            util.is_table_exists(table, with_=cmd.args.with_)

            return plot.pie(
                table=table,
                column=column,
                with_=cmd.args.with_,
                show_num=cmd.args.show_numbers,
                conn=None,
            )
        else:
            plot_str = util.pretty_print(SUPPORTED_PLOTS, last_delimiter="or")
            raise exceptions.UsageError(
                f"Unknown plot {cmd.args.line[0]!r}. Must be any of: " f"{plot_str}"
            )
