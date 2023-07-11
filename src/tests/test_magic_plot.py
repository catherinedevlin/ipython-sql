import sys
from pathlib import Path
import pytest
from IPython.core.error import UsageError
import matplotlib.pyplot as plt
from sql import util

from matplotlib.testing.decorators import image_comparison, _cleanup_cm

SUPPORTED_PLOTS = ["bar", "boxplot", "histogram", "pie"]
plot_str = util.pretty_print(SUPPORTED_PLOTS, last_delimiter="or")


@pytest.fixture
def ip_snippets(ip, tmp_empty):
    Path("data.csv").write_text(
        """\
x, y
0, 0
1, 1
2, 2
"""
    )
    ip.run_cell("%sql duckdb://")

    ip.run_cell(
        """%%sql --save subset --no-execute
SELECT *
FROM data.csv
WHERE x > -1
"""
    )
    ip.run_cell(
        """%%sql --save subset_another --no-execute
SELECT *
FROM subset
WHERE x > 2
"""
    )
    yield ip


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlplot someplot -t a -c b",
            UsageError,
            f"Unknown plot 'someplot'. Must be any of: {plot_str}",
        ],
        [
            "%sqlplot -t a -c b",
            UsageError,
            f"Missing the first argument, must be any of: {plot_str}",
        ],
    ],
)
def test_validate_plot_name(tmp_empty, ip, cell, error_type, error_message):
    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(error_message).lower() in str(out.error_in_exec).lower()


@pytest.mark.parametrize(
    "cell, error_type, error_message",
    [
        [
            "%sqlplot histogram --column a",
            UsageError,
            "the following arguments are required: -t/--table",
        ],
        [
            "%sqlplot histogram --table a",
            UsageError,
            "the following arguments are required: -c/--column",
        ],
    ],
)
def test_validate_arguments(tmp_empty, ip, cell, error_type, error_message):
    out = ip.run_cell(cell)

    assert isinstance(out.error_in_exec, error_type)
    assert str(out.error_in_exec) == (error_message)


@_cleanup_cm()
@pytest.mark.parametrize(
    "cell",
    [
        "%sqlplot histogram --table data.csv --column x",
        "%sqlplot hist --table data.csv --column x",
        "%sqlplot histogram --table data.csv --column x --bins 10",
        pytest.param(
            "%sqlplot histogram --table nas.csv --column x",
            marks=pytest.mark.xfail(reason="Not implemented yet"),
        ),
        "%sqlplot boxplot --table data.csv --column x",
        "%sqlplot box --table data.csv --column x",
        "%sqlplot boxplot --table data.csv --column x --orient h",
        "%sqlplot boxplot --table subset --column x",
        "%sqlplot boxplot --table subset --column x --with subset",
        "%sqlplot boxplot -t subset -c x -w subset -o h",
        "%sqlplot boxplot --table nas.csv --column x",
        "%sqlplot bar -t data.csv -c x",
        "%sqlplot bar --table subset --column x",
        "%sqlplot bar --table subset --column x --with subset",
        "%sqlplot bar -t data.csv -c x -S",
        "%sqlplot bar -t data.csv -c x -o h",
        "%sqlplot bar -t data.csv -c x y",
        "%sqlplot pie -t data.csv -c x",
        "%sqlplot pie --table subset --column x",
        "%sqlplot pie --table subset --column x --with subset",
        "%sqlplot pie -t data.csv -c x -S",
        "%sqlplot pie -t data.csv -c x y",
        '%sqlplot boxplot --table spaces.csv --column "some column"',
        '%sqlplot histogram --table spaces.csv --column "some column"',
        '%sqlplot bar --table spaces.csv --column "some column"',
        '%sqlplot pie --table spaces.csv --column "some column"',
        pytest.param(
            "%sqlplot boxplot --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot histogram --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot bar --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
        pytest.param(
            "%sqlplot pie --table 'file with spaces.csv' --column x",
            marks=pytest.mark.xfail(
                sys.platform == "win32",
                reason="problem in IPython.core.magic_arguments.parse_argstring",
            ),
        ),
    ],
    ids=[
        "histogram",
        "hist",
        "histogram-bins",
        "histogram-nas",
        "boxplot",
        "boxplot-with",
        "box",
        "boxplot-horizontal",
        "boxplot-with",
        "boxplot-shortcuts",
        "boxplot-nas",
        "bar-1-col",
        "bar-subset",
        "bar-subset-with",
        "bar-1-col-show_num",
        "bar-1-col-horizontal",
        "bar-2-col",
        "pie-1-col",
        "pie-subset",
        "pie-subset-with",
        "pie-1-col-show_num",
        "pie-2-col",
        "boxplot-column-name-with-spaces",
        "histogram-column-name-with-spaces",
        "bar-column-name-with-spaces",
        "pie-column-name-with-spaces",
        "boxplot-table-name-with-spaces",
        "histogram-table-name-with-spaces",
        "bar-table-name-with-spaces",
        "pie-table-name-with-spaces",
    ],
)
def test_sqlplot(tmp_empty, ip, cell):
    # clean current Axes
    plt.cla()

    Path("spaces.csv").write_text(
        """\
"some column", y
0, 0
1, 1
2, 2
"""
    )

    Path("data.csv").write_text(
        """\
x, y
0, 0
1, 1
2, 2
"""
    )

    Path("nas.csv").write_text(
        """\
x, y
, 0
1, 1
2, 2
"""
    )

    Path("file with spaces.csv").write_text(
        """\
x, y
0, 0
1, 1
2, 2
"""
    )
    ip.run_cell("%sql duckdb://")

    ip.run_cell(
        """%%sql --save subset --no-execute
SELECT *
FROM data.csv
WHERE x > -1
"""
    )

    out = ip.run_cell(cell)

    # maptlotlib >= 3.7 has Axes but earlier Python
    # versions are not compatible
    assert type(out.result).__name__ in {"Axes", "AxesSubplot"}


@pytest.fixture
def load_data_two_col(ip):
    if not Path("data_two.csv").is_file():
        Path("data_two.csv").write_text(
            """\
x, y
0, 0
1, 1
2, 2
5, 7"""
        )

    ip.run_cell("%sql duckdb://")


@pytest.fixture
def load_data_one_col(ip):
    if not Path("data_one.csv").is_file():
        Path("data_one.csv").write_text(
            """\
x
0
0
1
1
1
2
"""
        )
    ip.run_cell("%sql duckdb://")


@pytest.fixture
def load_data_one_col_null(ip):
    if not Path("data_one_null.csv").is_file():
        Path("data_one_null.csv").write_text(
            """\
x

0

0
1

1
1
2
"""
        )
    ip.run_cell("%sql duckdb://")


@_cleanup_cm()
@image_comparison(baseline_images=["bar_one_col"], extensions=["png"], remove_text=True)
def test_bar_one_col(load_data_one_col, ip):
    ip.run_cell("%sqlplot bar -t data_one.csv -c x")


@_cleanup_cm()
@image_comparison(
    baseline_images=["bar_one_col_null"], extensions=["png"], remove_text=True
)
def test_bar_one_col_null(load_data_one_col_null, ip):
    ip.run_cell("%sqlplot bar -t data_one_null.csv -c x")


@_cleanup_cm()
@image_comparison(
    baseline_images=["bar_one_col_h"], extensions=["png"], remove_text=True
)
def test_bar_one_col_h(load_data_one_col, ip):
    ip.run_cell("%sqlplot bar -t data_one.csv -c x -o h")


@_cleanup_cm()
@image_comparison(
    baseline_images=["bar_one_col_num_h"], extensions=["png"], remove_text=True
)
def test_bar_one_col_num_h(load_data_one_col, ip):
    ip.run_cell("%sqlplot bar -t data_one.csv -c x -o h -S")


@_cleanup_cm()
@image_comparison(
    baseline_images=["bar_one_col_num_v"], extensions=["png"], remove_text=True
)
def test_bar_one_col_num_v(load_data_one_col, ip):
    ip.run_cell("%sqlplot bar -t data_one.csv -c x -S")


@_cleanup_cm()
@image_comparison(baseline_images=["bar_two_col"], extensions=["png"], remove_text=True)
def test_bar_two_col(load_data_two_col, ip):
    ip.run_cell("%sqlplot bar -t data_two.csv -c x y")


@_cleanup_cm()
@image_comparison(baseline_images=["pie_one_col"], extensions=["png"], remove_text=True)
def test_pie_one_col(load_data_one_col, ip):
    ip.run_cell("%sqlplot pie -t data_one.csv -c x")


@_cleanup_cm()
@image_comparison(
    baseline_images=["pie_one_col_null"], extensions=["png"], remove_text=True
)
def test_pie_one_col_null(load_data_one_col_null, ip):
    ip.run_cell("%sqlplot pie -t data_one_null.csv -c x")


@_cleanup_cm()
@image_comparison(
    baseline_images=["pie_one_col_num"], extensions=["png"], remove_text=True
)
def test_pie_one_col_num(load_data_one_col, ip):
    ip.run_cell("%sqlplot pie -t data_one.csv -c x -S")


@_cleanup_cm()
@image_comparison(baseline_images=["pie_two_col"], extensions=["png"], remove_text=True)
def test_pie_two_col(load_data_two_col, ip):
    ip.run_cell("%sqlplot pie -t data_two.csv -c x y")


@_cleanup_cm()
@image_comparison(baseline_images=["boxplot"], extensions=["png"], remove_text=True)
def test_boxplot(load_penguin, ip):
    ip.run_cell("%sqlplot boxplot --table penguins.csv --column body_mass_g")


@_cleanup_cm()
@image_comparison(baseline_images=["boxplot_h"], extensions=["png"], remove_text=True)
def test_boxplot_h(load_penguin, ip):
    ip.run_cell("%sqlplot boxplot --table penguins.csv --column body_mass_g --orient h")


@_cleanup_cm()
@image_comparison(baseline_images=["boxplot_two"], extensions=["png"], remove_text=True)
def test_boxplot_two_col(load_penguin, ip):
    ip.run_cell(
        "%sqlplot boxplot --table penguins.csv --column bill_length_mm "
        "bill_depth_mm flipper_length_mm"
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["boxplot_null"], extensions=["png"], remove_text=True
)
def test_boxplot_null(load_penguin, ip):
    ip.run_cell("%sqlplot boxplot --table penguins.csv --column bill_length_mm ")


@_cleanup_cm()
@image_comparison(baseline_images=["hist"], extensions=["png"], remove_text=True)
def test_hist(load_penguin, ip):
    ip.run_cell("%sqlplot histogram --table penguins.csv --column body_mass_g")


@_cleanup_cm()
@image_comparison(baseline_images=["hist_bin"], extensions=["png"], remove_text=True)
def test_hist_bin(load_penguin, ip):
    ip.run_cell(
        "%sqlplot histogram --table penguins.csv --column body_mass_g --bins 300"
    )


@_cleanup_cm()
@image_comparison(baseline_images=["hist_two"], extensions=["png"], remove_text=True)
def test_hist_two(load_penguin, ip):
    ip.run_cell(
        "%sqlplot histogram --table penguins.csv --column bill_length_mm bill_depth_mm"
    )


@_cleanup_cm()
@image_comparison(baseline_images=["hist_null"], extensions=["png"], remove_text=True)
def test_hist_null(load_penguin, ip):
    ip.run_cell("%sqlplot histogram --table penguins.csv --column bill_length_mm ")


@_cleanup_cm()
@image_comparison(baseline_images=["hist_custom"], extensions=["png"], remove_text=True)
def test_hist_cust(load_penguin, ip):
    ax = ip.run_cell(
        "%sqlplot histogram --table penguins.csv --column bill_length_mm "
    ).result
    ax.set_title("Custom Title")
    _ = ax.grid(True)


@pytest.mark.parametrize(
    "arg", ["--delete", "-d", "--delete-force-all", "-A", "--delete-force", "-D"]
)
def test_sqlplot_snippet_deletion(ip_snippets, arg, capsys):
    ip_snippets.run_cell(f"%sqlcmd snippets {arg} subset_another")
    ip_snippets.run_cell("%sqlplot boxplot --table subset_another --column x")
    out, err = capsys.readouterr()
    assert "There is no table with name 'subset_another' in the default schema" in err


TABLE_NAME_TYPO_MSG = """
UsageError: There is no table with name 'subst' in the default schema
Did you mean : 'subset'
If you need help solving this issue, send us a message: https://ploomber.io/community
"""


def test_sqlplot_snippet_typo(ip_snippets, capsys):
    ip_snippets.run_cell("%sqlplot boxplot --table subst --column x")
    out, err = capsys.readouterr()
    assert TABLE_NAME_TYPO_MSG.strip() == err.strip()


MISSING_TABLE_ERROR_MSG = """
UsageError: There is no table with name 'missing' in the default schema
If you need help solving this issue, send us a message: https://ploomber.io/community
"""


def test_sqlplot_missing_table(ip_snippets, capsys):
    ip_snippets.run_cell("%sqlplot boxplot --table missing --column x")
    out, err = capsys.readouterr()
    assert MISSING_TABLE_ERROR_MSG.strip() == err.strip()
