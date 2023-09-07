from pathlib import Path
import pytest
from IPython.core.error import UsageError
import matplotlib.pyplot as plt
from sql import util
import duckdb

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


@pytest.fixture
def ip_with_schema_and_table(ip_empty, load_penguin):
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell(
        """%%sql
CREATE SCHEMA sqlalchemy_schema;
CREATE TABLE sqlalchemy_schema.penguins1 (
    species VARCHAR(255),
    island VARCHAR(255),
    bill_length_mm DECIMAL(5, 2),
    bill_depth_mm DECIMAL(5, 2),
    flipper_length_mm DECIMAL(5, 2),
    body_mass_g INTEGER,
    sex VARCHAR(255)
);

COPY sqlalchemy_schema.penguins1 FROM 'penguins.csv' WITH (FORMAT CSV, HEADER TRUE);
"""
    )

    conn = duckdb.connect(database=":memory:", read_only=False)
    ip_empty.push({"conn": conn})
    ip_empty.run_cell("%sql conn")
    ip_empty.run_cell(
        """%%sql
CREATE SCHEMA dbapi_schema;
CREATE TABLE dbapi_schema.penguins2 (
    species VARCHAR(255),
    island VARCHAR(255),
    bill_length_mm DECIMAL(5, 2),
    bill_depth_mm DECIMAL(5, 2),
    flipper_length_mm DECIMAL(5, 2),
    body_mass_g INTEGER,
    sex VARCHAR(255)
);

COPY dbapi_schema.penguins2 FROM 'penguins.csv' WITH (FORMAT CSV, HEADER TRUE);
"""
    )

    yield ip_empty


@pytest.mark.parametrize(
    "cell, error_message",
    [
        [
            "%sqlplot someplot -t a -c b",
            "argument plot_name: invalid choice: 'someplot' "
            "(choose from 'histogram', 'hist', 'boxplot', 'box', 'bar', 'pie')",
        ],
        [
            "%sqlplot -t a -c b",
            "the following arguments are required: plot_name",
        ],
    ],
    ids=["invalid_plot_name", "missing_plot_name"],
)
def test_validate_plot_name(tmp_empty, ip, cell, error_message):
    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(cell)

    assert excinfo.typename == "UsageError"
    assert str(error_message).lower() in str(excinfo.value).lower()


@pytest.mark.parametrize(
    "cell, error_message",
    [
        [
            "%sqlplot histogram --column a",
            "the following arguments are required: -t/--table",
        ],
        [
            "%sqlplot histogram --table a",
            "the following arguments are required: -c/--column",
        ],
    ],
)
def test_validate_arguments(tmp_empty, ip, cell, error_message):
    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(cell)

    assert str(error_message).lower() in str(excinfo.value).lower()


@pytest.mark.parametrize(
    "cell, error_message",
    [
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "--breaks 1000 2000 2699"
            ),
            "All break points are lower than the min data point of 2700.",
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "--breaks 7000 7100 7200"
            ),
            "All break points are higher than the max data point of 6300.",
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "--breaks 3000 4000 5000 --bins 50"
            ),
            "'bins', and 'breaks' are specified. You can only specify one of them.",
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --bins 50 --column body_mass_g"
                " --breaks 3000 4000 5000"
            ),
            "'bins', and 'breaks' are specified. You can only specify one of them.",
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --column bill_length_mm "
                "bill_depth_mm --breaks 30 40 50"
            ),
            "Multiple columns don't support breaks. Please use bins instead.",
        ],
    ],
)
def test_validate_breaks_arguments(load_penguin, ip, cell, error_message):
    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(cell)

    assert error_message in str(excinfo.value)


@pytest.mark.parametrize(
    "cell, error_message",
    [
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "--bins 50 --binwidth 1000"
            ),
            "'bins', and 'binwidth' are specified. You can only specify one of them.",
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "-W 50 --breaks 3000 4000 5000"
            ),
            "'binwidth', and 'breaks' are specified. You can only specify one of them.",
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "--binwidth 0"
            ),
            (
                "Binwidth given : 0.0. When using binwidth, "
                "please ensure to pass a positive value."
            ),
        ],
        [
            (
                "%sqlplot histogram --table penguins.csv --column body_mass_g "
                "--binwidth -10"
            ),
            (
                "Binwidth given : -10.0. When using binwidth, "
                "please ensure to pass a positive value."
            ),
        ],
    ],
)
def test_validate_binwidth_arguments(load_penguin, ip, cell, error_message):
    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(cell)

    assert error_message in str(excinfo.value)
    assert excinfo.value.error_type == "ValueError"


def test_validate_binwidth_text_argument(tmp_empty, ip):
    with pytest.raises(UsageError) as excinfo:
        ip.run_cell(
            "%sqlplot histogram --table penguins.csv "
            "--column body_mass_g --binwidth test"
        )

    assert "argument -W/--binwidth: invalid float value: 'test'" == str(excinfo.value)


def test_binwidth_larger_than_range(load_penguin, ip, capsys):
    ip.run_cell(
        "%sqlplot histogram --table penguins.csv --column body_mass_g --binwidth 3601"
    )
    out, _ = capsys.readouterr()
    assert (
        "Specified binwidth 3601.0 is larger than the range 3600. "
        "Please choose a smaller binwidth."
    ) in out


@_cleanup_cm()
@pytest.mark.parametrize(
    "cell",
    [
        "%sqlplot histogram --table data.csv --column x",
        "%sqlplot hist --table data.csv --column x",
        "%sqlplot histogram --table data.csv --column x --bins 10",
        "%sqlplot histogram --table data.csv --column x --binwidth 1",
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
        "%sqlplot boxplot --table 'file with spaces.csv' --column x",
        "%sqlplot histogram --table 'file with spaces.csv' --column x",
        "%sqlplot bar --table 'file with spaces.csv' --column x",
        "%sqlplot pie --table 'file with spaces.csv' --column x",
    ],
    ids=[
        "histogram",
        "hist",
        "histogram-bins",
        "histogram-binwidth",
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
@image_comparison(
    baseline_images=["boxplot_duckdb"], extensions=["png"], remove_text=True
)
def test_boxplot_duckdb(load_penguin, ip):
    conn = duckdb.connect(database=":memory:", read_only=False)
    ip.push({"conn": conn})
    ip.run_cell("%sql conn")
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


@_cleanup_cm()
@image_comparison(baseline_images=["hist_breaks"], extensions=["png"], remove_text=True)
def test_hist_breaks(load_penguin, ip):
    ip.run_cell(
        "%sqlplot histogram --table penguins.csv --column body_mass_g "
        "--breaks 3000 3100 3300 3700 4000 4600"
    )


@pytest.mark.parametrize(
    "binwidth",
    [
        "--binwidth",
        "-W",
    ],
)
@_cleanup_cm()
@image_comparison(
    baseline_images=["hist_binwidth"], extensions=["png"], remove_text=True
)
def test_hist_binwidth(load_penguin, ip, binwidth):
    ip.run_cell(
        f"%sqlplot histogram --table penguins.csv --column body_mass_g {binwidth} 150"
    )


@pytest.mark.parametrize(
    "cmd, conn",
    [
        (
            "%sqlplot boxplot --table sqlalchemy_schema.penguins1 --column body_mass_g",
            "%sql duckdb://",
        ),
        (
            (
                "%sqlplot boxplot --table penguins1 --schema sqlalchemy_schema "
                "--column body_mass_g"
            ),
            "%sql duckdb://",
        ),
        (
            "%sqlplot boxplot --table dbapi_schema.penguins2 --column body_mass_g",
            "%sql conn",
        ),
        (
            (
                "%sqlplot boxplot --table penguins2 --schema dbapi_schema "
                "--column body_mass_g"
            ),
            "%sql conn",
        ),
        (
            "%sqlplot boxplot --table penguins.csv --column body_mass_g",
            "%sql duckdb://",
        ),
    ],
)
@_cleanup_cm()
@image_comparison(
    baseline_images=["boxplot_with_table_in_schema"],
    extensions=["png"],
    remove_text=True,
)
def test_boxplot_with_table_in_schema(ip_with_schema_and_table, cmd, conn):
    ip_with_schema_and_table.run_cell(conn)
    ip_with_schema_and_table.run_cell(cmd)


@pytest.mark.parametrize(
    "cmd, conn",
    [
        (
            (
                "%sqlplot histogram --table sqlalchemy_schema.penguins1 "
                "--column body_mass_g"
            ),
            "%sql duckdb://",
        ),
        (
            (
                "%sqlplot histogram --table penguins1 --schema sqlalchemy_schema "
                "--column body_mass_g"
            ),
            "%sql duckdb://",
        ),
        (
            "%sqlplot histogram --table dbapi_schema.penguins2 --column body_mass_g",
            "%sql conn",
        ),
        (
            (
                "%sqlplot histogram --table penguins2 --schema dbapi_schema "
                "--column body_mass_g"
            ),
            "%sql conn",
        ),
        (
            "%sqlplot histogram --table penguins.csv --column body_mass_g",
            "%sql duckdb://",
        ),
    ],
)
@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_with_table_in_schema"],
    extensions=["png"],
    remove_text=True,
)
def test_histogram_with_table_in_schema(ip_with_schema_and_table, cmd, conn):
    ip_with_schema_and_table.run_cell(conn)
    ip_with_schema_and_table.run_cell(cmd)


@pytest.mark.parametrize(
    "cmd, conn",
    [
        (
            "%sqlplot bar --table sqlalchemy_schema.penguins1 --column species",
            "%sql duckdb://",
        ),
        (
            (
                "%sqlplot bar --table penguins1 --schema sqlalchemy_schema "
                "--column species"
            ),
            "%sql duckdb://",
        ),
        ("%sqlplot bar --table dbapi_schema.penguins2 --column species", "%sql conn"),
        (
            "%sqlplot bar --table penguins2 --schema dbapi_schema --column species",
            "%sql conn",
        ),
        ("%sqlplot bar --table penguins.csv --column species", "%sql duckdb://"),
    ],
)
@_cleanup_cm()
@image_comparison(
    baseline_images=["bar_with_table_in_schema"], extensions=["png"], remove_text=True
)
def test_bar_with_table_in_schema(ip_with_schema_and_table, cmd, conn):
    ip_with_schema_and_table.run_cell(conn)
    ip_with_schema_and_table.run_cell(cmd)


@pytest.mark.parametrize(
    "cmd, conn",
    [
        (
            "%sqlplot pie --table sqlalchemy_schema.penguins1 --column species",
            "%sql duckdb://",
        ),
        (
            (
                "%sqlplot pie --table penguins1 --schema sqlalchemy_schema "
                "--column species"
            ),
            "%sql duckdb://",
        ),
        ("%sqlplot pie --table dbapi_schema.penguins2 --column species", "%sql conn"),
        (
            "%sqlplot pie --table penguins2 --schema dbapi_schema --column species",
            "%sql conn",
        ),
        ("%sqlplot pie --table penguins.csv --column species", "%sql duckdb://"),
    ],
)
@_cleanup_cm()
@image_comparison(
    baseline_images=["pie_with_table_in_schema"], extensions=["png"], remove_text=True
)
def test_pie_with_table_in_schema(ip_with_schema_and_table, cmd, conn):
    ip_with_schema_and_table.run_cell(conn)
    ip_with_schema_and_table.run_cell(cmd)


@pytest.mark.parametrize(
    "arg",
    [
        "--delete",
        "-d",
        "--delete-force-all",
        "-A",
        "--delete-force",
        "-D",
    ],
)
def test_sqlplot_snippet_deletion(ip_snippets, arg):
    ip_snippets.run_cell(f"%sqlcmd snippets {arg} subset_another")

    with pytest.raises(UsageError) as excinfo:
        ip_snippets.run_cell("%sqlplot boxplot --table subset_another --column x")

    assert "There is no table with name 'subset_another' in the default schema" in str(
        excinfo.value
    )


TABLE_NAME_TYPO_MSG = """
There is no table with name 'subst' in the default schema
Did you mean: 'subset'
If you need help solving this issue, send us a message: https://ploomber.io/community
"""


def test_sqlplot_snippet_typo(ip_snippets):
    with pytest.raises(UsageError) as excinfo:
        ip_snippets.run_cell("%sqlplot boxplot --table subst --column x")

    assert TABLE_NAME_TYPO_MSG.strip() in str(excinfo.value).strip()


MISSING_TABLE_ERROR_MSG = """
There is no table with name 'missing' in the default schema
If you need help solving this issue, send us a message: https://ploomber.io/community
"""


def test_sqlplot_missing_table(ip_snippets, capsys):
    with pytest.raises(UsageError) as excinfo:
        ip_snippets.run_cell("%sqlplot boxplot --table missing --column x")

    assert MISSING_TABLE_ERROR_MSG.strip() in str(excinfo.value).strip()
