from sql.ggplot import ggplot, aes, geom_boxplot, geom_histogram, facet_wrap
from matplotlib.testing.decorators import image_comparison, _cleanup_cm
import pytest
from pathlib import Path
from urllib.request import urlretrieve
from IPython.core.error import UsageError


@pytest.fixture
def short_trips_data(ip, yellow_trip_data):
    ip.run_cell(
        """
        %sql duckdb://
        """
    )

    ip.run_cell(
        f"""
        %%sql --save short_trips --no-execute
        select * from "{yellow_trip_data}"
        WHERE trip_distance < 6.3
        """
    ).result


@pytest.fixture
def yellow_trip_data(ip, tmpdir):
    ip.run_cell(
        """
    %sql duckdb://
    """
    )

    file_path_str = str(tmpdir.join("yellow_tripdata_2021-01.parquet"))

    if not Path(file_path_str).is_file():
        urlretrieve(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            "yellow_tripdata_2021-01.parquet",
            file_path_str,
        )

    yield file_path_str


@pytest.fixture
def diamonds_data(ip, tmpdir):
    ip.run_cell(
        """
        %sql duckdb://
        """
    )

    file_path_str = str(tmpdir.join("diamonds.csv"))

    if not Path(file_path_str).is_file():
        urlretrieve(
            "https://raw.githubusercontent.com/tidyverse/ggplot2/main/data-raw/diamonds.csv",  # noqa breaks the check-for-broken-links
            file_path_str,
        )

    yield file_path_str


@pytest.fixture
def penguins_data(ip, tmpdir):
    file_path_str = str(tmpdir.join("penguins.csv"))

    ip.run_cell(
        """
        %sql duckdb://
        """
    )

    if not Path(file_path_str).is_file():
        urlretrieve(
            "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",  # noqa breaks the check-for-broken-links
            file_path_str,
        )

    yield file_path_str


@pytest.fixture
def nulls_data(ip, tmpdir):
    if not Path("data_nulls.csv").is_file():
        Path("data_nulls.csv").write_text(
            (
                "name,age,model\n"
                "Dan,33,BMW\nBob,19,BMW\nSheri,15,Audi\nVin,33,\nMick,93,Audi\n"
                "Jay,33,BMW\nSky,33,\nKay,48,BMW\nJan,86,Audi\n\nMike,,Audi"
            )
        )
    ip.run_cell("%sql duckdb://")


@pytest.fixture
def penguins_no_nulls(ip, penguins_data):
    ip.run_cell(
        """
        %sql duckdb://
        """
    )

    ip.run_cell(
        f"""
%%sql --save no_nulls --no-execute
SELECT *
FROM "{penguins_data}"
WHERE body_mass_g IS NOT NULL and
sex IS NOT NULL
    """
    ).result


@_cleanup_cm()
@image_comparison(baseline_images=["boxplot"], extensions=["png"], remove_text=True)
def test_ggplot_geom_boxplot(yellow_trip_data):
    (ggplot(yellow_trip_data, aes(x="trip_distance")) + geom_boxplot())


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_default"], extensions=["png"], remove_text=True
)
def test_ggplot_geom_histogram(yellow_trip_data):
    (
        ggplot(yellow_trip_data, aes(x="trip_distance", color="white"))
        + geom_histogram(bins=10)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_with_default"], extensions=["png"], remove_text=True
)
def test_ggplot_geom_histogram_with(short_trips_data):
    (
        ggplot(table="short_trips", with_="short_trips", mapping=aes(x="trip_distance"))
        + geom_histogram(bins=10)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_custom_color"], extensions=["png"], remove_text=True
)
def test_ggplot_geom_histogram_edge_color(short_trips_data):
    (
        ggplot(
            table="short_trips",
            with_="short_trips",
            mapping=aes(x="trip_distance", color="white"),
        )
        + geom_histogram(bins=10)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_custom_fill"], extensions=["png"], remove_text=True
)
def test_ggplot_geom_histogram_fill(short_trips_data):
    (
        ggplot(
            table="short_trips",
            with_="short_trips",
            mapping=aes(x="trip_distance", fill="red"),
        )
        + geom_histogram(bins=10)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_custom_fill_and_color"],
    extensions=["png"],
    remove_text=True,
)
def test_ggplot_geom_histogram_fill_and_color(short_trips_data):
    (
        ggplot(
            table="short_trips",
            with_="short_trips",
            mapping=aes(x="trip_distance", fill="red", color="#fff"),
        )
        + geom_histogram(bins=10)
    )


@pytest.mark.parametrize(
    "x",
    [
        "price",
        ["price"],
    ],
)
@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_default"],
    extensions=["png"],
    remove_text=True,
)
def test_example_histogram_stacked_default(diamonds_data, x):
    (ggplot(diamonds_data, aes(x=x)) + geom_histogram(bins=10, fill="cut"))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_custom_cmap"],
    extensions=["png"],
    remove_text=True,
)
def test_example_histogram_stacked_custom_cmap(diamonds_data):
    (
        ggplot(diamonds_data, aes(x="price"))
        + geom_histogram(bins=10, fill="cut", cmap="plasma")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_custom_color"],
    extensions=["png"],
    remove_text=True,
)
def test_example_histogram_stacked_custom_color(diamonds_data):
    (
        ggplot(diamonds_data, aes(x="price", color="k"))
        + geom_histogram(bins=10, cmap="plasma", fill="cut")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_custom_color_and_fill"],
    extensions=["png"],
    remove_text=True,
)
def test_example_histogram_stacked_custom_color_and_fill(diamonds_data):
    (
        ggplot(diamonds_data, aes(x="price", color="white", fill="red"))
        + geom_histogram(bins=10, cmap="plasma", fill="cut")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_custom_color_and_fill"],
    extensions=["png"],
    remove_text=True,
)
def test_ggplot_geom_histogram_fill_with_multi_color_warning(diamonds_data):
    with pytest.warns(UserWarning):
        (
            ggplot(diamonds_data, aes(x="price", color="white", fill=["red", "blue"]))
            + geom_histogram(bins=10, cmap="plasma", fill="cut")
        )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_large_bins"],
    extensions=["png"],
    remove_text=True,
)
def test_example_histogram_stacked_with_large_bins(diamonds_data):
    (ggplot(diamonds_data, aes(x="price")) + geom_histogram(bins=400, fill="cut"))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_categorical"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_histogram(diamonds_data):
    (ggplot(diamonds_data, aes(x=["cut"])) + geom_histogram())


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_categorical_combined"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_histogram_combined(diamonds_data):
    (ggplot(diamonds_data, aes(x=["color", "carat"])) + geom_histogram(bins=10))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_numeric_categorical_combined"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_and_numeric_histogram_combined(diamonds_data):
    (ggplot(diamonds_data, aes(x=["color", "carat"])) + geom_histogram(bins=20))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_numeric_categorical_combined_custom_fill"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_and_numeric_histogram_combined_custom_fill(diamonds_data):
    (
        ggplot(diamonds_data, aes(x=["color", "carat"], fill="red"))
        + geom_histogram(bins=20)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_numeric_categorical_combined_custom_multi_fill"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_and_numeric_histogram_combined_custom_multi_fill(diamonds_data):
    (
        ggplot(diamonds_data, aes(x=["color", "carat"], fill=["red", "blue"]))
        + geom_histogram(bins=20)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_numeric_categorical_combined_custom_multi_color"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_and_numeric_histogram_combined_custom_multi_color(diamonds_data):
    (
        ggplot(diamonds_data, aes(x=["color", "carat"], color=["green", "magenta"]))
        + geom_histogram(bins=20)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_default"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_default(penguins_no_nulls):
    (
        ggplot(table="no_nulls", with_="no_nulls", mapping=aes(x=["bill_depth_mm"]))
        + geom_histogram(bins=10)
        + facet_wrap("sex")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_default_no_legend"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_default_no_legend(penguins_no_nulls):
    (
        ggplot(table="no_nulls", with_="no_nulls", mapping=aes(x=["bill_depth_mm"]))
        + geom_histogram(bins=10)
        + facet_wrap("sex", legend=False)
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_custom_fill"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_custom_fill(penguins_no_nulls):
    (
        ggplot(
            table="no_nulls",
            with_="no_nulls",
            mapping=aes(x=["bill_depth_mm"], fill=["red"]),
        )
        + geom_histogram(bins=10)
        + facet_wrap("sex")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_custom_fill_and_color"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_custom_fill_and_color(penguins_no_nulls):
    (
        ggplot(
            table="no_nulls",
            with_="no_nulls",
            mapping=aes(x=["bill_depth_mm"], color="#fff", fill=["red"]),
        )
        + geom_histogram(bins=10)
        + facet_wrap("sex")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_custom_stacked_histogram"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_stacked_histogram(diamonds_data):
    (
        ggplot(diamonds_data, aes(x=["price"]))
        + geom_histogram(bins=10, fill="color")
        + facet_wrap("cut")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_custom_stacked_histogram_cmap"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_stacked_histogram_cmap(diamonds_data):
    (
        ggplot(diamonds_data, aes(x=["price"]))
        + geom_histogram(bins=10, fill="color", cmap="plasma")
        + facet_wrap("cut")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["test_facet_wrap_default_with_nulls"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_default_with_nulls(penguins_data):
    (
        ggplot(table=penguins_data, mapping=aes(x=["bill_depth_mm"]))
        + geom_histogram(bins=10)
        + facet_wrap("sex")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["facet_wrap_nulls_data"],
    extensions=["png"],
    remove_text=False,
)
def test_facet_wrap_default_with_dummy(nulls_data):
    (
        ggplot(table="data_nulls.csv", mapping=aes(x=["age"]))
        + geom_histogram(bins=10)
        + facet_wrap("model")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_with_breaks"],
    extensions=["png"],
    remove_text=True,
)
def test_histogram_with_breaks(penguins_data):
    (
        ggplot(table=penguins_data, mapping=aes(x="body_mass_g"))
        + geom_histogram(breaks=[3000, 3100, 3300, 3700, 4000, 4600])
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_with_breaks"],
    extensions=["png"],
    remove_text=True,
)
def test_histogram_stacked_with_breaks(penguins_data):
    (
        ggplot(table=penguins_data, mapping=aes(x="body_mass_g"))
        + geom_histogram(breaks=[3000, 3100, 3300, 3700, 4000, 4600], fill="species")
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_with_extreme_breaks"],
    extensions=["png"],
    remove_text=True,
)
def test_histogram_with_extreme_breaks(penguins_data):
    (
        ggplot(table=penguins_data, mapping=aes(x="body_mass_g"))
        + geom_histogram(breaks=[1000, 2000, 2500, 2700, 3000], fill="species")
    )


@pytest.mark.parametrize(
    "x, expected_error, expected_error_message",
    [
        ([], ValueError, "Column name has not been specified"),
        ([""], ValueError, "Column name has not been specified"),
        (None, ValueError, "Column name has not been specified"),
        ("", ValueError, "Column name has not been specified"),
        ([None, None], ValueError, "please ensure that you specify only one column"),
        (
            ["price", "table"],
            ValueError,
            "please ensure that you specify only one column",
        ),
        (
            ["price", "table", "color"],
            ValueError,
            "please ensure that you specify only one column",
        ),
        ([None], TypeError, "expected str instance, NoneType found"),
    ],
)
def test_example_histogram_stacked_input_error(
    diamonds_data, x, expected_error, expected_error_message
):
    with pytest.raises(expected_error) as error:
        (ggplot(diamonds_data, aes(x=x)) + geom_histogram(bins=500, fill="cut"))

    assert expected_error_message in str(error.value)


def test_histogram_no_bins_error(diamonds_data):
    with pytest.raises(ValueError) as error:
        (ggplot(diamonds_data, aes(x=["price"])) + geom_histogram())

    assert "Please specify a valid number of bins." in str(error.value)


@pytest.mark.parametrize(
    "bins, breaks, error_message",
    [
        (
            None,
            [3000.0],
            (
                "Breaks given : [3000.0]. When using breaks, "
                "please ensure to specify at least two points."
            ),
        ),
        (
            None,
            [3000.0, 4000.0, 3999.0],
            (
                "Breaks given : [3000.0, 4000.0, 3999.0]. When using breaks, "
                "please ensure that breaks are strictly increasing."
            ),
        ),
        (
            40,
            [3000.0, 4000.0, 5000.0],
            "Both bins and breaks are specified. Must specify only one of them.",
        ),
    ],
)
def test_hist_breaks_error(penguins_data, bins, breaks, error_message):
    with pytest.raises(UsageError) as error:
        (
            ggplot(penguins_data, aes(x="body_mass_g"))
            + geom_histogram(bins=bins, breaks=breaks)
        )

    assert error.value.error_type == "ValueError"
    assert error_message in str(error.value)
