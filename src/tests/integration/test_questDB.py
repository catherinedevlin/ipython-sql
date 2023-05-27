import pytest
import time
from dockerctx import new_container
from contextlib import contextmanager
import pandas as pd
import urllib.request
import requests
from sql.ggplot import ggplot, aes, geom_histogram, facet_wrap, geom_boxplot
from matplotlib.testing.decorators import image_comparison, _cleanup_cm
from sql.connection import CustomConnection, CustomSession
from IPython.core.error import UsageError

"""
This test class includes all QuestDB-related tests and specifically focuses
on testing the custom engine initialization.

TODO: We should generelize these tests to check different engines/connections.
"""

QUESTDB_CONNECTION_STRING = (
    "dbname='qdb' user='admin' host='127.0.0.1' port='8812' password='quest'"
)


@pytest.fixture
def penguins_data(tmpdir):
    """
    Downloads penguins dataset
    """
    file_path_str = str(tmpdir.join("penguins.csv"))

    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",  # noqa breaks the check-for-broken-links
        file_path_str,
    )

    yield file_path_str


@pytest.fixture
def diamonds_data(tmpdir):
    """
    Downloads diamonds dataset
    """
    file_path_str = "diamonds.csv"

    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/tidyverse/ggplot2/main/data-raw/diamonds.csv",  # noqa breaks the check-for-broken-links
        file_path_str,
    )

    yield file_path_str


def import_data(file_name, table_name):
    """
    Loads csv file to questdb container
    """
    url = "http://127.0.0.1:9000"
    query_url = f"{url}/imp"

    df = pd.read_csv(file_name, sep=",")
    df.drop_duplicates(subset=None, inplace=True)
    df.to_csv(file_name, index=False)

    with open(file_name, "rb") as csv:
        file_data = csv.read()
        files = {"data": (table_name, file_data)}
        requests.post(query_url, files=files)


def custom_database_ready(
    custom_connection,
    timeout=20,
    poll_freq=0.5,
):
    """Wait until the container is ready to receive connections.


    :type host: str
    :type port: int
    :type timeout: float
    :type poll_freq: float
    """

    errors = []

    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            custom_connection()
            return True
        except Exception as e:
            errors.append(str(e))

        time.sleep(poll_freq)

    # print all the errors so we know what's going on since failing to connect might be
    # to some misconfiguration error
    errors_ = "\n".join(errors)
    print(f"ERRORS: {errors_}")

    return False


@contextmanager
def questdb_container(is_bypass_init=False):
    if is_bypass_init:
        yield None
        return

    def test_questdb_connection():
        import psycopg as pg

        engine = pg.connect(QUESTDB_CONNECTION_STRING)
        engine.close()

    with new_container(
        image_name="questdb/questdb",
        ports={"8812": "8812", "9000": "9000", "9009": "9009"},
        ready_test=lambda: custom_database_ready(test_questdb_connection),
        healthcheck={
            "interval": 10000000000,
            "timeout": 5000000000,
            "retries": 5,
        },
    ) as container:
        yield container


@pytest.fixture
def ip_questdb(diamonds_data, penguins_data, ip_empty):
    """
    Initializes questdb database container and loads it with data
    """
    with questdb_container():
        ip_empty.run_cell(
            f"""
        import psycopg2 as pg
        engine = pg.connect(
            "{QUESTDB_CONNECTION_STRING}"
        )
        %sql engine
        """
        )

        # Load pre-defined datasets
        import_data(penguins_data, "penguins.csv")
        import_data(diamonds_data, "diamonds.csv")
        yield ip_empty


@pytest.fixture
def penguins_no_nulls_questdb(ip_questdb):
    ip_questdb.run_cell(
        """
%%sql --save no_nulls --no-execute
SELECT *
FROM penguins.csv
WHERE body_mass_g IS NOT NULL and
sex IS NOT NULL
    """
    ).result


# ggplot and %sqlplot


@_cleanup_cm()
@image_comparison(
    baseline_images=["custom_engine_histogram"],
    extensions=["png"],
    remove_text=False,
)
def test_ggplot_histogram(ip_questdb, penguins_no_nulls_questdb):
    (
        ggplot(
            table="no_nulls",
            with_="no_nulls",
            mapping=aes(x=["bill_length_mm", "bill_depth_mm"]),
        )
        + geom_histogram(bins=50)
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
def test_example_histogram_stacked_default(ip_questdb, diamonds_data, x):
    (ggplot(diamonds_data, aes(x=x)) + geom_histogram(bins=10, fill="cut"))


@_cleanup_cm()
@image_comparison(
    baseline_images=["custom_engine_histogram"],
    extensions=["png"],
    remove_text=False,
)
def test_sqlplot_histogram(ip_questdb, penguins_no_nulls_questdb):
    ip_questdb.run_cell(
        """%sqlplot histogram --column bill_length_mm bill_depth_mm --table no_nulls --with no_nulls"""  # noqa
    )


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_stacked_custom_cmap"],
    extensions=["png"],
    remove_text=True,
)
def test_example_histogram_stacked_custom_cmap(ip_questdb, diamonds_data):
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
def test_example_histogram_stacked_custom_color(ip_questdb, diamonds_data):
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
def test_example_histogram_stacked_custom_color_and_fill(ip_questdb, diamonds_data):
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
def test_ggplot_geom_histogram_fill_with_multi_color_warning(ip_questdb, diamonds_data):
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
def test_example_histogram_stacked_with_large_bins(ip_questdb, diamonds_data):
    (ggplot(diamonds_data, aes(x="price")) + geom_histogram(bins=400, fill="cut"))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_categorical"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_histogram(ip_questdb, diamonds_data):
    (ggplot(diamonds_data, aes(x=["cut"])) + geom_histogram())


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_categorical_combined"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_histogram_combined(ip_questdb, diamonds_data):
    (ggplot(diamonds_data, aes(x=["color", "carat"])) + geom_histogram(bins=10))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_numeric_categorical_combined"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_and_numeric_histogram_combined(ip_questdb, diamonds_data):
    (ggplot(diamonds_data, aes(x=["color", "carat"])) + geom_histogram(bins=20))


@_cleanup_cm()
@image_comparison(
    baseline_images=["histogram_numeric_categorical_combined_custom_fill"],
    extensions=["png"],
    remove_text=True,
)
def test_categorical_and_numeric_histogram_combined_custom_fill(
    ip_questdb, diamonds_data
):
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
def test_categorical_and_numeric_histogram_combined_custom_multi_fill(
    ip_questdb, diamonds_data
):
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
def test_categorical_and_numeric_histogram_combined_custom_multi_color(
    ip_questdb, diamonds_data
):
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
def test_facet_wrap_default(ip_questdb, penguins_no_nulls_questdb):
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
def test_facet_wrap_default_no_legend(ip_questdb, penguins_no_nulls_questdb):
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
def test_facet_wrap_custom_fill(ip_questdb, penguins_no_nulls_questdb):
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
def test_facet_wrap_custom_fill_and_color(ip_questdb, penguins_no_nulls_questdb):
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
def test_facet_wrap_stacked_histogram(ip_questdb, diamonds_data):
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
def test_facet_wrap_stacked_histogram_cmap(ip_questdb, diamonds_data):
    (
        ggplot(diamonds_data, aes(x=["price"]))
        + geom_histogram(bins=10, fill="color", cmap="plasma")
        + facet_wrap("cut")
    )


@_cleanup_cm()
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
    diamonds_data, ip_questdb, x, expected_error, expected_error_message
):
    with pytest.raises(expected_error) as error:
        (ggplot(diamonds_data, aes(x=x)) + geom_histogram(bins=500, fill="cut"))

    assert expected_error_message in str(error.value)


def test_histogram_no_bins_error(ip_questdb, diamonds_data):
    with pytest.raises(ValueError) as error:
        (ggplot(diamonds_data, aes(x=["price"])) + geom_histogram())

    assert "Please specify a valid number of bins." in str(error.value)


@pytest.mark.parametrize(
    "query, expected_results",
    [
        (
            "select * from penguins.csv limit 2",
            [
                ("Adelie", "Torgersen", 39.1, 18.7, 181, 3750, "MALE"),
                ("Adelie", "Torgersen", 39.5, 17.4, 186, 3800, "FEMALE"),
            ],
        ),
        (
            "select * from penguins.csv where sex = 'MALE' limit 2",
            [
                ("Adelie", "Torgersen", 39.1, 18.7, 181, 3750, "MALE"),
                ("Adelie", "Torgersen", 39.3, 20.6, 190, 3650, "MALE"),
            ],
        ),
        (
            "select species, island from penguins.csv where sex = 'MALE' limit 2",
            [("Adelie", "Torgersen"), ("Adelie", "Torgersen")],
        ),
    ],
)
def test_sql(ip_questdb, query, expected_results):
    resultSet = ip_questdb.run_cell(f"%sql {query}").result
    for i, row in enumerate(resultSet):
        assert row == expected_results[i]


# NOT SUPPORTED ERRORS


NOT_SUPPORTED_SUFFIX = "is not supported for a custom engine"


@pytest.mark.parametrize(
    "query",
    [
        ("%sqlcmd profile --table penguins.csv"),
        ("%sqlcmd tables"),
        ("%sqlcmd tables --schema some_schema"),
        ("%sqlcmd columns --table penguins.csv"),
        ("%sqlcmd test"),
        ("%sqlcmd test --table penguins.csv"),
    ],
)
def test_sqlcmd_not_supported_error(ip_questdb, query, capsys):
    expected_error_message = f"%sqlcmd {NOT_SUPPORTED_SUFFIX}"
    out = ip_questdb.run_cell(query)
    error_message = str(out.error_in_exec)
    assert isinstance(out.error_in_exec, UsageError)
    assert str(expected_error_message).lower() in error_message.lower()


@_cleanup_cm()
@pytest.mark.parametrize(
    "func, expected_error_message",
    [
        (
            lambda: (ggplot(penguins_data, aes(x="body_mass_g")) + geom_boxplot()),
            f"boxplot {NOT_SUPPORTED_SUFFIX}",
        ),
        (
            lambda: (
                ggplot(table="no_nulls", with_="no_nulls", mapping=aes(x="body_mass_g"))
                + geom_boxplot()
            ),
            f"boxplot {NOT_SUPPORTED_SUFFIX}",
        ),
    ],
)
def test_ggplot_boxplot_not_supported_error(
    ip_questdb, penguins_no_nulls_questdb, penguins_data, func, expected_error_message
):
    with pytest.raises(UsageError) as err:
        func()

    assert err.value.error_type == "RuntimeError"
    assert expected_error_message in str(err)


@_cleanup_cm()
@pytest.mark.parametrize(
    "query, expected_error_message",
    [
        (
            "%sqlplot boxplot --column body_mass_g --table penguins.csv",
            f"boxplot {NOT_SUPPORTED_SUFFIX}",
        ),
        (
            "%sqlplot boxplot --column body_mass_g --table no_nulls --with no_nulls",
            f"boxplot {NOT_SUPPORTED_SUFFIX}",
        ),
    ],
)
def test_sqlplot_not_supported_error(
    ip_questdb, penguins_data, penguins_no_nulls_questdb, query, expected_error_message
):
    ip_questdb.run_cell(query)
    out = ip_questdb.run_cell(query)
    error_message = str(out.error_in_exec)
    assert isinstance(out.error_in_exec, UsageError)
    assert str(expected_error_message).lower() in error_message.lower()


# Utils
@pytest.mark.parametrize(
    "alias",
    [None, "test_alias"],
)
def test_custom_connection(ip_questdb, alias):
    import psycopg as pg

    engine = pg.connect(QUESTDB_CONNECTION_STRING)

    expected_connection_name = "custom_driver"

    connection = CustomConnection(engine, alias)

    assert isinstance(connection, CustomConnection)
    assert connection.name is expected_connection_name
    assert connection.dialect is expected_connection_name
    assert connection.alias is alias
    assert len(connection.connections) > 0
    assert isinstance(connection.session, CustomSession)

    if alias:
        stored_connection = connection.connections[alias]
    else:
        stored_connection = connection.connections[expected_connection_name]

    assert isinstance(stored_connection, CustomConnection)


def test_custom_connection_error(ip_questdb):
    with pytest.raises(ValueError) as err:
        CustomConnection()

    assert "Engine cannot be None" in str(err)
