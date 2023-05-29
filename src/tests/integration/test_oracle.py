from matplotlib import pyplot as plt
import pytest


def test_query_count(ip_with_oracle, test_table_name_dict):
    # Oracle DB doesn't have LIMIT
    out = ip_with_oracle.run_line_magic(
        "sql",
        f"""
        SELECT *
        FROM {test_table_name_dict['taxi']} FETCH FIRST 3 ROWS ONLY
        """,
    )

    assert len(out) == 3


@pytest.mark.xfail(reason="Some issue with checking isidentifier part in persist")
def test_create_table_with_indexed_df(ip_with_oracle, test_table_name_dict):
    # Prepare DF
    ip_with_oracle.run_cell(
        f"""results = %sql SELECT * FROM {test_table_name_dict['taxi']} \
        FETCH FIRST 3 ROWS ONLY"""
    )
    ip_with_oracle.run_cell(
        f"{test_table_name_dict['new_table_from_df']} = results.DataFrame()"
    )
    # Create table from DF
    persist_out = ip_with_oracle.run_cell(
        f"%sql --persist {test_table_name_dict['new_table_from_df']}"
    )
    query_out = ip_with_oracle.run_cell(
        f"%sql SELECT * FROM {test_table_name_dict['new_table_from_df']}"
    )
    assert persist_out.error_in_exec is None and query_out.error_in_exec is None
    assert len(query_out.result) == 15


@pytest.mark.xfail(
    reason="Known table parameter issue with oracledb, \
                   addressing in #506"
)
@pytest.mark.parametrize(
    "cell",
    [
        (
            "%sqlplot histogram --with plot_something_subset \
            --table plot_something_subset --column x"
        ),
        (
            "%sqlplot hist --with plot_something_subset \
            --table plot_something_subset --column x"
        ),
        (
            "%sqlplot histogram --with plot_something_subset \
            --table plot_something_subset --column x --bins 10"
        ),
    ],
    ids=[
        "histogram",
        "hist",
        "histogram-bins",
    ],
)
def test_sqlplot_histogram(ip_with_oracle, cell, request, test_table_name_dict):
    # clean current Axes
    plt.cla()

    ip_with_oracle.run_cell(
        f"%sql --save plot_something_subset\
         --no-execute SELECT * from {test_table_name_dict['plot_something']} \
         FETCH FIRST 3 ROWS ONLY"
    )
    out = ip_with_oracle.run_cell(cell)

    assert type(out.result).__name__ in {"Axes", "AxesSubplot"}


@pytest.mark.xfail(
    reason="Known table parameter issue with oracledb, \
                   addressing in #506"
)
@pytest.mark.parametrize(
    "cell",
    [
        "%sqlplot boxplot --with plot_something_subset \
        --table plot_something_subset --column x",
        "%sqlplot box --with plot_something_subset \
        --table plot_something_subset --column x",
        "%sqlplot boxplot --with plot_something_subset \
        --table plot_something_subset --column x --orient h",
        "%sqlplot boxplot --with plot_something_subset \
        --table plot_something_subset --column x",
    ],
    ids=[
        "boxplot",
        "box",
        "boxplot-with-horizontal",
        "boxplot-with",
    ],
)
def test_sqlplot_boxplot(ip_with_oracle, cell, request, test_table_name_dict):
    # clean current Axes
    plt.cla()
    ip_with_oracle.run_cell(
        f"%sql --save plot_something_subset --no-execute\
          SELECT * from {test_table_name_dict['plot_something']} \
            FETCH FIRST 3 ROWS ONLY"
    )

    out = ip_with_oracle.run_cell(cell)

    assert type(out.result).__name__ in {"Axes", "AxesSubplot"}
