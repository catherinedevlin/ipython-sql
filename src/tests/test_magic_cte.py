def test_trailing_semicolons_removed_from_cte(ip):
    ip.run_cell(
        """%%sql --save positive_x
SELECT * FROM number_table WHERE x > 0;


"""
    )

    ip.run_cell(
        """%%sql --save positive_y
SELECT * FROM number_table WHERE y > 0;
"""
    )

    cell_execution = ip.run_cell(
        """%%sql --save final --with positive_x --with positive_y
SELECT * FROM positive_x
UNION
SELECT * FROM positive_y;
"""
    )

    cell_final_query = ip.run_cell(
        "%sqlrender final --with positive_x --with positive_y"
    )

    assert cell_execution.success
    assert cell_final_query.result == (
        "WITH `positive_x` AS (\nSELECT * "
        "FROM number_table WHERE x > 0), `positive_y` AS (\nSELECT * "
        "FROM number_table WHERE y > 0)\nSELECT * FROM positive_x\n"
        "UNION\nSELECT * FROM positive_y;"
    )
