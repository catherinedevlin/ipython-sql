from conftest import runsql


def test_compose(ip):
    ip.run_cell_magic(
        "sql",
        "--save author_sub",
        "SELECT last_name FROM author WHERE year_of_death > 1900",
    )

    ip.run_cell_magic(
        "sql",
        "--with author_sub --save final",
        "SELECT last_name FROM author_sub;",
    )

    result = ip.run_cell("%sqlrender final").result

    expected = """\
WITH author_sub AS (
    
SELECT last_name FROM author WHERE year_of_death > 1900
)

SELECT last_name FROM author_sub;\
"""

    assert result == expected