import pytest
from IPython.core.error import UsageError


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


def test_infer_dependencies(ip, capsys):
    ip.run_cell_magic(
        "sql",
        "--save author_sub",
        "SELECT last_name FROM author WHERE year_of_death > 1900",
    )

    ip.run_cell_magic(
        "sql",
        "--save final",
        "SELECT last_name FROM author_sub;",
    )
    out, _ = capsys.readouterr()
    result = ip.run_cell("%sqlrender final").result
    expected = (
        "WITH `author_sub` AS (\nSELECT last_name FROM author "
        "WHERE year_of_death > 1900)\nSELECT last_name FROM author_sub;"
    )

    assert result == expected
    assert "Generating CTE with stored snippets : author_sub" in out


def test_deprecation_warning(ip):
    ip.run_cell_magic(
        "sql",
        "--save author_sub",
        "SELECT last_name FROM author WHERE year_of_death > 1900",
    )

    with pytest.warns(FutureWarning) as record:
        ip.run_cell_magic(
            "sql",
            "--with author_sub --save final",
            "SELECT last_name FROM author_sub;",
        )
        assert len(record) == 1
        assert (
            "CTE dependencies are now automatically inferred,"
            " you can omit the --with arguments. Using --with will "
            "raise an exception in the next major release so please "
            "remove it." in record[0].message.args[0]
        )


TABLE_NAME_TYPO_ERR_MSG = """
There is no table with name 'author_subb'.
Did you mean : 'author_sub'
If you need help solving this issue, send us a message: https://ploomber.io/community
"""


def test_table_name_typo(ip):
    ip.run_cell_magic(
        "sql",
        "--save author_sub",
        "SELECT last_name FROM author WHERE year_of_death > 1900",
    )

    with pytest.raises(UsageError) as excinfo:
        ip.run_cell_magic(
            "sql",
            "--save final",
            "SELECT last_name FROM author_subb;",
        )

    assert excinfo.value.error_type == "TableNotFoundError"
    assert str(excinfo.value) == TABLE_NAME_TYPO_ERR_MSG.strip()


def test_snippets_delete(ip, capsys):
    ip.run_cell(
        """
    %%sql sqlite://
    CREATE TABLE orders (order_id int, customer_id int, order_value float);
    INSERT INTO orders VALUES (123, 15, 150.67);
    INSERT INTO orders VALUES (124, 25, 200.66);
    INSERT INTO orders VALUES (211, 15, 251.43);
    INSERT INTO orders VALUES (312, 5, 333.41);
    CREATE TABLE another_orders (order_id int, customer_id int, order_value float);
    INSERT INTO another_orders VALUES (511,15, 150.67);
    INSERT INTO another_orders VALUES (512, 30, 200.66);
    CREATE TABLE customers (customer_id int, name varchar(25));
    INSERT INTO customers VALUES (15, 'John');
    INSERT INTO customers VALUES (25, 'Sheryl');
    INSERT INTO customers VALUES (5, 'Mike');
    INSERT INTO customers VALUES (30, 'Daisy');
    """
    )
    ip.run_cell_magic(
        "sql",
        "--save orders_less",
        "SELECT * FROM orders WHERE order_value < 250.0",
    )

    ip.run_cell_magic(
        "sql",
        "--save another_orders",
        "SELECT * FROM orders WHERE order_value > 250.0",
    )

    ip.run_cell_magic(
        "sql",
        "--save final",
        """
        SELECT o.order_id, customers.name, o.order_value
        FROM another_orders o
        INNER JOIN customers ON o.customer_id=customers.customer_id;
        """,
    )

    out, _ = capsys.readouterr()
    assert "Generating CTE with stored snippets : another_orders" in out
    result_del = ip.run_cell(
        "%sqlcmd snippets --delete-force-all another_orders"
    ).result
    assert "final, another_orders has been deleted.\n" in result_del
    stored_snippets = result_del[
        result_del.find("Stored snippets") + len("Stored snippets: ") :
    ]
    assert "orders_less" in stored_snippets
    ip.run_cell_magic(
        "sql",
        "--save final",
        """
        SELECT o.order_id, customers.name, o.order_value
        FROM another_orders o
        INNER JOIN customers ON o.customer_id=customers.customer_id;
        """,
    )
    result = ip.run_cell("%sqlrender final").result
    expected = (
        "SELECT o.order_id, customers.name, "
        "o.order_value\n        "
        "FROM another_orders o\n        INNER JOIN customers "
        "ON o.customer_id=customers.customer_id"
    )
    assert expected in result


SYNTAX_ERROR_MESSAGE = """
Syntax Error in WITH `author_sub` AS (
SELECT last_name FRM author WHERE year_of_death > 1900)
SELECT last_name FROM author_sub: Expecting ( at Line 1, Column 16
"""


def test_query_syntax_error(ip):
    ip.run_cell_magic(
        "sql",
        "--save author_sub --no-execute",
        "SELECT last_name FRM author WHERE year_of_death > 1900",
    )

    with pytest.raises(UsageError) as excinfo:
        ip.run_cell_magic(
            "sql",
            "--save final",
            "SELECT last_name FROM author_sub;",
        )

    assert excinfo.value.error_type == "UsageError"
    assert SYNTAX_ERROR_MESSAGE.strip() in str(excinfo.value)
