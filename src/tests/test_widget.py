from sql.widgets import TableWidget
import pytest
from sql.widgets import utils
import js2py


@pytest.mark.parametrize(
    "source, function_to_extract, expected",
    [
        (
            """
        function aaa() {
          return "a"
        }
        """,
            "aaa",
            """function aaa() {
          return "a"
        }""",
        ),
        (
            """
        function aaa() {
          return "a"
        }
        function bbb() {
          return "b"
        }
        function ccc() {
          return "c"
        }
        """,
            "bbb",
            """function bbb() {
          return "b"
        }""",
        ),
        (
            """
        function aaa() {
          return "a"
        }
        function bbb() {
          return "b"
        }
        function c_c() {
          return "c"
        }
        """,
            "c_c",
            """function c_c() {
          return "c"
        }""",
        ),
        (
            """
        function aaa() {
          return "a"
        }
        function bbb() {
          return "b"
        }
        function c_c() {
          return "c"
        }
        """,
            "ddd",
            None,
        ),
        (
            """
        """,
            "aaa",
            None,
        ),
    ],
)
def test_widget_utils_extract_function_by_name(source, function_to_extract, expected):
    result = utils.extract_function_by_name(source, function_to_extract)
    assert result == expected


def test_widget_utils_set_template_params():
    result = utils.set_template_params(a=1, b=2, c=3)

    assert result["a"] == 1
    assert result["b"] == 2
    assert result["c"] == 3


def test_widget_utils_load_css(tmpdir):
    test_file = str(tmpdir.join("test.css"))

    css_ = """
        .rule_one {
            background-color : red;
        }

        .rule_two {
            background-color: blue;
        }
    """
    with open(test_file, "w") as file:
        file.write(css_)

    style = utils.load_css(test_file)

    expected = f"""
    <style>{css_}</style>
    """
    assert style == expected


def test_widget_utils_load_js(tmpdir):
    test_file = str(tmpdir.join("test.js"))

    js_ = """
        function aaa() {
            return "a"
        }

        function bbb() {
            return "b"
        }
    """
    with open(test_file, "w") as file:
        file.write(js_)

    js = utils.load_js(test_file)

    expected = f"""
    <script>{js_}</script>
    """

    assert js == expected


@pytest.mark.parametrize(
    "rows, expected",
    [
        (
            [{"x": 4, "y": -2, "z": 3}, {"x": -5, "y": 0, "z": 4}],
            "<tr><td>4</td><td>-2</td><td>3</td></tr>"
            + "<tr><td>-5</td><td>0</td><td>4</td></tr>",
        ),
        (
            [{"x": 4}, {"x": -5}, {"x": "textual value"}],
            "<tr><td>4</td></tr><tr><td>-5</td></tr><tr><td>textual value</td></tr>",
        ),
        ([{"x": 4}], "<tr><td>4</td></tr>"),
        ([{"x": None}], "<tr><td>undefined</td></tr>"),
        ([{"x": ""}], "<tr><td></td></tr>"),
        ([], ""),
    ],
)
def test_table_widget_create_table_rows(ip, rows, expected):
    """
    Test the functionality of table rows creation from dict
    """
    table_widget = TableWidget("empty_table")

    create_table_rows = js2py.eval_js(table_widget.tests["createTableRows"])

    table_rows = create_table_rows(rows)

    assert table_rows == expected
