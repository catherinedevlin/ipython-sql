from sql.connection import ConnectionManager
from IPython import get_ipython
import math
import time
from sql.util import parse_sql_results_to_json
from sql.inspect import fetch_sql_with_pagination, is_table_exists
from sql.widgets import utils
from sql.telemetry import telemetry

import os
from ploomber_core.dependencies import check_installed

# Widget base dir
BASE_DIR = os.path.dirname(__file__)


class TableWidget:
    @telemetry.log_call("TableWidget-init")
    def __init__(self, table, schema=None):
        """
        Creates an HTML table element and populates it with SQL table

        Parameters
        ----------
        table : str
            Table name where the data is located
        """

        self.html = ""

        is_table_exists(table, schema)

        # load css
        html_style = utils.load_css(f"{BASE_DIR}/css/tableWidget.css")
        self.add_to_html(html_style)

        self.create_table(table, schema)

        # register listener for jupyter lab
        self.register_comm()

        # load_tests
        self.load_tests()

    def _repr_html_(self):
        return self.html

    def add_to_html(self, html):
        self.html += html

    def create_table(self, table, schema):
        """
        Creates an HTML table with default data
        """
        if schema:
            table_ = f"{schema}.{table}"
        else:
            table_ = table

        rows_per_page = 10
        rows, columns = fetch_sql_with_pagination(table_, 0, rows_per_page)
        rows = parse_sql_results_to_json(rows, columns)

        query = f"SELECT count(*) FROM {table_}"
        n_total = ConnectionManager.current.raw_execute(query).fetchone()[0]
        table_name = table_.strip('"').strip("'")

        n_pages = math.ceil(n_total / rows_per_page)

        unique_id = str(int(time.time()))
        table_container_id = f"tableContainer_{unique_id}"

        # Create table container with unique id
        table_container_html = f"""
            <div id="{table_container_id}" class="table-container"></div>
            """
        self.add_to_html(table_container_html)

        html_scripts = utils.load_js(
            [
                f"{BASE_DIR}/js/tableWidget.js",
                utils.set_template_params(
                    columns=list(columns),
                    rows_per_page=rows_per_page,
                    n_pages=n_pages,
                    n_total=n_total,
                    table_name=table_name,
                    table_container_id=table_container_id,
                    table=table_,
                    initialRows=rows,
                ),
            ]
        )
        self.add_to_html(html_scripts)

    def load_tests(self):
        """
        Define which JS functions we should
        include in this widget's test unit.

        Example:

        Given following the html:

        <script>

            function drawList() {
                return `<ul><li>item1</li><li>item2</li></ul>`
            }

            function drawTable() {
                return `
                        <table>
                            <tr>
                                <td>1</td>
                                <td>2</td>
                            </tr>
                        </table>
                    `
            }

        </script>

        We can include `drawList` in the test unit by extracting it
        from the html and add it to this widget's test property.

        self.tests["drawList"] = utils.extract_function_by_name(
            html, "drawList"
        )


        Testing with pytest:

        import js2py

        def test_draw_list(expected):
            expected = "<ul><li>item1</li><li>item2</li></ul>"

            table_widget = TableWidget("empty_table")

            result = js2py.eval_js(table_widget.tests["drawList"])

            assert result == expected

        """
        self.tests = dict()
        self.tests["createTableRows"] = utils.extract_function_by_name(
            self.html, "createTableRows"
        )

    def register_comm(self):
        """
        Register communication between the frontend and the kernel.
        """

        check_installed(
            ["jupysql_plugin"], "jupysql-plugin", pip_names=["jupysql-plugin"]
        )

        def comm_handler(comm, open_msg):
            """
            Handle received messages from the frontend
            """

            @comm.on_msg
            def _recv(msg):
                data = msg["content"]["data"]
                n_rows = data["nRows"]
                page = data["page"]

                sort_column = None
                sort_order = None
                table_name = data["table"]

                if "sort" in data:
                    sort = data["sort"]
                    sort_column = sort["column"]
                    sort_order = sort["order"]

                offset = page * n_rows

                rows, columns = fetch_sql_with_pagination(
                    table_name,
                    offset,
                    n_rows,
                    sort_column=sort_column,
                    sort_order=sort_order,
                )
                rows_json = parse_sql_results_to_json(rows, columns)

                comm.send({"rows": rows_json})

        ipython = get_ipython()

        if hasattr(ipython, "kernel"):
            ipython.kernel.comm_manager.register_target(
                "comm_target_handle_table_widget", comm_handler
            )
