function isJupyterNotebook() {
    return window["Jupyter"];
}

function getTable(element) {
    let table;
    if (element) {
        const tableContainer = element.closest(".table-container");
        table = tableContainer.querySelector("table");
    } else {
        const _isJupyterNotebook = isJupyterNotebook();
        if (_isJupyterNotebook) {
            table = document.querySelector(".selected .table-container table");
        } else {
            table = document.querySelector(".jp-Cell.jp-mod-active .table-container table");
        }
    }

    return table;
}

function getSortDetails() {
    let sort = undefined;

    const table = getTable();
    if (table) {
        const column = table.getAttribute("sort-by-column");
        const order = table.getAttribute("sort-by-order");

        if (column && order) {
            sort = {
                "column" : column,
                "order" : order
            }
        }
    }

    return sort;
}

function sortColumnClick(element, column, order, callback) {
    // fetch data with sort logic
    const table = getTable(element);
    table.setAttribute("sort-by-column", column);
    table.setAttribute("sort-by-order", order);
    const rowsPerPage = table.getAttribute("rows-per-page");
    const currrPage = table.getAttribute("curr-page-idx");

    const sort = {
        'column' : column,
        'order' : order
    }

    const fetchParameters = {
        rowsPerPage : parseInt(rowsPerPage),
        page : parseInt(currrPage),
        sort : sort,
        table : table.getAttribute("table-name")
    }

    fetchTableData(fetchParameters, callback)
}

function fetchTableData(fetchParameters, callback) {
    
    sendObject = {
        'nRows' : fetchParameters.rowsPerPage,
        'page': fetchParameters.page,
        'table' : fetchParameters.table
    }

    if (fetchParameters.sort) {
        sendObject.sort = fetchParameters.sort
    }

    const _isJupyterNotebook = isJupyterNotebook();


    if (_isJupyterNotebook) {
        // for Jupyter Notebook
        const comm =
        Jupyter.notebook.kernel.comm_manager.new_comm('comm_target_handle_table_widget', {})
        comm.send(sendObject)
        comm.on_msg(function(msg) {
            const rows = JSON.parse(msg.content.data['rows']);
            if (callback) {
                callback(rows)
            }
        });
    } else{
        // for JupyterLab
        dispatchEventToKernel(sendObject)

        const controller = new AbortController();
    
        document.addEventListener('onTableWidgetRowsReady', (customEvent) => {
            const rows = JSON.parse(customEvent.detail.data.rows)
            controller.abort()
            if (callback) {
                callback(rows)
            }
        }, {signal: controller.signal})
    }


}


function dispatchEventToKernel(data) {
    let customEvent = new CustomEvent('onUpdateTableWidget', {
    bubbles: true,
    cancelable: true,
    composed: false,
    detail : {
        data : data
    }
    });
    document.body.dispatchEvent(customEvent)
}         

function handleRowsNumberOfRowsChange(e) {
    const rowsPerPage = parseInt(e.value);
    let table = getTable();
    table.setAttribute('rows-per-page', rowsPerPage);

    const nTotal = table.getAttribute('n-total');

    const maxPages = Math.ceil(nTotal / rowsPerPage)
    table.setAttribute('max-pages', maxPages);

    const fetchParameters = {
        rowsPerPage : rowsPerPage,
        page : 0,
        sort : getSortDetails(),
        table : table.getAttribute("table-name")
    }

    setTimeout(() => {
        fetchTableData(fetchParameters, (rows) => {
            updateTable(rows);
        })
    }, 100);
}

function updateTable(rows, currPage, tableToUpdate) {
    const table = tableToUpdate || getTable();
    const trs = table.querySelectorAll("tbody tr");
    const tbody = table.querySelector("tbody");
    tbody.innerHTML = "";

    const _html = createTableRows(rows)

    tbody.innerHTML = _html

    setTimeout(() => {
        updatePaginationBar(table, currPage || 0)
    }, 100)
}

function createTableRows(rows) {
    const _html = rows.map(function(row) {
        const tds =
        Object.keys(row).map(function(key) {

            return "<td>" + row[key] + "</td>"
        }).join("") ;
        return "<tr>" + tds + "</tr>";
    }).join("");

    return _html
}

function showTablePage(page, rowsPerPage, data) {
    const table = getTable();
    const trs = table.querySelectorAll("tbody tr");
    const tbody = table.querySelector("tbody");
    tbody.innerHTML = "";

    const rows = data;
    const startIndex = page * rowsPerPage;
    const endIndex = startIndex + rowsPerPage;
    const _html = rows.map(row => {
        const tds =
        Object.keys(row).map(key => `<td>${row[key]}</td>`).join("");
        return `<tr>${tds}</tr>`;
    }).join("");

    tbody.innerHTML = _html;

    table.setAttribute("curr-page-idx", page);
    updatePaginationBar(table, page);
}

function nextPageClick(element) {
    const table = getTable(element);
    const currPageIndex = parseInt(table.getAttribute("curr-page-idx"));
    const rowsPerPage = parseInt(table.getAttribute("rows-per-page"));
    const maxPages = parseInt(table.getAttribute("max-pages"));

    const nextPage = currPageIndex + 1;
    if (nextPage < maxPages) {
        const fetchParameters = {
            rowsPerPage : rowsPerPage,
            page : nextPage,
            sort : getSortDetails(),
            table : table.getAttribute("table-name")
        }

        fetchTableData(fetchParameters, (rows) => {
            showTablePage(nextPage, rowsPerPage, rows)
        });
    }

}

function prevPageClick() {
    const table = getTable();
    const currPageIndex = parseInt(table.getAttribute("curr-page-idx"));
    const rowsPerPage = parseInt(table.getAttribute("rows-per-page"));
    const prevPage = currPageIndex - 1;
    if (prevPage >= 0) {
        const fetchParameters = {
            rowsPerPage : rowsPerPage,
            page : prevPage,
            sort : getSortDetails(),
            table : table.getAttribute("table-name")
        }

        fetchTableData(fetchParameters, (rows) => {
            showTablePage(prevPage, rowsPerPage, rows)
        });
    }
}

function setPageButton(table, label, navigateTo, isSelected) {
    const rowsPerPage = parseInt(table.getAttribute("rows-per-page"));
    const selected = isSelected ? "selected" : "";

    const button = `
    <button class="${selected}"
            onclick="
            fetchTableData({
                rowsPerPage : ${rowsPerPage},
                page : ${navigateTo},
                sort : getSortDetails(),
                table : getTable(this).getAttribute('table-name')
            },
            (rows) => {
                showTablePage(${navigateTo}, ${rowsPerPage}, rows);
                })"
    >
        ${label}
    </button>
    `
    return button;
}

function updatePaginationBar(table, currPage) {
    const maxPages = parseInt(table.getAttribute("max-pages"));
    const maxPagesInRow = 6;
    const rowsPerPage = parseInt(table.getAttribute("rows-per-page"));
    table.setAttribute("curr-page-idx", currPage);

    let buttonsArray = []

    let startEllipsisAdded = false
    let endEllipsisAdded = false

    // add first
    let selected = currPage === 0;
    buttonsArray.push(setPageButton(table, "1", 0, selected));

    for (i = 1; i < maxPages - 1; i++) {
        const navigateTo = i;
        const label = i + 1;
        selected = currPage === i;
        const inStartRange = currPage < maxPagesInRow;
        const inEndRange = maxPages - 1 - currPage < maxPagesInRow;

        if (inStartRange) {
            if (i < maxPagesInRow) {
                buttonsArray
                .push(setPageButton(table, label, navigateTo, selected));
            } else {
            if (!startEllipsisAdded) {
                buttonsArray.push("...");
                startEllipsisAdded = true;
            }
            }
        } else if (inEndRange) {
            if (maxPages - 1 - i < maxPagesInRow) {
                buttonsArray
                .push(setPageButton(table, label, navigateTo, selected));
            } else {
            if (!endEllipsisAdded) {
                buttonsArray.push("...");
                endEllipsisAdded = true;
            }
            }
        }

        if (!inStartRange && !inEndRange) {
            if (currPage === i-2) {
                buttonsArray.push("...");
            }
            if (
                currPage === i - 1 ||
                currPage === i ||
                currPage === i + 1
            ) {
                buttonsArray
                .push(setPageButton(table, label, navigateTo, selected))
            }

            if (currPage === i+2) {
                buttonsArray.push("...");
            }

        }
    }

    selected = currPage === maxPages - 1 ? "selected" : "";

    buttonsArray.
    push(setPageButton(table, maxPages, maxPages - 1, selected))

    const buttonsHtml = buttonsArray.join("");
    table.parentNode
    .querySelector(".pages-buttons").innerHTML = buttonsHtml;
}

function removeSelectionFromAllSortButtons() {
    document.querySelectorAll(".sort-button")
    .forEach(el => el.classList.remove("selected"))
}

function initTable() {
    // template variables we should pass
    const initialRows = {{initialRows}};
    const columns = {{columns}};
    const rowsPerPage={{rows_per_page}};
    const nPages={{n_pages}};
    const nTotal={{n_total}};
    const tableName="{{table_name}}";
    const tableContainerId = "{{table_container_id}}";
    const options = [10, 25, 50, 100];
    options_html =
    options.map(option => `<option value=${option}>${option}</option>`);


    let ths_ = columns.map(col => `<th>${col}</th>`).join("");

    let table = `
    <div>
        <span style="margin-right: 5px">Show</span>
        <select
        onchange="handleRowsNumberOfRowsChange(this)">
            ${options_html}
        </select>
        <span style="margin-left: 5px">entries</span>
    </div>

    <table
        class="jupysql-table-widget"
        style='width:100%'
        curr-page-idx=0
        rows-per-page=${rowsPerPage}
        max-pages = ${nPages}
        n-total=${nTotal}
        table-name=${tableName}
    >
        <thead>
            <tr>
                ${ths_}
            </tr>
        </thead>

        <tbody>
        </tbody>
    </table>


    <div style="padding-bottom: 20px;">
        <button onclick="prevPageClick(this)">Previous</button>
        <div
            id = "pagesButtons"
            class = "pages-buttons"
            style = "display: inline-flex">
        </div>
        <button onclick="nextPageClick(this)">Next</button>
    </div>
    `

    let tableContainer = document.querySelector(`#${tableContainerId}`);

    tableContainer.innerHTML = table

    if (initialRows) {
        initializeTableRows(tableContainer, rowsPerPage, initialRows)

    } else {
        setTimeout(() => {
            const fetchParameters = {
                rowsPerPage : rowsPerPage,
                page : 0,
                sort : getSortDetails(),
                table : tableName
            }

            fetchTableData(fetchParameters, (rows) => {
                initializeTableRows(tableContainer, rowsPerPage, rows)
            })
        }, 100);
    }
    
}

function initializeTableRows(tableContainer, rowsPerPage, rows) {
    updateTable(rows, 0,
        tableContainer.querySelector("table"));
    // update ths_ to make sure order columns
    // are matching the data
    if (rows.length > 0) {
        let row = rows[0];
        let ths_ =
        Object.keys(row).map(col =>
        `<th>
            <div style="display: inline-flex; height: 40px">
                <span style="line-height: 40px">${col}</span>
                <span style="width: 40px;">
                    <button
                        class = "sort-button"
                        onclick='sortColumnClick(this,
                        "${col}", "ASC",
                        (rows) => {
                            const table = getTable(this);
                            const currPage =
                            parseInt(table.getAttribute("curr-page-idx"));
                            updateTable(rows, currPage);
                            removeSelectionFromAllSortButtons()
                            this.className += " selected"
                            }
                        )'
                        title="Sort"
                        >▴
                    </button>
                    <button
                        class = "sort-button"
                        onclick='sortColumnClick(this,
                        "${col}", "DESC",
                        (rows) => {
                            const table = getTable(this);
                            const currPage = parseInt(
                                table.getAttribute("curr-page-idx"));
                            updateTable(rows, currPage);
                            removeSelectionFromAllSortButtons()
                            this.className += " selected"
                            }
                        )'
                        title="Sort"
                        >▾
                    </button>
                </span>
            </div>

            </th>`).join("");
        let thead = tableContainer.querySelector("thead")
        thead.innerHTML = ths_
    }
}

initTable()