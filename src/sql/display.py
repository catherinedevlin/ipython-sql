"""
A module to display confirmation messages and contextual information to the user
"""
import html

from prettytable import PrettyTable
from IPython.display import display, HTML
from IPython import get_ipython


class Table:
    """Provides a txt and html representation of tabular data"""

    TITLE = ""

    def __init__(self, headers, rows) -> None:
        self._headers = headers
        self._rows = rows
        self._table = PrettyTable()
        self._table.field_names = headers

        for row in rows:
            self._table.add_row(row)

        self._table_html = self._table.get_html_string()
        self._table_txt = self._table.get_string()

    def __repr__(self) -> str:
        return self.TITLE + "\n" + self._table_txt

    def _repr_html_(self) -> str:
        return self.TITLE + "\n" + self._table_html


class ConnectionsTable(Table):
    TITLE = "Active connections:"

    def __init__(self, headers, rows_maps) -> None:
        def get_values(d):
            d = {k: v for k, v in d.items() if k not in {"connection", "key"}}
            return list(d.values())

        rows = [get_values(r) for r in rows_maps]

        self._mapping = {}

        for row in rows_maps:
            self._mapping[row["key"]] = row["connection"]

        super().__init__(headers=headers, rows=rows)

    def __getitem__(self, key: str):
        """
        This method is provided for backwards compatibility. Before
        creating ConnectionsTable, `%sql --connections` returned a dictionary,
        hence users could retrieve connections using __getitem__. Note that this
        was undocumented so we might decide to remove it in the future.
        """
        return self._mapping[key]

    def __iter__(self):
        """Also provided for backwards compatibility"""
        for key in self._mapping:
            yield key

    def __len__(self):
        """Also provided for backwards compatibility"""
        return len(self._mapping)


class Message:
    """Message for the user"""

    def __init__(self, message, style=None) -> None:
        if isinstance(message, str):
            self._message = message
        elif isinstance(message, list):
            self._message = " ".join([str(msg) for msg in message])
        # escape html and replace newlines with <br> tags so newlines are displayed
        self._message_html = html.escape(self._message).replace("\n\n", "<br>")
        self._style = "" or style

    def _repr_html_(self):
        return f'<span style="{self._style}">{self._message_html}</span>'

    def __repr__(self) -> str:
        return self._message


class Link:
    """Formatting of link depending on the running environment"""

    def __init__(self, text, url):
        self.text = text
        self.url = url

    def __str__(self):
        if get_ipython():
            return f'<a href="{self.url}">{self.text}</a>'
        else:
            return f"{self.text} ({self.url})"


def message(message):
    """Display a generic message"""
    display(Message(message))


def message_success(message):
    """Display a success message"""
    display(Message(message, style="color: green"))


def message_warning(message):
    """Display a warning message"""
    display(
        Message(
            message,
            style="background-color:#fff3cd;color:#d39e00",
        )
    )


def message_html(message):
    """Display a message with link"""
    if get_ipython():
        display(HTML(str(Message(message))))
    else:
        display(Message(message))


def table(headers, rows):
    """Display a table"""
    display(Table(headers, rows))
