from sql import display
from sql.display import Message, Link, message_html


def test_html_escaping():
    message = display.Message("<>")

    assert "<>" in str(message)
    assert "&lt;&gt;" in message._repr_html_()


def test_message_html_with_list_input(capsys):
    message_html(["go to our", Link("home", "https://ploomber.io"), "page"])
    out, _ = capsys.readouterr()
    assert "go to our home (https://ploomber.io) page" in out


def test_message_with_link_object():
    assert "go to our home (https://ploomber.io) page" == str(
        Message(["go to our", Link("home", "https://ploomber.io"), "page"])
    )
