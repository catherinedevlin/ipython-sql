from sql import display


def test_html_escaping():
    message = display.Message("<>")

    assert "<>" in str(message)
    assert "&lt;&gt;" in message._repr_html_()
