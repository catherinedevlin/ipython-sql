import pytest

from sql._testing import TestingShell


@pytest.fixture(scope="module")
def ip():
    return TestingShell()


def test_testingshell_raises_code_errors(ip):
    with pytest.raises(ZeroDivisionError):
        ip.run_cell("1 / 0")


def test_testingshell_raises_syntax_errors(ip):
    with pytest.raises(SyntaxError):
        ip.run_cell("SELECT * FROM penguins.csv where species = :species")
