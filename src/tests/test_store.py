import pytest
from sql.connection import Connection
from IPython.core.error import UsageError
from sql.store import SQLStore


@pytest.fixture(autouse=True)
def setup_no_current_connect(monkeypatch):
    monkeypatch.setattr(Connection, "current", None)


def test_sqlstore_setitem():
    store = SQLStore()
    store["a"] = "SELECT * FROM a"
    assert store["a"] == "SELECT * FROM a"


def test_sqlstore_getitem():
    store = SQLStore()

    # Test case 1: Test for a valid key
    store["first"] = "SELECT * FROM a"
    assert store.__getitem__("first") == "SELECT * FROM a"

    # Test case 2: Test for an invalid key with no matches
    with pytest.raises(UsageError) as err_info:
        store.__getitem__("second")
    assert (
        str(err_info.value)
        == '"second" is not a valid snippet identifier. Valid identifiers are "first".'
    )

    # Test case 3: Test for invalid key with close match
    with pytest.raises(UsageError) as err_info:
        store.__getitem__("firs")
    assert (
        str(err_info.value)
        == '"firs" is not a valid snippet identifier. Did you mean "first"?'
    )

    # Test case 4: Test for multiple keys with close match
    store["first2"] = "SELECT * FROM b"
    with pytest.raises(UsageError) as err_info:
        store.__getitem__("firs")
    assert (
        str(err_info.value)
        == '"firs" is not a valid snippet identifier. Did you mean "first"?'
    )

    # Test case 5: Test for multiple keys with no close match
    with pytest.raises(UsageError) as err_info:
        store.__getitem__("second")
    assert (
        str(err_info.value)
        == '"second" is not a valid snippet identifier. '
        + 'Valid identifiers are "first", "first2".'
    )

    # Test case 6: Test for empty dictionary:
    store2 = SQLStore()
    with pytest.raises(UsageError) as err_info:
        store2.__getitem__("second")
    assert str(err_info.value) == "No saved SQL"

    # Test case 7: Test for special character in key:
    store["$%#"] = "SELECT * FROM a"
    assert store.__getitem__("$%#") == "SELECT * FROM a"


def test_key():
    store = SQLStore()

    with pytest.raises(ValueError):
        store.store("first", "SELECT * FROM first WHERE x > 20", with_=["first"])


@pytest.mark.parametrize(
    "with_",
    [
        ["third"],
        ["first", "third"],
        ["first", "third", "first"],
        ["third", "first"],
    ],
    ids=[
        "simple",
        "redundant",
        "duplicated",
        "redundant-end",
    ],
)
def test_serial(with_):
    store = SQLStore()
    store.store("first", "SELECT * FROM a WHERE x > 10")
    store.store("second", "SELECT * FROM first WHERE x > 20", with_=["first"])

    store.store("third", "SELECT * FROM second WHERE x > 30", with_=["second", "first"])

    result = store.render("SELECT * FROM third", with_=with_)
    assert (
        str(result) == "WITH first AS (SELECT * FROM a WHERE x > 10), "
        "second AS (SELECT * FROM first WHERE x > 20), "
        "third AS (SELECT * FROM second WHERE x > 30) SELECT * FROM third"
    )


def test_branch_root():
    store = SQLStore()

    store.store("first_a", "SELECT * FROM a WHERE x > 10")
    store.store("second_a", "SELECT * FROM first_a WHERE x > 20", with_=["first_a"])
    store.store("third_a", "SELECT * FROM second_a WHERE x > 30", with_=["second_a"])

    store.store("first_b", "SELECT * FROM b WHERE y > 10")

    result = store.render("SELECT * FROM third", with_=["third_a", "first_b"])
    assert (
        str(result) == "WITH first_a AS (SELECT * FROM a WHERE x > 10), "
        "second_a AS (SELECT * FROM first_a WHERE x > 20), "
        "third_a AS (SELECT * FROM second_a WHERE x > 30), "
        "first_b AS (SELECT * FROM b WHERE y > 10) SELECT * FROM third"
    )


def test_branch_root_reverse_final_with():
    store = SQLStore()

    store.store("first_a", "SELECT * FROM a WHERE x > 10")
    store.store("second_a", "SELECT * FROM first_a WHERE x > 20", with_=["first_a"])
    store.store("third_a", "SELECT * FROM second_a WHERE x > 30", with_=["second_a"])

    store.store("first_b", "SELECT * FROM b WHERE y > 10")

    result = store.render("SELECT * FROM third", with_=["first_b", "third_a"])
    assert (
        str(result) == "WITH first_a AS (SELECT * FROM a WHERE x > 10), "
        "second_a AS (SELECT * FROM first_a WHERE x > 20), "
        "first_b AS (SELECT * FROM b WHERE y > 10), "
        "third_a AS (SELECT * FROM second_a WHERE x > 30) SELECT * FROM third"
    )


def test_branch():
    store = SQLStore()

    store.store("first_a", "SELECT * FROM a WHERE x > 10")
    store.store("second_a", "SELECT * FROM first_a WHERE x > 20", with_=["first_a"])
    store.store("third_a", "SELECT * FROM second_a WHERE x > 30", with_=["second_a"])

    store.store("first_b", "SELECT * FROM second_a WHERE y > 10", with_=["second_a"])

    result = store.render("SELECT * FROM third", with_=["first_b", "third_a"])
    assert (
        str(result) == "WITH first_a AS (SELECT * FROM a WHERE x > 10), "
        "second_a AS (SELECT * FROM first_a WHERE x > 20), "
        "first_b AS (SELECT * FROM second_a WHERE y > 10), "
        "third_a AS (SELECT * FROM second_a WHERE x > 30) SELECT * FROM third"
    )
