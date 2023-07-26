import pytest
from sql.connection import SQLAlchemyConnection, ConnectionManager
from IPython.core.error import UsageError
from sql.store import SQLStore, SQLQuery
from sqlalchemy import create_engine


@pytest.fixture(autouse=True)
def setup_no_current_connect(monkeypatch):
    monkeypatch.setattr(ConnectionManager, "current", None)


def test_sqlstore_setitem():
    store = SQLStore()
    store["a"] = "SELECT * FROM a"
    assert store["a"] == "SELECT * FROM a"


def test_sqlstore_getitem():
    store = SQLStore()

    # Test case 1: Test for a valid key
    store["first"] = "SELECT * FROM a"
    assert store["first"] == "SELECT * FROM a"

    # Test case 2: Test for an invalid key with no matches
    with pytest.raises(UsageError) as excinfo:
        store["second"]

    assert excinfo.value.error_type == "UsageError"
    assert (
        str(excinfo.value)
        == '"second" is not a valid snippet identifier. Valid identifiers are "first".'
    )

    # Test case 3: Test for invalid key with close match
    with pytest.raises(UsageError) as excinfo:
        store["firs"]

    assert excinfo.value.error_type == "UsageError"
    assert (
        str(excinfo.value)
        == '"firs" is not a valid snippet identifier. Did you mean "first"?'
    )

    # Test case 4: Test for multiple keys with close match
    store["first2"] = "SELECT * FROM b"
    with pytest.raises(UsageError) as excinfo:
        store["firs"]

    assert excinfo.value.error_type == "UsageError"
    assert (
        str(excinfo.value)
        == '"firs" is not a valid snippet identifier. Did you mean "first"?'
    )

    # Test case 5: Test for multiple keys with no close match
    with pytest.raises(UsageError) as excinfo:
        store["second"]

    assert excinfo.value.error_type == "UsageError"
    assert (
        str(excinfo.value)
        == '"second" is not a valid snippet identifier. '
        + 'Valid identifiers are "first", "first2".'
    )

    # Test case 6: Test for empty dictionary:
    store2 = SQLStore()
    with pytest.raises(UsageError) as excinfo:
        store2["second"]

    assert excinfo.value.error_type == "UsageError"
    assert str(excinfo.value) == "No saved SQL"

    # Test case 7: Test for special character in key:
    store["$%#"] = "SELECT * FROM a"
    assert store["$%#"] == "SELECT * FROM a"


def test_hyphen():
    store = SQLStore()

    with pytest.raises(UsageError) as excinfo:
        SQLQuery(store, "SELECT * FROM a", with_=["first-"])

    assert "Using hyphens is not allowed." in str(excinfo.value)


def test_key():
    store = SQLStore()

    with pytest.raises(UsageError) as excinfo:
        store.store("first", "SELECT * FROM first WHERE x > 20", with_=["first"])

    assert "cannot appear in with_ argument" in str(excinfo.value)


@pytest.mark.parametrize(
    "is_dialect_support_backtick",
    [(True), (False)],
)
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
def test_serial(with_, is_dialect_support_backtick, monkeypatch):
    """To test if SQLStore can store multiple with sql clause
    and parse into final combined sql clause

    Parameters
    ----------
    with_ : string
        The key to use in with sql clause
    is_dialect_support_backtick : bool
        If the current connected dialect support `(backtick) syntax
    monkeypatch : Monkeypatch
        A convenient fixture for monkey-patching
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(
        conn,
        "is_use_backtick_template",
        lambda: is_dialect_support_backtick,
    )
    identifier = "`" if is_dialect_support_backtick else ""

    store = SQLStore()
    store.store("first", "SELECT * FROM a WHERE x > 10")
    store.store("second", "SELECT * FROM first WHERE x > 20", with_=["first"])

    store.store("third", "SELECT * FROM second WHERE x > 30", with_=["second", "first"])

    result = store.render("SELECT * FROM third", with_=with_)

    assert (
        str(result)
        == "WITH {0}first{0} AS (SELECT * FROM a WHERE x > 10), \
{0}second{0} AS (SELECT * FROM first WHERE x > 20), \
{0}third{0} AS (SELECT * FROM second WHERE x > 30)SELECT * FROM third".format(
            identifier
        )
    )


@pytest.mark.parametrize(
    "is_dialect_support_backtick",
    [(True), (False)],
)
def test_branch_root(is_dialect_support_backtick, monkeypatch):
    """To test if SQLStore can store multiple with sql clause,
    but with each with clause has it's own sub-query.
    To see if SQLStore can parse into final combined sql clause

    Parameters
    ----------
    with_ : string
        The key to use in with sql clause
    is_dialect_support_backtick : bool
        If the current connected dialect support `(backtick) syntax
    monkeypatch : Monkeypatch
        A convenient fixture for monkey-patching
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))
    monkeypatch.setattr(
        conn,
        "is_use_backtick_template",
        lambda: is_dialect_support_backtick,
    )
    identifier = "`" if is_dialect_support_backtick else ""

    store = SQLStore()
    store.store("first_a", "SELECT * FROM a WHERE x > 10")
    store.store("second_a", "SELECT * FROM first_a WHERE x > 20", with_=["first_a"])
    store.store("third_a", "SELECT * FROM second_a WHERE x > 30", with_=["second_a"])

    store.store("first_b", "SELECT * FROM b WHERE y > 10")

    result = store.render("SELECT * FROM third", with_=["third_a", "first_b"])
    assert (
        str(result)
        == "WITH {0}first_a{0} AS (SELECT * FROM a WHERE x > 10), \
{0}second_a{0} AS (SELECT * FROM first_a WHERE x > 20), \
{0}third_a{0} AS (SELECT * FROM second_a WHERE x > 30), \
{0}first_b{0} AS (SELECT * FROM b WHERE y > 10)SELECT * FROM third".format(
            identifier
        )
    )


@pytest.mark.parametrize(
    "is_dialect_support_backtick",
    [(True), (False)],
)
def test_branch_root_reverse_final_with(is_dialect_support_backtick, monkeypatch):
    """To test if SQLStore can store multiple with sql clause,
    but with different reverse order in with_ parameter.
    To see if SQLStore can parse into final combined sql clause

    Parameters
    ----------
    with_ : string
        The key to use in with sql clause
    is_dialect_support_backtick : bool
        If the current connected dialect support `(backtick) syntax
    monkeypatch : Monkeypatch
        A convenient fixture for monkey-patching
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(
        conn,
        "is_use_backtick_template",
        lambda: is_dialect_support_backtick,
    )
    identifier = "`" if is_dialect_support_backtick else ""

    store = SQLStore()

    store.store("first_a", "SELECT * FROM a WHERE x > 10")
    store.store("second_a", "SELECT * FROM first_a WHERE x > 20", with_=["first_a"])
    store.store("third_a", "SELECT * FROM second_a WHERE x > 30", with_=["second_a"])

    store.store("first_b", "SELECT * FROM b WHERE y > 10")

    result = store.render("SELECT * FROM third", with_=["first_b", "third_a"])
    assert (
        str(result)
        == "WITH {0}first_a{0} AS (SELECT * FROM a WHERE x > 10), \
{0}second_a{0} AS (SELECT * FROM first_a WHERE x > 20), \
{0}first_b{0} AS (SELECT * FROM b WHERE y > 10), \
{0}third_a{0} AS (SELECT * FROM second_a WHERE x > 30)SELECT * FROM third".format(
            identifier
        )
    )


@pytest.mark.parametrize(
    "is_dialect_support_backtick",
    [(True), (False)],
)
def test_branch(is_dialect_support_backtick, monkeypatch):
    """To test if SQLStore can store multiple with sql clause,
    but some sub-queries have same with_ dependency.
    To see if SQLStore can parse into final combined sql clause

    Parameters
    ----------
    with_ : string
        The key to use in with sql clause
    monkeypatch : Monkeypatch
        A convenient fixture for monkey-patching
    """
    conn = SQLAlchemyConnection(engine=create_engine("sqlite://"))

    monkeypatch.setattr(
        conn,
        "is_use_backtick_template",
        lambda: is_dialect_support_backtick,
    )
    identifier = "`" if is_dialect_support_backtick else ""

    store = SQLStore()

    store.store("first_a", "SELECT * FROM a WHERE x > 10")
    store.store("second_a", "SELECT * FROM first_a WHERE x > 20", with_=["first_a"])
    store.store("third_a", "SELECT * FROM second_a WHERE x > 30", with_=["second_a"])

    store.store("first_b", "SELECT * FROM second_a WHERE y > 10", with_=["second_a"])

    result = store.render("SELECT * FROM third", with_=["first_b", "third_a"])
    assert (
        str(result)
        == "WITH {0}first_a{0} AS (SELECT * FROM a WHERE x > 10), \
{0}second_a{0} AS (SELECT * FROM first_a WHERE x > 20), \
{0}first_b{0} AS (SELECT * FROM second_a WHERE y > 10), \
{0}third_a{0} AS (SELECT * FROM second_a WHERE x > 30)SELECT * FROM third".format(
            identifier
        )
    )
