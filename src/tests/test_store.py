import pytest

from sql.store import SQLStore


def test_sqlstore_setitem():
    store = SQLStore()
    store['a'] = 'SELECT * FROM a'
    assert store['a'] == 'SELECT * FROM a'


def test_key():
    store = SQLStore()

    with pytest.raises(ValueError):
        store.store('first',
                    'SELECT * FROM first WHERE x > 20',
                    with_=['first'])


@pytest.mark.parametrize('with_', [
    ['third'],
    ['first', 'third'],
    ['first', 'third', 'first'],
    ['third', 'first'],
],
                         ids=[
                             'simple',
                             'redundant',
                             'duplicated',
                             'redundant-end',
                         ])
def test_serial(with_):
    store = SQLStore()
    store.store('first', 'SELECT * FROM a WHERE x > 10')
    store.store('second', 'SELECT * FROM first WHERE x > 20', with_=['first'])

    store.store('third',
                'SELECT * FROM second WHERE x > 30',
                with_=['second', 'first'])

    result = store.render('SELECT * FROM third', with_=with_)
    assert str(result) == """\
WITH first AS (
    SELECT * FROM a WHERE x > 10
), second AS (
    SELECT * FROM first WHERE x > 20
), third AS (
    SELECT * FROM second WHERE x > 30
)
SELECT * FROM third\
"""


def test_branch_root():
    store = SQLStore()

    store.store('first_a', 'SELECT * FROM a WHERE x > 10')
    store.store('second_a',
                'SELECT * FROM first_a WHERE x > 20',
                with_=['first_a'])
    store.store('third_a',
                'SELECT * FROM second_a WHERE x > 30',
                with_=['second_a'])

    store.store('first_b', 'SELECT * FROM b WHERE y > 10')

    result = store.render('SELECT * FROM third', with_=['third_a', 'first_b'])
    assert str(result) == """\
WITH first_a AS (
    SELECT * FROM a WHERE x > 10
), second_a AS (
    SELECT * FROM first_a WHERE x > 20
), third_a AS (
    SELECT * FROM second_a WHERE x > 30
), first_b AS (
    SELECT * FROM b WHERE y > 10
)
SELECT * FROM third\
"""


def test_branch_root_reverse_final_with():
    store = SQLStore()

    store.store('first_a', 'SELECT * FROM a WHERE x > 10')
    store.store('second_a',
                'SELECT * FROM first_a WHERE x > 20',
                with_=['first_a'])
    store.store('third_a',
                'SELECT * FROM second_a WHERE x > 30',
                with_=['second_a'])

    store.store('first_b', 'SELECT * FROM b WHERE y > 10')

    result = store.render('SELECT * FROM third', with_=['first_b', 'third_a'])
    assert str(result) == """\
WITH first_a AS (
    SELECT * FROM a WHERE x > 10
), second_a AS (
    SELECT * FROM first_a WHERE x > 20
), first_b AS (
    SELECT * FROM b WHERE y > 10
), third_a AS (
    SELECT * FROM second_a WHERE x > 30
)
SELECT * FROM third\
"""


def test_branch():
    store = SQLStore()

    store.store('first_a', 'SELECT * FROM a WHERE x > 10')
    store.store('second_a',
                'SELECT * FROM first_a WHERE x > 20',
                with_=['first_a'])
    store.store('third_a',
                'SELECT * FROM second_a WHERE x > 30',
                with_=['second_a'])

    store.store('first_b',
                'SELECT * FROM second_a WHERE y > 10',
                with_=['second_a'])

    result = store.render('SELECT * FROM third', with_=['first_b', 'third_a'])
    assert str(result) == """\
WITH first_a AS (
    SELECT * FROM a WHERE x > 10
), second_a AS (
    SELECT * FROM first_a WHERE x > 20
), first_b AS (
    SELECT * FROM second_a WHERE y > 10
), third_a AS (
    SELECT * FROM second_a WHERE x > 30
)
SELECT * FROM third\
"""