from typing import Iterator
from collections.abc import Mapping

import duckdb
import numpy as np
from matplotlib import cbook
from sql import plot


class DictOfFloats(Mapping):

    def __init__(self, data) -> None:
        self._data = data

    def __eq__(self, other: object) -> bool:
        same_keys = set(self._data) == set(other)

        if not same_keys:
            return False

        for key, value in self._data.items():
            isclose = np.isclose(value, other[key])

            if isinstance(isclose, np.bool_):
                if not isclose:
                    return False
            elif not all(isclose):
                return False

        return True

    def __iter__(self) -> Iterator[str]:
        for key in self._data:
            yield key

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, key: str):
        return self._data[key]

    def __repr__(self) -> str:
        return repr(self._data)


def test_boxplot_stats(chinook_db):
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL 'sqlite_scanner';")
    con.execute("LOAD 'sqlite_scanner';")
    con.execute(f"CALL sqlite_attach({chinook_db!r});")

    res = con.execute('SELECT * FROM Invoice')
    X = res.df().Total
    expected = cbook.boxplot_stats(X)

    result = plot.boxplot_stats(con, 'Invoice', 'Total')

    assert DictOfFloats(result[0]) == DictOfFloats(expected[0])