from typing import Iterator
from collections.abc import Mapping

import numpy as np
from matplotlib import cbook
from sql import plot
from sql.connection import ConnectionManager
from pathlib import Path
import pytest
from sqlalchemy.exc import OperationalError
import matplotlib


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


def test_boxplot_stats(chinook_db, ip_empty):
    # there's some werid behavior in duckdb-engine that will cause the
    # table not to be found if we call commit
    ip_empty.run_cell("%config SqlMagic.autocommit=False")
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql INSTALL 'sqlite_scanner';")
    ip_empty.run_cell("%sql commit")
    ip_empty.run_cell("%sql LOAD 'sqlite_scanner';")
    ip_empty.run_cell(f"%sql CALL sqlite_attach({chinook_db!r});")

    res = ip_empty.run_cell("%sql SELECT * FROM Invoice").result
    X = res.DataFrame().Total
    expected = cbook.boxplot_stats(X)
    result = plot._boxplot_stats(ConnectionManager.current, "Invoice", "Total")

    assert DictOfFloats(result) == DictOfFloats(expected[0])


def test_boxplot_stats_exception(chinook_db, ip_empty):
    # there's some werid behavior in duckdb-engine that will cause the
    # table not to be found if we call commit
    ip_empty.run_cell("%config SqlMagic.autocommit=False")
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql INSTALL 'sqlite_scanner';")
    ip_empty.run_cell("%sql commit")
    ip_empty.run_cell("%sql LOAD 'sqlite_scanner';")
    ip_empty.run_cell(f"%sql CALL sqlite_attach({chinook_db!r});")

    res = ip_empty.run_cell("%sql SELECT * FROM Invoice").result

    X = res.DataFrame().Total
    cbook.boxplot_stats(X)
    with pytest.raises(
        BaseException, match="whis must be a float or list of percentiles.*"
    ):
        plot._boxplot_stats(
            ConnectionManager.current,
            "Invoice",
            "Total",
            "Not a float or list of percentiles whis param",
        )


def test_summary_stats(chinook_db, ip_empty, tmp_empty):
    Path("data.csv").write_text(
        """\
x, y
0, 0
1, 1
2, 2
5, 7
9, 9
"""
    )

    # there's some werid behavior in duckdb-engine that will cause the
    # table not to be found if we call commit
    ip_empty.run_cell("%config SqlMagic.autocommit=False")
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql INSTALL 'sqlite_scanner';")
    ip_empty.run_cell("%sql commit")
    ip_empty.run_cell("%sql LOAD 'sqlite_scanner';")
    result = plot._summary_stats(ConnectionManager.current, "data.csv", column="x")
    expected = {"q1": 1.0, "med": 2.0, "q3": 5.0, "mean": 3.4, "N": 5.0}
    assert result == expected


def test_summary_stats_missing_file(chinook_db, ip_empty):
    # there's some werid behavior in duckdb-engine that will cause the
    # table not to be found if we call commit
    ip_empty.run_cell("%config SqlMagic.autocommit=False")
    ip_empty.run_cell("%sql duckdb://")
    ip_empty.run_cell("%sql INSTALL 'sqlite_scanner';")
    ip_empty.run_cell("%sql commit")
    ip_empty.run_cell("%sql LOAD 'sqlite_scanner';")
    with pytest.raises(OperationalError) as e:
        plot._summary_stats(ConnectionManager.current, "data.csv", column="x")
    assert 'No files found that match the pattern "data.csv"' in str(e)


def test_internal_histogram_with_nulls(tmp_empty, ip):
    # sheri, mick missing age
    Path("data.csv").write_text(
        "name,age\nDan,33\nBob,19\nSheri,\nVin,33\nMick,\nJay,33\nSky,33"
    )
    ip.run_cell("%sql duckdb://")
    ip.run_cell(
        """%%sql --save test_dataset --no-execute
SELECT *
FROM data.csv
"""
    )
    out = ip.run_cell(
        "%sqlplot histogram --table data.csv --column age --table data.csv"
    )
    assert isinstance(out.result, matplotlib.axes._axes.Axes)


def test_internal_histogram_no_nulls(tmp_empty, ip):
    Path("data.csv").write_text(
        "name,age\nDan,33\nBob,19\nSheri,45\nVin,33\nMick,38\nJay,33\nSky,33"
    )
    ip.run_cell("%sql duckdb://")
    ip.run_cell(
        """%%sql --save test_dataset --no-execute
SELECT *
FROM data.csv
"""
    )
    out = ip.run_cell(
        "%sqlplot histogram --table data.csv --column age --table data.csv"
    )
    assert isinstance(out.result, matplotlib.axes._axes.Axes)
