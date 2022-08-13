from typing import Iterator, Iterable
from collections.abc import MutableMapping

from jinja2 import Template


class SQLStore(MutableMapping):
    """Stores SQL scripts to render large queries with CTEs"""

    def __init__(self):
        self._data = dict()

    def __setitem__(self, key: str, value: str) -> None:
        self._data[key] = value

    def __getitem__(self, key) -> str:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        for key in self._data:
            yield key

    def __len__(self) -> int:
        return len(self._data)

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def render(self, query, with_=None):
        # TODO: if with is false, WITH should not appear
        return SQLQuery(self, query, with_)

    def store(self, key, query, with_=None):
        if with_ and key in with_:
            raise ValueError(f"Script name ({key!r}) cannot appear in with_ argument")

        self._data[key] = SQLQuery(self, query, with_)


_template = Template(
    """\
WITH{% for name in with_ %} {{name}} AS (
    {{saved[name]._query}}
){{ "," if not loop.last }}{% endfor %}
{{query}}
"""
)


class SQLQuery:
    """Holds queries and renders them"""

    def __init__(self, store: SQLStore, query: str, with_: Iterable = None):
        self._store = store
        self._query = query
        self._with_ = with_ or []

    def __str__(self) -> str:
        with_all = _get_dependencies(self._store, self._with_)
        return _template.render(
            query=self._query, saved=self._store._data, with_=with_all
        )


def _get_dependencies(store, keys):
    """Get a list of all dependencies to reconstruct the CTEs in keys"""
    # get the dependencies for each key
    deps = _flatten([_get_dependencies_for_key(store, key) for key in keys])
    # remove duplicates but preserve order
    return list(dict.fromkeys(deps + keys))


def _get_dependencies_for_key(store, key):
    """Retrieve dependencies for a single key"""
    deps = store[key]._with_
    deps_of_deps = _flatten([_get_dependencies_for_key(store, dep) for dep in deps])
    return deps_of_deps + deps


def _flatten(l):
    """Flatten a list of lists"""
    return [element for sub in l for element in sub]


# session-wide store
store = SQLStore()
