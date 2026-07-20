"""Shared DB-API 2.0 types used by both the FFI-backed and serverless drivers.

This module contains the exception hierarchy, Row class, and SQL classification
helpers that are shared between turso.lib (local/FFI driver) and
turso_serverless (pure-Python HTTP driver).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any


# DB-API 2.0 exception hierarchy
class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# SQL classification helpers

def _first_keyword(sql: str) -> str:
    """
    Return the first SQL keyword (uppercased) ignoring leading whitespace
    and single-line and multi-line comments.

    This is intentionally minimal and only used to detect DML for implicit
    transaction handling. It may not handle all edge cases (e.g. complex WITH).
    """
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            # line comment
            i += 2
            while i < n and sql[i] not in ("\r", "\n"):
                i += 1
            continue
        if c == "/" and i + 1 < n and sql[i + 1] == "*":
            # block comment
            i += 2
            while i + 1 < n and not (sql[i] == "*" and sql[i + 1] == "/"):
                i += 1
            i = min(i + 2, n)
            continue
        break
    # read token
    j = i
    while j < n and (sql[j].isalpha() or sql[j] == "_"):
        j += 1
    return sql[i:j].upper()


def _is_dml(sql: str) -> bool:
    kw = _first_keyword(sql)
    if kw in ("INSERT", "UPDATE", "DELETE", "REPLACE"):
        return True
    # "WITH" can also prefix DML, but we conservatively skip it to avoid false positives.
    return False


def _is_insert_or_replace(sql: str) -> bool:
    kw = _first_keyword(sql)
    return kw in ("INSERT", "REPLACE")


# Row class — duck-typed cursor (accepts Any for cursor parameter)

class Row(Sequence[Any]):
    """
    sqlite3.Row-like container supporting index and name-based access.
    """

    def __new__(cls, cursor: Any, data: tuple[Any, ...], /) -> "Row":
        obj = super().__new__(cls)
        # Attach metadata
        obj._cursor = cursor
        obj._data = data
        # Build mapping from column name to index
        desc = cursor.description or ()
        obj._keys = tuple(col[0] for col in desc)
        obj._index = {name: idx for idx, name in enumerate(obj._keys)}
        return obj

    def keys(self) -> list[str]:
        return list(self._keys)

    def __getitem__(self, key: int | str | slice, /) -> Any:
        if isinstance(key, slice):
            return self._data[key]
        if isinstance(key, int):
            return self._data[key]
        # key is column name
        idx = self._index.get(key)
        if idx is None:
            raise KeyError(key)
        return self._data[idx]

    def __hash__(self) -> int:
        return hash((self._keys, self._data))

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __eq__(self, value: object, /) -> bool:
        if not isinstance(value, Row):
            return NotImplemented  # type: ignore[return-value]
        return self._keys == value._keys and self._data == value._data

    def __ne__(self, value: object, /) -> bool:
        if not isinstance(value, Row):
            return NotImplemented  # type: ignore[return-value]
        return not self.__eq__(value)

    # The rest return NotImplemented for non-Row comparisons
    def __lt__(self, value: object, /) -> bool:
        if not isinstance(value, Row):
            return NotImplemented  # type: ignore[return-value]
        return (self._keys, self._data) < (value._keys, value._data)

    def __le__(self, value: object, /) -> bool:
        if not isinstance(value, Row):
            return NotImplemented  # type: ignore[return-value]
        return (self._keys, self._data) <= (value._keys, value._data)

    def __gt__(self, value: object, /) -> bool:
        if not isinstance(value, Row):
            return NotImplemented  # type: ignore[return-value]
        return (self._keys, self._data) > (value._keys, value._data)

    def __ge__(self, value: object, /) -> bool:
        if not isinstance(value, Row):
            return NotImplemented  # type: ignore[return-value]
        return (self._keys, self._data) >= (value._keys, value._data)
