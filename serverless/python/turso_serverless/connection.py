"""DB-API 2.0 Connection and Cursor backed by hrana v3 HTTP session.

Re-uses Row, exception hierarchy, and helpers from _dbapi_common.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional, TypeVar

from ._dbapi_common import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Row,
    Warning,
    _first_keyword,
    _is_dml,
    _is_insert_or_replace,
)
from .protocol import build_batch_step, decode_value
from .session import Session

_DBCursorT = TypeVar("_DBCursorT", bound="Cursor")


def _classify_error(msg: str) -> Exception:
    """Pattern-match server error messages into DB-API exception types."""
    lower = msg.lower()
    if "constraint" in lower or "unique" in lower or "primary key" in lower:
        return IntegrityError(msg)
    return OperationalError(msg)


class Connection:
    """DB-API 2.0 Connection backed by a remote hrana v3 HTTP session."""

    # Exception classes as attributes (like sqlite3.Connection)
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
    Warning = Warning

    def __init__(
        self,
        session: Session,
        *,
        isolation_level: Optional[str] = "DEFERRED",
    ) -> None:
        self._session = session
        self.isolation_level = isolation_level
        self.row_factory: Callable | type[Row] | None = None
        self.text_factory: Any = str
        self._autocommit_mode: object | bool = "LEGACY"
        self._closed = False
        self._in_transaction = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed connection")

    def _execute_stmt(  # noqa: C901
        self,
        sql: str,
        params: Optional[list] = None,
        named_params: Optional[list[tuple[str, Any]]] = None,
        want_rows: bool = True,
    ) -> tuple[list[str], list[tuple], int, Optional[int]]:
        """Execute a single statement and return (columns, rows, affected_rows, last_rowid)."""
        self._ensure_open()
        step = build_batch_step(sql, args=params, named_args=named_params, want_rows=want_rows)

        # Enable keep_alive before BEGIN so the baton is preserved
        kw = _first_keyword(sql)
        if kw == "BEGIN":
            self._session.keep_alive = True

        try:
            entries = self._session.execute_cursor([step])
        except RuntimeError as e:
            if kw == "BEGIN":
                self._session.keep_alive = False
            raise _classify_error(str(e))

        columns: list[str] = []
        rows: list[tuple] = []
        affected_rows = 0
        last_rowid: Optional[int] = None
        current_row_cols: list[str] = []

        for entry in entries:
            etype = entry.get("type")
            if etype == "step_begin":
                cols = entry.get("cols") or []
                current_row_cols = [c.get("name", "") for c in cols]
                columns = current_row_cols
            elif etype == "row":
                raw_row = entry.get("row") or []
                rows.append(tuple(decode_value(v) for v in raw_row))
            elif etype == "step_end":
                affected_rows = entry.get("affected_row_count", 0) or 0
                rowid_str = entry.get("last_insert_rowid")
                if rowid_str is not None:
                    try:
                        last_rowid = int(rowid_str)
                    except (ValueError, TypeError):
                        pass
            elif etype == "step_error":
                err = entry.get("error")
                if err:
                    raise _classify_error(err.get("message", "Unknown error"))
                raise OperationalError("Unknown step error")
            elif etype == "error":
                err = entry.get("error")
                if err:
                    raise _classify_error(err.get("message", "Unknown error"))
                raise OperationalError("Unknown error")

        # Track transaction state — keep_alive must be set to True before
        # BEGIN so the baton is preserved, but set to False after
        # COMMIT/ROLLBACK so the baton is still sent with the final request.
        kw = _first_keyword(sql)
        if kw == "BEGIN":
            self._in_transaction = True
        elif kw in ("COMMIT", "END"):
            self._in_transaction = False
            self._session.keep_alive = False
        elif kw == "ROLLBACK":
            self._in_transaction = False
            self._session.keep_alive = False

        return columns, rows, affected_rows, last_rowid

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    @property
    def autocommit(self) -> object | bool:
        return self._autocommit_mode

    @autocommit.setter
    def autocommit(self, val: object | bool) -> None:
        if val not in (True, False, "LEGACY"):
            raise ProgrammingError("autocommit must be True, False, or 'LEGACY'")
        self._autocommit_mode = val

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._in_transaction:
                try:
                    self._execute_stmt("ROLLBACK", want_rows=False)
                except Exception:
                    pass
            self._session.close()
        finally:
            self._closed = True

    def commit(self) -> None:
        self._ensure_open()
        if self._in_transaction:
            self._execute_stmt("COMMIT", want_rows=False)

    def rollback(self) -> None:
        self._ensure_open()
        if self._in_transaction:
            self._execute_stmt("ROLLBACK", want_rows=False)

    def cursor(self, factory: Optional[Callable[[Connection], _DBCursorT]] = None) -> _DBCursorT | Cursor:
        self._ensure_open()
        if factory is None:
            return Cursor(self)
        return factory(self)

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> Cursor:
        cur = self.cursor()
        cur.execute(sql, parameters)
        return cur

    def executemany(self, sql: str, parameters: Iterable[Sequence[Any] | Mapping[str, Any]]) -> Cursor:
        cur = self.cursor()
        cur.executemany(sql, parameters)
        return cur

    def executescript(self, sql_script: str) -> Cursor:
        cur = self.cursor()
        cur.executescript(sql_script)
        return cur

    def _maybe_implicit_begin(self, sql: str) -> None:
        """Legacy implicit transaction behavior."""
        if self.isolation_level is not None and not self._in_transaction and _is_dml(sql):
            level = self.isolation_level or "DEFERRED"
            self._execute_stmt(f"BEGIN {level}", want_rows=False)

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            return False


class Cursor:
    """DB-API 2.0 Cursor backed by eager row fetching from the remote server."""

    arraysize: int

    def __init__(self, connection: Connection, /) -> None:
        self._connection = connection
        self.arraysize = 1
        self.row_factory: Callable | type[Row] | None = connection.row_factory
        self._rows: list[tuple] = []
        self._row_index = 0
        self._description: Optional[tuple[tuple[str, None, None, None, None, None, None], ...]] = None
        self._lastrowid: Optional[int] = None
        self._rowcount: int = -1
        self._closed = False

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | None:
        return self._description

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def close(self) -> None:
        self._closed = True
        self._rows = []

    def _ensure_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor")

    @staticmethod
    def _convert_params(
        parameters: Sequence[Any] | Mapping[str, Any],
    ) -> tuple[Optional[list], Optional[list[tuple[str, Any]]]]:
        """Convert DB-API parameters to protocol args/named_args."""
        if isinstance(parameters, Mapping):
            named = []
            for key, val in parameters.items():
                # Try :name, $name, @name prefixes
                if isinstance(key, str) and not key.startswith((":","$","@")):
                    named.append((f":{key}", val))
                else:
                    named.append((key, val))
            return None, named
        params = list(parameters) if parameters else []
        return params if params else None, None

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> Cursor:
        self._ensure_open()
        self._rows = []
        self._row_index = 0

        # Implicit transaction
        self._connection._maybe_implicit_begin(sql)

        args, named_args = self._convert_params(parameters)
        columns, rows, affected, last_rowid = self._connection._execute_stmt(
            sql, params=args, named_params=named_args, want_rows=True,
        )

        if columns:
            self._description = tuple(
                (name, None, None, None, None, None, None) for name in columns
            )
            self._rows = rows
            self._rowcount = -1
        else:
            self._description = None
            self._rows = []
            self._rowcount = affected

        if last_rowid is not None and _is_insert_or_replace(sql):
            self._lastrowid = last_rowid

        return self

    def executemany(self, sql: str, seq_of_parameters: Iterable[Sequence[Any] | Mapping[str, Any]]) -> Cursor:
        self._ensure_open()
        self._rows = []
        self._row_index = 0
        self._description = None

        if not _is_dml(sql):
            raise ProgrammingError("executemany() requires a single DML statement")

        self._connection._maybe_implicit_begin(sql)

        total = 0
        for parameters in seq_of_parameters:
            args, named_args = self._convert_params(parameters)
            _, _, affected, _ = self._connection._execute_stmt(
                sql, params=args, named_params=named_args, want_rows=False,
            )
            total += affected

        self._rowcount = total
        return self

    def executescript(self, sql_script: str) -> Cursor:
        """Execute multiple statements via the pipeline sequence endpoint."""
        self._ensure_open()
        self._rows = []
        self._row_index = 0
        self._description = None

        # Commit any pending transaction first (sqlite3 behavior)
        if self._connection._in_transaction:
            try:
                self._connection._execute_stmt("COMMIT", want_rows=False)
            except Exception:
                pass

        try:
            resp = self._connection._session.execute_pipeline(
                [{"type": "sequence", "sql": sql_script}]
            )
        except RuntimeError as e:
            raise _classify_error(str(e))

        # Check for errors in pipeline results
        results = resp.get("results", [])
        for result in results:
            if result.get("type") == "error":
                err = result.get("error", {})
                raise _classify_error(err.get("message", "Unknown error"))
            resp_inner = result.get("response", {})
            if resp_inner and resp_inner.get("type") == "error":
                err_inner = resp_inner.get("error", {})
                raise _classify_error(err_inner.get("message", "Unknown error"))

        self._rowcount = -1
        return self

    def _apply_row_factory(self, row_values: tuple) -> Any:
        rf = self.row_factory
        if rf is None:
            return row_values
        if isinstance(rf, type) and issubclass(rf, Row):
            return rf(self, Row(self, row_values))
        if callable(rf):
            return rf(self, Row(self, row_values))
        return row_values

    def fetchone(self) -> Any:
        self._ensure_open()
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return self._apply_row_factory(row)

    def fetchmany(self, size: Optional[int] = None) -> list[Any]:
        self._ensure_open()
        if size is None:
            size = self.arraysize
        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self) -> list[Any]:
        self._ensure_open()
        result = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def setinputsizes(self, sizes: Any, /) -> None:
        return None

    def setoutputsize(self, size: Any, column: Any = None, /) -> None:
        return None

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


def connect(
    url: str,
    *,
    auth_token: Optional[str] = None,
    isolation_level: Optional[str] = "DEFERRED",
) -> Connection:
    """Open a remote connection to a Turso database.

    Parameters:
    - url: Database URL (turso://, https://, http://, or libsql://)
    - auth_token: Authentication token
    - isolation_level: Transaction isolation level (default: DEFERRED)
    """
    session = Session(url, auth_token=auth_token)
    return Connection(session, isolation_level=isolation_level)
