from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Callable, Optional, TypeVar

from ._turso import (
    Busy,
    Constraint,
    Corrupt,
    DatabaseFull,
    Interrupt,
    Misuse,
    NotAdb,
    PyTursoConnection,
    PyTursoDatabase,
    PyTursoDatabaseConfig,
    PyTursoExecutionResult,
    PyTursoLog,
    PyTursoSetupConfig,
    PyTursoStatement,
    PyTursoStatusCode,
    py_turso_database_open,
    py_turso_setup,
)
from ._turso import (
    Error as TursoError,
)
from ._turso import (
    PyTursoStatusCode as Status,
)

# DB-API 2.0 module attributes
apilevel = "2.0"
threadsafety = 1  # 1 means: Threads may share the module, but not connections.
paramstyle = "qmark"  # Only positional parameters are supported.


# Exception hierarchy following DB-API 2.0
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


def _map_turso_exception(exc: Exception) -> Exception:
    """Maps Turso-specific exceptions to DB-API 2.0 exception hierarchy"""
    if isinstance(exc, Busy):
        return OperationalError(str(exc))
    if isinstance(exc, Interrupt):
        return OperationalError(str(exc))
    if isinstance(exc, Misuse):
        return InterfaceError(str(exc))
    if isinstance(exc, Constraint):
        return IntegrityError(str(exc))
    if isinstance(exc, TursoError):
        # Generic Turso error -> DatabaseError
        return DatabaseError(str(exc))
    if isinstance(exc, DatabaseFull):
        return OperationalError(str(exc))
    if isinstance(exc, NotAdb):
        return DatabaseError(str(exc))
    if isinstance(exc, Corrupt):
        return DatabaseError(str(exc))
    return exc


# Internal helpers

_DBCursorT = TypeVar("_DBCursorT", bound="Cursor")


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


def _run_execute_with_io(stmt: PyTursoStatement, extra_io: Optional[Callable[[], None]]) -> PyTursoExecutionResult:
    """
    Run PyTursoStatement.execute() handling potential async IO loops.
    """
    while True:
        result = stmt.execute()
        status = result.status
        if status == Status.Io:
            stmt.run_io()
            if extra_io:
                extra_io()
            continue
        return result


def _step_once_with_io(stmt: PyTursoStatement, extra_io: Optional[Callable[[], None]]) -> PyTursoStatusCode:
    """
    Run PyTursoStatement.step() once handling potential async IO loops.
    """
    while True:
        status = stmt.step()
        if status == Status.Io:
            stmt.run_io()
            if extra_io:
                extra_io()
            continue
        return status


@dataclass
class _Prepared:
    stmt: PyTursoStatement
    tail_index: int
    has_columns: bool
    column_names: tuple[str, ...]


# Connection goes FIRST
class Connection:
    """
    A connection to a Turso (SQLite-compatible) database.

    Similar to sqlite3.Connection with a subset of features focusing on DB-API 2.0.
    """

    # Expose exception classes as attributes like sqlite3.Connection does
    @property
    def DataError(self) -> type[DataError]:
        return DataError

    @property
    def DatabaseError(self) -> type[DatabaseError]:
        return DatabaseError

    @property
    def Error(self) -> type[Error]:
        return Error

    @property
    def IntegrityError(self) -> type[IntegrityError]:
        return IntegrityError

    @property
    def InterfaceError(self) -> type[InterfaceError]:
        return InterfaceError

    @property
    def InternalError(self) -> type[InternalError]:
        return InternalError

    @property
    def NotSupportedError(self) -> type[NotSupportedError]:
        return NotSupportedError

    @property
    def OperationalError(self) -> type[OperationalError]:
        return OperationalError

    @property
    def ProgrammingError(self) -> type[ProgrammingError]:
        return ProgrammingError

    @property
    def Warning(self) -> type[Warning]:
        return Warning

    def __init__(
        self,
        conn: PyTursoConnection,
        *,
        isolation_level: Optional[str] = "DEFERRED",
        extra_io: Optional[Callable[[], None]] = None,
    ) -> None:
        self._conn: PyTursoConnection = conn
        # autocommit behavior:
        # - True: SQLite autocommit mode; commit/rollback are no-ops.
        # - False: PEP 249 compliant: ensure a transaction is always open.
        #   We'll use BEGIN DEFERRED after commit/rollback.
        # - "LEGACY": implicit transactions on DML when isolation_level is not None.
        self._autocommit_mode: object | bool = "LEGACY"
        self.isolation_level: Optional[str] = isolation_level
        self.row_factory: Callable[[Cursor, Row], object] | type[Row] | None = None
        self.text_factory: Any = str
        self.extra_io = extra_io

        # If autocommit is False, ensure a transaction is open
        if self._autocommit_mode is False:
            self._ensure_transaction_open()

    def _ensure_transaction_open(self) -> None:
        """
        Ensure a transaction is open when autocommit is False.
        """
        try:
            if self._conn.get_auto_commit():
                # No transaction active -> open new one according to isolation_level (default to DEFERRED)
                level = self.isolation_level or "DEFERRED"
                self._exec_ddl_only(f"BEGIN {level}")
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    def _exec_ddl_only(self, sql: str) -> None:
        """
        Execute a SQL statement that does not produce rows and ignore any result rows.
        """
        try:
            stmt = self._conn.prepare_single(sql)
            _run_execute_with_io(stmt, self.extra_io)
            # finalize to ensure completion; finalize never mixes with execute
            stmt.finalize()
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    def _prepare_first(self, sql: str) -> _Prepared:
        """
        Prepare the first statement in the given SQL string and return metadata.
        """
        try:
            opt = self._conn.prepare_first(sql)
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)
        if opt is None:
            raise ProgrammingError("no SQL statements to execute")

        stmt, tail_idx = opt
        # Determine whether statement returns columns (rows)
        try:
            columns = tuple(stmt.columns())
        except Exception as exc:  # noqa: BLE001
            # Clean up statement before re-raising
            try:
                stmt.finalize()
            except Exception:
                pass
            raise _map_turso_exception(exc)
        has_cols = len(columns) > 0
        return _Prepared(stmt=stmt, tail_index=tail_idx, has_columns=has_cols, column_names=columns)

    def _raise_if_multiple_statements(self, sql: str, tail_index: int) -> None:
        """
        Ensure there is no second statement after the first one; otherwise raise ProgrammingError.
        """
        # Skip any trailing whitespace/comments after tail_index, and check if another statement exists.
        rest = sql[tail_index:]
        try:
            nxt = self._conn.prepare_first(rest)
            if nxt is not None:
                # Clean-up the prepared second statement immediately
                second_stmt, _ = nxt
                try:
                    second_stmt.finalize()
                except Exception:
                    pass
                raise ProgrammingError("You can only execute one statement at a time")
        except ProgrammingError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    @property
    def in_transaction(self) -> bool:
        try:
            return not self._conn.get_auto_commit()
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    # Provide autocommit property for sqlite3-like API (optional)
    @property
    def autocommit(self) -> object | bool:
        return self._autocommit_mode

    @autocommit.setter
    def autocommit(self, val: object | bool) -> None:
        # Accept True, False, or "LEGACY"
        if val not in (True, False, "LEGACY"):
            raise ProgrammingError("autocommit must be True, False, or 'LEGACY'")
        self._autocommit_mode = val
        # If switching to False, ensure a transaction is open
        if val is False:
            self._ensure_transaction_open()
        # If switching to True or LEGACY, nothing else to do immediately.

    def close(self) -> None:
        # In sqlite3: If autocommit is False, pending transaction is implicitly rolled back.
        try:
            if self._autocommit_mode is False and self.in_transaction:
                try:
                    self._exec_ddl_only("ROLLBACK")
                except Exception:
                    # As sqlite3 does, ignore rollback failure on close
                    pass
            self._conn.close()
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    def commit(self) -> None:
        try:
            if self._autocommit_mode is True:
                # No-op in SQLite autocommit mode
                return
            if self.in_transaction:
                self._exec_ddl_only("COMMIT")
            if self._autocommit_mode is False:
                # Re-open a transaction to maintain PEP 249 behavior
                self._ensure_transaction_open()
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    def rollback(self) -> None:
        try:
            if self._autocommit_mode is True:
                # No-op in SQLite autocommit mode
                return
            if self.in_transaction:
                self._exec_ddl_only("ROLLBACK")
            if self._autocommit_mode is False:
                # Re-open a transaction to maintain PEP 249 behavior
                self._ensure_transaction_open()
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    def _maybe_implicit_begin(self, sql: str) -> None:
        """
        Implement sqlite3 legacy implicit transaction behavior:

        If autocommit is LEGACY_TRANSACTION_CONTROL, isolation_level is not None, sql is a DML
        (INSERT/UPDATE/DELETE/REPLACE), and there is no open transaction, issue:
            BEGIN <isolation_level>
        """
        if self._autocommit_mode == "LEGACY" and self.isolation_level is not None:
            if not self.in_transaction and _is_dml(sql):
                level = self.isolation_level or "DEFERRED"
                self._exec_ddl_only(f"BEGIN {level}")

    def cursor(self, factory: Optional[Callable[[Connection], _DBCursorT]] = None) -> _DBCursorT | Cursor:
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

    def __call__(self, sql: str) -> PyTursoStatement:
        # Shortcut to prepare a single statement
        try:
            return self._conn.prepare_single(sql)
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        # sqlite3 behavior: In context manager, if no exception -> commit, else rollback (legacy and PEP 249 modes)
        try:
            if type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            # Always propagate exceptions (returning False)
            return False


# Cursor goes SECOND
class Cursor:
    arraysize: int

    def __init__(self, connection: Connection, /) -> None:
        self._connection: Connection = connection
        self.arraysize = 1
        self.row_factory: Callable[[Cursor, Row], object] | type[Row] | None = connection.row_factory

        # State for the last executed statement
        self._active_stmt: Optional[PyTursoStatement] = None
        self._active_has_rows: bool = False
        self._description: Optional[tuple[tuple[str, None, None, None, None, None, None], ...]] = None
        self._lastrowid: Optional[int] = None
        self._rowcount: int = -1
        self._closed: bool = False

    @property
    def connection(self) -> Connection:
        return self._connection

    def close(self) -> None:
        if self._closed:
            return
        try:
            # Finalize any active statement to ensure completion.
            if self._active_stmt is not None:
                try:
                    self._active_stmt.finalize()
                except Exception:
                    pass
        finally:
            self._active_stmt = None
            self._active_has_rows = False
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor")

    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | None:
        return self._description

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def _reset_last_result(self) -> None:
        # Ensure any previous statement is finalized to not leak resources
        if self._active_stmt is not None:
            try:
                self._active_stmt.finalize()
            except Exception:
                pass
        self._active_stmt = None
        self._active_has_rows = False
        self._description = None
        self._rowcount = -1
        # Do not reset lastrowid here; sqlite3 preserves lastrowid until next insert.

    @staticmethod
    def _to_positional_params(parameters: Sequence[Any] | Mapping[str, Any]) -> tuple[Any, ...]:
        if isinstance(parameters, Mapping):
            # Named placeholders are not supported
            raise ProgrammingError("Named parameters are not supported; use positional parameters with '?'")
        if parameters is None:
            return ()
        if isinstance(parameters, tuple):
            return parameters
        # Convert arbitrary sequences to tuple efficiently
        return tuple(parameters)

    def _maybe_implicit_begin(self, sql: str) -> None:
        self._connection._maybe_implicit_begin(sql)

    def _prepare_single_statement(self, sql: str) -> _Prepared:
        prepared = self._connection._prepare_first(sql)
        # Ensure there are no further statements
        self._connection._raise_if_multiple_statements(sql, prepared.tail_index)
        return prepared

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> "Cursor":
        self._ensure_open()
        self._reset_last_result()

        # Implement legacy implicit transactions if needed
        self._maybe_implicit_begin(sql)

        # Prepare exactly one statement
        prepared = self._prepare_single_statement(sql)

        stmt = prepared.stmt
        try:
            # Bind positional parameters
            params = self._to_positional_params(parameters)
            if params:
                stmt.bind(params)

            if prepared.has_columns:
                # Stepped statement (e.g., SELECT or DML with RETURNING)
                self._active_stmt = stmt
                self._active_has_rows = True
                # Set description immediately (even if there are no rows)
                self._description = tuple((name, None, None, None, None, None, None) for name in prepared.column_names)
                # For statements that return rows, DB-API specifies rowcount is -1
                self._rowcount = -1
                # Do not compute lastrowid here
            else:
                # Executed statement (no rows returned)
                result = _run_execute_with_io(stmt, self._connection.extra_io)
                # rows_changed from execution result
                self._rowcount = int(result.rows_changed)
                # Set description to None
                self._description = None
                # Set lastrowid for INSERT/REPLACE (best-effort)
                self._lastrowid = self._fetch_last_insert_rowid_if_needed(sql, result.rows_changed)
                # Finalize the statement to release resources
                stmt.finalize()
        except Exception as exc:  # noqa: BLE001
            # Ensure cleanup on error
            try:
                stmt.finalize()
            except Exception:
                pass
            raise _map_turso_exception(exc)

        return self

    def _fetch_last_insert_rowid_if_needed(self, sql: str, rows_changed: int) -> Optional[int]:
        if rows_changed <= 0 or not _is_insert_or_replace(sql):
            return self._lastrowid
        # Query last_insert_rowid(); this is connection-scoped and cheap
        try:
            q = self._connection._conn.prepare_single("SELECT last_insert_rowid()")
            # No parameters; this produces a single-row single-column result
            # Use stepping to fetch the row
            status = _step_once_with_io(q, self._connection.extra_io)
            if status == Status.Row:
                py_row = q.row()
                # row() returns a Python tuple with one element
                # We avoid complex conversions: take first item
                value = tuple(py_row)[0]  # type: ignore[call-arg]
                # Finalize to complete
                q.finalize()
                if isinstance(value, int):
                    return value
                try:
                    return int(value)
                except Exception:
                    return self._lastrowid
            # Finalize anyway
            q.finalize()
        except Exception:
            # Ignore errors; lastrowid remains unchanged on failure
            pass
        return self._lastrowid

    def executemany(self, sql: str, seq_of_parameters: Iterable[Sequence[Any] | Mapping[str, Any]]) -> "Cursor":
        self._ensure_open()
        self._reset_last_result()

        # executemany only accepts DML; enforce this to match sqlite3 semantics
        if not _is_dml(sql):
            raise ProgrammingError("executemany() requires a single DML (INSERT/UPDATE/DELETE/REPLACE) statement")

        # Implement legacy implicit transaction: same as execute()
        self._maybe_implicit_begin(sql)

        prepared = self._prepare_single_statement(sql)
        stmt = prepared.stmt
        try:
            # For executemany, discard any rows produced (even if RETURNING was used)
            # Therefore we ALWAYS use execute() path per-iteration.
            for parameters in seq_of_parameters:
                # Reset previous bindings and program memory before reusing
                stmt.reset()
                params = self._to_positional_params(parameters)
                if params:
                    stmt.bind(params)
                result = _run_execute_with_io(stmt, self._connection.extra_io)
                # rowcount is "the number of modified rows" for the LAST executed statement only
                self._rowcount = int(result.rows_changed) + (self._rowcount if self._rowcount != -1 else 0)
            # After loop, finalize statement
            stmt.finalize()
            # Cursor description is None for DML executed via executemany()
            self._description = None
            # sqlite3 leaves lastrowid unchanged for executemany
        except Exception as exc:  # noqa: BLE001
            try:
                stmt.finalize()
            except Exception:
                pass
            raise _map_turso_exception(exc)
        return self

    def executescript(self, sql_script: str) -> "Cursor":
        self._ensure_open()
        self._reset_last_result()

        # sqlite3 behavior: If autocommit is LEGACY and there is a pending transaction, implicitly COMMIT first
        if self._connection._autocommit_mode == "LEGACY" and self._connection.in_transaction:
            try:
                self._connection._exec_ddl_only("COMMIT")
            except Exception as exc:  # noqa: BLE001
                raise _map_turso_exception(exc)

        # Iterate over statements in the script and execute them, discarding rows
        sql = sql_script
        total_rowcount = -1
        try:
            offset = 0
            while True:
                opt = self._connection._conn.prepare_first(sql[offset:])
                if opt is None:
                    break
                stmt, tail = opt
                # Note: per DB-API, any resulting rows are discarded
                result = _run_execute_with_io(stmt, self._connection.extra_io)
                total_rowcount = int(result.rows_changed) if result.rows_changed > 0 else total_rowcount
                # finalize to ensure completion
                stmt.finalize()
                offset += tail
        except Exception as exc:  # noqa: BLE001
            raise _map_turso_exception(exc)

        self._description = None
        self._rowcount = total_rowcount
        return self

    def _fetchone_tuple(self) -> Optional[tuple[Any, ...]]:
        """
        Fetch one row as a plain Python tuple, or return None if no more rows.
        """
        if not self._active_has_rows or self._active_stmt is None:
            return None
        try:
            status = _step_once_with_io(self._active_stmt, self._connection.extra_io)
            if status == Status.Row:
                row_tuple = tuple(self._active_stmt.row())  # type: ignore[call-arg]
                return row_tuple
            # status == Done: finalize and clean up
            self._active_stmt.finalize()
            self._active_stmt = None
            self._active_has_rows = False
            return None
        except Exception as exc:  # noqa: BLE001
            # Finalize and clean up on error
            try:
                if self._active_stmt is not None:
                    self._active_stmt.finalize()
            except Exception:
                pass
            self._active_stmt = None
            self._active_has_rows = False
            raise _map_turso_exception(exc)

    def _apply_row_factory(self, row_values: tuple[Any, ...]) -> Any:
        rf = self.row_factory
        if rf is None:
            return row_values
        if isinstance(rf, type) and issubclass(rf, Row):
            return rf(self, Row(self, row_values))  # type: ignore[call-arg]
        if callable(rf):
            return rf(self, Row(self, row_values))  # type: ignore[misc]
        # Fallback: return tuple
        return row_values

    def fetchone(self) -> Any:
        self._ensure_open()
        row = self._fetchone_tuple()
        if row is None:
            return None
        return self._apply_row_factory(row)

    def fetchmany(self, size: Optional[int] = None) -> list[Any]:
        self._ensure_open()
        if size is None:
            size = self.arraysize
        if size < 0:
            raise ValueError("size must be non-negative")
        result: list[Any] = []
        for _ in range(size):
            row = self._fetchone_tuple()
            if row is None:
                break
            result.append(self._apply_row_factory(row))
        return result

    def fetchall(self) -> list[Any]:
        self._ensure_open()
        result: list[Any] = []
        while True:
            row = self._fetchone_tuple()
            if row is None:
                break
            result.append(self._apply_row_factory(row))
        return result

    def setinputsizes(self, sizes: Any, /) -> None:
        # No-op for DB-API compliance
        return None

    def setoutputsize(self, size: Any, column: Any = None, /) -> None:
        # No-op for DB-API compliance
        return None

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


# Row goes THIRD
class Row(Sequence[Any]):
    """
    sqlite3.Row-like container supporting index and name-based access.
    """

    def __new__(cls, cursor: Cursor, data: tuple[Any, ...], /) -> "Row":
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


def connect(
    database: str,
    *,
    experimental_features: Optional[str] = None,
    isolation_level: Optional[str] = "DEFERRED",
    extra_io: Optional[Callable[[], None]] = None,
) -> Connection:
    """
    Open a Turso (SQLite-compatible) database and return a Connection.

    Parameters:
    - database: path or identifier of the database.
    - experimental_features: comma-separated list of features to enable.
    - isolation_level: one of "DEFERRED" (default), "IMMEDIATE", "EXCLUSIVE", or None.
    """
    try:
        cfg = PyTursoDatabaseConfig(
            path=database,
            experimental_features=experimental_features,
            async_io=False,  # Let the Rust layer drive IO internally by default
        )
        db: PyTursoDatabase = py_turso_database_open(cfg)
        conn: PyTursoConnection = db.connect()
        return Connection(conn, isolation_level=isolation_level, extra_io=extra_io)
    except Exception as exc:  # noqa: BLE001
        raise _map_turso_exception(exc)


# Make it easy to enable logging with native `logging` Python module
def setup_logging(level: Optional[int] = None) -> None:
    """
    Setup Turso logging to integrate with Python's logging module.

    Usage:
        import turso
        turso.setup_logging(logging.DEBUG)
    """
    import logging

    level = level or logging.INFO
    logger = logging.getLogger("turso")
    logger.setLevel(level)

    def _py_logger(log: PyTursoLog) -> None:
        # Map Rust/Turso log level strings to Python logging levels (best-effort)
        lvl_map = {
            "ERROR": logging.ERROR,
            "WARN": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "TRACE": logging.DEBUG,
        }
        py_level = lvl_map.get(log.level.upper(), level)
        logger.log(
            py_level,
            "%s [%s:%s] %s",
            log.target,
            log.file,
            log.line,
            log.message,
        )

    try:
        py_turso_setup(
            PyTursoSetupConfig(
                logger=_py_logger,
                log_level={
                    logging.ERROR: "error",
                    logging.WARN: "warn",
                    logging.INFO: "info",
                    logging.DEBUG: "debug",
                }[level],
            )
        )
    except Exception as exc:  # noqa: BLE001
        raise _map_turso_exception(exc)
