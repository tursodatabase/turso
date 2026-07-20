"""DB-API 2.0 Connection and Cursor backed by hrana v3 HTTP session.

Re-uses Row, exception hierarchy, and helpers from _dbapi_common.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
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
    _is_dml,
    _is_insert_or_replace,
)
from .protocol import build_batch_step, decode_value
from .session import Session, normalize_batch_mode


class PreparedStatement:
    """A lightweight prepared statement returned by ``Connection.__call__``.

    The serverless transport re-sends the SQL text on each execution, so this
    just carries the SQL to be run by ``Cursor.execute``/``Connection.execute``
    (mirroring the shortcut shape of the embedded driver's ``__call__``).
    """

    __slots__ = ("sql",)

    def __init__(self, sql: str) -> None:
        self.sql = sql


class BatchResult:
    """Per-statement result from ``Connection.batch``."""

    __slots__ = ("columns", "column_types", "rows", "rows_affected")

    def __init__(self, columns, column_types, rows, rows_affected) -> None:
        self.columns = columns
        self.column_types = column_types
        self.rows = rows
        self.rows_affected = rows_affected

    @property
    def rowcount(self) -> int:
        return self.rows_affected

    def __repr__(self) -> str:
        return (
            f"BatchResult(columns={self.columns!r}, rows={self.rows!r}, "
            f"rows_affected={self.rows_affected!r})"
        )

_DBCursorT = TypeVar("_DBCursorT", bound="Cursor")


@dataclass
class EncryptionOpts:
    """Encryption options, accepted for signature parity with the embedded
    ``turso`` driver. For the serverless transport only ``hexkey`` is used, as
    the ``x-turso-encryption-key`` header value."""

    cipher: str
    hexkey: str


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
        """Execute a single statement and return (columns, rows, affected_rows, last_rowid).

        Transaction state is tracked by the session from the server's
        authoritative autocommit answer — no SQL string-sniffing here.
        """
        self._ensure_open()
        step = build_batch_step(sql, args=params, named_args=named_params, want_rows=want_rows)

        try:
            entries = self._session.execute_cursor([step])
        except RuntimeError as e:
            raise _classify_error(str(e))

        columns: list[str] = []
        rows: list[tuple] = []
        affected_rows = 0
        last_rowid: Optional[int] = None

        for entry in entries:
            etype = entry.get("type")
            if etype == "step_begin":
                cols = entry.get("cols") or []
                columns = [c.get("name", "") for c in cols]
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

        return columns, rows, affected_rows, last_rowid

    @property
    def in_transaction(self) -> bool:
        """Whether a transaction is open, from the server's authoritative
        autocommit state (mirrors the embedded driver's ``in_transaction``)."""
        return self._session.in_transaction

    @property
    def autocommit(self) -> object | bool:
        return self._autocommit_mode

    @autocommit.setter
    def autocommit(self, val: object | bool) -> None:
        if val not in (True, False, "LEGACY"):
            raise ProgrammingError("autocommit must be True, False, or 'LEGACY'")
        self._autocommit_mode = val
        # PEP 249 mode: keep a transaction open while autocommit is False.
        if val is False:
            self._ensure_transaction_open()

    def _ensure_transaction_open(self) -> None:
        """Open a transaction if none is active (used in autocommit=False mode)."""
        if not self.in_transaction:
            level = self.isolation_level or "DEFERRED"
            self._execute_stmt(f"BEGIN {level}", want_rows=False)

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self.in_transaction:
                try:
                    self._execute_stmt("ROLLBACK", want_rows=False)
                except Exception:
                    pass
            self._session.close()
        finally:
            self._closed = True

    def commit(self) -> None:
        self._ensure_open()
        if self._autocommit_mode is True:
            return
        if self.in_transaction:
            self._execute_stmt("COMMIT", want_rows=False)
        if self._autocommit_mode is False:
            # Re-open a transaction to maintain PEP 249 behavior.
            self._ensure_transaction_open()

    def rollback(self) -> None:
        self._ensure_open()
        if self._autocommit_mode is True:
            return
        if self.in_transaction:
            self._execute_stmt("ROLLBACK", want_rows=False)
        if self._autocommit_mode is False:
            # Re-open a transaction to maintain PEP 249 behavior.
            self._ensure_transaction_open()

    @contextmanager
    def transaction(self, mode: Optional[str] = None):
        """Run a block inside a transaction, committing on success and rolling
        back on error.

        ``mode`` selects the ``BEGIN`` variant — ``"deferred"`` (default),
        ``"immediate"``, ``"exclusive"``, or ``"concurrent"`` (BEGIN CONCURRENT)
        — mirroring the JavaScript driver's ``transaction()`` modes.

        Example::

            with conn.transaction("concurrent"):
                conn.execute("INSERT INTO t VALUES (1)")
                conn.execute("INSERT INTO t VALUES (2)")
        """
        self._ensure_open()
        begin = f"BEGIN {normalize_batch_mode(mode)}" if mode else "BEGIN"
        self._execute_stmt(begin, want_rows=False)
        try:
            yield self
        except Exception:
            try:
                self._execute_stmt("ROLLBACK", want_rows=False)
            except Exception:
                pass
            raise
        else:
            self._execute_stmt("COMMIT", want_rows=False)

    def cursor(self, factory: Optional[Callable[[Connection], _DBCursorT]] = None) -> _DBCursorT | Cursor:
        self._ensure_open()
        if factory is None:
            return Cursor(self)
        return factory(self)

    def execute(self, sql: str | PreparedStatement, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> Cursor:
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

    def batch(  # noqa: C901
        self,
        statements: Iterable[Any],
        mode: Optional[str] = None,
    ) -> list[BatchResult]:
        """Execute several statements in one round trip, returning one
        :class:`BatchResult` per input statement, in order.

        Each item may be a SQL string, a ``(sql, params)`` pair, or a mapping
        with ``sql`` and optional ``args``/``params``. When ``mode`` is set
        (``"deferred"``/``"immediate"``/``"exclusive"``/``"concurrent"``/…) the
        batch runs atomically: the server wraps it in ``BEGIN <mode>`` /
        ``COMMIT`` with a ``ROLLBACK`` fallback via a condition chain, so either
        all statements commit or none do. Ported from the JS ``batch()``.
        """
        self._ensure_open()

        # Inside an outer transaction the surrounding BEGIN already opened one on
        # this stream; emitting another BEGIN would fail, so drop the mode and run
        # the statements within the existing transaction (matches the JS driver).
        if mode is not None and self.in_transaction:
            mode = None

        user_steps: list[dict] = []
        for statement in statements:
            if isinstance(statement, str):
                user_steps.append(build_batch_step(statement, want_rows=True))
                continue
            if isinstance(statement, Mapping):
                sql = statement["sql"]
                params = statement.get("args", statement.get("params", ()))
            else:
                sql = statement[0]
                params = statement[1] if len(statement) > 1 else ()
            args, named = Cursor._convert_params(params or (), sql)
            user_steps.append(build_batch_step(sql, args=args, named_args=named, want_rows=True))

        n = len(user_steps)
        if mode is None:
            steps = list(user_steps)
            first_user_idx = 0
            last_user_idx = n - 1
            rollback_idx = -1
        else:
            # Atomic batch: BEGIN <mode>, each user step gated on its
            # predecessor, COMMIT gated on the last user step, then ROLLBACK
            # gated on BEGIN having run but COMMIT not (matches JS batch()).
            begin_idx = 0
            first_user_idx = 1
            last_user_idx = n  # user steps occupy 1..n inclusive
            commit_idx = last_user_idx + 1
            rollback_idx = commit_idx + 1
            steps = [build_batch_step(f"BEGIN {normalize_batch_mode(mode)}", want_rows=False)]
            for i, ustep in enumerate(user_steps):
                s = dict(ustep)
                s["condition"] = {"type": "ok", "step": begin_idx if i == 0 else first_user_idx + i - 1}
                steps.append(s)
            steps.append({
                **build_batch_step("COMMIT", want_rows=False),
                "condition": {"type": "ok", "step": last_user_idx},
            })
            steps.append({
                **build_batch_step("ROLLBACK", want_rows=False),
                "condition": {
                    "type": "and",
                    "conds": [
                        {"type": "ok", "step": begin_idx},
                        {"type": "not", "cond": {"type": "ok", "step": commit_idx}},
                    ],
                },
            })

        try:
            entries = self._session.execute_cursor(steps)
        except RuntimeError as e:
            raise _classify_error(str(e))

        results = [BatchResult([], [], [], 0) for _ in range(n)]
        deferred_error: Optional[Exception] = None
        current_idx: Optional[int] = None
        next_non_atomic_idx = 0

        def step_to_result_idx(step: Optional[int]) -> Optional[int]:
            if mode is None:
                return step if step is not None else next_non_atomic_idx
            if step is not None and first_user_idx <= step <= last_user_idx:
                return step - first_user_idx
            return None

        for entry in entries:
            etype = entry.get("type")
            if deferred_error is not None and etype != "error":
                continue
            if etype == "step_begin":
                current_idx = step_to_result_idx(entry.get("step"))
                cols = entry.get("cols") or []
                if current_idx is not None and current_idx < n and cols:
                    results[current_idx].columns = [c.get("name", "") for c in cols]
                    results[current_idx].column_types = [c.get("decltype") or "" for c in cols]
            elif etype == "row":
                if current_idx is not None and current_idx < n:
                    raw_row = entry.get("row") or []
                    results[current_idx].rows.append(tuple(decode_value(v) for v in raw_row))
            elif etype == "step_end":
                idx = current_idx
                if idx is None and mode is None:
                    idx = next_non_atomic_idx
                if idx is not None and idx < n:
                    affected = entry.get("affected_row_count", 0) or 0
                    results[idx].rows_affected = 0 if results[idx].columns else affected
                if mode is None and idx is not None:
                    next_non_atomic_idx = idx + 1
                current_idx = None
            elif etype == "step_error":
                # Suppress errors on the synthetic ROLLBACK step — by the time
                # it runs the transaction is already undone and surfacing its
                # error would mask the real cause captured earlier.
                if deferred_error is None and entry.get("step") != rollback_idx:
                    err = entry.get("error") or {}
                    deferred_error = _classify_error(err.get("message", "Batch execution failed"))
                current_idx = None
            elif etype == "error":
                err = entry.get("error") or {}
                raise _classify_error(err.get("message", "Batch execution failed"))

        if deferred_error is not None:
            raise deferred_error

        # Apply row_factory to batch rows so batch() and execute() return the
        # same row representation (execute() applies it via the cursor). Without
        # this, a connection with a row_factory set would get factory rows from
        # execute() but raw tuples from batch().
        if self.row_factory is not None:
            cur = Cursor(self)
            for result in results:
                result.rows = [cur._apply_row_factory(row) for row in result.rows]

        return results

    def interrupt(self) -> None:
        """Abort any statement currently executing on this connection.

        Best-effort over HTTP: aborts the in-flight request so the blocked
        call raises ``OperationalError`` ("interrupted"). No-op if idle.
        Mirrors the embedded driver's ``interrupt()``.
        """
        self._session.interrupt()

    def set_query_timeout(self, milliseconds: int) -> None:
        """Set the maximum time (ms) a single request may run before it is
        interrupted (raising ``OperationalError``). ``0`` disables the timeout.
        Implemented as the HTTP request timeout. Mirrors the embedded driver.
        """
        if milliseconds < 0:
            raise ProgrammingError("query timeout must be non-negative")
        self._session.set_default_query_timeout(milliseconds or None)

    def get_query_timeout(self) -> int:
        """Return the current per-statement query timeout in ms (``0`` = disabled)."""
        ms = self._session.get_default_query_timeout()
        return int(ms) if ms else 0

    def __call__(self, sql: str) -> PreparedStatement:
        """Shortcut to prepare a single statement (embedded-driver parity)."""
        self._ensure_open()
        return PreparedStatement(sql)

    def _maybe_implicit_begin(self, sql: str) -> None:
        """Legacy implicit transaction behavior."""
        if self.isolation_level is not None and not self.in_transaction and _is_dml(sql):
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
    def _resolve_named_key(sql: str, key: Any) -> str:
        """Resolve a mapping key to its sigil-prefixed parameter name.

        Emulates sqlite3 / the embedded driver: a plain key `k` binds to
        whichever of `:k`, `@k`, `$k` appears in the SQL (or `?k` for a numeric
        key), so `execute("SELECT @a", {"a": 1})` works. Keys that already carry
        a sigil are used as-is. Falls back to `:k`.
        """
        if not isinstance(key, str):
            key = str(key)
        if key[:1] in (":", "@", "$", "?"):
            return key
        if key.isdigit() and re.search(rf"\?{key}(?![0-9])", sql):
            return f"?{key}"
        for sigil in (":", "@", "$"):
            token = f"{sigil}{key}"
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])", sql):
                return token
        return f":{key}"

    @staticmethod
    def _convert_params(
        parameters: Sequence[Any] | Mapping[str, Any],
        sql: str = "",
    ) -> tuple[Optional[list], Optional[list[tuple[str, Any]]]]:
        """Convert DB-API parameters to protocol args/named_args."""
        if isinstance(parameters, Mapping):
            # `?NNN` parameters are positional in SQLite — they cannot be bound
            # by name over the wire, so a numeric mapping key `{"1": v}` binds
            # positionally at index N-1; other keys bind as named args.
            positional: dict[int, Any] = {}
            named: list[tuple[str, Any]] = []
            for key, val in parameters.items():
                resolved = Cursor._resolve_named_key(sql, key)
                if resolved.startswith("?") and resolved[1:].isdigit():
                    positional[int(resolved[1:]) - 1] = val
                else:
                    named.append((resolved, val))
            args = None
            if positional:
                args = [positional.get(i) for i in range(max(positional) + 1)]
            return args, (named or None)
        params = list(parameters) if parameters else []
        return params if params else None, None

    def execute(self, sql: str | PreparedStatement, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> Cursor:
        self._ensure_open()
        if isinstance(sql, PreparedStatement):
            sql = sql.sql
        self._rows = []
        self._row_index = 0

        # Implicit transaction
        self._connection._maybe_implicit_begin(sql)

        args, named_args = self._convert_params(parameters, sql)
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
            args, named_args = self._convert_params(parameters, sql)
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
        if self._connection.in_transaction:
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
    database: str,
    *,
    auth_token: Optional[str] = None,
    isolation_level: Optional[str] = "DEFERRED",
    remote_encryption_key: Optional[str] = None,
    encryption: Optional["EncryptionOpts"] = None,
    request_headers: Optional[dict[str, str]] = None,
    default_query_timeout: Optional[float] = None,
    extra_io: Optional[Callable[[], None]] = None,
) -> Connection:
    """Open a remote connection to a Turso database.

    Parameters mirror the embedded ``turso.connect`` where they apply:
    - database: Database URL (turso://, https://, http://, or libsql://). Also
      accepted as the first positional arg for embedded parity.
    - auth_token: Authentication token.
    - isolation_level: Transaction isolation level (default: DEFERRED).
    - remote_encryption_key: base64 key sent as ``x-turso-encryption-key`` to
      access encrypted Turso Cloud databases.
    - encryption: accepted for signature parity with the embedded driver; its
      ``hexkey`` is used as ``remote_encryption_key`` when the latter is unset.
    - request_headers: extra HTTP headers attached to every request (passing a
      ``Host`` key raises).
    - default_query_timeout: default per-statement timeout in milliseconds.
    - extra_io: accepted for embedded parity; unused by the HTTP transport.
    """
    if remote_encryption_key is None and encryption is not None:
        remote_encryption_key = encryption.hexkey
    session = Session(
        database,
        auth_token=auth_token,
        remote_encryption_key=remote_encryption_key,
        request_headers=request_headers,
        default_query_timeout=default_query_timeout,
    )
    return Connection(session, isolation_level=isolation_level)
