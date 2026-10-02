"""DB-API 2.0 Connection and Cursor backed by a SQL over HTTP session."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

from .dbapi import (
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
from .protocol import ProtocolError, build_batch_step, decode_value
from .session import Session, StmtResult, _server_error

_DBCursorT = TypeVar("_DBCursorT", bound="Cursor")


@dataclass
class BatchResult:
    """The result of one statement of a [`Connection.batch`] call, in
    DB-API vocabulary: `description` and `rows` for statements that return
    rows, `rowcount` for DML (−1 when the statement returned rows), and
    `lastrowid` for INSERT/REPLACE. `rows_read`, `rows_written`, and
    `query_duration_ms` are the server-side execution statistics
    (PROTOCOL.md section 8.4); the embedded driver reports None for them."""

    rows: list[tuple]
    description: tuple[tuple[str, None, None, None, None, None, None], ...] | None
    rowcount: int
    lastrowid: int | None
    rows_read: int | None = None
    rows_written: int | None = None
    query_duration_ms: float | None = None


# Accepted values for the `mode` argument of Connection.batch and the
# BEGIN statement each maps to.
_BATCH_MODES = {
    "deferred": "BEGIN DEFERRED",
    "immediate": "BEGIN IMMEDIATE",
    "exclusive": "BEGIN EXCLUSIVE",
    "concurrent": "BEGIN CONCURRENT",
}

_TRANSACTION_CONTROL_KEYWORDS = {
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
}


def _batch_begin_sql(mode: str | None) -> str | None:
    if mode is None:
        return None
    begin_sql = _BATCH_MODES.get(str(mode).lower())
    if begin_sql is None:
        raise ProgrammingError(
            f"batch mode must be one of {sorted(_BATCH_MODES)} or None, got {mode!r}"
        )
    return begin_sql


def _normalize_batch_statements(
    statements: Iterable[Any],
) -> list[tuple[str, Sequence[Any] | Mapping[str, Any]]]:
    """Normalize batch input to (sql, parameters) pairs. Accepts SQL
    strings and (sql, parameters) pairs, like the JavaScript driver."""
    normalized = []
    for index, statement in enumerate(statements):
        if isinstance(statement, str):
            normalized.append((statement, ()))
            continue
        if (
            isinstance(statement, (tuple, list))
            and len(statement) == 2
            and isinstance(statement[0], str)
        ):
            normalized.append((statement[0], statement[1]))
            continue
        raise ProgrammingError(
            f"batch statement {index} must be a SQL string or a (sql, parameters) pair"
        )
    return normalized


def _reject_transaction_control_statements(
    statements: list[tuple[str, Sequence[Any] | Mapping[str, Any]]],
) -> None:
    for index, (sql, _parameters) in enumerate(statements):
        if _first_keyword(sql) in _TRANSACTION_CONTROL_KEYWORDS:
            error = ProgrammingError(
                "transaction-control SQL is not allowed in a batch with a transaction mode"
            )
            raise _batch_statement_error(index, error) from error


def _batch_statement_error(
    index: int,
    error: Exception,
    results: list | None = None,
) -> Exception:
    """Wrap a statement failure so the raised exception identifies which
    statement failed, preserving the DB-API exception class. The zero-based
    index is available as the `batch_index` attribute, and `batch_results`
    is empty when validation fails before execution. Otherwise it carries
    one entry per statement: the completed statement's `BatchResult`, or
    None for the failing statement and the statements that did not run."""
    wrapped = type(error)(f"batch statement {index} failed: {error}")
    wrapped.batch_index = index
    wrapped.batch_results = results if results is not None else []
    return wrapped


def _classify_error(e: Exception) -> Exception:
    """Map a driver error onto the DB-API exception hierarchy using the
    protocol error code (PROTOCOL.md section 9.2). Error messages echo SQL
    text, so they are only matched as a fallback when the server sent no
    code."""
    msg = str(e)
    code = getattr(e, "code", None)
    if code:
        if code.startswith("SQLITE_CONSTRAINT"):
            return IntegrityError(msg)
        return OperationalError(msg)
    lower = msg.lower()
    if "constraint" in lower or "unique" in lower or "primary key" in lower:
        return IntegrityError(msg)
    return OperationalError(msg)


class Connection:
    """DB-API 2.0 Connection backed by a remote SQL over HTTP session."""

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
        isolation_level: str | None = "DEFERRED",
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

    def _execute_stmt(
        self,
        sql: str,
        params: list | None = None,
        named_params: list[tuple[str, Any]] | None = None,
        want_rows: bool = True,
    ) -> StmtResult:
        self._ensure_open()
        try:
            return self._session.execute_stmt(
                sql, args=params, named_args=named_params, want_rows=want_rows
            )
        except RuntimeError as e:
            raise _classify_error(e) from None

    @property
    def in_transaction(self) -> bool:
        """Whether an explicit transaction is open, from the server's answer
        as of the most recently completed statement."""
        return not self._session.autocommit

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
            # Closing the stream rolls back any open transaction server-side,
            # matching sqlite3: uncommitted changes are lost on close.
            self._session.close()
        finally:
            self._closed = True

    def commit(self) -> None:
        self._ensure_open()
        if self.in_transaction:
            self._execute_stmt("COMMIT", want_rows=False)

    def rollback(self) -> None:
        self._ensure_open()
        if self.in_transaction:
            self._execute_stmt("ROLLBACK", want_rows=False)

    def cursor(self, factory: Callable[[Connection], _DBCursorT] | None = None) -> _DBCursorT | Cursor:
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

    def batch(
        self,
        statements: Iterable[str | tuple[str, Sequence[Any] | Mapping[str, Any]]],
        mode: str | None = None,
    ) -> list[BatchResult]:
        """Execute multiple parameterized statements in a single HTTP
        request.

        The statements — SQL strings or (sql, parameters) pairs, with the
        same parameter forms as `execute()` — are sent as one batch
        request (PROTOCOL.md section 6.2) and execute in order. Execution
        stops at the first statement that fails: the remaining statements
        are skipped and the raised exception identifies the failing
        statement by its zero-based index (the `batch_index` attribute)
        and carries the per-statement results in the `batch_results`
        attribute. It is empty when parameter validation fails before
        execution; otherwise it has one entry per statement — the completed
        statement's `BatchResult`, or None for the failing statement and
        the statements that did not run.

        With `mode` set to "deferred", "immediate", "exclusive", or
        "concurrent", the statements are wrapped in `BEGIN <mode>` /
        `COMMIT`, with a `ROLLBACK` on failure, all carried by the same
        request: either every statement commits or none does. The
        statements must not contain their own transaction-control SQL in
        that case. If `ROLLBACK` itself fails, the primary exception has a
        `rollback_error` attribute and the stream's transaction state is
        unknown. With `mode` unset the batch is not transactional: each
        statement commits as it executes.

        Unlike `execute()`, `batch()` never opens a legacy implicit
        transaction. If a transaction is already open on this connection,
        the statements join it (and `mode` is ignored).

        Returns one `BatchResult` per statement, in order.
        """
        self._ensure_open()
        begin_sql = _batch_begin_sql(mode)
        stmts = _normalize_batch_statements(statements)
        if not stmts:
            return []
        if self.in_transaction:
            begin_sql = None

        steps, offset, commit_index = self._build_batch_steps(stmts, begin_sql)
        try:
            result = self._session.execute_batch(steps)
        except RuntimeError as e:
            raise _classify_error(e) from None
        return self._decode_batch_result(result, stmts, offset, commit_index, len(steps))

    @staticmethod
    def _build_batch_steps(
        stmts: list[tuple[str, Any]],
        begin_sql: str | None,
    ) -> tuple[list[dict], int, int | None]:
        """Build the wire-level steps of a batch request: one step per
        statement, each conditional on the previous one succeeding, wrapped
        in BEGIN/COMMIT/ROLLBACK when `begin_sql` is set. Returns the steps,
        the index of the first statement step, and the index of the COMMIT
        step (None when the batch is not transactional)."""
        user_steps = []
        for i, (sql, parameters) in enumerate(stmts):
            try:
                args, named_args = Cursor._convert_params(parameters)
                user_steps.append(
                    build_batch_step(sql, args=args, named_args=named_args, want_rows=True)
                )
            except Exception as e:
                raise _batch_statement_error(i, e) from e

        if begin_sql is None:
            for i, step in enumerate(user_steps):
                if i > 0:
                    step["condition"] = {"type": "ok", "step": i - 1}
            return user_steps, 0, None

        _reject_transaction_control_statements(stmts)
        steps = [build_batch_step(begin_sql, want_rows=False)]
        for i, step in enumerate(user_steps):
            step["condition"] = {"type": "ok", "step": i}
            steps.append(step)
        commit_index = 1 + len(stmts)
        steps.append(
            build_batch_step(
                "COMMIT",
                want_rows=False,
                condition={"type": "ok", "step": commit_index - 1},
            )
        )
        # ROLLBACK runs only when BEGIN succeeded and COMMIT did not.
        # The ok(BEGIN) guard prevents it from aborting a transaction
        # opened on the stream out of band.
        steps.append(
            build_batch_step(
                "ROLLBACK",
                want_rows=False,
                condition={
                    "type": "and",
                    "conds": [
                        {"type": "ok", "step": 0},
                        {"type": "not", "cond": {"type": "ok", "step": commit_index}},
                    ],
                },
            )
        )
        return steps, 1, commit_index

    @staticmethod
    def _decode_batch_result(
        result: dict,
        stmts: list[tuple[str, Any]],
        offset: int,
        commit_index: int | None,
        total_steps: int,
    ) -> list[BatchResult]:
        """Decode a wire-level batch result into per-statement results, or
        raise the error of the step that failed. Failures of the synthetic
        BEGIN/COMMIT steps surface as-is. A ROLLBACK failure is attached to
        that primary error as `rollback_error`."""
        step_results = result.get("step_results")
        step_errors = result.get("step_errors")
        if (
            not isinstance(step_results, list)
            or not isinstance(step_errors, list)
            or len(step_results) != total_steps
            or len(step_errors) != total_steps
        ):
            raise ProtocolError(
                f"batch response does not have one result and one error per step: {result}"
            )
        # Decode the results of the statements that executed before looking
        # at the errors, so a failure can still report what completed.
        results: list[BatchResult | None] = [
            Connection._decode_batch_statement_result(step_results[offset + i], sql)
            for i, (sql, _parameters) in enumerate(stmts)
        ]
        rollback_error = None
        if commit_index is not None and step_errors[commit_index + 1] is not None:
            rollback_error = _classify_error(_server_error(step_errors[commit_index + 1]))
        try:
            Connection._raise_batch_step_error(step_errors, len(stmts), offset, commit_index, results)
        except Exception as primary_error:
            if rollback_error is not None:
                primary_error.rollback_error = rollback_error
            raise
        if rollback_error is not None:
            raise rollback_error
        for i, result in enumerate(results):
            if result is None:
                raise ProtocolError(f"batch response is missing the result for statement {i}")
        return results

    @staticmethod
    def _decode_batch_statement_result(step_result: Any, sql: str) -> BatchResult | None:
        """Decode one statement result of a batch response (section 8.4),
        or None when the statement did not complete."""
        if not isinstance(step_result, dict):
            return None
        columns = [c.get("name") or "" for c in step_result.get("cols") or []]
        rows = [tuple(decode_value(v) for v in row) for row in step_result.get("rows") or []]
        if columns:
            description = tuple((name, None, None, None, None, None, None) for name in columns)
            rowcount = -1
        else:
            description = None
            rowcount = step_result.get("affected_row_count") or 0
        lastrowid = None
        raw_rowid = step_result.get("last_insert_rowid")
        if raw_rowid is not None and _is_insert_or_replace(sql):
            try:
                lastrowid = int(raw_rowid)
            except (TypeError, ValueError) as e:
                raise ProtocolError(f"invalid rowid in server response: {e}") from None
        return BatchResult(
            rows=rows,
            description=description,
            rowcount=rowcount,
            lastrowid=lastrowid,
            rows_read=step_result.get("rows_read"),
            rows_written=step_result.get("rows_written"),
            query_duration_ms=step_result.get("query_duration_ms"),
        )

    @staticmethod
    def _raise_batch_step_error(
        step_errors: list,
        statement_count: int,
        offset: int,
        commit_index: int | None,
        results: list,
    ) -> None:
        """Raise the error of the batch step that failed, if any: the
        synthetic BEGIN first, then the user statements (with their index
        and the per-statement results), then the synthetic COMMIT."""
        if offset and step_errors[0] is not None:
            raise _classify_error(_server_error(step_errors[0]))
        for i in range(statement_count):
            error = step_errors[offset + i]
            if error is not None:
                raise _batch_statement_error(i, _classify_error(_server_error(error)), results)
        if commit_index is not None and step_errors[commit_index] is not None:
            raise _classify_error(_server_error(step_errors[commit_index]))

    def _maybe_implicit_begin(self, sql: str) -> None:
        """Legacy implicit transaction behavior."""
        if self.isolation_level is not None and not self.in_transaction and _is_dml(sql):
            level = self.isolation_level or "DEFERRED"
            self._execute_stmt(f"BEGIN {level}", want_rows=False)

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
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
        self._description: tuple[tuple[str, None, None, None, None, None, None], ...] | None = None
        self._lastrowid: int | None = None
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
    ) -> tuple[list | None, list[tuple[str, Any]] | None]:
        """Convert DB-API parameters to protocol args/named_args."""
        if isinstance(parameters, Mapping):
            named = []
            for key, val in parameters.items():
                # Try :name, $name, @name prefixes
                if isinstance(key, str) and not key.startswith((":", "$", "@")):
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
        result = self._connection._execute_stmt(
            sql, params=args, named_params=named_args, want_rows=True,
        )

        if result.columns:
            self._description = tuple(
                (name, None, None, None, None, None, None) for name in result.columns
            )
            self._rows = result.rows
            self._rowcount = -1
        else:
            self._description = None
            self._rows = []
            self._rowcount = result.affected_rows

        if result.last_insert_rowid is not None and _is_insert_or_replace(sql):
            self._lastrowid = result.last_insert_rowid

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
            result = self._connection._execute_stmt(
                sql, params=args, named_params=named_args, want_rows=False,
            )
            total += result.affected_rows

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
            self._connection._execute_stmt("COMMIT", want_rows=False)

        try:
            results = self._connection._session.execute_pipeline(
                [{"type": "sequence", "sql": sql_script}]
            )
        except RuntimeError as e:
            raise _classify_error(e) from None

        result = results[0]
        if result.get("type") == "error":
            raise _classify_error(_server_error(result.get("error")))

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

    def fetchmany(self, size: int | None = None) -> list[Any]:
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
    auth_token: str | None = None,
    remote_encryption_key: str | None = None,
    isolation_level: str | None = "DEFERRED",
) -> Connection:
    """Open a remote connection to a Turso database.

    Parameters:
    - url: Database URL (turso://, https://, http://, or libsql://)
    - auth_token: Authentication token
    - remote_encryption_key: base64-encoded key for a database encrypted
      with a customer-managed key
    - isolation_level: Transaction isolation level (default: DEFERRED)
    """
    session = Session(url, auth_token=auth_token, remote_encryption_key=remote_encryption_key)
    return Connection(session, isolation_level=isolation_level)
