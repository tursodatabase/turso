"""DB-API 2.0 integration tests for turso_serverless against a live server.

Configure with TURSO_DATABASE_URL and (optionally) TURSO_AUTH_TOKEN; the
tests that need a server skip themselves when TURSO_DATABASE_URL is unset.
"""

from __future__ import annotations

import http.client
import math
import os
import urllib.request

import pytest
from turso_serverless import IntegrityError, OperationalError, ProgrammingError, connect
from turso_serverless.connection import Connection, _classify_error
from turso_serverless.protocol import ProtocolError, ServerError, decode_value, encode_value
from turso_serverless.session import Session, _server_error, normalize_url

SERVER_URL = os.environ.get("TURSO_DATABASE_URL")
AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

needs_server = pytest.mark.skipif(
    SERVER_URL is None,
    reason="TURSO_DATABASE_URL is not set",
)


def make_conn():
    kwargs = {}
    if AUTH_TOKEN:
        kwargs["auth_token"] = AUTH_TOKEN
    return connect(SERVER_URL, **kwargs)


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


@needs_server
class TestQueryExecution:
    def test_single_value(self):
        conn = make_conn()
        cur = conn.execute("SELECT 42")
        assert cur.fetchone() == (42,)
        conn.close()

    def test_single_row(self):
        conn = make_conn()
        cur = conn.execute("SELECT 1 AS one, 'two' AS two, 0.5 AS three")
        assert cur.description is not None
        names = [d[0] for d in cur.description]
        assert names == ["one", "two", "three"]
        row = cur.fetchone()
        assert row == (1, "two", 0.5)
        conn.close()

    def test_multiple_rows(self):
        conn = make_conn()
        cur = conn.execute("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "one")
        assert rows[1] == (2, "two")
        assert rows[2] == (3, "three")
        conn.close()

    def test_error_on_invalid_sql(self):
        conn = make_conn()
        with pytest.raises(OperationalError):
            conn.execute("SELECT foobar")
        conn.close()

    def test_insert_returning(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_ret_py")
        conn.execute("CREATE TABLE t_ret_py (a)")
        cur = conn.execute("INSERT INTO t_ret_py VALUES (1) RETURNING 42 AS x, 'foo' AS y")
        assert cur.description is not None
        names = [d[0] for d in cur.description]
        assert names == ["x", "y"]
        row = cur.fetchone()
        assert row == (42, "foo")
        conn.close()


# ---------------------------------------------------------------------------
# Rows affected
# ---------------------------------------------------------------------------


@needs_server
class TestRowsAffected:
    def test_insert_rowcount(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_ins_rc")
        conn.execute("CREATE TABLE t_ins_rc (a)")
        cur = conn.execute("INSERT INTO t_ins_rc VALUES (1), (2)")
        assert cur.rowcount == 2
        conn.close()

    def test_delete_rowcount(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_del_rc")
        conn.execute("CREATE TABLE t_del_rc (a)")
        conn.execute("INSERT INTO t_del_rc VALUES (1), (2), (3), (4), (5)")
        conn.commit()
        cur = conn.execute("DELETE FROM t_del_rc WHERE a >= 3")
        assert cur.rowcount == 3
        conn.close()

    def test_lastrowid(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_rowid_py")
        conn.execute("CREATE TABLE t_rowid_py (id INTEGER PRIMARY KEY, a)")
        cur = conn.execute("INSERT INTO t_rowid_py VALUES (7, 'x')")
        assert cur.lastrowid == 7
        conn.close()


# ---------------------------------------------------------------------------
# Value roundtrip
# ---------------------------------------------------------------------------


@needs_server
class TestValueRoundtrip:
    def test_string(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", ("boomerang",))
        assert cur.fetchone() == ("boomerang",)
        conn.close()

    def test_unicode(self):
        conn = make_conn()
        text = "žluťoučký kůň úpěl ďábelské ódy"
        cur = conn.execute("SELECT ?", (text,))
        assert cur.fetchone() == (text,)
        conn.close()

    def test_integer(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (-2023,))
        assert cur.fetchone() == (-2023,)
        conn.close()

    def test_large_integer(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (2**62,))
        assert cur.fetchone() == (2**62,)
        conn.close()

    def test_float(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (12.345,))
        assert cur.fetchone() == (12.345,)
        conn.close()

    def test_null(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (None,))
        assert cur.fetchone() == (None,)
        conn.close()

    def test_bool_true(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (True,))
        # SQLite stores bools as integers
        assert cur.fetchone() == (1,)
        conn.close()

    def test_bool_false(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (False,))
        assert cur.fetchone() == (0,)
        conn.close()

    def test_blob(self):
        conn = make_conn()
        blob = bytes(range(256))
        cur = conn.execute("SELECT ?", (blob,))
        assert cur.fetchone() == (blob,)
        conn.close()

    def test_nan_binds_as_null(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?", (float("nan"),))
        assert cur.fetchone() == (None,)
        conn.close()

    def test_non_finite_result_decodes_as_nan(self):
        conn = make_conn()
        cur = conn.execute("SELECT 1e308 * 10")
        (value,) = cur.fetchone()
        assert math.isnan(value)
        conn.close()


# ---------------------------------------------------------------------------
# Non-finite floats (no server needed)
# ---------------------------------------------------------------------------


class TestNonFiniteFloatEncoding:
    def test_nan_encodes_as_null(self):
        assert encode_value(float("nan")) == {"type": "null"}

    def test_infinity_rejected(self):
        with pytest.raises(ValueError):
            encode_value(float("inf"))
        with pytest.raises(ValueError):
            encode_value(float("-inf"))

    def test_finite_float_unchanged(self):
        assert encode_value(12.345) == {"type": "float", "value": 12.345}

    def test_null_float_decodes_as_nan(self):
        assert math.isnan(decode_value({"type": "float", "value": None}))


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------


@needs_server
class TestParameters:
    def test_positional(self):
        conn = make_conn()
        cur = conn.execute("SELECT ?, ?", ("one", "two"))
        assert cur.fetchone() == ("one", "two")
        conn.close()

    def test_named(self):
        conn = make_conn()
        cur = conn.execute("SELECT :a, :b", {"a": "one", "b": "two"})
        assert cur.fetchone() == ("one", "two")
        conn.close()


# ---------------------------------------------------------------------------
# executescript
# ---------------------------------------------------------------------------


@needs_server
class TestExecuteScript:
    def test_multiple_statements(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batch_py")
        cur = conn.cursor()
        cur.executescript(
            "CREATE TABLE t_batch_py (a);"
            "INSERT INTO t_batch_py VALUES (1), (2), (4), (8);"
        )
        cur2 = conn.execute("SELECT SUM(a) FROM t_batch_py")
        assert cur2.fetchone() == (15,)
        conn.close()

    def test_error_stops_execution(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batch_err_py")
        cur = conn.cursor()
        with pytest.raises(OperationalError):
            cur.executescript(
                "CREATE TABLE t_batch_err_py (a);"
                "INSERT INTO t_batch_err_py VALUES (1), (2), (4);"
                "INSERT INTO t_batch_err_py VALUES (foo());"
                "INSERT INTO t_batch_err_py VALUES (8), (16);"
            )
        conn.close()

    def test_manual_transaction(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batch_tx_py")
        cur = conn.cursor()
        cur.executescript(
            "CREATE TABLE t_batch_tx_py (a);"
            "BEGIN;"
            "INSERT INTO t_batch_tx_py VALUES (1), (2), (4);"
            "INSERT INTO t_batch_tx_py VALUES (8), (16);"
            "COMMIT;"
        )
        cur2 = conn.execute("SELECT SUM(a) FROM t_batch_tx_py")
        assert cur2.fetchone() == (31,)
        conn.close()


# ---------------------------------------------------------------------------
# Parameterized batches
# ---------------------------------------------------------------------------


class TestBatchValidation:
    """Input validation happens before any HTTP request, so these tests
    need no server."""

    def _offline_conn(self):
        return connect("http://localhost:0")

    def test_empty_batch_returns_no_results(self):
        conn = self._offline_conn()
        assert conn.batch([]) == []
        assert conn.batch([], mode="immediate") == []

    def test_invalid_mode_is_rejected(self):
        conn = self._offline_conn()
        with pytest.raises(ProgrammingError, match="batch mode"):
            conn.batch(["SELECT 1"], mode="bogus")

    def test_invalid_statement_is_rejected_with_index(self):
        conn = self._offline_conn()
        with pytest.raises(ProgrammingError, match="batch statement 1"):
            conn.batch(["SELECT 1", 42])

    @pytest.mark.parametrize("mode", [None, "immediate"])
    def test_every_parameter_is_validated_before_the_request(self, monkeypatch, mode):
        conn = self._offline_conn()

        def fail_if_requested(_steps):
            pytest.fail("parameter validation must finish before the request")

        monkeypatch.setattr(conn._session, "execute_batch", fail_if_requested)
        with pytest.raises(TypeError, match="batch statement 1 failed") as excinfo:
            conn.batch(
                [
                    ("INSERT INTO t VALUES (?)", (1,)),
                    ("INSERT INTO t VALUES (?)", (object(),)),
                ],
                mode=mode,
            )
        assert excinfo.value.batch_index == 1
        assert excinfo.value.batch_results == []

    @pytest.mark.parametrize(
        "sql",
        [
            "BEGIN",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT batch_savepoint",
            "RELEASE batch_savepoint",
            "; /* empty statement */ COMMIT",
            "\ufeffCOMMIT",
        ],
    )
    def test_transaction_control_is_rejected_before_the_request(self, monkeypatch, sql):
        conn = self._offline_conn()

        def fail_if_requested(_steps):
            pytest.fail("transaction-control SQL must be rejected before the request")

        monkeypatch.setattr(conn._session, "execute_batch", fail_if_requested)
        with pytest.raises(
            ProgrammingError, match="transaction-control SQL is not allowed"
        ) as excinfo:
            conn.batch([sql], mode="immediate")
        assert excinfo.value.batch_index == 0
        assert excinfo.value.batch_results == []

    def test_rollback_failure_is_attached_to_the_statement_error(self):
        response = {
            "step_results": [{}, None, None, None],
            "step_errors": [
                None,
                {"message": "statement failed", "code": "SQLITE_ERROR"},
                None,
                {"message": "rollback failed", "code": "SQLITE_ERROR"},
            ],
        }
        with pytest.raises(OperationalError, match="batch statement 0 failed") as excinfo:
            Connection._decode_batch_result(
                response,
                [("INSERT INTO t VALUES (?)", (1,))],
                offset=1,
                commit_index=2,
                total_steps=4,
            )
        assert excinfo.value.batch_index == 0
        assert excinfo.value.batch_results == [None]
        assert isinstance(excinfo.value.rollback_error, OperationalError)
        assert str(excinfo.value.rollback_error) == "rollback failed"


@needs_server
class TestBatch:
    def test_batch_executes_parameterized_statements_in_order(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batch_py")
        results = conn.batch(
            [
                "CREATE TABLE t_batch_py (id INTEGER PRIMARY KEY, name TEXT)",
                ("INSERT INTO t_batch_py (name) VALUES (?)", ("Alice",)),
                ("INSERT INTO t_batch_py (name) VALUES (:name)", {"name": "Bob"}),
                "SELECT name FROM t_batch_py ORDER BY id",
            ]
        )
        assert len(results) == 4
        assert results[1].rowcount == 1
        assert results[1].lastrowid == 1
        assert results[2].lastrowid == 2
        select = results[3]
        assert select.description is not None
        assert select.description[0][0] == "name"
        assert select.rows == [("Alice",), ("Bob",)]
        assert select.rowcount == -1
        # Server-side execution statistics are reported per statement
        # (PROTOCOL.md section 8.4).
        for result in results:
            assert isinstance(result.rows_read, int)
            assert isinstance(result.rows_written, int)
            assert isinstance(result.query_duration_ms, (int, float))
        assert results[1].rows_written >= 1
        conn.close()

    def test_batch_error_identifies_the_failing_statement(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batcherr_py")
        conn.execute("CREATE TABLE t_batcherr_py (x)")
        with pytest.raises(OperationalError, match="batch statement 1 failed") as excinfo:
            conn.batch(
                [
                    ("INSERT INTO t_batcherr_py VALUES (?)", (1,)),
                    ("INSERT INTO no_such_table_py VALUES (?)", (2,)),
                    ("INSERT INTO t_batcherr_py VALUES (?)", (3,)),
                ]
            )
        assert excinfo.value.batch_index == 1
        # One entry per statement: the completed first statement's result,
        # None for the failing and skipped ones.
        partial = excinfo.value.batch_results
        assert len(partial) == 3
        assert partial[0].rowcount == 1
        assert partial[1] is None
        assert partial[2] is None
        # The batch is not transactional: the statement before the failing
        # one keeps its effect, and the one after it never ran.
        cur = conn.execute("SELECT COUNT(*) FROM t_batcherr_py")
        assert cur.fetchone() == (1,)
        conn.close()

    def test_batch_joins_an_open_transaction(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batchtx_py")
        conn.execute("CREATE TABLE t_batchtx_py (x)")
        conn.execute("BEGIN")
        conn.batch(
            [
                ("INSERT INTO t_batchtx_py VALUES (?)", (1,)),
                ("INSERT INTO t_batchtx_py VALUES (?)", (2,)),
            ]
        )
        assert conn.in_transaction
        conn.rollback()
        cur = conn.execute("SELECT COUNT(*) FROM t_batchtx_py")
        assert cur.fetchone() == (0,)
        conn.close()

    def test_batch_mode_commits_atomically(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_tbatch_py")
        conn.execute("CREATE TABLE t_tbatch_py (x)")
        results = conn.batch(
            [
                ("INSERT INTO t_tbatch_py VALUES (?)", (1,)),
                ("INSERT INTO t_tbatch_py VALUES (?)", (2,)),
            ],
            mode="immediate",
        )
        assert len(results) == 2
        assert not conn.in_transaction
        cur = conn.execute("SELECT COUNT(*) FROM t_tbatch_py")
        assert cur.fetchone() == (2,)
        conn.close()

    def test_batch_mode_rolls_back_on_failure(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_tbatcherr_py")
        conn.execute("CREATE TABLE t_tbatcherr_py (x)")
        with pytest.raises(OperationalError) as excinfo:
            conn.batch(
                [
                    ("INSERT INTO t_tbatcherr_py VALUES (?)", (1,)),
                    ("INSERT INTO no_such_table_py VALUES (?)", (2,)),
                ],
                mode="immediate",
            )
        assert excinfo.value.batch_index == 1
        assert not conn.in_transaction
        # The ROLLBACK step undid the first insert.
        cur = conn.execute("SELECT COUNT(*) FROM t_tbatcherr_py")
        assert cur.fetchone() == (0,)
        conn.close()

    def test_batch_constraint_error_preserves_exception_class(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_batchuniq_py")
        conn.execute("CREATE TABLE t_batchuniq_py (x UNIQUE)")
        with pytest.raises(IntegrityError) as excinfo:
            conn.batch(
                [
                    ("INSERT INTO t_batchuniq_py VALUES (?)", (1,)),
                    ("INSERT INTO t_batchuniq_py VALUES (?)", (1,)),
                ]
            )
        assert excinfo.value.batch_index == 1
        conn.close()


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


@needs_server
class TestTransaction:
    def test_commit(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_tx_commit_py")
        conn.execute("CREATE TABLE t_tx_commit_py (a)")
        conn.execute("BEGIN")
        conn.execute("INSERT INTO t_tx_commit_py VALUES ('one')")
        conn.execute("INSERT INTO t_tx_commit_py VALUES ('two')")
        conn.commit()
        cur = conn.execute("SELECT COUNT(*) FROM t_tx_commit_py")
        assert cur.fetchone() == (2,)
        conn.close()

    def test_rollback(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_tx_rb_py")
        conn.execute("CREATE TABLE t_tx_rb_py (a)")
        conn.execute("BEGIN")
        conn.execute("INSERT INTO t_tx_rb_py VALUES ('one')")
        conn.rollback()
        cur = conn.execute("SELECT COUNT(*) FROM t_tx_rb_py")
        assert cur.fetchone() == (0,)
        conn.close()

    def test_in_transaction_tracks_server_state(self):
        conn = make_conn()
        assert conn.in_transaction is False
        conn.execute("BEGIN")
        assert conn.in_transaction is True
        conn.execute("SELECT 1")
        assert conn.in_transaction is True
        conn.commit()
        assert conn.in_transaction is False
        conn.close()

    def test_in_transaction_survives_statement_error(self):
        conn = make_conn()
        conn.execute("BEGIN")
        with pytest.raises(OperationalError):
            conn.execute("SELECT foobar")
        assert conn.in_transaction is True
        conn.rollback()
        assert conn.in_transaction is False
        conn.close()

    def test_implicit_transaction_on_dml(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_tx_impl_py")
        conn.execute("CREATE TABLE t_tx_impl_py (a)")
        conn.execute("INSERT INTO t_tx_impl_py VALUES (1)")
        assert conn.in_transaction is True
        conn.commit()
        assert conn.in_transaction is False
        cur = conn.execute("SELECT COUNT(*) FROM t_tx_impl_py")
        assert cur.fetchone() == (1,)
        conn.close()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@needs_server
class TestErrorHandling:
    def test_nonexistent_table(self):
        conn = make_conn()
        with pytest.raises(OperationalError):
            conn.execute("SELECT * FROM nonexistent_table_py")
        conn.close()

    def test_recovery_after_error(self):
        conn = make_conn()
        with pytest.raises(OperationalError):
            conn.execute("SELECT foobar")
        # Connection should still be usable
        cur = conn.execute("SELECT 42")
        assert cur.fetchone() == (42,)
        conn.close()

    def test_pk_constraint(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_pk_err_py")
        conn.execute("CREATE TABLE t_pk_err_py (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO t_pk_err_py VALUES (1, 'first')")
        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t_pk_err_py VALUES (1, 'duplicate')")
        conn.close()

    def test_unique_constraint(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_uq_err_py")
        conn.execute("CREATE TABLE t_uq_err_py (id INTEGER, name TEXT UNIQUE)")
        conn.execute("INSERT INTO t_uq_err_py VALUES (1, 'unique_name')")
        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t_uq_err_py VALUES (2, 'unique_name')")
        conn.close()


# ---------------------------------------------------------------------------
# DB-API compliance
# ---------------------------------------------------------------------------


class TestDBAPICompliance:
    def test_module_attributes(self):
        import turso_serverless

        assert turso_serverless.apilevel == "2.0"
        assert turso_serverless.paramstyle == "qmark"
        assert turso_serverless.threadsafety == 1

    def test_exception_hierarchy(self):
        import turso_serverless

        assert issubclass(turso_serverless.Warning, Exception)
        assert issubclass(turso_serverless.Error, Exception)
        assert issubclass(turso_serverless.InterfaceError, turso_serverless.Error)
        assert issubclass(turso_serverless.DatabaseError, turso_serverless.Error)
        assert issubclass(turso_serverless.DataError, turso_serverless.DatabaseError)
        assert issubclass(turso_serverless.OperationalError, turso_serverless.DatabaseError)
        assert issubclass(turso_serverless.IntegrityError, turso_serverless.DatabaseError)
        assert issubclass(turso_serverless.InternalError, turso_serverless.DatabaseError)
        assert issubclass(turso_serverless.ProgrammingError, turso_serverless.DatabaseError)
        assert issubclass(turso_serverless.NotSupportedError, turso_serverless.DatabaseError)


@needs_server
class TestDBAPICursor:
    def test_cursor_description(self):
        conn = make_conn()
        cur = conn.execute("SELECT 1 AS a, 2 AS b")
        assert cur.description is not None
        assert len(cur.description) == 2
        assert cur.description[0][0] == "a"
        assert cur.description[1][0] == "b"
        # Remaining fields are None per DB-API spec
        for desc in cur.description:
            assert all(d is None for d in desc[1:])
        conn.close()

    def test_fetchone_fetchall(self):
        conn = make_conn()
        cur = conn.execute("VALUES (1), (2), (3)")
        assert cur.fetchone() == (1,)
        rest = cur.fetchall()
        assert rest == [(2,), (3,)]
        assert cur.fetchone() is None
        conn.close()

    def test_fetchmany(self):
        conn = make_conn()
        cur = conn.execute("VALUES (1), (2), (3), (4), (5)")
        batch = cur.fetchmany(3)
        assert len(batch) == 3
        rest = cur.fetchall()
        assert len(rest) == 2
        conn.close()

    def test_context_manager(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_ctx_py")
        conn.execute("CREATE TABLE t_ctx_py (a)")
        with conn:
            conn.execute("INSERT INTO t_ctx_py VALUES (1)")
        # After __exit__ without error, should be committed
        cur = conn.execute("SELECT * FROM t_ctx_py")
        assert cur.fetchone() == (1,)
        conn.close()


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------


@needs_server
class TestClose:
    def test_close_connection(self):
        conn = make_conn()
        conn.execute("SELECT 1")
        conn.close()
        # Calling close again should not error
        conn.close()

    def test_close_rolls_back_open_transaction(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_close_tx_py")
        conn.execute("CREATE TABLE t_close_tx_py (a)")
        conn.commit()
        conn.execute("BEGIN")
        conn.execute("INSERT INTO t_close_tx_py VALUES (1)")
        conn.close()
        conn2 = make_conn()
        cur = conn2.execute("SELECT COUNT(*) FROM t_close_tx_py")
        assert cur.fetchone() == (0,)
        conn2.close()


# ---------------------------------------------------------------------------
# DB-API error classification (no server needed)
# ---------------------------------------------------------------------------


class TestErrorClassification:
    def test_constraint_code(self):
        e = ServerError("UNIQUE constraint failed: t.x", code="SQLITE_CONSTRAINT")
        assert isinstance(_classify_error(e), IntegrityError)

    def test_extended_constraint_code(self):
        e = ServerError(
            "UNIQUE constraint failed: t.x",
            code="SQLITE_CONSTRAINT_UNIQUE",
            extended_code="SQLITE_CONSTRAINT_UNIQUE",
        )
        assert isinstance(_classify_error(e), IntegrityError)

    def test_code_wins_over_message_text(self):
        # The message echoes SQL text containing "unique"; the parse error
        # code must prevent misclassification as IntegrityError.
        e = ServerError('near "unique": syntax error', code="SQL_PARSE_ERROR")
        assert isinstance(_classify_error(e), OperationalError)
        assert not isinstance(_classify_error(e), IntegrityError)

    def test_codeless_message_fallback(self):
        e = ServerError("UNIQUE constraint failed: t.x")
        assert isinstance(_classify_error(e), IntegrityError)

    def test_server_error_carries_codes(self):
        e = _server_error(
            {
                "message": "UNIQUE constraint failed: t.x",
                "code": "SQLITE_CONSTRAINT",
                "extended_code": "SQLITE_CONSTRAINT_UNIQUE",
            }
        )
        assert str(e) == "UNIQUE constraint failed: t.x"
        assert e.code == "SQLITE_CONSTRAINT"
        assert e.extended_code == "SQLITE_CONSTRAINT_UNIQUE"

    def test_server_error_tolerates_missing_fields(self):
        e = _server_error(None)
        assert str(e) == "unknown error"
        assert e.code is None
        assert e.extended_code is None


# ---------------------------------------------------------------------------
# Transport failures while reading the response body (no server needed)
# ---------------------------------------------------------------------------


class _FailingReadResponse:
    """Stand-in for the urlopen response whose body read fails mid-stream."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def __enter__(self) -> _FailingReadResponse:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def read(self) -> bytes:
        raise self._exc


class TestBodyReadFailure:
    @pytest.mark.parametrize(
        "exc",
        [
            http.client.IncompleteRead(b"partial"),
            ConnectionResetError(54, "Connection reset by peer"),
            TimeoutError("timed out"),
        ],
        ids=["incomplete-read", "connection-reset", "timeout"],
    )
    def test_read_failure_is_fatal_for_stream(self, monkeypatch, exc):
        session = Session("https://db.example.com")
        session._baton = "baton-1"
        session._autocommit = False
        monkeypatch.setattr(
            urllib.request, "urlopen", lambda req: _FailingReadResponse(exc)
        )
        with pytest.raises(ProtocolError):
            session.execute_pipeline([], track_autocommit=True)
        assert session._baton is None
        assert session.autocommit

    def test_read_failure_maps_to_dbapi_error(self, monkeypatch):
        conn = Connection(Session("https://db.example.com"))
        monkeypatch.setattr(
            urllib.request,
            "urlopen",
            lambda req: _FailingReadResponse(http.client.IncompleteRead(b"partial")),
        )
        with pytest.raises(OperationalError):
            conn.execute("SELECT 1")


# ---------------------------------------------------------------------------
# URL normalization (no server needed)
# ---------------------------------------------------------------------------


class TestNormalizeUrl:
    def test_libsql_scheme(self):
        assert normalize_url("libsql://my-db.turso.io") == "https://my-db.turso.io"

    def test_turso_scheme(self):
        assert normalize_url("turso://my-db.turso.io") == "https://my-db.turso.io"

    def test_https_passthrough(self):
        assert normalize_url("https://my-db.turso.io") == "https://my-db.turso.io"

    def test_http_passthrough(self):
        assert normalize_url("http://localhost:8080") == "http://localhost:8080"

    def test_trailing_slash_stripped(self):
        assert normalize_url("https://my-db.turso.io/") == "https://my-db.turso.io"
        assert normalize_url("libsql://my-db.turso.io/") == "https://my-db.turso.io"
        assert normalize_url("turso://my-db.turso.io/") == "https://my-db.turso.io"
        assert normalize_url("http://localhost:8080//") == "http://localhost:8080"

    def test_turso_with_port(self):
        assert normalize_url("turso://my-db.turso.io:443") == "https://my-db.turso.io:443"

    def test_libsql_with_port(self):
        assert normalize_url("libsql://my-db.turso.io:8080") == "https://my-db.turso.io:8080"

    def test_with_path(self):
        assert normalize_url("turso://my-db.turso.io/v1/db") == "https://my-db.turso.io/v1/db"

    def test_with_query_params(self):
        assert normalize_url("libsql://my-db.turso.io?foo=bar") == "https://my-db.turso.io?foo=bar"
