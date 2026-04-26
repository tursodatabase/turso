"""DB-API 2.0 integration tests for turso_serverless against a real libsql-server.

Requires TURSO_DATABASE_URL env var (default: http://localhost:8080).
Start a server with:
    docker run -d -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
"""

from __future__ import annotations

import os

import pytest

# Skip all tests if no server URL is configured and we're not in CI
SERVER_URL = os.environ.get("TURSO_DATABASE_URL", "http://localhost:8080")
AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

# Try to import; these are pure Python so no build needed
turso_serverless = pytest.importorskip("turso_serverless")

from turso_serverless import connect  # noqa: E402


def make_conn():
    kwargs = {}
    if AUTH_TOKEN:
        kwargs["auth_token"] = AUTH_TOKEN
    return connect(SERVER_URL, **kwargs)


def try_connect():
    """Check if the server is reachable."""
    try:
        conn = make_conn()
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not try_connect(),
    reason=f"libsql-server not reachable at {SERVER_URL}",
)


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


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
        with pytest.raises(Exception):
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
        cur = conn.execute("DELETE FROM t_del_rc WHERE a >= 3")
        assert cur.rowcount == 3
        conn.close()


# ---------------------------------------------------------------------------
# Value roundtrip
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------


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
        with pytest.raises(Exception):
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
# Transaction
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_nonexistent_table(self):
        conn = make_conn()
        with pytest.raises(Exception):
            conn.execute("SELECT * FROM nonexistent_table_py")
        conn.close()

    def test_recovery_after_error(self):
        conn = make_conn()
        with pytest.raises(Exception):
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
        from turso_serverless import IntegrityError

        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t_pk_err_py VALUES (1, 'duplicate')")
        conn.close()

    def test_unique_constraint(self):
        conn = make_conn()
        conn.execute("DROP TABLE IF EXISTS t_uq_err_py")
        conn.execute("CREATE TABLE t_uq_err_py (id INTEGER, name TEXT UNIQUE)")
        conn.execute("INSERT INTO t_uq_err_py VALUES (1, 'unique_name')")
        from turso_serverless import IntegrityError

        with pytest.raises(IntegrityError):
            conn.execute("INSERT INTO t_uq_err_py VALUES (2, 'unique_name')")
        conn.close()


# ---------------------------------------------------------------------------
# DB-API compliance
# ---------------------------------------------------------------------------


class TestDBAPICompliance:
    def test_module_attributes(self):
        assert turso_serverless.apilevel == "2.0"
        assert turso_serverless.paramstyle == "qmark"
        assert turso_serverless.threadsafety == 1

    def test_exception_hierarchy(self):
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


class TestClose:
    def test_close_connection(self):
        conn = make_conn()
        conn.execute("SELECT 1")
        conn.close()
        # Calling close again should not error
        conn.close()


# ---------------------------------------------------------------------------
# URL normalization (no server needed)
# ---------------------------------------------------------------------------


class TestNormalizeUrl:
    """Unit tests for turso_serverless.session.normalize_url."""

    def test_libsql_scheme(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("libsql://my-db.turso.io") == "https://my-db.turso.io"

    def test_turso_scheme(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("turso://my-db.turso.io") == "https://my-db.turso.io"

    def test_https_passthrough(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("https://my-db.turso.io") == "https://my-db.turso.io"

    def test_http_passthrough(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("http://localhost:8080") == "http://localhost:8080"

    def test_turso_with_port(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("turso://my-db.turso.io:443") == "https://my-db.turso.io:443"

    def test_libsql_with_port(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("libsql://my-db.turso.io:8080") == "https://my-db.turso.io:8080"

    def test_with_path(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("turso://my-db.turso.io/v1/db") == "https://my-db.turso.io/v1/db"

    def test_with_query_params(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("libsql://my-db.turso.io?foo=bar") == "https://my-db.turso.io?foo=bar"

    def test_ws_passthrough(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("ws://localhost:8080") == "ws://localhost:8080"

    def test_wss_passthrough(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("wss://my-db.turso.io") == "wss://my-db.turso.io"

    def test_empty_string(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("") == ""

    def test_libsql_with_path_and_query(self):
        from turso_serverless.session import normalize_url

        assert normalize_url("libsql://my-db.turso.io/db?timeout=30") == "https://my-db.turso.io/db?timeout=30"
