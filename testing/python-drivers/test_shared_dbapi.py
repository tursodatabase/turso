"""Shared DB-API 2.0 behavioral tests run against both local and remote drivers.

Tests the common API surface that both drivers must support identically.
Provider-specific features (file ops, sync, SQLAlchemy) stay in their own
test files.

For the remote driver, set TURSO_DATABASE_URL (default http://localhost:8080).
Start a server with:
    docker run -d -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
"""

from __future__ import annotations

import os

import pytest

# ---------------------------------------------------------------------------
# Availability helpers
# ---------------------------------------------------------------------------

SERVER_URL = os.environ.get("TURSO_DATABASE_URL", "http://localhost:8080")
AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")


def _local_available():
    try:
        import turso

        conn = turso.connect(":memory:")
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


def _remote_available():
    try:
        from turso_serverless import connect

        kwargs = {}
        if AUTH_TOKEN:
            kwargs["auth_token"] = AUTH_TOKEN
        conn = connect(SERVER_URL, **kwargs)
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Parametrized connection fixture
# ---------------------------------------------------------------------------


@pytest.fixture(
    params=[
        pytest.param(
            "local",
            marks=pytest.mark.skipif(
                not _local_available(), reason="native library not available"
            ),
        ),
        pytest.param(
            "remote",
            marks=pytest.mark.skipif(
                not _remote_available(), reason="libsql-server not reachable"
            ),
        ),
    ]
)
def conn(request):
    if request.param == "local":
        import turso

        c = turso.connect(":memory:")
    else:
        from turso_serverless import connect

        kwargs = {}
        if AUTH_TOKEN:
            kwargs["auth_token"] = AUTH_TOKEN
        c = connect(SERVER_URL, **kwargs)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


class TestQueryExecution:
    def test_single_value(self, conn):
        cur = conn.execute("SELECT 42")
        assert cur.fetchone() == (42,)

    def test_single_row(self, conn):
        cur = conn.execute("SELECT 1 AS one, 'two' AS two, 0.5 AS three")
        assert cur.description is not None
        names = [d[0] for d in cur.description]
        assert names == ["one", "two", "three"]
        row = cur.fetchone()
        assert row == (1, "two", 0.5)

    def test_multiple_rows(self, conn):
        cur = conn.execute("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "one")
        assert rows[1] == (2, "two")
        assert rows[2] == (3, "three")

    def test_error_on_invalid_sql(self, conn):
        with pytest.raises(Exception):
            conn.execute("SELECT foobar")

    def test_insert_returning(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_ret_py")
        conn.execute("CREATE TABLE t_shared_ret_py (a)")
        cur = conn.execute(
            "INSERT INTO t_shared_ret_py VALUES (1) RETURNING 42 AS x, 'foo' AS y"
        )
        assert cur.description is not None
        names = [d[0] for d in cur.description]
        assert names == ["x", "y"]
        row = cur.fetchone()
        assert row == (42, "foo")


# ---------------------------------------------------------------------------
# Rows affected
# ---------------------------------------------------------------------------


class TestRowsAffected:
    def test_insert_rowcount(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_ins_rc")
        conn.execute("CREATE TABLE t_shared_ins_rc (a)")
        cur = conn.execute("INSERT INTO t_shared_ins_rc VALUES (1), (2)")
        assert cur.rowcount == 2

    def test_delete_rowcount(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_del_rc")
        conn.execute("CREATE TABLE t_shared_del_rc (a)")
        conn.execute("INSERT INTO t_shared_del_rc VALUES (1), (2), (3), (4), (5)")
        cur = conn.execute("DELETE FROM t_shared_del_rc WHERE a >= 3")
        assert cur.rowcount == 3


# ---------------------------------------------------------------------------
# Value roundtrip
# ---------------------------------------------------------------------------


class TestValueRoundtrip:
    def test_string(self, conn):
        cur = conn.execute("SELECT ?", ("boomerang",))
        assert cur.fetchone() == ("boomerang",)

    def test_unicode(self, conn):
        text = "žluťoučký kůň úpěl ďábelské ódy"
        cur = conn.execute("SELECT ?", (text,))
        assert cur.fetchone() == (text,)

    def test_integer(self, conn):
        cur = conn.execute("SELECT ?", (-2023,))
        assert cur.fetchone() == (-2023,)

    def test_float(self, conn):
        cur = conn.execute("SELECT ?", (12.345,))
        assert cur.fetchone() == (12.345,)

    def test_null(self, conn):
        cur = conn.execute("SELECT ?", (None,))
        assert cur.fetchone() == (None,)

    def test_bool_true(self, conn):
        cur = conn.execute("SELECT ?", (True,))
        # SQLite stores bools as integers
        assert cur.fetchone() == (1,)

    def test_bool_false(self, conn):
        cur = conn.execute("SELECT ?", (False,))
        assert cur.fetchone() == (0,)

    def test_blob(self, conn):
        blob = bytes(range(256))
        cur = conn.execute("SELECT ?", (blob,))
        assert cur.fetchone() == (blob,)


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------


class TestParameters:
    def test_positional(self, conn):
        cur = conn.execute("SELECT ?, ?", ("one", "two"))
        assert cur.fetchone() == ("one", "two")

    def test_named(self, conn):
        cur = conn.execute("SELECT :a, :b", {"a": "one", "b": "two"})
        assert cur.fetchone() == ("one", "two")


# ---------------------------------------------------------------------------
# Batch execution (executescript)
# ---------------------------------------------------------------------------


class TestExecuteScript:
    def test_multiple_statements(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_batch_py")
        cur = conn.cursor()
        cur.executescript(
            "CREATE TABLE t_shared_batch_py (a);"
            "INSERT INTO t_shared_batch_py VALUES (1), (2), (4), (8);"
        )
        cur2 = conn.execute("SELECT SUM(a) FROM t_shared_batch_py")
        assert cur2.fetchone() == (15,)

    def test_error_stops_execution(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_batch_err_py")
        cur = conn.cursor()
        with pytest.raises(Exception):
            cur.executescript(
                "CREATE TABLE t_shared_batch_err_py (a);"
                "INSERT INTO t_shared_batch_err_py VALUES (1), (2), (4);"
                "INSERT INTO t_shared_batch_err_py VALUES (foo());"
                "INSERT INTO t_shared_batch_err_py VALUES (8), (16);"
            )

    def test_manual_transaction(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_batch_tx_py")
        cur = conn.cursor()
        cur.executescript(
            "CREATE TABLE t_shared_batch_tx_py (a);"
            "BEGIN;"
            "INSERT INTO t_shared_batch_tx_py VALUES (1), (2), (4);"
            "INSERT INTO t_shared_batch_tx_py VALUES (8), (16);"
            "COMMIT;"
        )
        cur2 = conn.execute("SELECT SUM(a) FROM t_shared_batch_tx_py")
        assert cur2.fetchone() == (31,)


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


class TestTransaction:
    def test_commit(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_tx_commit_py")
        conn.execute("CREATE TABLE t_shared_tx_commit_py (a)")
        conn.execute("BEGIN")
        conn.execute("INSERT INTO t_shared_tx_commit_py VALUES ('one')")
        conn.execute("INSERT INTO t_shared_tx_commit_py VALUES ('two')")
        conn.commit()
        cur = conn.execute("SELECT COUNT(*) FROM t_shared_tx_commit_py")
        assert cur.fetchone() == (2,)

    def test_rollback(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_tx_rb_py")
        conn.execute("CREATE TABLE t_shared_tx_rb_py (a)")
        conn.execute("BEGIN")
        conn.execute("INSERT INTO t_shared_tx_rb_py VALUES ('one')")
        conn.rollback()
        cur = conn.execute("SELECT COUNT(*) FROM t_shared_tx_rb_py")
        assert cur.fetchone() == (0,)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_nonexistent_table(self, conn):
        with pytest.raises(Exception):
            conn.execute("SELECT * FROM nonexistent_table_shared_py")

    def test_recovery_after_error(self, conn):
        with pytest.raises(Exception):
            conn.execute("SELECT foobar")
        # Connection should still be usable
        cur = conn.execute("SELECT 42")
        assert cur.fetchone() == (42,)

    def test_pk_constraint(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_pk_err_py")
        conn.execute(
            "CREATE TABLE t_shared_pk_err_py (id INTEGER PRIMARY KEY, name TEXT)"
        )
        conn.execute("INSERT INTO t_shared_pk_err_py VALUES (1, 'first')")
        with pytest.raises(Exception):
            conn.execute("INSERT INTO t_shared_pk_err_py VALUES (1, 'duplicate')")

    def test_unique_constraint(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_uq_err_py")
        conn.execute(
            "CREATE TABLE t_shared_uq_err_py (id INTEGER, name TEXT UNIQUE)"
        )
        conn.execute("INSERT INTO t_shared_uq_err_py VALUES (1, 'unique_name')")
        with pytest.raises(Exception):
            conn.execute("INSERT INTO t_shared_uq_err_py VALUES (2, 'unique_name')")


# ---------------------------------------------------------------------------
# DB-API compliance
# ---------------------------------------------------------------------------


class TestDBAPICompliance:
    def test_cursor_description(self, conn):
        cur = conn.execute("SELECT 1 AS a, 2 AS b")
        assert cur.description is not None
        assert len(cur.description) == 2
        assert cur.description[0][0] == "a"
        assert cur.description[1][0] == "b"

    def test_fetchone_fetchall(self, conn):
        cur = conn.execute("VALUES (1), (2), (3)")
        assert cur.fetchone() == (1,)
        rest = cur.fetchall()
        assert rest == [(2,), (3,)]
        assert cur.fetchone() is None

    def test_fetchmany(self, conn):
        cur = conn.execute("VALUES (1), (2), (3), (4), (5)")
        batch = cur.fetchmany(3)
        assert len(batch) == 3
        rest = cur.fetchall()
        assert len(rest) == 2

    def test_cursor_rowcount(self, conn):
        conn.execute("DROP TABLE IF EXISTS t_shared_rc_py")
        conn.execute("CREATE TABLE t_shared_rc_py (a)")
        cur = conn.execute("INSERT INTO t_shared_rc_py VALUES (1), (2), (3)")
        assert cur.rowcount == 3


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------


class TestClose:
    def test_close_connection(self, conn):
        conn.execute("SELECT 1")
        # conn is closed by the fixture teardown
