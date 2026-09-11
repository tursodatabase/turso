"""
Tests for query cancellation: Connection.interrupt(), Connection.set_query_timeout(),
and the GIL-release behaviour that makes them usable from a watchdog thread.

`interrupt()` mirrors stdlib sqlite3.Connection.interrupt(); `set_query_timeout()`
is a Turso extension (stdlib sqlite3 has no query-execution-time limit).
"""

import sqlite3
import threading
import time

import pytest
import turso

# A query that spins the VDBE effectively forever but is checked for interruption
# on every step: a 4-way cross join over a small table (~8.1e9 row combinations).
# It never completes on its own, so any test that reaches the end of it without an
# interrupt has failed.
_SLOW_QUERY = "SELECT count(*) FROM t a, t b, t c, t d"


def _make_slow_turso(rows: int = 300) -> turso.Connection:
    conn = turso.connect(":memory:")
    conn.execute("CREATE TABLE t(x)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(rows)])
    return conn


def _make_slow_sqlite(rows: int = 300) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("CREATE TABLE t(x)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(rows)])
    return conn


def test_set_query_timeout_aborts_long_query():
    """A query that exceeds the per-statement timeout is interrupted, not run to completion."""
    conn = _make_slow_turso()
    conn.set_query_timeout(250)  # ms
    start = time.monotonic()
    with pytest.raises(turso.OperationalError, match="interrupt"):
        conn.execute(_SLOW_QUERY).fetchone()
    elapsed = time.monotonic() - start
    # Fired near the deadline, and nowhere near the (effectively infinite) full runtime.
    assert elapsed < 5.0, f"timeout took too long to fire: {elapsed:.2f}s"


def test_query_timeout_zero_disables():
    conn = _make_slow_turso()
    conn.set_query_timeout(500)
    assert conn.get_query_timeout() == 500
    conn.set_query_timeout(0)
    assert conn.get_query_timeout() == 0


def test_query_timeout_does_not_fire_for_fast_query():
    conn = _make_slow_turso()
    conn.set_query_timeout(10_000)
    # A trivial query finishes well under the deadline and must not be interrupted.
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 300


def test_set_query_timeout_rejects_negative():
    conn = turso.connect(":memory:")
    with pytest.raises(turso.ProgrammingError):
        conn.set_query_timeout(-1)


def test_interrupt_from_watchdog_thread_aborts_query():
    """
    interrupt() called from another thread aborts the in-flight query.

    This can ONLY pass if the execution methods release the GIL — otherwise the
    watchdog thread could never run while execute() is busy.
    """
    conn = _make_slow_turso()

    def watchdog():
        time.sleep(0.3)
        conn.interrupt()

    th = threading.Thread(target=watchdog)
    th.start()
    start = time.monotonic()
    with pytest.raises(turso.OperationalError, match="interrupt"):
        conn.execute(_SLOW_QUERY).fetchone()
    elapsed = time.monotonic() - start
    th.join()
    assert 0.2 < elapsed < 5.0, f"interrupt fired at unexpected time: {elapsed:.2f}s"


def test_interrupt_with_no_active_statement_is_noop():
    """interrupt() with nothing running is harmless and does not poison the next query."""
    conn = _make_slow_turso()
    conn.interrupt()
    # Subsequent queries still work.
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 300


def test_interrupt_matches_sqlite3_semantics():
    """stdlib sqlite3 exposes the same interrupt(); behaviour should match."""
    conn = _make_slow_sqlite()

    def watchdog():
        time.sleep(0.3)
        conn.interrupt()

    th = threading.Thread(target=watchdog)
    th.start()
    with pytest.raises(sqlite3.OperationalError, match="interrupt"):
        conn.execute(_SLOW_QUERY).fetchone()
    th.join()


def test_gil_is_released_during_execution():
    """
    Directly assert the GIL is released while a query runs: a background thread
    spinning a pure-Python counter must make substantial progress *during* the
    query. Without GIL release the counter would be frozen until execute() returns.
    """
    conn = _make_slow_turso()
    conn.set_query_timeout(400)

    counter = 0
    stop = False

    def spinner():
        nonlocal counter
        while not stop:
            counter += 1

    th = threading.Thread(target=spinner)
    th.start()
    try:
        with pytest.raises(turso.OperationalError):
            conn.execute(_SLOW_QUERY).fetchone()
    finally:
        stop = True
        th.join()

    # If the GIL had been held for the whole ~400ms query, the spinner could not
    # have advanced. A few hundred ms of free GIL yields millions of increments;
    # assert a conservative floor.
    assert counter > 100_000, f"counter only reached {counter}; GIL appears to be held"
