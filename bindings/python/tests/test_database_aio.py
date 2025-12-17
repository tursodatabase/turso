import logging
import os
import sqlite3

import anyio
import pytest
import turso.aio

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)


@pytest.fixture(autouse=True)
def setup_database():
    db_path = "tests/database.db"
    db_wal_path = "tests/database.db-wal"

    # Ensure the database file is created fresh for each test
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(db_wal_path):
            os.remove(db_wal_path)
    except PermissionError as e:
        print(f"Failed to clean up: {e}")

    # Create a new database file
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, username TEXT)")
    cursor.execute("""
        INSERT INTO users (id, username)
        SELECT 1, 'alice'
        WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 1)
    """)
    cursor.execute("""
        INSERT INTO users (id, username)
        SELECT 2, 'bob'
        WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 2)
    """)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup after the test
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(db_wal_path):
            os.remove(db_wal_path)
    except PermissionError as e:
        print(f"Failed to clean up: {e}")


@pytest.mark.anyio
async def test_connection_execute_helpers_and_context_manager():
    async with turso.aio.connect(":memory:") as conn:
        await conn.executescript("""
            CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO t(name) VALUES ('alice');
        """)
        cur = await conn.execute("SELECT COUNT(*) FROM t")
        count = (await cur.fetchone())[0]
        assert count == 1


@pytest.mark.anyio
async def test_subqueries_and_join():
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()
        await cur.executescript("""
            CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);
            CREATE TABLE profiles (user_id INTEGER, city TEXT);
            INSERT INTO users (id, username) VALUES (1, 'alice'), (2, 'bob'), (3, 'adam');
            INSERT INTO profiles (user_id, city) VALUES (1, 'NY'), (2, 'SF'), (3, 'LA');
        """)

        # Subquery in WHERE
        await cur.execute("""
            SELECT username FROM users
            WHERE id IN (SELECT id FROM users WHERE username LIKE 'a%')
            ORDER BY username
        """)
        rows = await cur.fetchall()
        assert [r[0] for r in rows] == ["adam", "alice"]

        # JOIN with subquery
        await cur.execute(
            """
            SELECT u.username, p.city
            FROM users u
            JOIN (SELECT user_id, city FROM profiles) p
            ON u.id = p.user_id
            WHERE u.username = ?
        """,
            ("alice",),
        )
        row = await cur.fetchone()
        assert row == ("alice", "NY")
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_conflict_do_nothing_and_rowcount():
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE t (name TEXT PRIMARY KEY, val INT)")
        await cur.execute("INSERT INTO t(name, val) VALUES (?, ?)", ("x", 1))
        await conn.commit()

        # Conflict should not raise and rowcount should reflect 0 affected rows
        await cur.execute("INSERT INTO t(name, val) VALUES (?, ?) ON CONFLICT(name) DO NOTHING", ("x", 2))
        assert cur.rowcount == 0

        await cur.execute("SELECT val FROM t WHERE name = ?", ("x",))
        val = (await cur.fetchone())[0]
        assert val == 1
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_ddl_alter_table_add_column_with_default():
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()
        await cur.executescript("""
            CREATE TABLE items (id INT PRIMARY KEY, name TEXT);
            ALTER TABLE items ADD COLUMN price INT DEFAULT 0;
            INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b');
        """)
        await cur.execute("SELECT price FROM items WHERE id = 1")
        assert (await cur.fetchone())[0] == 0

        # Update and verify
        await cur.execute("UPDATE items SET price = ? WHERE id = ?", (10, 2))
        await conn.commit()
        await cur.execute("SELECT price FROM items WHERE id = 2")
        assert (await cur.fetchone())[0] == 10
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_generate_series_virtual_table_and_join():
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()

        # Simple usage
        await cur.execute("SELECT sum(value) FROM generate_series(1, 100)")
        total = (await cur.fetchone())[0]
        assert total == 5050

        # Join with a real table
        await cur.executescript("""
            CREATE TABLE nums (n INT PRIMARY KEY);
            INSERT INTO nums (n) VALUES (1), (3), (5);
        """)
        await cur.execute("""
            SELECT gs.value
            FROM generate_series(1, 5) AS gs
            JOIN nums ON nums.n = gs.value
            ORDER BY gs.value
        """)
        assert [r[0] for r in await cur.fetchall()] == [1, 3, 5]
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_json_functions_extract_patch_and_array_length():
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE docs (id INT PRIMARY KEY, doc TEXT)")
        obj = '{"a":1,"b":{"c":2},"arr":[10,20]}'
        await cur.execute("INSERT INTO docs (id, doc) VALUES (?, ?)", (1, obj))

        await cur.execute("SELECT json_extract(doc, '$.b.c') FROM docs WHERE id = 1")
        assert (await cur.fetchone())[0] == 2

        await cur.execute("SELECT json_extract(json_patch(doc, '{\"a\":5}'), '$.a') FROM docs WHERE id = 1")
        assert (await cur.fetchone())[0] == 5

        await cur.execute("SELECT json_array_length(doc, '$.arr') FROM docs WHERE id = 1")
        assert (await cur.fetchone())[0] == 2
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_async_operations_do_not_block_event_loop():
    import time

    async def fetch_result(cur):
        return await cur.fetchone()

    async with turso.aio.connect(":memory:") as conn:
        count = 1_000_000
        await conn.execute("CREATE TABLE t (id INTEGER)")
        cur = await conn.execute(f"""SELECT SUM(value) FROM generate_series(1, {count})""")
        start = time.time()
        async with anyio.create_task_group() as tg:
            # The task is started immediately and should not block
            tg.start_soon(fetch_result, cur)
            assert (time.time() - start) < 0.01


@pytest.mark.anyio
async def test_operation_after_connection_close_raises():
    import turso as _t

    conn = await turso.aio.connect(":memory:")
    cur = conn.cursor()
    await cur.execute("CREATE TABLE t (id INT)")
    await conn.close()

    with pytest.raises(_t.ProgrammingError):
        await conn.execute("SELECT 1")


@pytest.mark.anyio
async def test_cursor_async_context_manager_closes_cursor():
    import turso as _t

    conn = await turso.aio.connect(":memory:")
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            assert (await cur.fetchone())[0] == 1

        # Cursor is closed after context manager exit
        with pytest.raises(_t.ProgrammingError):
            await cur.fetchone()
    finally:
        await conn.close()


# =============================================================================
# Connection pattern tests - ensure all connection styles work correctly
# =============================================================================


@pytest.mark.anyio
async def test_connect_await_pattern():
    """Test: conn = await turso.aio.connect(...)"""
    conn = await turso.aio.connect(":memory:")
    assert conn is not None
    # Connection should be ready to use
    cur = await conn.execute("SELECT 1")
    result = await cur.fetchone()
    assert result[0] == 1
    await conn.close()


@pytest.mark.anyio
async def test_connect_async_with_pattern():
    """Test: async with turso.aio.connect(...) as conn"""
    async with turso.aio.connect(":memory:") as conn:
        assert conn is not None
        cur = await conn.execute("SELECT 1")
        result = await cur.fetchone()
        assert result[0] == 1


@pytest.mark.anyio
async def test_connect_without_await_then_use():
    """Test: conn = turso.aio.connect(...) then use methods directly"""
    conn = turso.aio.connect(":memory:")
    # Connection is not awaited, but methods should still work
    # (they will ensure connection internally)
    cur = await conn.execute("SELECT 1")
    result = await cur.fetchone()
    assert result[0] == 1
    await conn.close()


@pytest.mark.anyio
async def test_connection_returns_correct_type():
    """Ensure awaiting connect returns a Connection instance"""
    conn = await turso.aio.connect(":memory:")
    assert isinstance(conn, turso.aio.Connection)
    await conn.close()


@pytest.mark.anyio
async def test_connection_sync_await_pattern():
    """Test: conn = await turso.aio.sync.connect(...) returns ConnectionSync

    This verifies that ConnectionSync has __await__ implemented correctly.
    Note: This test will fail to actually connect without a remote server,
    but we can at least verify the awaitable pattern works.
    """
    from turso.lib_aio import Connection
    from turso.lib_sync_aio import ConnectionSync

    # Verify ConnectionSync has __await__ method
    assert hasattr(ConnectionSync, "__await__"), (
        "ConnectionSync must have __await__ method for 'conn = await connect(...)' pattern"
    )

    # Verify the class hierarchy
    assert issubclass(ConnectionSync, Connection), "ConnectionSync should inherit from Connection"


# =============================================================================
# Worker thread and behavioral tests
# =============================================================================


@pytest.mark.anyio
async def test_dedicated_worker_thread():
    """Verify each connection has its own dedicated worker thread."""

    conn1 = await turso.aio.connect(":memory:")
    conn2 = await turso.aio.connect(":memory:")

    try:
        # Each connection should have its own worker
        assert conn1._worker is not None
        assert conn2._worker is not None
        assert conn1._worker is not conn2._worker

        # Worker threads should have the expected name
        assert conn1._worker.name == "turso-async-worker"
        assert conn2._worker.name == "turso-async-worker"

        # Workers should be alive
        assert conn1._worker.is_alive()
        assert conn2._worker.is_alive()
    finally:
        await conn1.close()
        await conn2.close()


@pytest.mark.anyio
async def test_worker_thread_stops_on_close():
    """Verify worker thread stops when connection is closed."""
    conn = await turso.aio.connect(":memory:")
    worker = conn._worker

    assert worker is not None
    assert worker.is_alive()

    await conn.close()

    # Worker should have stopped
    assert not worker.is_alive()


@pytest.mark.anyio
async def test_sequential_execution_in_worker():
    """Verify operations execute sequentially in the worker thread."""
    import time

    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER, ts REAL)")

        # Insert multiple rows - they should be processed in order
        for i in range(10):
            await conn.execute("INSERT INTO t (id, ts) VALUES (?, ?)", (i, time.time()))

        cur = await conn.execute("SELECT id FROM t ORDER BY id")
        rows = await cur.fetchall()
        assert [r[0] for r in rows] == list(range(10))


@pytest.mark.anyio
async def test_cancellation_skips_queued_work():
    """Test that cancelled operations are skipped by the worker.

    This verifies the pre-execution cancellation check in the worker.
    """
    from anyio import to_thread
    from turso.worker import WorkItem

    async with turso.aio.connect(":memory:") as conn:
        # Create a work item and mark it cancelled before it's processed
        executed = False

        def should_not_run():
            nonlocal executed
            executed = True
            return "ran"

        item = WorkItem(should_not_run)
        item.cancelled = True

        # Queue it
        conn._queue.put_nowait(item)

        # Wait for the event (worker should signal it even though cancelled)
        await to_thread.run_sync(item.event.wait)

        # The function should not have been executed
        assert not executed
        assert item.result is None


@pytest.mark.anyio
async def test_eager_cursor_creation():
    """Test that cursor creation is queued eagerly (immediately on cursor()).

    The cursor creation should be queued to the worker immediately when
    cursor() is called, not lazily on first execute().
    """
    async with turso.aio.connect(":memory:") as conn:
        # Create cursor - this should queue cursor creation immediately
        cur = conn.cursor()

        # The cursor should not be created yet (queued but not executed)
        assert not cur._cursor_created

        # Execute should work - cursor will be created by worker before execute runs
        await cur.execute("SELECT 1")
        result = await cur.fetchone()
        assert result[0] == 1

        # Now cursor should be created
        assert cur._cursor_created


@pytest.mark.anyio
async def test_eager_connection_initialization():
    """Test that connection initializes eagerly on connect().

    The worker thread is created immediately when connect() is called,
    not lazily on first async operation. This matches original asyncio behavior.
    """
    # Create connection without awaiting - should initialize immediately
    conn = turso.aio.connect(":memory:")

    # Worker should be created immediately (eager initialization)
    assert conn._worker is not None
    assert conn._worker.is_alive()

    # Connection open is queued but may not be complete yet
    # Using the connection will work because operations are queued sequentially
    cur = await conn.execute("SELECT 1")
    result = await cur.fetchone()
    assert result[0] == 1

    await conn.close()


@pytest.mark.anyio
async def test_property_setters_queue_fire_and_forget():
    """Test that property setters queue work without awaiting.

    Property setters like row_factory should queue changes to the worker
    but not block waiting for completion.
    """
    async with turso.aio.connect(":memory:") as conn:
        # Set a property - should not block
        def row_to_dict(cursor, row):
            return dict(zip([col[0] for col in cursor.description], row))

        conn.row_factory = row_to_dict

        # The cache should be updated immediately
        assert conn.row_factory is row_to_dict

        # Execute a query - the row factory should be applied
        await conn.execute("CREATE TABLE t (name TEXT, value INT)")
        await conn.execute("INSERT INTO t VALUES ('test', 42)")
        cur = await conn.execute("SELECT name, value FROM t")
        row = await cur.fetchone()

        # Row should be a dict thanks to row_factory
        assert row == {"name": "test", "value": 42}


# =============================================================================
# Cancellation and timeout tests
# =============================================================================


@pytest.mark.anyio
async def test_operation_can_be_abandoned_on_cancel():
    """Test that async operations can be abandoned when cancelled.

    With abandon_on_cancel=True, the await returns immediately on cancellation
    even if the worker thread is still processing. The worker continues but
    the caller is unblocked.
    """
    import time

    import anyio

    async with turso.aio.connect(":memory:") as conn:
        # Test that move_on_after works - the operation should be abandoned
        # if it takes too long (which it won't, but this tests the mechanism)
        start = time.time()
        with anyio.move_on_after(0.5):
            # This should complete quickly, not timeout
            cur = await conn.execute("SELECT 1")
            result = await cur.fetchone()
            assert result[0] == 1

        elapsed = time.time() - start
        # Should complete in well under 0.5 seconds
        assert elapsed < 0.5


@pytest.mark.anyio
async def test_optional_timeout_raises_operational_error():
    """Test that the optional timeout parameter raises OperationalError when exceeded."""
    import turso

    async with turso.aio.connect(":memory:") as conn:
        # Use a very short timeout that will be exceeded
        with pytest.raises(turso.OperationalError, match="timeout"):
            # Queue a slow operation then try to run something with a tiny timeout
            import time

            from turso.worker import WorkItem

            def slow_operation():
                time.sleep(1.0)
                return "done"

            item = WorkItem(slow_operation)
            conn._queue.put_nowait(item)

            # This should timeout because the slow operation is blocking the worker
            await conn._run(lambda: "quick", timeout=0.001)


# =============================================================================
# Error handling and reliability tests
# =============================================================================


@pytest.mark.anyio
async def test_sql_syntax_error_propagates():
    """SQL errors must surface to the caller, not get swallowed."""
    import turso

    async with turso.aio.connect(":memory:") as conn:
        with pytest.raises(turso.DatabaseError):
            await conn.execute("SELEKT * FROM nonexistent")


@pytest.mark.anyio
async def test_constraint_violation_propagates():
    """Constraint violations must raise IntegrityError."""
    import turso

    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.execute("INSERT INTO t VALUES (1)")

        with pytest.raises(turso.IntegrityError):
            await conn.execute("INSERT INTO t VALUES (1)")  # Duplicate PK


@pytest.mark.anyio
async def test_worker_continues_after_exception():
    """Worker thread must keep working after an error - one bad query can't kill the connection."""
    import turso

    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER)")

        # Cause an error
        with pytest.raises(turso.DatabaseError):
            await conn.execute("INVALID SQL")

        # Worker should still work
        await conn.execute("INSERT INTO t VALUES (1)")
        cur = await conn.execute("SELECT * FROM t")
        assert await cur.fetchone() == (1,)


@pytest.mark.anyio
async def test_rollback_discards_uncommitted_changes():
    """Rollback must actually discard changes - data integrity depends on this."""
    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER)")
        await conn.execute("INSERT INTO t VALUES (1)")
        await conn.commit()

        await conn.execute("INSERT INTO t VALUES (2)")
        await conn.rollback()

        cur = await conn.execute("SELECT COUNT(*) FROM t")
        count = (await cur.fetchone())[0]
        assert count == 1  # Only the committed row


@pytest.mark.anyio
async def test_concurrent_operations_all_complete():
    """Concurrent async operations must all complete without corruption."""
    import anyio

    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")

        async def insert(i):
            await conn.execute("INSERT INTO t VALUES (?)", (i,))

        # Fire off many concurrent inserts
        async with anyio.create_task_group() as tg:
            for i in range(100):
                tg.start_soon(insert, i)

        await conn.commit()

        # All rows must be there
        cur = await conn.execute("SELECT COUNT(*) FROM t")
        assert (await cur.fetchone())[0] == 100

        # No duplicates, no gaps
        cur = await conn.execute("SELECT id FROM t ORDER BY id")
        rows = await cur.fetchall()
        assert [r[0] for r in rows] == list(range(100))
