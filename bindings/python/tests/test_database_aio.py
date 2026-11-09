import asyncio
import logging
import os
import sqlite3
import time

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


@pytest.mark.asyncio
async def test_connection_execute_helpers_and_context_manager():
    async with turso.aio.connect(":memory:") as conn:
        await conn.executescript("""
            CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO t(name) VALUES ('alice');
        """)
        cur = await conn.execute("SELECT COUNT(*) FROM t")
        count = (await cur.fetchone())[0]
        assert count == 1


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_async_operations_do_not_block_event_loop():
    import time

    async with turso.aio.connect(":memory:") as conn:
        count = 1_000_000
        await conn.execute("CREATE TABLE t (id INTEGER)")
        cur = await conn.execute(f"""SELECT SUM(value) FROM generate_series(1, {count})""")
        start = time.time()
        task = asyncio.create_task(cur.fetchone())
        assert (time.time() - start) < 0.001
        assert await task == (count * (count + 1) // 2,)


@pytest.mark.asyncio
async def test_operation_after_connection_close_raises():
    import turso as _t

    conn = await turso.aio.connect(":memory:")
    cur = conn.cursor()
    await cur.execute("CREATE TABLE t (id INT)")
    await conn.close()

    with pytest.raises(_t.ProgrammingError):
        await conn.execute("SELECT 1")


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_batch_executes_parameterized_statements():
    async with turso.aio.connect(":memory:") as conn:
        results = await conn.batch(
            [
                "CREATE TABLE t_batch (id INTEGER PRIMARY KEY, name TEXT)",
                ("INSERT INTO t_batch (name) VALUES (?)", ("Alice",)),
                "SELECT name FROM t_batch",
            ]
        )
        assert len(results) == 3
        assert results[1].rowcount == 1
        assert results[1].lastrowid == 1
        assert results[2].rows == [("Alice",)]


@pytest.mark.asyncio
async def test_batch_error_identifies_the_failing_statement():
    async with turso.aio.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t_batch (x)")
        with pytest.raises(turso.DatabaseError, match="batch statement 1 failed") as excinfo:
            await conn.batch(
                [
                    ("INSERT INTO t_batch VALUES (?)", (1,)),
                    ("INSERT INTO no_such_table VALUES (?)", (2,)),
                ]
            )
        assert excinfo.value.batch_index == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_kwargs", [{}, {"timeout": 5.0}])
async def test_writer_waits_for_lock_held_by_another_connection(timeout_kwargs, tmp_path):
    db_file = str(tmp_path / "busy.db")
    holder = await turso.aio.connect(db_file, isolation_level=None)
    writer = await turso.aio.connect(db_file, isolation_level=None, **timeout_kwargs)
    await holder.execute("CREATE TABLE t (x)")

    await holder.execute("BEGIN IMMEDIATE")
    await holder.execute("INSERT INTO t VALUES (1)")
    insert = asyncio.create_task(writer.execute("INSERT INTO t VALUES (2)"))
    await asyncio.sleep(0.3)
    await holder.execute("COMMIT")
    await insert

    cursor = await writer.execute("SELECT x FROM t ORDER BY x")
    assert await cursor.fetchall() == [(1,), (2,)]
    await writer.close()
    await holder.close()


@pytest.mark.asyncio
async def test_zero_timeout_fails_at_once_on_locked_database(tmp_path):
    db_file = str(tmp_path / "busy.db")
    holder = await turso.aio.connect(db_file, isolation_level=None)
    writer = await turso.aio.connect(db_file, isolation_level=None, timeout=0)
    await holder.execute("CREATE TABLE t (x)")

    await holder.execute("BEGIN IMMEDIATE")
    start = time.monotonic()
    with pytest.raises(turso.OperationalError, match="database is locked"):
        await writer.execute("INSERT INTO t VALUES (1)")
    assert time.monotonic() - start < 3
    await holder.execute("ROLLBACK")
    await writer.close()
    await holder.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout, busy_timeout", [(10.0, 10000), (float("nan"), 0), (float("inf"), 2147483647)])
async def test_timeout_sets_busy_timeout_in_milliseconds(timeout, busy_timeout):
    conn = await turso.aio.connect(":memory:", timeout)
    cursor = await conn.execute("PRAGMA busy_timeout")
    assert await cursor.fetchall() == [(busy_timeout,)]
    await conn.close()
