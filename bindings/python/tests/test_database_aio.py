import os
import asyncio
import sqlite3

import pytest
import logging

import turso.aio
import turso

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
        await cur.execute("""
            SELECT u.username, p.city
            FROM users u
            JOIN (SELECT user_id, city FROM profiles) p
            ON u.id = p.user_id
            WHERE u.username = ?
        """, ("alice",))
        row = await cur.fetchone()
        assert row == ("alice", "NY")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_insert_returning_single_and_multiple_commit_without_consuming():
    turso.setup_logging(level=logging.DEBUG)
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

        # Single INSERT ... RETURNING
        await cur.execute("INSERT INTO t(name) VALUES (?) RETURNING id, name", ("alice",))
        one = await cur.fetchone()
        assert one[1] == "alice"
        await conn.commit()

        # Multiple rows with RETURNING; consume only one row and commit
        await cur.execute(
            "INSERT INTO t(name) VALUES (?), (?), (?) RETURNING id, name",
            ("bob", "charlie", "dora"),
        )
        first = await cur.fetchone()
        assert first[1] in ("bob", "charlie", "dora")
        # Do not consume remaining rows, but commit explicitly
        await conn.commit()

        # Ensure all 3 were inserted despite not consuming all RETURNING rows
        await cur.execute("SELECT COUNT(*) FROM t WHERE name = 'bob' OR name = 'charlie' OR name = 'dora'")
        total = (await cur.fetchone())[0]
        assert total == 3
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
    # Slow down the worker thread with extra_io to make the test robust
    conn = await turso.aio.connect(":memory:", extra_io=lambda: time.sleep(0.0005))
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE t (id INTEGER)")
        payload = [(i,) for i in range(5000)]

        ticks = 0
        done = False

        async def ticker():
            nonlocal ticks, done
            while not done:
                ticks += 1
                await asyncio.sleep(0.001)

        task = asyncio.create_task(ticker())
        await cur.executemany("INSERT INTO t (id) VALUES (?)", payload)
        done = True
        await task

        # Event loop should have run ticker while DB work was running
        assert ticks > 0

        await cur.execute("SELECT COUNT(*) FROM t")
        count = (await cur.fetchone())[0]
        assert count == 5000
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_named_parameters_and_delete_rowcount():
    conn = await turso.aio.connect(":memory:")
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        await cur.executemany("INSERT INTO t (id, v) VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])

        await cur.execute("DELETE FROM t WHERE id = :id", {"id": 2})
        assert cur.rowcount == 1

        await cur.execute("SELECT id FROM t ORDER BY id")
        assert [r[0] for r in await cur.fetchall()] == [1, 3]
    finally:
        await conn.close()


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