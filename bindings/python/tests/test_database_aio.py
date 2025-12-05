import os
import sqlite3

import pytest
import turso.aio

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
async def test_executemany_with_parameters():
    conn = await turso.aio.connect(":memory:")
    cursor = conn.cursor()
    await cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")

    data = [(1, "alice"), (2, "bob"), (3, "charlie")]
    await cursor.executemany("INSERT INTO test VALUES (?, ?)", data)

    await cursor.execute("SELECT COUNT(*) FROM test")
    count = (await cursor.fetchone())[0]
    assert count == 3

    await conn.close()