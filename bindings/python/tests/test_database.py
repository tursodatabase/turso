import os
import sqlite3

import pytest
import turso


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


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_fetchall_select_all_users(provider, setup_database):
    conn = connect(provider, setup_database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()

    conn.close()
    assert users
    assert users == [(1, "alice"), (2, "bob")]


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_fetchall_select_user_ids(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users")

    user_ids = cursor.fetchall()

    conn.close()
    assert user_ids
    assert user_ids == [(1,), (2,)]


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_in_memory_fetchone_select_all_users(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'alice')")

    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()

    conn.close()
    assert alice
    assert alice == (1, "alice")


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_fetchone_select_all_users(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()
    assert alice
    assert alice == (1, "alice")

    bob = cursor.fetchone()

    conn.close()
    assert bob
    assert bob == (2, "bob")


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_fetchone_select_max_user_id(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM users")

    max_id = cursor.fetchone()

    conn.close()
    assert max_id
    assert max_id == (2,)


# Test case for: https://github.com/tursodatabase/turso/issues/494
@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_commit(provider):
    conn = connect(provider, "tests/database.db")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users_b (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    conn.commit()

    sample_users = [
        ("alice", "alice@example.com", "admin"),
        ("bob", "bob@example.com", "user"),
        ("charlie", "charlie@example.com", "moderator"),
        ("diana", "diana@example.com", "user"),
    ]

    for username, email, role in sample_users:
        cur.execute("INSERT INTO users_b (username, email, role) VALUES (?, ?, ?)", (username, email, role))

    conn.commit()

    # Now query the table
    res = cur.execute("SELECT * FROM users_b")
    record = res.fetchone()

    conn.close()
    assert record


# Test case for: https://github.com/tursodatabase/turso/issues/2002
@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_first_rollback(provider, tmp_path):
    db_file = tmp_path / "test_first_rollback.db"

    conn = connect(provider, str(db_file))
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)")
    cur.execute("INSERT INTO users VALUES (1, 'alice')")
    cur.execute("INSERT INTO users VALUES (2, 'bob')")

    conn.rollback()

    cur.execute("SELECT * FROM users")
    users = cur.fetchall()

    assert users == []
    conn.close()

@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_with_statement(provider):
    with connect(provider, "tests/database.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM users")

        max_id = cursor.fetchone()

        assert max_id
        assert max_id == (2,)


def connect(provider, database):
    if provider == "turso":
        return turso.connect(database)
    if provider == "sqlite3":
        return sqlite3.connect(database)
    raise Exception(f"Provider `{provider}` is not supported")
