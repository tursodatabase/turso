import logging
import os
import sqlite3

import pytest
import turso

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)

def connect(provider, database):
    if provider == "turso":
        return turso.connect(database)
    if provider == "sqlite3":
        return sqlite3.connect(database)
    raise Exception(f"Provider `{provider}` is not supported")


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
def test_in_memory_index(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (name TEXT PRIMARY KEY, email TEXT)")
    cursor.execute("CREATE INDEX email_idx ON users(email)")
    cursor.execute("INSERT INTO users VALUES ('alice', 'a@b.c'), ('bob', 'b@d.e')")

    cursor.execute("SELECT * FROM users WHERE email = 'a@b.c'")
    alice = cursor.fetchall()

    cursor.execute("SELECT * FROM users WHERE email = 'b@d.e'")
    bob = cursor.fetchall()

    conn.close()
    assert alice == [("alice", "a@b.c")]
    assert bob == [("bob", "b@d.e")]


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


# DB-API 2.0 tests


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_description(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT, value REAL)")
    cursor.execute("INSERT INTO test VALUES (1, 'test', 3.14)")
    cursor.execute("SELECT * FROM test")

    assert cursor.description is not None
    assert len(cursor.description) == 3
    assert cursor.description[0][0] == "id"
    assert cursor.description[1][0] == "name"
    assert cursor.description[2][0] == "value"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_rowcount_insert(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

    assert cursor.rowcount == 3

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_rowcount_update(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob')")
    cursor.execute("UPDATE test SET name = 'updated' WHERE id = 1")

    assert cursor.rowcount == 1

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_rowcount_delete(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
    cursor.execute("DELETE FROM test WHERE id > 1")

    assert cursor.rowcount == 2

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_fetchmany(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    cursor.execute("SELECT * FROM test")

    cursor.arraysize = 2
    rows = cursor.fetchmany()
    assert len(rows) == 2
    assert rows == [(1,), (2,)]

    rows = cursor.fetchmany(3)
    assert len(rows) == 3
    assert rows == [(3,), (4,), (5,)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_iterator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1), (2), (3)")
    cursor.execute("SELECT * FROM test")

    rows = list(cursor)
    assert rows == [(1,), (2,), (3,)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_close(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.close()

    with pytest.raises(Exception):
        cursor.execute("SELECT * FROM test")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_connection_execute(provider):
    conn = connect(provider, ":memory:")
    conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor = conn.execute("INSERT INTO test VALUES (?, ?)", (1, "alice"))

    assert cursor.rowcount == 1

    cursor = conn.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert rows == [(1, "alice")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_connection_executemany(provider):
    conn = connect(provider, ":memory:")
    conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")

    data = [(1, "alice"), (2, "bob"), (3, "charlie")]
    cursor = conn.executemany("INSERT INTO test VALUES (?, ?)", data)

    assert cursor.rowcount == 3

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_connection_executescript(provider):
    conn = connect(provider, ":memory:")
    script = """
        CREATE TABLE test (id INTEGER, name TEXT);
        INSERT INTO test VALUES (1, 'alice');
        INSERT INTO test VALUES (2, 'bob');
    """
    conn.executescript(script)

    cursor = conn.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert len(rows) == 2

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_row_factory(provider):
    conn = connect(provider, ":memory:")
    conn.row_factory = turso.Row if provider == "turso" else sqlite3.Row

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice')")
    cursor.execute("SELECT * FROM test")

    row = cursor.fetchone()
    assert row["id"] == 1
    assert row["name"] == "alice"
    assert row[0] == 1
    assert row[1] == "alice"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_row_factory_keys(provider):
    conn = connect(provider, ":memory:")
    conn.row_factory = turso.Row if provider == "turso" else sqlite3.Row

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT, value REAL)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice', 3.14)")
    cursor.execute("SELECT * FROM test")

    row = cursor.fetchone()
    keys = row.keys()
    assert keys == ["id", "name", "value"]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_parameterized_query(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (?, ?)", (1, "alice"))
    cursor.execute("INSERT INTO test VALUES (?, ?)", (2, "bob"))

    cursor.execute("SELECT * FROM test WHERE id = ?", (1,))
    row = cursor.fetchone()
    assert row == (1, "alice")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_executemany_with_parameters(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")

    data = [(1, "alice"), (2, "bob"), (3, "charlie")]
    cursor.executemany("INSERT INTO test VALUES (?, ?)", data)

    cursor.execute("SELECT COUNT(*) FROM test")
    count = cursor.fetchone()[0]
    assert count == 3

    conn.close()


# SQL tests


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_subquery(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")

    cursor.execute("SELECT id FROM test WHERE value > (SELECT AVG(value) FROM test)")
    rows = cursor.fetchall()
    assert rows == [(3,)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_insert_returning(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO test (id, name) VALUES (1, 'alice') RETURNING id, name")

    row = cursor.fetchone()
    assert row == (1, "alice")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_insert_returning_partial_fetch(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie') RETURNING id, name")

    row = cursor.fetchone()
    assert row == (1, "alice")

    cursor.close()

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM test")
    count = cursor.fetchone()[0]
    assert count == 3

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_conflict_clause_ignore(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice')")
    cursor.execute("INSERT OR IGNORE INTO test VALUES (1, 'bob')")

    cursor.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert rows == [(1, "alice")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_conflict_clause_replace(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice')")
    cursor.execute("INSERT OR REPLACE INTO test VALUES (1, 'bob')")

    cursor.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert rows == [(1, "bob")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_conflict_clause_rollback(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice')")

    try:
        cursor.execute("INSERT OR ROLLBACK INTO test VALUES (1, 'bob')")
    except Exception:
        pass

    cursor.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert len(rows) <= 1

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_drop_table(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("DROP TABLE test")

    try:
        cursor.execute("SELECT * FROM test")
        assert False, "Table should not exist"
    except Exception:
        pass

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_alter_table_add_column(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1)")
    cursor.execute("ALTER TABLE test ADD COLUMN name TEXT")
    cursor.execute("UPDATE test SET name = 'alice' WHERE id = 1")

    cursor.execute("SELECT * FROM test")
    row = cursor.fetchone()
    assert row == (1, "alice")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_alter_table_rename(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1)")
    cursor.execute("ALTER TABLE test RENAME TO new_test")

    cursor.execute("SELECT * FROM new_test")
    row = cursor.fetchone()
    assert row == (1,)

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_inner_join(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    cursor.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, item TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
    cursor.execute("INSERT INTO orders VALUES (1, 1, 'book'), (2, 1, 'pen'), (3, 2, 'notebook')")

    cursor.execute("""
        SELECT users.name, orders.item
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        WHERE users.id = 1
    """)
    rows = cursor.fetchall()
    assert rows == [("alice", "book"), ("alice", "pen")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_left_join(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    cursor.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, item TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
    cursor.execute("INSERT INTO orders VALUES (1, 1, 'book'), (2, 2, 'pen')")

    cursor.execute("""
        SELECT users.name, orders.item
        FROM users
        LEFT JOIN orders ON users.id = orders.user_id
    """)
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert ("charlie", None) in rows

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_json_extract(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, data TEXT)")
    cursor.execute('INSERT INTO test VALUES (1, \'{"name": "alice", "age": 30}\')')

    cursor.execute("SELECT json_extract(data, '$.name') FROM test")
    row = cursor.fetchone()
    assert row[0] == "alice"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_json_array(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT json_array(1, 2, 3)")

    row = cursor.fetchone()
    assert row[0] == "[1,2,3]"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_json_object(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT json_object('name', 'alice', 'age', 30)")

    row = cursor.fetchone()
    assert "alice" in row[0]
    assert "30" in row[0]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_aggregate_functions(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (10), (20), (30), (40)")

    cursor.execute("SELECT AVG(value), SUM(value), MIN(value), MAX(value), COUNT(*) FROM test")
    row = cursor.fetchone()
    assert row == (25.0, 100, 10, 40, 4)

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_group_by(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (category TEXT, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40)")

    cursor.execute("SELECT category, SUM(value) FROM test GROUP BY category")
    rows = cursor.fetchall()
    assert rows == [("A", 30), ("B", 70)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_having_clause(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (category TEXT, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES ('A', 10), ('A', 20), ('B', 5), ('B', 10)")

    cursor.execute("SELECT category, SUM(value) as total FROM test GROUP BY category HAVING total > 20")
    rows = cursor.fetchall()
    assert rows == [("A", 30)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_create_view(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
    cursor.execute("CREATE VIEW test_view AS SELECT id, value * 2 as doubled FROM test")

    cursor.execute("SELECT * FROM test_view WHERE id = 2")
    row = cursor.fetchone()
    assert row == (2, 40)

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_drop_view(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("CREATE VIEW test_view AS SELECT * FROM test")
    cursor.execute("DROP VIEW test_view")

    try:
        cursor.execute("SELECT * FROM test_view")
        assert False, "View should not exist"
    except Exception:
        pass

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_with_cte(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")

    cursor.execute("""
        WITH doubled AS (SELECT id, value * 2 as doubled_value FROM test)
        SELECT * FROM doubled WHERE doubled_value > 30
    """)
    rows = cursor.fetchall()
    assert rows == [(2, 40), (3, 60)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_case_expression(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")

    cursor.execute("""
        SELECT id,
               CASE
                   WHEN value < 15 THEN 'low'
                   WHEN value < 25 THEN 'medium'
                   ELSE 'high'
               END as category
        FROM test
    """)
    rows = cursor.fetchall()
    assert rows == [(1, "low"), (2, "medium"), (3, "high")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_between_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40)")

    cursor.execute("SELECT * FROM test WHERE value BETWEEN 15 AND 35")
    rows = cursor.fetchall()
    assert rows == [(2, 20), (3, 30)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_in_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

    cursor.execute("SELECT * FROM test WHERE name IN ('alice', 'charlie')")
    rows = cursor.fetchall()
    assert rows == [(1, "alice"), (3, "charlie")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_like_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'alicia')")

    cursor.execute("SELECT * FROM test WHERE name LIKE 'ali%'")
    rows = cursor.fetchall()
    assert len(rows) == 2

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_glob_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'alicia')")

    cursor.execute("SELECT * FROM test WHERE name GLOB 'ali*'")
    rows = cursor.fetchall()
    assert len(rows) == 2

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_exists_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    cursor.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
    cursor.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
    cursor.execute("INSERT INTO orders VALUES (1, 1)")

    cursor.execute("""
        SELECT name FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    """)
    rows = cursor.fetchall()
    assert rows == [("alice",)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_transaction_begin_commit(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value TEXT)")

    cursor.execute("BEGIN")
    cursor.execute("INSERT INTO test VALUES (1, 'test')")
    cursor.execute("COMMIT")

    cursor.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert rows == [(1, "test")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_transaction_begin_rollback(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value TEXT)")

    cursor.execute("BEGIN")
    cursor.execute("INSERT INTO test VALUES (2, 'rollback')")
    cursor.execute("ROLLBACK")

    cursor.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert rows == []

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_multiple_cursors_same_connection(provider):
    conn = connect(provider, ":memory:")
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    cursor1.execute("CREATE TABLE test (id INTEGER)")
    cursor1.execute("INSERT INTO test VALUES (1), (2)")

    cursor2.execute("SELECT * FROM test")
    rows = cursor2.fetchall()
    assert len(rows) == 2

    cursor1.close()
    cursor2.close()
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_description_before_execute(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()

    assert cursor.description is None

    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("SELECT * FROM test")

    assert cursor.description is not None

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_arraysize_default(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()

    assert cursor.arraysize == 1

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_empty_fetchall(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("SELECT * FROM test")

    rows = cursor.fetchall()
    assert rows == []

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_empty_fetchone(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("SELECT * FROM test")

    row = cursor.fetchone()
    assert row is None

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_empty_fetchmany(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("SELECT * FROM test")

    rows = cursor.fetchmany(5)
    assert rows == []

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_unicode_data(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, text TEXT)")
    cursor.execute("INSERT INTO test VALUES (?, ?)", (1, "Hello 世界 🌍"))

    cursor.execute("SELECT text FROM test")
    row = cursor.fetchone()
    assert row[0] == "Hello 世界 🌍"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_null_values(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, NULL)")

    cursor.execute("SELECT * FROM test")
    row = cursor.fetchone()
    assert row == (1, None)

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_blob_data(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, data BLOB)")

    blob_data = b"\x00\x01\x02\x03\x04"
    cursor.execute("INSERT INTO test VALUES (?, ?)", (1, blob_data))

    cursor.execute("SELECT data FROM test")
    row = cursor.fetchone()
    assert row[0] == blob_data

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_limit_offset(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")

    cursor.execute("SELECT * FROM test LIMIT 2 OFFSET 2")
    rows = cursor.fetchall()
    assert rows == [(3,), (4,)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_order_by_desc(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 30), (2, 10), (3, 20)")

    cursor.execute("SELECT * FROM test ORDER BY value DESC")
    rows = cursor.fetchall()
    assert rows == [(1, 30), (3, 20), (2, 10)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_distinct(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (value TEXT)")
    cursor.execute("INSERT INTO test VALUES ('a'), ('b'), ('a'), ('c'), ('b')")

    cursor.execute("SELECT DISTINCT value FROM test ORDER BY value")
    rows = cursor.fetchall()
    assert rows == [("a",), ("b",), ("c",)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_coalesce_function(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, a TEXT, b TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, NULL, 'fallback'), (2, 'value', 'fallback')")

    cursor.execute("SELECT COALESCE(a, b) FROM test ORDER BY id")
    rows = cursor.fetchall()
    assert rows == [("fallback",), ("value",)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_union_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE t1 (value INTEGER)")
    cursor.execute("CREATE TABLE t2 (value INTEGER)")
    cursor.execute("INSERT INTO t1 VALUES (1), (2)")
    cursor.execute("INSERT INTO t2 VALUES (2), (3)")

    cursor.execute("SELECT value FROM t1 UNION SELECT value FROM t2")
    rows = cursor.fetchall()
    assert len(rows) == 3

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_union_all_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE t1 (value INTEGER)")
    cursor.execute("CREATE TABLE t2 (value INTEGER)")
    cursor.execute("INSERT INTO t1 VALUES (1), (2)")
    cursor.execute("INSERT INTO t2 VALUES (2), (3)")

    cursor.execute("SELECT value FROM t1 UNION ALL SELECT value FROM t2")
    rows = cursor.fetchall()
    assert len(rows) == 4

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_is_null_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    cursor.execute("SELECT id FROM test WHERE value IS NULL")
    rows = cursor.fetchall()
    assert rows == [(2,)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_is_not_null_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'a'), (2, NULL), (3, 'c')")

    cursor.execute("SELECT id FROM test WHERE value IS NOT NULL ORDER BY id")
    rows = cursor.fetchall()
    assert rows == [(1,), (3,)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_not_in_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cursor.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

    cursor.execute("SELECT * FROM test WHERE name NOT IN ('alice', 'charlie') ORDER BY id")
    rows = cursor.fetchall()
    assert rows == [(2, "bob")]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_not_exists_operator(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    cursor.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
    cursor.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
    cursor.execute("INSERT INTO orders VALUES (1, 1)")

    cursor.execute("""
        SELECT name FROM users
        WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    """)
    rows = cursor.fetchall()
    assert rows == [("bob",)]

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_substr_function(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT substr('Hello World', 1, 5)")

    row = cursor.fetchone()
    assert row[0] == "Hello"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_length_function(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT length('Hello')")

    row = cursor.fetchone()
    assert row[0] == 5

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_upper_lower_functions(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT upper('hello'), lower('WORLD')")

    row = cursor.fetchone()
    assert row == ("HELLO", "world")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_trim_functions(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT trim('  hello  '), ltrim('  hello'), rtrim('hello  ')")

    row = cursor.fetchone()
    assert row == ("hello", "hello", "hello")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_replace_function(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT replace('Hello World', 'World', 'Python')")

    row = cursor.fetchone()
    assert row[0] == "Hello Python"

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_abs_function(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT abs(-42), abs(42)")

    row = cursor.fetchone()
    assert row == (42, 42)

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_typeof_function(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT typeof(123), typeof('text'), typeof(NULL), typeof(3.14)")

    row = cursor.fetchone()
    assert row == ("integer", "text", "null", "real")

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_create_table_if_not_exists(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
    row = cursor.fetchone()
    assert row is not None

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_drop_table_if_exists(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("DROP TABLE IF EXISTS test")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
    row = cursor.fetchone()
    assert row is None

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_multiple_ctes(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (10), (20), (30)")

    cursor.execute("""
        WITH
            doubled AS (SELECT value * 2 as v FROM test),
            tripled AS (SELECT value * 3 as v FROM test)
        SELECT doubled.v, tripled.v FROM doubled, tripled WHERE doubled.v = 20 AND tripled.v = 30
    """)
    row = cursor.fetchone()
    assert row == (20, 30)

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_nested_subqueries(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40)")

    cursor.execute("""
        SELECT id FROM test
        WHERE value > (
            SELECT AVG(value) FROM test
            WHERE value > (SELECT MIN(value) FROM test)
        )
    """)
    rows = cursor.fetchall()
    assert len(rows) > 0

    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_correlated_subquery(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, category TEXT, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 15), (4, 'B', 25)")

    cursor.execute("""
        SELECT t1.id, t1.value
        FROM test t1
        WHERE t1.value > (SELECT AVG(t2.value) FROM test t2 WHERE t2.category = t1.category)
    """)
    rows = cursor.fetchall()
    assert len(rows) == 2

    conn.close()


# Additional tests appended


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_executemany_requires_dml(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    with pytest.raises(Exception):
        cur.executemany("SELECT 1", [()])
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_execute_multiple_statements_prohibited(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    with pytest.raises(Exception):
        cur.execute("SELECT 1; SELECT 2")
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_description_none_after_insert(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (id INTEGER)")
    cur.execute("INSERT INTO test VALUES (1), (2)")
    assert cur.description is None
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_rowcount_select_is_minus_one(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (id INTEGER)")
    cur.execute("INSERT INTO test VALUES (1), (2)")
    cur.execute("SELECT * FROM test")
    assert cur.rowcount == -1
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_cursor_setinput_output_size_noop(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    # Should not raise
    cur.setinputsizes([None])
    cur.setoutputsize(1024, 0)
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_custom_row_factory_callable(provider):
    conn = connect(provider, ":memory:")

    # row factory that returns a dict for each row
    def dict_factory(cursor, row):
        return {cursor.description[i][0]: row[i] for i in range(len(row))}

    conn.row_factory = dict_factory
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (id INTEGER, name TEXT)")
    cur.execute("INSERT INTO test VALUES (1, 'alice')")
    cur.execute("SELECT * FROM test")
    row = cur.fetchone()
    assert isinstance(row, dict)
    assert row["id"] == 1
    assert row["name"] == "alice"
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_in_transaction_toggle_with_commit(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (id INTEGER)")
    # Before DML, should not be in a transaction
    assert not conn.in_transaction
    # DML should start a transaction in legacy mode
    cur.execute("INSERT INTO test VALUES (1)")
    assert conn.in_transaction
    conn.commit()
    assert not conn.in_transaction
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_isolation_level_none_autocommit(provider, tmp_path):
    db_file = tmp_path / "auto_commit.db"

    if provider == "turso":
        conn = turso.connect(str(db_file), isolation_level=None)
    else:
        conn = sqlite3.connect(str(db_file))
        conn.isolation_level = None

    cur = conn.cursor()
    cur.execute("CREATE TABLE test (id INTEGER)")
    cur.execute("INSERT INTO test VALUES (1)")
    # No explicit commit; in autocommit mode data should persist
    conn.close()

    conn2 = connect(provider, str(db_file))
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM test")
    count = cur2.fetchone()[0]
    assert count == 1
    conn2.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_generate_series_virtual_table(provider):
    conn = connect(provider, ":memory:")
    cur = conn.cursor()
    if provider == "turso":
        cur.execute("SELECT value FROM generate_series(1, 3)")
        rows = cur.fetchall()
        assert rows == [(1,), (2,), (3,)]
    else:
        with pytest.raises(Exception):
            cur.execute("SELECT value FROM generate_series(1, 3)")
    conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_connection_exception_attributes_present(provider):
    conn = connect(provider, ":memory:")
    # Ensure DB-API exception classes are exposed on the connection
    assert issubclass(conn.Error, Exception)
    assert issubclass(conn.DatabaseError, Exception)
    assert issubclass(conn.ProgrammingError, Exception)
    conn.close()

@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_insert_returning_single_and_multiple_commit_without_consuming(provider):
    # turso.setup_logging(level=logging.DEBUG)
    conn = connect(provider, ":memory:")
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO t(name) VALUES (?), (?) RETURNING id", ("bob", "alice"),)
        cur.fetchone()
        with pytest.raises(Exception):
            conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize("provider", ["sqlite3", "turso"])
def test_pragma_integrity_check(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("PRAGMA integrity_check")

    # Verify fetchone returns the expected result, not None
    # Bug: missing add_pragma_result_column in translate_integrity_check
    # caused column_count to be 0, making execute() finalize the statement
    # and leaving fetchone() to return None
    row = cursor.fetchone()
    assert row is not None, "PRAGMA integrity_check should return a row"
    assert row == ("ok",)

    conn.close()

