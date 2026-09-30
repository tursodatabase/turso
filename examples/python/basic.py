import turso

# Use the context manager to automatically close the connection
with turso.connect("sqlite.db") as con:
    cur = con.cursor()
    cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT (datetime('now'))
            )
        """)

    # Insert some sample data
    sample_users = [
        ("alice", "alice@example.com", "admin"),
        ("bob", "bob@example.com", "user"),
        ("charlie", "charlie@example.com", "moderator"),
        ("diana", "diana@example.com", "user"),
    ]
    for username, email, role in sample_users:
        cur.execute(
            """
             INSERT INTO users (username, email, role)
             VALUES (?, ?, ?)
         """,
            (username, email, role),
        )

    # Use commit to ensure the data is saved
    con.commit()

    # Query the table
    res = cur.execute("SELECT * FROM users")
    record = res.fetchone()
    print(record)
