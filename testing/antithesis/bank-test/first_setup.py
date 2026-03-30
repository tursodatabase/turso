#!/usr/bin/env -S python3 -u

import turso
from antithesis.random import get_random

try:
    con = turso.connect("bank_test.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()

# drop accounts table if it exists and create a new table
cur.execute("""
    DROP TABLE IF EXISTS accounts;
""")

cur.execute("""
    CREATE TABLE accounts (
        account_id INTEGER PRIMARY KEY AUTOINCREMENT,
        balance REAL NOT NULL DEFAULT 0.0
    );
""")

# randomly create up to 100 accounts with a balance up to 1e9
total = 0
num_accts = get_random() % 100 + 1
for i in range(num_accts):
    bal = get_random() % 1e9
    total += bal
    cur.execute(f"""
        INSERT INTO accounts (balance)
        VALUES ({bal})
    """)

# drop initial_state table if it exists and create a new table
cur.execute("""
    DROP TABLE IF EXISTS initial_state;
""")
cur.execute("""
    CREATE TABLE initial_state (
        num_accts INTEGER,
        total REAL
    );
""")

# store initial state in the table
cur.execute(f"""
    INSERT INTO initial_state (num_accts, total)
    VALUES ({num_accts}, {total})
""")

con.commit()
