#!/usr/bin/env -S python3 -u

from antithesis.random import get_random
from helper_common import connect

NUM_ACCOUNTS = 2000
PAD = "x" * 64

try:
    con = connect()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS accounts")
cur.execute("DROP TABLE IF EXISTS initial_state")
cur.execute(
    """
    CREATE TABLE accounts (
        id INTEGER PRIMARY KEY,
        balance INTEGER NOT NULL,
        generation INTEGER NOT NULL,
        pad TEXT NOT NULL
    )
    """
)
cur.execute("CREATE TABLE initial_state (num_accts INTEGER, total INTEGER)")

cur.execute("BEGIN")
total = 0
for _ in range(NUM_ACCOUNTS):
    balance = get_random() % 1_000_000
    total += balance
    cur.execute(f"INSERT INTO accounts (balance, generation, pad) VALUES ({balance}, 0, '{PAD}')")
cur.execute(f"INSERT INTO initial_state (num_accts, total) VALUES ({NUM_ACCOUNTS}, {total})")
cur.execute("COMMIT")

cur.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
print(f"created {NUM_ACCOUNTS} accounts with total {total}")
