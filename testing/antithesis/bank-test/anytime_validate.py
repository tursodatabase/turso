#!/usr/bin/env -S python3 -u

import turso
from antithesis.assertions import always

try:
    con = turso.connect("bank_test.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()

initial_state = cur.execute("""
    SELECT * FROM initial_state
""").fetchone()

curr_total = cur.execute("""
    SELECT SUM(balance) AS total FROM accounts;
""").fetchone()

always(
    initial_state[1] == curr_total[0],
    "[Anytime] Initial balance always equals current balance",
    {"init_bal": initial_state[1], "curr_bal": curr_total[0]},
)
