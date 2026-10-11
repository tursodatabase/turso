#!/usr/bin/env -S python3 -u

from antithesis.assertions import sometimes
from antithesis.random import get_random
from helper_common import connect, execute_with_busy_retry

try:
    con = connect()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()
num_accts = cur.execute("SELECT num_accts FROM initial_state").fetchone()[0]

committed = 0
iterations = get_random() % 20 + 1
for _ in range(iterations):
    sender = get_random() % num_accts + 1
    recipient = get_random() % num_accts + 1
    if sender == recipient:
        continue
    value = get_random() % 1000
    if execute_with_busy_retry(cur, "BEGIN IMMEDIATE") is None:
        continue
    try:
        cur.execute(f"UPDATE accounts SET balance = balance - {value} WHERE id = {sender}")
        cur.execute(f"UPDATE accounts SET balance = balance + {value} WHERE id = {recipient}")
        cur.execute("UPDATE accounts SET generation = generation + 1")
        cur.execute("COMMIT")
        committed += 1
    except Exception as e:
        print(f"write failed, rolling back: {e}")
        try:
            cur.execute("ROLLBACK")
        except Exception as rollback_error:
            print(f"rollback failed: {rollback_error}")

sometimes(committed > 0, "a writer process committed a transaction", {"committed": committed})
print(f"committed {committed} of {iterations} transactions")
