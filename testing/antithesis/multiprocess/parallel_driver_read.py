#!/usr/bin/env -S python3 -u

import os
import subprocess
import sys
import time

from antithesis.assertions import always, sometimes
from antithesis.random import get_random
from helper_common import check_snapshot, connect, execute_with_busy_retry, read_snapshot

CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "helper_child.py")

try:
    con = connect()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()
num_accts, total = cur.execute("SELECT num_accts, total FROM initial_state").fetchone()

iterations = get_random() % 5 + 1
for _ in range(iterations):
    if get_random() % 2 == 0:
        try:
            busy, log, checkpointed = cur.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            sometimes(
                busy == 0 and log == checkpointed,
                "[Reader] a read transaction begins right after the WAL was fully checkpointed",
                {"log": log},
            )
        except Exception as e:
            print(f"checkpoint before read failed: {e}")

    if execute_with_busy_retry(cur, "BEGIN") is None:
        continue
    try:
        pinned_id = get_random() % num_accts + 1
        pinned_generation = cur.execute(f"SELECT generation FROM accounts WHERE id = {pinned_id}").fetchone()[0]
        if get_random() % 2 == 0:
            subprocess.run([sys.executable, CHILD, "commit-and-checkpoint"], check=False)
        time.sleep((get_random() % 1000) / 1000.0)
        rows = read_snapshot(cur)
        check_snapshot(rows, num_accts, total, "Reader")
        always(
            all(row[2] == pinned_generation for row in rows),
            "[Reader] pages read later in a transaction come from the same commit as the first page",
            {"pinned_generation": pinned_generation, "seen": sorted({row[2] for row in rows})[:10]},
        )
        again = read_snapshot(cur)
        always(
            rows == again,
            "[Reader] a read transaction sees the same rows for its whole lifetime",
            {"first_generation": rows[0][2] if rows else None, "second_generation": again[0][2] if again else None},
        )
    finally:
        cur.execute("COMMIT")

    latest = cur.execute("SELECT max(generation) FROM accounts").fetchone()[0]
    sometimes(
        latest > pinned_generation,
        "[Reader] another process committed while a read transaction was open",
        {"snapshot_generation": pinned_generation, "latest": latest},
    )
