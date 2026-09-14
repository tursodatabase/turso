#!/usr/bin/env -S python3 -u

from antithesis.assertions import always
from helper_common import check_snapshot, connect, read_snapshot

try:
    con = connect()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()
num_accts, total = cur.execute("SELECT num_accts, total FROM initial_state").fetchone()
cur.execute("BEGIN")
check_snapshot(read_snapshot(cur), num_accts, total, "Anytime")
cur.execute("COMMIT")
row = cur.execute("PRAGMA integrity_check").fetchone()
always(row == ("ok",), "[Anytime] integrity_check passes", {"result": row})
