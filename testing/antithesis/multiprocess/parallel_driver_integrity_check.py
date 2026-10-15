#!/usr/bin/env -S python3 -u

from antithesis.assertions import always
from helper_common import connect

try:
    con = connect()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()
row = cur.execute("PRAGMA integrity_check").fetchone()
always(row == ("ok",), "[Parallel] integrity_check passes while other processes run", {"result": row})
