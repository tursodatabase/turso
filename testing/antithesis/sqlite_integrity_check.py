#!/usr/bin/env -S python3 -u

import os
import sqlite3
import sys

import turso
from antithesis.assertions import always, unreachable

db_file = "/tmp/stress.db"

if not os.path.exists(db_file):
    print(f"{db_file} does not exist")
    sys.exit(1)

try:
    turso_con = turso.connect(db_file)
    mode = turso_con.execute("pragma journal_mode = 'wal'").fetchall()
    turso_con.close()
    print(f"journal_mode: {mode}")
except Exception as e:
    print(f"error switching database to wal mode: {e}")
    unreachable("error switching database to wal mode", {"path": db_file, "error": str(e)})
    sys.exit(0)

try:
    con = sqlite3.connect(db_file)
    rows = [row[0] for row in con.execute("pragma integrity_check")]
except sqlite3.Error as e:
    print(f"error performing sqlite integrity check: {e}")
    unreachable("error performing sqlite integrity check", {"path": db_file, "error": str(e)})
    sys.exit(0)

print("\n".join(map(str, rows)))

always(len(rows) > 0, "sqlite integrity check returned rows", {"path": db_file})

ok = len(rows) > 0 and str(rows[0]).lower() == "ok"
always(ok, "sqlite integrity check passed", {"path": db_file, "rows": rows})
