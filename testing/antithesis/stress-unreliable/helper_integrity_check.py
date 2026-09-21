#!/usr/bin/env -S python3 -u

import sqlite3
import sys

import turso
from antithesis.assertions import always

db_path = sys.argv[1]


def turso_integrity_check():
    con = turso.connect(db_path)
    rows = con.cursor().execute("PRAGMA integrity_check").fetchall()
    return [row[0] for row in rows]


def sqlite_integrity_check():
    con = sqlite3.connect(db_path)
    rows = con.execute("PRAGMA integrity_check").fetchall()
    return [row[0] for row in rows]


ok = True
for engine, check in (("turso", turso_integrity_check), ("sqlite", sqlite_integrity_check)):
    try:
        rows = check()
    except Exception as e:
        always(False, f"[{engine}] integrity check runs after unreliable libc workload", {"error": str(e)})
        ok = False
        continue
    passed = rows == ["ok"]
    always(passed, f"[{engine}] integrity check passes after unreliable libc workload", {"rows": rows[:20]})
    print(f"{engine} integrity check: {rows[:20]}")
    ok = ok and passed

sys.exit(0 if ok else 1)
