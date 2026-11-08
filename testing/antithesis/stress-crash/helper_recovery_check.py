#!/usr/bin/env -S python3 -u

import sqlite3
import sys

import turso
from antithesis.assertions import always

db_path = sys.argv[1]


def turso_integrity_check():
    # Opening the database makes Turso recover it from the WAL or the MVCC log.
    con = turso.connect(db_path)
    cur = con.cursor()
    rows = [row[0] for row in cur.execute("PRAGMA integrity_check").fetchall()]
    # SQLite cannot read MVCC databases. Switching to WAL moves the MVCC log
    # into the database file, and is a no-op for databases already in WAL mode.
    cur.execute("PRAGMA journal_mode = wal").fetchall()
    con.close()
    return rows


def sqlite_integrity_check():
    con = sqlite3.connect(db_path)
    rows = con.execute("PRAGMA integrity_check").fetchall()
    return [row[0] for row in rows]


ok = True
for engine, check in (("turso", turso_integrity_check), ("sqlite", sqlite_integrity_check)):
    try:
        rows = check()
    except Exception as e:
        always(False, f"[{engine}] database opens after the stress process is killed", {"error": str(e)})
        ok = False
        break
    passed = rows == ["ok"]
    always(passed, f"[{engine}] integrity check passes after the stress process is killed", {"rows": rows[:20]})
    print(f"{engine} integrity check: {rows[:20]}")
    ok = ok and passed

sys.exit(0 if ok else 1)
