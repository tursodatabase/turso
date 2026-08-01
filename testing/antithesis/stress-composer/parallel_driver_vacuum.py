#!/usr/bin/env -S python3 -u

import os

import turso
from antithesis.assertions import always
from antithesis.random import get_random

try:
    con = turso.connect("stress_composer.db", experimental_features="vacuum")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

# Choose between in-place VACUUM (experimental) and VACUUM INTO.
run_into = get_random() % 5 < 2  # 40% chance for VACUUM INTO

if run_into:
    dest = f"vacuum_copy_{os.getpid()}_{get_random() % 1000000}.db"
    for f in (dest, f"{dest}-wal"):
        try:
            os.remove(f)
        except OSError:
            pass

    print(f"Running VACUUM INTO '{dest}'...")
    try:
        cur.execute(f"VACUUM INTO '{dest}'")
    except turso.OperationalError as e:
        # Can fail under contention with parallel writers - this is expected
        print(f"VACUUM INTO failed: {e}")
        exit(0)

    # The copy is a private snapshot, so unlike the shared database it can be
    # validated without racing against parallel writers.
    con_copy = turso.connect(dest)
    cur_copy = con_copy.cursor()

    row = cur_copy.execute("PRAGMA integrity_check").fetchone()
    always(row == ("ok",), f"Integrity check failed on VACUUM INTO copy: {row}", {})

    freelist = cur_copy.execute("PRAGMA freelist_count").fetchone()[0]
    always(
        freelist == 0,
        "VACUUM INTO copy must have an empty freelist",
        {"freelist_count": str(freelist)},
    )

    tables = cur_copy.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'tbl_%'
    """).fetchall()
    for (tbl_name,) in tables:
        count = cur_copy.execute(f"SELECT count(*) FROM {tbl_name}").fetchone()[0]
        print(f"Copy {tbl_name}: {count} rows")

    con_copy.close()

    for f in (dest, f"{dest}-wal"):
        try:
            os.remove(f)
        except OSError:
            pass

    print("VACUUM INTO copy validated")
else:
    print("Running VACUUM...")
    try:
        cur.execute("VACUUM")
    except turso.OperationalError as e:
        # Can fail under contention with parallel writers - this is expected
        print(f"VACUUM failed: {e}")
        exit(0)

    # integrity_check runs on a consistent snapshot, so this is safe even with
    # parallel writers. The freelist, however, can be repopulated by a parallel
    # DELETE committing right after the VACUUM, so it cannot be asserted here.
    row = cur.execute("PRAGMA integrity_check").fetchone()
    always(row == ("ok",), f"Integrity check failed after VACUUM: {row}", {})

    print("VACUUM completed and integrity verified")

con.close()
