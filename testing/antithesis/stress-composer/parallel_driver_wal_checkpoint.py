#!/usr/bin/env -S python3 -u

import turso
from antithesis.assertions import always
from antithesis.random import get_random

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

# Choose a random checkpoint mode
modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"]
selected_mode = modes[get_random() % len(modes)]

print(f"Running wal_checkpoint({selected_mode})...")

try:
    result = cur.execute(f"PRAGMA wal_checkpoint({selected_mode})")
    row = result.fetchone()
    # wal_checkpoint returns (busy, log, checkpointed)
    # busy should be 0 on success, 1 if blocked
    # log is total frames in WAL
    # checkpointed is frames successfully checkpointed
    if row is not None:
        busy, log, checkpointed = row
        always(busy in (0, 1), f"wal_checkpoint returned unexpected busy value: {busy}", {})
        always(log >= 0, f"wal_checkpoint returned negative log value: {log}", {})
        always(checkpointed >= 0, f"wal_checkpoint returned negative checkpointed value: {checkpointed}", {})
        always(checkpointed <= log, f"checkpointed ({checkpointed}) > log ({log})", {})
        print(f"wal_checkpoint result: busy={busy}, log={log}, checkpointed={checkpointed}")
    else:
        print("wal_checkpoint returned no result (database may not be in WAL mode)")
except Exception as e:
    # wal_checkpoint can fail if database is busy or not in WAL mode
    print(f"wal_checkpoint failed: {e}")
