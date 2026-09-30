#!/usr/bin/env -S python3 -u

import time

from antithesis.assertions import always, sometimes
from antithesis.random import get_random
from helper_common import connect

try:
    con = connect()
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()
modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"]

iterations = get_random() % 30 + 1
for _ in range(iterations):
    mode = modes[get_random() % len(modes)]
    try:
        row = cur.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
    except Exception as e:
        print(f"wal_checkpoint({mode}) failed: {e}")
        continue

    busy, log, checkpointed = row
    print(f"wal_checkpoint({mode}): busy={busy} log={log} checkpointed={checkpointed}")
    always(busy in (0, 1), "wal_checkpoint reports busy as 0 or 1", {"busy": busy})
    if busy == 0:
        always(
            0 <= checkpointed <= log,
            "wal_checkpoint never reports more frames copied than logged",
            {"log": log, "checkpointed": checkpointed},
        )
        sometimes(
            log > 0 and checkpointed == log,
            "a checkpoint copied every WAL frame into the database file",
            {"mode": mode, "log": log},
        )
        sometimes(
            checkpointed < log,
            "a reader in another process stopped a checkpoint before the end of the WAL",
            {"mode": mode, "log": log, "checkpointed": checkpointed},
        )
    time.sleep((get_random() % 200) / 1000.0)
