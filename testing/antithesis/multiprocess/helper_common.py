import time

import turso
from antithesis.assertions import always

DB_PATH = "multiprocess.db"
BUSY_RETRIES = 50
BUSY_SLEEP_SECONDS = 0.01


def connect():
    return turso.connect(DB_PATH, experimental_features="multiprocess_wal", isolation_level=None)


def is_busy(exc):
    text = str(exc).lower()
    return "busy" in text or "locked" in text


def execute_with_busy_retry(cur, sql):
    for _ in range(BUSY_RETRIES):
        try:
            return cur.execute(sql)
        except turso.OperationalError as exc:
            if not is_busy(exc):
                raise
            if cur.connection.in_transaction:
                print(f"{sql} failed with busy but left a transaction open; rolling back")
                cur.execute("ROLLBACK")
            time.sleep(BUSY_SLEEP_SECONDS)
    return None


def read_snapshot(cur):
    rows = cur.execute("SELECT id, balance, generation FROM accounts ORDER BY id").fetchall()
    return [tuple(row) for row in rows]


def check_snapshot(rows, expected_count, expected_total, label):
    generations = {row[2] for row in rows}
    total = sum(row[1] for row in rows)
    always(
        len(rows) == expected_count,
        f"[{label}] every account is visible",
        {"seen": len(rows), "expected": expected_count},
    )
    always(
        len(generations) == 1,
        f"[{label}] every row comes from the same commit",
        {"generations": sorted(generations)[:10], "count": len(generations)},
    )
    always(
        total == expected_total,
        f"[{label}] total balance is preserved",
        {"total": total, "expected": expected_total},
    )
