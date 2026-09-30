#!/usr/bin/env python3
"""
Concurrent writes with MVCC

BEGIN CONCURRENT lets multiple connections write at the same time without
holding an exclusive lock.  Conflicts are detected at commit time: if two
transactions touched the same rows, the later one receives a conflict
error and must roll back and retry.
"""

import asyncio
import os
import random
import tempfile

import turso


def is_retryable(e: Exception) -> bool:
    msg = str(e).lower()
    return "conflict" in msg or "busy" in msg


def write_worker(db_path: str) -> int:
    """Open a connection, insert one random value using BEGIN CONCURRENT."""
    val = random.randint(1, 100)
    conn = turso.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = 'mvcc'").fetchone()
        while True:
            conn.execute("BEGIN CONCURRENT")
            try:
                conn.execute("INSERT INTO hits VALUES (?)", (val,))
                conn.execute("COMMIT")
                return val
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                if not is_retryable(e):
                    raise
    finally:
        conn.close()


async def main() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        setup = turso.connect(db_path)
        setup.execute("PRAGMA journal_mode = 'mvcc'").fetchone()
        setup.execute("CREATE TABLE hits (val INTEGER)")
        setup.commit()
        setup.close()

        # Run 16 workers concurrently, each in its own thread.
        tasks = [asyncio.to_thread(write_worker, db_path) for _ in range(16)]
        results = await asyncio.gather(*tasks)
        for val in results:
            print(f"inserted val={val}")

        final = turso.connect(db_path)
        final.execute("PRAGMA journal_mode = 'mvcc'").fetchone()
        cur = final.execute("SELECT COUNT(*) FROM hits")
        row = cur.fetchone()
        print(f"total rows: {row[0]}")
        final.close()
    finally:
        os.unlink(db_path)
        for ext in ("-wal", "-log"):
            path = db_path + ext
            if os.path.exists(path):
                os.unlink(path)


if __name__ == "__main__":
    asyncio.run(main())
