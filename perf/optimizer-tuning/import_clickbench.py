#!/usr/bin/env python3
"""Import `hits.csv` into a ClickBench database with the bundled schema.

`perf/clickbench/benchmark.sh` does this with the sqlite3 shell. Use this
script when that shell is not installed. Values go in as text, so the column
affinity of the schema decides the stored type, the same as `.import --csv`.
"""

import argparse
import csv
import os
import sqlite3
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="perf/clickbench/hits.csv")
    parser.add_argument("--db", default="perf/clickbench/mydb")
    parser.add_argument("--create-sql", default="perf/clickbench/create.sql")
    args = parser.parse_args()

    for suffix in ("", "-wal", "-shm", "-journal"):
        path = args.db + suffix
        if os.path.exists(path):
            os.remove(path)

    conn = sqlite3.connect(args.db)
    conn.executescript(open(args.create_sql).read())
    columns = [row[1] for row in conn.execute("PRAGMA table_info(hits)")]
    placeholders = ",".join("?" * len(columns))
    insert = f"INSERT INTO hits VALUES ({placeholders})"

    conn.execute("PRAGMA journal_mode=off")
    conn.execute("PRAGMA synchronous=off")
    conn.execute("PRAGMA cache_size=-400000")

    csv.field_size_limit(10**7)
    total = 0
    with open(args.csv, newline="") as handle:
        reader = csv.reader(handle)
        batch = []
        for row in reader:
            if len(row) != len(columns):
                sys.exit(f"row {total} has {len(row)} fields, schema has {len(columns)}")
            batch.append(row)
            if len(batch) == 20_000:
                conn.executemany(insert, batch)
                total += len(batch)
                batch.clear()
                print(f"  {total} rows", flush=True)
        if batch:
            conn.executemany(insert, batch)
            total += len(batch)
    conn.commit()
    conn.close()
    print(f"imported {total} rows into {args.db}")


if __name__ == "__main__":
    main()
