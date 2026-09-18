#!/usr/bin/env python3
"""Small queries with real data that must not get slower.

The TPC-H objective says nothing about a table with no statistics, a narrow
range on a primary key, or an index intersection on ten thousand rows. The plan
snapshots in the conformance corpus cover those shapes, but a snapshot only
says a plan changed, not whether the new plan is slower. These guards run the
same shapes against data of a realistic size and time them, so a candidate that
makes one of them slower can be rejected with a measurement rather than a
guess.

Each guard is timed once under the compiled defaults this experiment started
from and once under the candidate. A candidate fails when any guard takes more
than `--limit` times its reference time.
"""

import argparse
import json
import os
import sqlite3
import statistics
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cost_vector

GUARDS = {
    # A narrow range on the driving table's primary key, joined to a large
    # child table through a covering index, with no statistics. The optimizer
    # has to see that the range is selective enough to drive the join.
    "range_join_no_stats": {
        "schema": """
            CREATE TABLE customers(customer_id INTEGER PRIMARY KEY NOT NULL,
              name TEXT NOT NULL, city TEXT NOT NULL);
            CREATE TABLE orders(order_id INTEGER PRIMARY KEY NOT NULL,
              customer_id INTEGER NOT NULL, total INTEGER NOT NULL);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000)
            INSERT INTO customers SELECT x, 'name'||x, 'city'||(x%50) FROM n;
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200000)
            INSERT INTO orders SELECT x, (x%10000)+1, x FROM n;
            CREATE INDEX idx_orders_customer_id ON orders(customer_id);
        """,
        "analyze": False,
        "sql": """SELECT c.name, COUNT(o.order_id)
                  FROM customers c
                  INNER JOIN orders o ON c.customer_id = o.customer_id
                  WHERE c.customer_id BETWEEN 10 AND 20
                  GROUP BY c.customer_id, c.name;""",
    },
    # The same shape after ANALYZE, so the statistics path is covered too.
    "range_join_analyzed": {
        "schema": None,  # same schema as range_join_no_stats
        "same_schema_as": "range_join_no_stats",
        "analyze": True,
        "sql": """SELECT c.name, COUNT(o.order_id)
                  FROM customers c
                  INNER JOIN orders o ON c.customer_id = o.customer_id
                  WHERE c.customer_id BETWEEN 10 AND 20
                  GROUP BY c.customer_id, c.name;""",
    },
    # Two equalities on two single-column indexes over ten thousand rows.
    "two_index_equalities": {
        "schema": """
            CREATE TABLE t(a, b, c0, c1, c2, c3, c4, c5, c6, c7, c8);
            CREATE INDEX ia ON t(a);
            CREATE INDEX ib ON t(b);
            WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<9999)
            INSERT INTO t SELECT x%10, (x/10)%10,
              NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL FROM n;
        """,
        "analyze": True,
        "sql": "SELECT * FROM t WHERE a = 1 AND b = 2;",
    },
    # An equality on an indexed column of a child table, driven from a small
    # parent, with no statistics.
    "indexed_lookup_no_stats": {
        "schema": """
            CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);
            CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER,
              payload TEXT);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200)
            INSERT INTO parent SELECT x, 'p'||x FROM n;
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200000)
            INSERT INTO child SELECT x, (x%200)+1, 'payload'||x FROM n;
            CREATE INDEX idx_child_parent ON child(parent_id);
        """,
        "analyze": False,
        "sql": """SELECT p.label, COUNT(*) FROM parent p
                  JOIN child c ON c.parent_id = p.id
                  WHERE p.id = 7 GROUP BY p.label;""",
    },
    # A range predicate on a secondary index that keeps a small part of the
    # table. The optimizer has to prefer the index over a full scan.
    "range_on_index_no_stats": {
        "schema": """
            CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, payload TEXT);
            CREATE INDEX ta ON t(a);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200000)
            INSERT INTO t SELECT x, x, 'payload'||x FROM n;
        """,
        "analyze": False,
        "sql": "SELECT COUNT(*), MAX(payload) FROM t WHERE a > 198000;",
    },
    # The same shape after ANALYZE.
    "range_on_index_analyzed": {
        "same_schema_as": "range_on_index_no_stats",
        "analyze": True,
        "sql": "SELECT COUNT(*), MAX(payload) FROM t WHERE a > 198000;",
    },
    # An equality and a date range on one composite index, with statistics.
    "composite_range_analyzed": {
        "schema": """
            CREATE TABLE orders(order_id INTEGER PRIMARY KEY, customer_id INTEGER,
              order_date TEXT, total_amount REAL);
            CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200000)
            INSERT INTO orders SELECT x, x%500,
              '2024-' || printf('%02d', (x%12)+1) || '-01', x * 1.5 FROM n;
        """,
        "analyze": True,
        "sql": """SELECT order_id, order_date, total_amount FROM orders
                  WHERE customer_id = 7 AND order_date >= '2024-03-01'
                    AND order_date < '2024-06-01';""",
    },
    # Two equalities joined by OR, each on its own index.
    "or_two_indexes_no_stats": {
        "schema": """
            CREATE TABLE events(id INTEGER PRIMARY KEY, type TEXT, severity TEXT,
              payload TEXT);
            CREATE INDEX idx_events_type ON events(type);
            CREATE INDEX idx_events_severity ON events(severity);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200000)
            INSERT INTO events SELECT x, 'type'||(x%500), 'sev'||(x%500),
              'payload'||x FROM n;
        """,
        "analyze": False,
        "sql": """SELECT COUNT(*) FROM events
                  WHERE type = 'type7' OR severity = 'sev9';""",
    },
    # An ORDER BY that an index can satisfy, behind a range on its first column.
    "sort_elimination_range_no_stats": {
        "schema": """
            CREATE TABLE articles(id INTEGER PRIMARY KEY, status INTEGER,
              category INTEGER, published TEXT);
            CREATE INDEX idx_articles_status_cat_pub
              ON articles(status, category, published);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200000)
            INSERT INTO articles SELECT x, x%100, x%7,
              '2024-' || printf('%02d', (x%12)+1) || '-01' FROM n;
        """,
        "analyze": False,
        "sql": """SELECT id, published FROM articles
                  WHERE status > 97 ORDER BY status, category, published LIMIT 50;""",
    },
    # A correlated subquery over a large table with no statistics.
    "correlated_exists_no_stats": {
        "same_schema_as": "indexed_lookup_no_stats",
        "analyze": False,
        "sql": """SELECT COUNT(*) FROM parent p
                  WHERE EXISTS (SELECT 1 FROM child c
                                WHERE c.parent_id = p.id AND c.id < 500);""",
    },
}


def build(data_dir):
    """Create one database per distinct schema, once."""
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(os.path.join(data_dir, "queries"), exist_ok=True)
    for name, guard in GUARDS.items():
        source = guard.get("same_schema_as", name)
        schema = GUARDS[source]["schema"]
        path = os.path.join(data_dir, f"{name}.db")
        if not os.path.exists(path):
            connection = sqlite3.connect(path)
            connection.executescript(schema)
            if guard["analyze"]:
                connection.execute("ANALYZE")
            connection.commit()
            connection.close()
        with open(os.path.join(data_dir, "queries", f"{name}.sql"), "w",
                  encoding="utf-8") as handle:
            handle.write(guard["sql"].strip() + "\n")


def measure(binary, data_dir, name, vector_path, warmups, repetitions):
    environment = dict(os.environ)
    if vector_path is None:
        environment.pop("TURSO_OPTIMIZER_PARAMS", None)
    else:
        environment["TURSO_OPTIMIZER_PARAMS"] = vector_path
    result = subprocess.run(
        [binary, "--database", os.path.join(data_dir, f"{name}.db"),
         "--query-dir", os.path.join(data_dir, "queries"), "--query", name,
         "--warmups", str(warmups), "--repetitions", str(repetitions),
         "--timeout-seconds", "120"],
        capture_output=True, env=environment, timeout=600,
    )
    samples = [json.loads(line) for line in
               result.stdout.decode("utf-8", "replace").splitlines()
               if line.startswith("{")]
    if len(samples) != repetitions:
        raise RuntimeError(
            f"guard {name} did not run: "
            f"{result.stderr.decode('utf-8', 'replace')[:400]}"
        )
    return {
        "median_ms": statistics.median(s["elapsed_ns"] / 1e6 for s in samples),
        "rows_read": samples[0]["rows_read"],
        "result_rows": samples[0]["result_rows"],
        "btree_seeks": samples[0]["btree_seeks"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--reference", required=True,
                        help="the vector the guards are compared against")
    parser.add_argument("--vectors", nargs="+", required=True)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--limit", type=float, default=1.25)
    parser.add_argument("--time-floor-ms", type=float, default=0.05,
                        help="below this reference time the elapsed ratio is "
                             "noise, so only the work counter decides")
    parser.add_argument("--out")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    build(args.data_dir)
    reference_path = os.path.abspath(args.reference)
    cost_vector.read(reference_path)
    reference = {
        name: measure(args.binary, args.data_dir, name, reference_path,
                      args.warmups, args.repetitions)
        for name in GUARDS
    }
    if not args.quiet:
        print("reference (the defaults this experiment started from):")
        for name, row in reference.items():
            print(f"  {name:28} {row['median_ms']:9.3f} ms  "
                  f"rows_read={row['rows_read']}")

    records = []
    for path in args.vectors:
        path = os.path.abspath(path)
        cost_vector.read(path)
        name = os.path.splitext(os.path.basename(path))[0]
        worst, failures, rows = 1.0, [], {}
        for guard in GUARDS:
            outcome = measure(args.binary, args.data_dir, guard, path,
                              args.warmups, args.repetitions)
            if outcome["result_rows"] != reference[guard]["result_rows"]:
                raise RuntimeError(f"{name}: guard {guard} returned a "
                                   "different number of rows")
            # Rows read is the primary signal. It counts the work the plan
            # does, it does not move between runs, and a guard that takes a
            # few microseconds cannot be timed well enough to decide on its
            # own. The elapsed time still decides for the slower guards, where
            # it can catch work the row counter does not see.
            work = outcome["rows_read"] / max(reference[guard]["rows_read"], 1)
            elapsed = outcome["median_ms"] / reference[guard]["median_ms"]
            ratio = work
            if reference[guard]["median_ms"] >= args.time_floor_ms:
                ratio = max(work, elapsed)
            rows[guard] = {"ratio": ratio, "work_ratio": work,
                           "elapsed_ratio": elapsed, **outcome}
            worst = max(worst, ratio)
            if ratio > args.limit:
                failures.append((guard, ratio))
        records.append({"name": name, "vector": path, "worst_ratio": worst,
                        "failures": failures, "guards": rows})
        print(f"{name:34} worst={worst:6.2f}x  "
              f"{'FAIL ' + ', '.join(f'{g} {r:.1f}x' for g, r in failures) if failures else 'ok'}",
              flush=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
