#!/usr/bin/env -S python3 -u

import json

import turso
from antithesis.assertions import always
from antithesis.random import get_random
from helper_utils import generate_random_value

# Get initial state
try:
    con_init = turso.connect("init_state.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur_init = con_init.cursor()

# Get all existing tables from schemas
existing_schemas = cur_init.execute("SELECT tbl, schema FROM schemas").fetchall()
if not existing_schemas:
    print("No tables found in schemas")
    exit(0)

# Select a random table
selected_idx = get_random() % len(existing_schemas)
selected_tbl, schema_json = existing_schemas[selected_idx]
tbl_schema = json.loads(schema_json)
col_count = tbl_schema["colCount"]
cols = ", ".join([f"col_{col}" for col in range(col_count)])
tbl_name = f"tbl_{selected_tbl}"

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()


def random_values():
    return [generate_random_value(tbl_schema[f"col_{col}"]["data_type"]) for col in range(col_count)]


def random_values_with_large_blobs():
    """Generate values where BLOB columns use zeroblob() to allocate new pages."""
    vals = []
    for col in range(col_count):
        dtype = tbl_schema[f"col_{col}"]["data_type"]
        if dtype == "BLOB" and get_random() % 2 == 0:
            size = 1000 + get_random() % 8000
            vals.append(f"zeroblob({size})")
        else:
            vals.append(generate_random_value(dtype))
    return vals


def try_insert(cur, con, vals):
    try:
        cur.execute(f"INSERT INTO {tbl_name} ({cols}) VALUES ({', '.join(vals)})")
    except turso.IntegrityError:
        pass


try:
    cur.execute("BEGIN TRANSACTION")

    # Insert baseline rows.
    baseline = 1 + get_random() % 3
    for _ in range(baseline):
        try_insert(cur, con, random_values())

    cur.execute(f"SELECT count(*) FROM {tbl_name}")
    count_baseline = cur.fetchone()[0]

    # Create 2-4 nested savepoints, each adding rows (some with large blobs).
    depth = 2 + get_random() % 3
    sp_names = [f"sp_nest_{i}_{get_random() % 1000}" for i in range(depth)]
    counts_at_sp = []

    print(f"Creating {depth} nested savepoints in {tbl_name}...")

    for i, sp in enumerate(sp_names):
        cur.execute(f"SAVEPOINT {sp}")
        cur.execute(f"SELECT count(*) FROM {tbl_name}")
        counts_at_sp.append(cur.fetchone()[0])

        inserts = 1 + get_random() % 5
        for _ in range(inserts):
            try_insert(cur, con, random_values_with_large_blobs())

    # Now randomly rollback to one of the savepoints (not necessarily the
    # innermost) and release it — this tests partial stack unwinding.
    rollback_to = get_random() % depth
    target_sp = sp_names[rollback_to]
    expected_count = counts_at_sp[rollback_to]

    print(f"Rolling back to savepoint {rollback_to} of {depth}: {target_sp}")
    cur.execute(f"ROLLBACK TO {target_sp}")
    cur.execute(f"RELEASE {target_sp}")

    cur.execute(f"SELECT count(*) FROM {tbl_name}")
    count_after = cur.fetchone()[0]

    always(
        count_after == expected_count,
        "Row count must match snapshot at the savepoint we rolled back to",
        {
            "depth": str(depth),
            "rollback_to": str(rollback_to),
            "expected": str(expected_count),
            "actual": str(count_after),
        },
    )

    # The baseline rows from before any savepoint must still be present.
    always(
        count_after >= count_baseline,
        "Baseline rows must survive nested savepoint rollback",
        {"count_baseline": str(count_baseline), "count_after": str(count_after)},
    )

    con.commit()
    print("Committed")

except turso.ProgrammingError as e:
    # Table/column might have been dropped in parallel — expected
    print(f"Table {tbl_name} modified in parallel: {e}")
    con.rollback()
except turso.OperationalError as e:
    print(f"Operational error: {e}")
    con.rollback()
