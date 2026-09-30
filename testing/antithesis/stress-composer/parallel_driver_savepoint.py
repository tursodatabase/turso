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


try:
    # Count rows before transaction to verify correctness after rollback.
    cur.execute(f"SELECT count(*) FROM {tbl_name}")
    count_before = cur.fetchone()[0]

    cur.execute("BEGIN TRANSACTION")

    # Insert "outer" rows before the savepoint.
    outer_inserts = 1 + get_random() % 5
    print(f"Inserting {outer_inserts} outer rows into {tbl_name}...")
    for _ in range(outer_inserts):
        values = random_values()
        try:
            cur.execute(f"INSERT INTO {tbl_name} ({cols}) VALUES ({', '.join(values)})")
        except turso.IntegrityError:
            pass

    # Snapshot row count after outer inserts (within the transaction).
    cur.execute(f"SELECT count(*) FROM {tbl_name}")
    count_after_outer = cur.fetchone()[0]

    # Create savepoint, insert rows with large blobs, then rollback.
    sp_name = f"sp_{get_random() % 1000}"
    cur.execute(f"SAVEPOINT {sp_name}")

    inner_inserts = 1 + get_random() % 10
    print(f"Inserting {inner_inserts} inner rows (with large blobs) into {tbl_name}, then rolling back...")
    for _ in range(inner_inserts):
        values = random_values_with_large_blobs()
        try:
            cur.execute(f"INSERT INTO {tbl_name} ({cols}) VALUES ({', '.join(values)})")
        except turso.IntegrityError:
            pass

    cur.execute(f"ROLLBACK TO {sp_name}")
    cur.execute(f"RELEASE {sp_name}")

    # The outer rows must still be visible after ROLLBACK TO savepoint.
    cur.execute(f"SELECT count(*) FROM {tbl_name}")
    count_after_rollback = cur.fetchone()[0]

    always(
        count_after_rollback == count_after_outer,
        "Row count must match after ROLLBACK TO savepoint — outer rows must survive",
        {"count_after_outer": str(count_after_outer), "count_after_rollback": str(count_after_rollback)},
    )

    # Randomly decide: commit the outer rows or rollback everything.
    if get_random() % 2 == 0:
        con.commit()
        print("Committed outer rows")

        # Verify committed data survives by re-reading.
        cur.execute(f"SELECT count(*) FROM {tbl_name}")
        count_final = cur.fetchone()[0]
        always(
            count_final >= count_after_outer,
            "Row count must not decrease after commit",
            {"count_after_outer": str(count_after_outer), "count_final": str(count_final)},
        )
    else:
        con.rollback()
        print("Rolled back entire transaction")

        # Verify we're back to the pre-transaction state.
        cur.execute(f"SELECT count(*) FROM {tbl_name}")
        count_final = cur.fetchone()[0]
        always(
            count_final <= count_before,
            "Row count must not increase after full rollback",
            {"count_before": str(count_before), "count_final": str(count_final)},
        )

except turso.ProgrammingError as e:
    # Table/column might have been dropped in parallel — expected
    print(f"Table {tbl_name} modified in parallel: {e}")
    con.rollback()
except turso.OperationalError as e:
    print(f"Operational error: {e}")
    con.rollback()
